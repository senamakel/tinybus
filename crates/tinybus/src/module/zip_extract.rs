//! Bounded ZIP extraction for module release archives.
//!
//! `rawzip` parses ZIP structure but intentionally leaves extraction policy to
//! its callers. This adapter accepts only normalized portable paths, STORE and
//! DEFLATE entries, and writes regular files into the host-owned staging
//! directory without applying archive permissions or creating links.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};

use rawzip::{CompressionMethod, ZipArchive, ZipArchiveEntryWayfinder};

use crate::error::{Error, Result};
use crate::module::github::refused;

const MAX_ENTRY_COUNT: u64 = 100_000;
const MAX_ENTRY_EXPANDED_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_ARCHIVE_EXPANDED_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_DEFLATE_RATIO: u64 = 1032;
const MAX_NAME_BYTES: usize = 4096;

#[derive(Clone)]
struct EntryPlan {
    wayfinder: ZipArchiveEntryWayfinder,
    path: PathBuf,
    portable_key: String,
    is_dir: bool,
    method: CompressionMethod,
    flags: rawzip::EntryFlags,
    compressed_size: u64,
    expanded_size: u64,
    data_range: (u64, u64),
}

/// Extract a ZIP archive into a fresh private staging directory.
///
/// The caller owns `destination` and must discard it if this function fails.
pub(super) fn extract(archive_path: &Path, destination: &Path) -> Result<()> {
    extract_with_limits(
        archive_path,
        destination,
        MAX_ENTRY_EXPANDED_BYTES,
        MAX_ARCHIVE_EXPANDED_BYTES,
        MAX_ENTRY_COUNT,
    )
}

fn extract_with_limits(
    archive_path: &Path,
    destination: &Path,
    max_entry_bytes: u64,
    max_total_bytes: u64,
    max_entries: u64,
) -> Result<()> {
    let file = File::open(archive_path).map_err(|_| refused("release archive is unreadable"))?;
    let file_len = file
        .metadata()
        .map_err(|_| refused("release archive is unreadable"))?
        .len();
    let mut buffer = vec![0; rawzip::RECOMMENDED_BUFFER_SIZE];
    let archive = ZipArchive::from_file(file, &mut buffer)
        .map_err(|_| refused("release archive is invalid"))?;
    let expected_count = archive.entries_hint();
    if expected_count > max_entries {
        return Err(refused("release archive contains too many entries"));
    }

    let mut plans = Vec::with_capacity(expected_count as usize);
    let mut seen_paths = HashSet::new();
    let mut hinted_expanded_total = 0u64;
    let mut entries = archive.entries(&mut buffer);
    while let Some(entry) = entries
        .next_entry()
        .map_err(|_| refused("release archive is invalid"))?
    {
        if plans.len() as u64 >= max_entries {
            return Err(refused("release archive contains too many entries"));
        }

        let raw_path = entry.file_path();
        let original = std::str::from_utf8(raw_path.as_ref())
            .map_err(|_| refused("release archive contains an unsafe path"))?;
        let (path, portable_key, is_dir) = checked_path(original)?;
        if !seen_paths.insert(portable_key.clone()) {
            return Err(refused("release archive contains duplicate paths"));
        }

        let method = entry.compression_method();
        let flags = entry.flags();
        validate_flags(flags, method)?;
        if method != CompressionMethod::STORE && method != CompressionMethod::DEFLATE {
            return Err(refused("release archive uses an unsupported compression method"));
        }

        let compressed_size = entry.compressed_size_hint();
        let expanded_size = entry.uncompressed_size_hint();
        if expanded_size > max_entry_bytes {
            return Err(refused("release archive entry exceeds the extraction limit"));
        }
        hinted_expanded_total = hinted_expanded_total
            .checked_add(expanded_size)
            .ok_or_else(|| refused("release archive exceeds the extraction limit"))?;
        if hinted_expanded_total > max_total_bytes {
            return Err(refused("release archive exceeds the extraction limit"));
        }
        if method == CompressionMethod::DEFLATE {
            let max_by_ratio = compressed_size
                .checked_mul(MAX_DEFLATE_RATIO)
                .unwrap_or(u64::MAX);
            if expanded_size > max_by_ratio {
                return Err(refused("release archive entry exceeds the compression ratio limit"));
            }
        }

        let local = archive
            .get_entry(entry.wayfinder())
            .map_err(|_| refused("release archive is invalid"))?;
        let range = local.compressed_data_range();
        if range.0 > range.1
            || range.1 > file_len
            || range.1 > archive.directory_offset()
            || range.1 - range.0 != compressed_size
        {
            return Err(refused("release archive contains invalid entry offsets"));
        }
        let mut local_header_buffer = vec![0; rawzip::MAX_CENTRAL_DIRECTORY_RECORD_SIZE];
        let local_header = local
            .local_header(&mut local_header_buffer)
            .map_err(|_| refused("release archive is invalid"))?;
        if local_header.file_path().as_ref() != raw_path.as_ref()
            || local_header.compression_method() != method
            || local_header.flags() != flags
        {
            return Err(refused("release archive headers disagree"));
        }
        if flags.has_data_descriptor() {
            let descriptor = local
                .reader()
                .data_descriptor()
                .map_err(|_| refused("release archive has an invalid data descriptor"))?
                .ok_or_else(|| refused("release archive has a missing data descriptor"))?;
            if descriptor.crc32() != entry.crc32()
                || descriptor.compressed_size() != compressed_size
                || descriptor.uncompressed_size() != expanded_size
            {
                return Err(refused("release archive data descriptor disagrees"));
            }
        } else if local_header.crc32() != entry.crc32()
            || local_header.compressed_size_hint() != compressed_size
            || local_header.uncompressed_size_hint() != expanded_size
        {
            return Err(refused("release archive headers disagree"));
        }

        plans.push(EntryPlan {
            wayfinder: entry.wayfinder().clone(),
            path,
            portable_key,
            is_dir,
            method,
            flags,
            compressed_size,
            expanded_size,
            data_range: range,
        });
    }
    if plans.len() as u64 != expected_count {
        return Err(refused("release archive entry count is inconsistent"));
    }

    let mut ranges: Vec<_> = plans
        .iter()
        .map(|plan| plan.data_range)
        .filter(|(start, end)| start < end)
        .collect();
    ranges.sort_unstable_by_key(|range| range.0);
    if ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(refused("release archive contains overlapping entries"));
    }

    let mut expanded_total = 0u64;
    for plan in plans {
        let target = destination.join(&plan.path);
        if plan.is_dir {
            fs::create_dir_all(&target)
                .map_err(|_| refused("release archive extraction failed"))?;
            continue;
        }
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .map_err(|_| refused("release archive extraction failed"))?;
        }
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)
            .map_err(|_| refused("release archive extraction failed"))?;
        let local = archive
            .get_entry(plan.wayfinder)
            .map_err(|_| refused("release archive is invalid"))?;
        if local.compressed_data_range() != plan.data_range {
            return Err(refused("release archive entry offsets changed"));
        }
        let reader = local.reader();
        let mut actual = 0u64;
        match plan.method {
            CompressionMethod::STORE => {
                let mut verified = local.verifying_reader(reader);
                copy_bounded(
                    &mut verified,
                    &mut output,
                    max_entry_bytes.min(max_total_bytes - expanded_total),
                    &mut actual,
                )?;
            }
            CompressionMethod::DEFLATE => {
                let mut decoder = flate2::read::DeflateDecoder::new(reader);
                {
                    let mut verified = local.verifying_reader(&mut decoder);
                    copy_bounded(
                        &mut verified,
                        &mut output,
                        max_entry_bytes.min(max_total_bytes - expanded_total),
                        &mut actual,
                    )?;
                }
                if decoder.total_in() != plan.compressed_size {
                    return Err(refused("release archive has a truncated DEFLATE stream"));
                }
            }
            _ => return Err(refused("release archive uses an unsupported compression method")),
        }
        if actual != plan.expanded_size {
            return Err(refused("release archive entry size is inconsistent"));
        }
        expanded_total = expanded_total
            .checked_add(actual)
            .ok_or_else(|| refused("release archive exceeds the extraction limit"))?;
    }
    Ok(())
}

fn copy_bounded(
    input: &mut impl Read,
    output: &mut impl Write,
    remaining: u64,
    actual: &mut u64,
) -> Result<()> {
    let mut limited = input.take(remaining.saturating_add(1));
    *actual = io::copy(&mut limited, output)
        .map_err(|_| refused("release archive extraction failed"))?;
    if *actual > remaining {
        return Err(refused("release archive exceeds the extraction limit"));
    }
    Ok(())
}

fn validate_flags(flags: rawzip::EntryFlags, method: CompressionMethod) -> Result<()> {
    if flags.is_encrypted() || flags.has_strong_encryption() || flags.is_masked() {
        return Err(refused("release archive contains encrypted entries"));
    }
    let allowed = 1 << 3 | 1 << 11 | if method == CompressionMethod::DEFLATE { 0b110 } else { 0 };
    if flags.bits() & !allowed != 0 {
        return Err(refused("release archive contains unsupported entry flags"));
    }
    Ok(())
}

fn checked_path(original: &str) -> Result<(PathBuf, String, bool)> {
    if original.is_empty()
        || original.len() > MAX_NAME_BYTES
        || original.as_bytes().contains(&0)
        || original.starts_with('/')
        || original.contains('\\')
        || original.contains(':')
        || original.chars().any(|ch| ch.is_control() || "<>|?*\"".contains(ch))
    {
        return Err(refused("release archive contains an unsafe path"));
    }
    let is_dir = original.ends_with('/');
    let body = if is_dir {
        original.strip_suffix('/').unwrap_or_default()
    } else {
        original
    };
    let parts: Vec<_> = body.split('/').collect();
    if parts.is_empty() || parts.iter().any(|part| part.is_empty() || *part == "." || *part == "..") {
        return Err(refused("release archive contains an unsafe path"));
    }
    for part in &parts {
        if part.ends_with(['.', ' ']) || is_windows_device_name(part) {
            return Err(refused("release archive contains an unsafe path"));
        }
    }

    let normalized = rawzip::path::ZipFilePath::from_str(original)
        .try_normalize()
        .map_err(|_| refused("release archive contains an unsafe path"))?;
    if normalized.as_ref() != original {
        return Err(refused("release archive contains an unsafe path"));
    }

    let mut path = PathBuf::new();
    for part in &parts {
        path.push(part);
    }
    if path.components().any(|component| !matches!(component, Component::Normal(_))) {
        return Err(refused("release archive contains an unsafe path"));
    }
    let portable_key = parts.join("/").to_lowercase();
    Ok((path, portable_key, is_dir))
}

fn is_windows_device_name(component: &str) -> bool {
    let stem = component.split('.').next().unwrap_or_default();
    let upper = stem.to_ascii_uppercase();
    matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ["COM", "LPT"].iter().any(|prefix| {
            upper.strip_prefix(prefix).is_some_and(|number| {
                number.len() == 1 && matches!(number.as_bytes()[0], b'1'..=b'9')
            })
        })
}
