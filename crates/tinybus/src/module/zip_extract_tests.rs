use super::*;

fn extract_fixture(name: &str) -> (tempfile::TempDir, PathBuf, Result<()>) {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    let bytes: &[u8] = match name {
        "compat_store.zip" => include_bytes!("fixtures/compat_store.zip"),
        "compat_deflate_descriptor.zip" => {
            include_bytes!("fixtures/compat_deflate_descriptor.zip")
        }
        "unsafe_parent.zip" => include_bytes!("fixtures/unsafe_parent.zip"),
        "windows_drive.zip" => include_bytes!("fixtures/windows_drive.zip"),
        "windows_backslash.zip" => include_bytes!("fixtures/windows_backslash.zip"),
        "unix_link_mode.zip" => include_bytes!("fixtures/unix_link_mode.zip"),
        "suid_mode.zip" => include_bytes!("fixtures/suid_mode.zip"),
        "duplicate.zip" => include_bytes!("fixtures/duplicate.zip"),
        "archive_collision.zip" => include_bytes!("fixtures/archive_collision.zip"),
        "case_collision.zip" => include_bytes!("fixtures/case_collision.zip"),
        "compat_zip64_local.zip" => include_bytes!("fixtures/compat_zip64_local.zip"),
        "encrypted.zip" => include_bytes!("fixtures/encrypted.zip"),
        "unsupported_method.zip" => include_bytes!("fixtures/unsupported_method.zip"),
        "overlap.zip" => include_bytes!("fixtures/overlap.zip"),
        "one_module.zip" => include_bytes!("fixtures/one_module.zip"),
        _ => panic!("unknown test fixture"),
    };
    fs::write(&archive, bytes).unwrap();
    let result = extract(&archive, &destination);
    (root, destination, result)
}

fn extract_fixture_bytes(bytes: &[u8]) -> (tempfile::TempDir, PathBuf, Result<()>) {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    fs::write(&archive, bytes).unwrap();
    let result = extract(&archive, &destination);
    (root, destination, result)
}

fn extract_through_cache_staging(bytes: &[u8]) -> (tempfile::TempDir, Result<()>) {
    let root = tempfile::tempdir().unwrap();
    let cache_dir = root.path().join("cache");
    fs::create_dir(&cache_dir).unwrap();
    let result = (|| {
        let staging = crate::module::cache::stage(&cache_dir)?;
        let archive = staging.path().join("fixture.zip");
        fs::write(&archive, bytes).map_err(|_| refused("test fixture write failed"))?;
        extract(&archive, staging.path())?;
        crate::module::cache::commit(staging, &cache_dir)
    })();
    (root, result)
}

#[test]
fn extracts_independent_stored_and_deflated_archives() {
    let (_root, destination, result) = extract_fixture("compat_store.zip");
    result.unwrap();
    assert_eq!(
        fs::read(destination.join("folder/module.so")).unwrap(),
        b"module-payload"
    );
    assert_eq!(fs::read(destination.join("empty")).unwrap(), b"");

    let (_root, destination, result) = extract_fixture("compat_deflate_descriptor.zip");
    result.unwrap();
    assert_eq!(
        fs::read(destination.join("folder/module.so")).unwrap(),
        b"deflated-module-payload".repeat(16)
    );
}

#[test]
fn extracts_a_small_archive_with_zip64_local_size_fields() {
    let (_root, destination, result) = extract_fixture("compat_zip64_local.zip");
    result.unwrap();
    assert_eq!(
        fs::read(destination.join("module.so")).unwrap(),
        b"zip64-local-module"
    );
}

#[test]
fn rejects_encryption_and_unsupported_methods() {
    for fixture in ["encrypted.zip", "unsupported_method.zip"] {
        let (_root, _destination, result) = extract_fixture(fixture);
        assert!(result.is_err(), "accepted fixture {fixture}");
    }
}

#[test]
fn rejects_overlapping_compressed_entry_ranges() {
    let (_root, _destination, result) = extract_fixture("overlap.zip");
    assert!(result.is_err());
}

#[test]
fn central_directory_entry_count_must_match_the_entries_read() {
    let mut bytes = include_bytes!("fixtures/one_module.zip").to_vec();
    let eocd = bytes
        .windows(4)
        .position(|signature| signature == b"PK\x05\x06")
        .unwrap();
    bytes[eocd + 10..eocd + 12].copy_from_slice(&0u16.to_le_bytes());
    let (_root, _destination, result) = extract_fixture_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn path_policy_rejects_unportable_or_ambiguous_names_before_normalization() {
    for name in [
        "../escape",
        "a/../escape",
        "/absolute",
        "C:/drive",
        "C:relative-drive",
        r"..\escape",
        r"\\server\share\escape",
        "a:stream",
        "NUL.txt",
        "trailing.",
        "trailing ",
        "a//b",
        "./file",
        "",
    ] {
        assert!(checked_path(name).is_err(), "accepted {name:?}");
    }
    let (root, _destination, result) = extract_fixture("unsafe_parent.zip");
    assert!(result.is_err());
    assert!(!root.path().join("outside.txt").exists());
}

#[test]
fn windows_drive_and_backslash_paths_are_rejected_on_every_platform() {
    for fixture in ["windows_drive.zip", "windows_backslash.zip"] {
        let (root, _destination, result) = extract_fixture(fixture);
        assert!(result.is_err(), "accepted fixture {fixture}");
        assert!(!root.path().join("outside.txt").exists());
    }
}

#[test]
fn duplicate_names_are_refused_before_any_file_is_written() {
    let (_root, destination, result) = extract_fixture("duplicate.zip");
    assert!(result.is_err());
    assert!(!destination.join("module.so").exists());
}

#[test]
fn case_colliding_components_are_refused_portably() {
    let (_root, destination, result) = extract_fixture("case_collision.zip");
    assert!(result.is_err());
    assert!(!destination.join("foo/a").exists());
    assert!(!destination.join("FOO/b").exists());
}

#[test]
fn link_and_special_mode_metadata_never_create_links_or_privileges() {
    let (_root, destination, result) = extract_fixture("unix_link_mode.zip");
    result.unwrap();
    let link = fs::symlink_metadata(destination.join("link")).unwrap();
    assert!(link.file_type().is_file());
    assert_eq!(
        fs::read(destination.join("link")).unwrap(),
        b"../outside.txt"
    );

    let (_root, destination, result) = extract_fixture("suid_mode.zip");
    result.unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(destination.join("module.so"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & (libc::S_ISUID | libc::S_ISGID), 0);
    }
}

#[test]
fn an_archive_entry_cannot_overwrite_the_host_created_archive() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    let archive = destination.join("fixture.zip");
    fs::write(&archive, include_bytes!("fixtures/archive_collision.zip")).unwrap();
    let original = fs::read(&archive).unwrap();

    assert!(extract(&archive, &destination).is_err());
    assert_eq!(fs::read(&archive).unwrap(), original);
}

#[test]
fn inflated_output_is_bounded_even_when_the_archive_is_small() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    fs::write(
        &archive,
        include_bytes!("fixtures/compat_deflate_descriptor.zip"),
    )
    .unwrap();

    let result = extract_with_limits(&archive, &destination, 8, 16, 8);
    assert!(result.is_err());
    assert!(!destination.join("folder/module.so").exists());
}

#[test]
fn corrupt_central_crc_is_refused_without_promoting_partial_output() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    let mut bytes = include_bytes!("fixtures/compat_store.zip").to_vec();
    let central = bytes
        .windows(4)
        .position(|window| window == b"PK\x01\x02")
        .unwrap();
    bytes[central + 16] ^= 0x80;
    bytes[14] ^= 0x80;
    fs::write(&archive, bytes).unwrap();

    assert!(extract(&archive, &destination).is_err());
}

#[test]
fn central_and_local_size_claims_must_match_the_decompressed_bytes() {
    let mut bytes = include_bytes!("fixtures/one_module.zip").to_vec();
    set_member_size(&mut bytes, "module.so", 6);
    let (_root, _destination, result) = extract_fixture_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn central_and_local_names_must_agree() {
    let mut bytes = include_bytes!("fixtures/one_module.zip").to_vec();
    let central = bytes
        .windows(4)
        .position(|signature| signature == b"PK\x01\x02")
        .unwrap();
    let name = central + 46;
    bytes[name..name + 9].copy_from_slice(b"module.xx");
    let (_root, _destination, result) = extract_fixture_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn crc_zero_is_still_verified_for_an_empty_file() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    let mut bytes = include_bytes!("fixtures/compat_store.zip").to_vec();
    set_member_crc(&mut bytes, "empty", 1);
    fs::write(&archive, bytes).unwrap();

    assert!(extract(&archive, &destination).is_err());
}

#[test]
fn corrupt_crc_never_promotes_partial_cache_staging() {
    let mut bytes = include_bytes!("fixtures/compat_store.zip").to_vec();
    let central = bytes
        .windows(4)
        .position(|window| window == b"PK\x01\x02")
        .unwrap();
    bytes[central + 16] ^= 0x80;
    bytes[14] ^= 0x80;

    let (root, result) = extract_through_cache_staging(&bytes);
    assert!(result.is_err());
    assert_eq!(fs::read_dir(root.path().join("cache")).unwrap().count(), 0);
}

#[test]
fn data_descriptor_must_match_the_central_directory() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    let mut bytes = include_bytes!("fixtures/compat_deflate_descriptor.zip").to_vec();
    let descriptor = bytes
        .windows(4)
        .position(|window| window == b"PK\x07\x08")
        .unwrap();
    bytes[descriptor + 4] ^= 0x80;
    fs::write(&archive, bytes).unwrap();

    assert!(extract(&archive, &destination).is_err());
    assert!(!destination.join("folder/module.so").exists());
}

#[test]
fn an_incomplete_deflate_stream_is_not_accepted_as_eof() {
    let mut encoder =
        flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(b"payload whose final DEFLATE marker matters")
        .unwrap();
    let mut compressed = encoder.finish().unwrap();
    compressed.pop();

    let mut decoder = StrictDeflate::new(compressed.as_slice(), compressed.len() as u64);
    let mut output = Vec::new();
    assert!(decoder.read_to_end(&mut output).is_err());
}

fn set_member_crc(bytes: &mut [u8], name: &str, crc: u32) {
    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(4)
        .position(|sig| sig == b"PK\x03\x04")
    {
        let local = offset + relative;
        let name_len = u16::from_le_bytes([bytes[local + 26], bytes[local + 27]]) as usize;
        let name_start = local + 30;
        if &bytes[name_start..name_start + name_len] == name.as_bytes() {
            bytes[local + 14..local + 18].copy_from_slice(&crc.to_le_bytes());
            break;
        }
        offset = name_start + name_len;
    }

    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(4)
        .position(|sig| sig == b"PK\x01\x02")
    {
        let central = offset + relative;
        let name_len = u16::from_le_bytes([bytes[central + 28], bytes[central + 29]]) as usize;
        let name_start = central + 46;
        if &bytes[name_start..name_start + name_len] == name.as_bytes() {
            bytes[central + 16..central + 20].copy_from_slice(&crc.to_le_bytes());
            return;
        }
        offset = name_start + name_len;
    }
    panic!("fixture entry not found: {name}");
}

fn set_member_size(bytes: &mut [u8], name: &str, size: u32) {
    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(4)
        .position(|sig| sig == b"PK\x03\x04")
    {
        let local = offset + relative;
        let name_len = u16::from_le_bytes([bytes[local + 26], bytes[local + 27]]) as usize;
        let name_start = local + 30;
        if &bytes[name_start..name_start + name_len] == name.as_bytes() {
            bytes[local + 22..local + 26].copy_from_slice(&size.to_le_bytes());
            break;
        }
        offset = name_start + name_len;
    }

    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(4)
        .position(|sig| sig == b"PK\x01\x02")
    {
        let central = offset + relative;
        let name_len = u16::from_le_bytes([bytes[central + 28], bytes[central + 29]]) as usize;
        let name_start = central + 46;
        if &bytes[name_start..name_start + name_len] == name.as_bytes() {
            bytes[central + 24..central + 28].copy_from_slice(&size.to_le_bytes());
            return;
        }
        offset = name_start + name_len;
    }
    panic!("fixture entry not found: {name}");
}
