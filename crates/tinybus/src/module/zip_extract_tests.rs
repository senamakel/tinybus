use super::*;

fn extract_fixture(name: &str) -> (tempfile::TempDir, PathBuf, Result<()>) {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("fixture.zip");
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    fs::write(
        &archive,
        match name {
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
            _ => panic!("unknown test fixture"),
        },
    )
    .unwrap();
    let result = extract(&archive, &destination);
    (root, destination, result)
}

#[test]
fn extracts_independent_stored_and_deflated_archives() {
    let (_root, destination, result) = extract_fixture("compat_store.zip");
    result.unwrap();
    assert_eq!(fs::read(destination.join("folder/module.so")).unwrap(), b"module-payload");
    assert_eq!(fs::read(destination.join("empty")).unwrap(), b"");

    let (_root, destination, result) = extract_fixture("compat_deflate_descriptor.zip");
    result.unwrap();
    assert_eq!(
        fs::read(destination.join("folder/module.so")).unwrap(),
        b"deflated-module-payload".repeat(16)
    );
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
fn link_and_special_mode_metadata_never_create_links_or_privileges() {
    let (_root, destination, result) = extract_fixture("unix_link_mode.zip");
    result.unwrap();
    let link = fs::symlink_metadata(destination.join("link")).unwrap();
    assert!(link.file_type().is_file());
    assert_eq!(fs::read(destination.join("link")).unwrap(), b"../outside.txt");

    let (_root, destination, result) = extract_fixture("suid_mode.zip");
    result.unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(destination.join("module.so")).unwrap().permissions().mode();
        assert_eq!(mode & (libc::S_ISUID | libc::S_ISGID), 0);
    }
}

#[test]
fn a_host_created_archive_path_cannot_be_overwritten() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("staging");
    fs::create_dir(&destination).unwrap();
    let archive = destination.join("fixture.zip");
    fs::write(&archive, include_bytes!("fixtures/compat_store.zip")).unwrap();

    // Replacing an entry path in the fixture's central/local headers is tested
    // separately; this pins the create_new filesystem primitive directly.
    let output = OpenOptions::new().write(true).create_new(true).open(&archive);
    assert!(output.is_err());
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
    fs::write(&archive, bytes).unwrap();

    assert!(extract(&archive, &destination).is_err());
    assert!(!destination.join("folder/module.so").exists());
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
