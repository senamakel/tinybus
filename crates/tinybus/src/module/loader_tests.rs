use super::*;

#[test]
fn descriptor_gate_rejects_invalid_size_and_endianness_declarations() {
    let path = Path::new("module.so");
    let mut descriptor = TbAbiDescriptor::current("module", crate::VERSION);
    descriptor.descriptor_size = DESCRIPTOR_PREFIX_SIZE - 1;
    assert!(gate_descriptor(path, &descriptor, false).is_err());

    let mut descriptor = TbAbiDescriptor::current("module", crate::VERSION);
    descriptor.flags ^= 1 << 2;
    assert!(gate_descriptor(path, &descriptor, false).is_err());
}

#[test]
fn a_replaced_path_cannot_promote_an_unverified_native_handle() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("module.so");
    std::fs::write(&path, b"first mapped image").unwrap();
    let first = crate::module::sha256_file(&path).unwrap();
    let mut images = HashMap::new();
    assert_eq!(
        record_mapped_image(&mut images, 7, first.clone(), None),
        Ok(None)
    );

    std::fs::write(&path, b"replacement image").unwrap();
    let replacement = crate::module::sha256_file(&path).unwrap();
    assert_ne!(first, replacement);
    assert!(
        record_mapped_image(&mut images, 7, replacement.clone(), Some(&replacement)).is_err(),
        "a new pathname digest must not become proof for the old handle"
    );
}

#[test]
fn one_native_handle_cannot_be_reused_with_a_different_proof() {
    let mut images = HashMap::new();
    let first = "a".repeat(64);
    let second = "b".repeat(64);
    assert_eq!(
        record_mapped_image(&mut images, 11, first.clone(), Some(&first)),
        Ok(Some(first.clone()))
    );
    assert!(record_mapped_image(&mut images, 11, first.clone(), Some(&second)).is_err());
    assert_eq!(
        record_mapped_image(&mut images, 11, first.clone(), Some(&first)),
        Ok(Some(first))
    );
}

#[test]
fn a_library_pin_is_validated_against_file_bytes_before_mapping() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("not-a-library.so");
    std::fs::write(&path, b"library bytes with a compiled pin").unwrap();

    let invalid_pin = load_verified(&path, false, Some("not-a-sha256"))
        .unwrap_err()
        .to_string();
    assert!(invalid_pin.contains("library digest pin is invalid"));

    let wrong_pin = "a".repeat(64);
    let mismatched_pin = load_verified(&path, false, Some(&wrong_pin))
        .unwrap_err()
        .to_string();
    assert!(mismatched_pin.contains("module library hash does not match its pin"));
}

#[cfg(unix)]
#[test]
fn unix_loader_refuses_missing_and_nul_containing_paths() {
    let missing = Path::new("/definitely/not/a/tinybus-module.so");
    let missing_error = match load(missing, false) {
        Ok(_) => panic!("missing module unexpectedly loaded"),
        Err(error) => error,
    };
    assert!(
        missing_error
            .to_string()
            .contains("dynamic loader rejected the artifact")
    );

    use std::os::unix::ffi::OsStrExt;
    let nul_path = Path::new(std::ffi::OsStr::from_bytes(b"module\0name"));
    let nul_error = match platform::open(nul_path) {
        Ok(_) => panic!("NUL-containing module path unexpectedly loaded"),
        Err(error) => error,
    };
    assert!(nul_error.to_string().contains("artifact path is invalid"));
}
