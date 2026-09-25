//! Two modules in one executable must keep distinct symbols and manifests.

mod first {
    async fn setup(_: tinybus::Connection) -> tinybus::Result<()> {
        Ok(())
    }

    crate::module_export_static! {
        setup = setup,
        worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.StaticFirst"],
        methods = ["First"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

mod second {
    async fn setup(_: tinybus::Connection) -> tinybus::Result<()> {
        Ok(())
    }

    crate::module_export_static! {
        setup = setup,
        worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.StaticSecond"],
        methods = ["Second"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

mod configured {
    #[derive(serde::Deserialize)]
    struct Config {}

    async fn setup(_: tinybus::Connection, _: Config) -> tinybus::Result<()> {
        Ok(())
    }

    crate::module_export_static! {
        setup = setup,
        config = Config,
        worker_threads = 1,
        provides = ["ai.tinyhumans.tinybus.StaticConfigured"],
        methods = ["Configured"],
        signals = [],
        requires = [],
        optional = [],
        lazy = true,
    }
}

#[test]
fn linked_modules_retain_distinct_manifests() {
    fn manifest(slice: tinybus::module::abi::TbSlice) -> tinybus::module::manifest::ModuleManifest {
        let bytes = unsafe { std::slice::from_raw_parts(slice.ptr, slice.len) };
        serde_json::from_slice(bytes).expect("generated manifest")
    }

    let first_manifest = manifest(first::tinybus_module_manifest_v1());
    let second_manifest = manifest(second::tinybus_module_manifest_v1());
    let configured_manifest = manifest(configured::tinybus_module_manifest_v1());
    assert_eq!(
        first_manifest.bus_name.as_str(),
        "ai.tinyhumans.tinybus.StaticFirst"
    );
    assert_eq!(
        second_manifest.bus_name.as_str(),
        "ai.tinyhumans.tinybus.StaticSecond"
    );
    assert_eq!(
        configured_manifest.bus_name.as_str(),
        "ai.tinyhumans.tinybus.StaticConfigured"
    );
    let _entries = (
        &first::TINYBUS_MODULE_ABI_V1,
        first::tinybus_module_init_v1 as tinybus::module::abi::TbModuleInit,
        &second::TINYBUS_MODULE_ABI_V1,
        second::tinybus_module_init_v1 as tinybus::module::abi::TbModuleInit,
        &configured::TINYBUS_MODULE_ABI_V1,
        configured::tinybus_module_init_v1 as tinybus::module::abi::TbModuleInit,
    );
}

#[test]
fn invalid_linked_manifest_never_exposes_partial_bytes() {
    static BYTES: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    let slice = crate::manifest_slice_in(
        &BYTES,
        crate::ManifestDeclaration {
            name: "invalid",
            version: "not-semver",
            provides: &["ai.tinyhumans.tinybus.Invalid"],
            methods: &[],
            signals: &[],
            requires: &[],
            optional: &[],
            lazy: false,
            worker_threads: 1,
        },
    );
    assert!(slice.ptr.is_null());
    assert_eq!(slice.len, 0);
    assert!(BYTES.get().is_none());
}
