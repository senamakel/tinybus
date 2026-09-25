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

#[test]
fn linked_modules_retain_distinct_manifests() {
    fn manifest(slice: tinybus::module::abi::TbSlice) -> tinybus::module::manifest::ModuleManifest {
        let bytes = unsafe { std::slice::from_raw_parts(slice.ptr, slice.len) };
        serde_json::from_slice(bytes).expect("generated manifest")
    }

    let first_manifest = manifest(first::tinybus_module_manifest_v1());
    let second_manifest = manifest(second::tinybus_module_manifest_v1());
    assert_eq!(
        first_manifest.bus_name.as_str(),
        "ai.tinyhumans.tinybus.StaticFirst"
    );
    assert_eq!(
        second_manifest.bus_name.as_str(),
        "ai.tinyhumans.tinybus.StaticSecond"
    );
    let _entries = (
        &first::TINYBUS_MODULE_ABI_V1,
        first::tinybus_module_init_v1 as tinybus::module::abi::TbModuleInit,
        &second::TINYBUS_MODULE_ABI_V1,
        second::tinybus_module_init_v1 as tinybus::module::abi::TbModuleInit,
    );
}
