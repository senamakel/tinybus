use super::{ReleaseAsset, ReleasePlan, load_first_admitted};
use crate::broker::Broker;
use crate::module::{ModuleHost, artifact_dir};

const ASSET: ReleaseAsset<'static> = ReleaseAsset {
    host_key: "ubuntu-24.04-x86_64",
    archive: "demo-ubuntu-24.04-x86_64.tar.gz",
    sha256: "0000000000000000000000000000000000000000000000000000000000000000",
    library_sha256: None,
};

fn plan<'a>(
    assets: &'a [ReleaseAsset<'a>],
    install_root: &'a std::path::Path,
    bundled_root: Option<&'a std::path::Path>,
) -> ReleasePlan<'a> {
    ReleasePlan {
        id: "demo",
        version: "1.0.0",
        release_url: "https://github.com/tinyhumansai/demo/releases/tag/v1.0.0",
        assets,
        install_root,
        bundled_root,
        allow_download: false,
    }
}

#[test]
fn no_candidates_means_unavailable_on_this_platform() {
    let host = ModuleHost::new(Broker::new());
    let root = tempfile::tempdir().unwrap();
    let error = load_first_admitted(&host, &plan(&[], root.path(), None), &serde_json::json!({}))
        .unwrap_err();
    assert!(error.contains("not available for this platform"), "{error}");
}

#[test]
fn an_invalid_installer_bundle_is_reported_without_falling_back_to_the_cache() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let dir = artifact_dir(bundled.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(ASSET.archive), b"").unwrap();

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    assert!(error.contains("installer bundle"), "{error}");
    assert!(error.contains("repairing the installation"), "{error}");
    assert!(!user_cache.path().join("demo").exists());
}

#[test]
fn an_absent_installer_bundle_uses_the_release_cache_path() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    assert!(error.contains("downloads are disabled"), "{error}");
    assert!(!error.contains("installer bundle"), "{error}");
}

#[test]
fn an_unbuildable_cache_path_falls_through_then_reports_the_miss() {
    let host = ModuleHost::new(Broker::new());
    let root = tempfile::tempdir().unwrap();
    let bad = ReleaseAsset {
        host_key: "../escape",
        ..ASSET
    };
    let assets = [bad];
    let error = load_first_admitted(
        &host,
        &plan(&assets, root.path(), None),
        &serde_json::json!({}),
    )
    .unwrap_err();
    assert!(error.contains("downloads are disabled"), "{error}");
}

#[test]
fn an_unbuildable_bundle_path_is_skipped_and_the_release_cache_is_used() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let bad = ReleaseAsset {
        host_key: "../escape",
        ..ASSET
    };
    let assets = [bad];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();
    assert!(error.contains("downloads are disabled"), "{error}");
    assert!(!error.contains("installer bundle"), "{error}");
}

#[test]
fn a_terminal_failure_with_downloads_enabled_says_to_restart() {
    let host = ModuleHost::new(Broker::new());
    let root = tempfile::tempdir().unwrap();
    let bad = ReleaseAsset {
        host_key: "../escape",
        ..ASSET
    };
    let assets = [bad];
    let mut release = plan(&assets, root.path(), None);
    release.allow_download = true;
    let error = load_first_admitted(&host, &release, &serde_json::json!({})).unwrap_err();
    assert!(error.contains("could not be loaded"), "{error}");
    assert!(error.contains("restart the app"), "{error}");
}

#[test]
fn a_bundle_marker_that_does_not_match_the_pin_is_refused_without_falling_back() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let dir = artifact_dir(bundled.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(format!("{}.sha256", ASSET.archive)),
        format!("{}\n", "f".repeat(64)),
    )
    .unwrap();

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    assert!(error.contains("installer bundle"), "{error}");
    assert!(error.contains("does not match the pin"), "{error}");
    assert!(!user_cache.path().join("demo").exists());
}

#[test]
fn a_bundle_marker_is_never_honoured_from_the_download_cache() {
    let host = ModuleHost::new(Broker::new());
    let user_cache = tempfile::tempdir().unwrap();
    let dir = artifact_dir(user_cache.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(format!("{}.sha256", ASSET.archive)),
        format!("{}\n", ASSET.sha256),
    )
    .unwrap();
    std::fs::write(dir.join("libdemo_module.so"), b"").unwrap();
    std::fs::write(dir.join("libdemo_module.dylib"), b"").unwrap();
    std::fs::write(dir.join("demo_module.dll"), b"").unwrap();

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), None),
        &serde_json::json!({}),
    )
    .unwrap_err();
    assert!(error.contains("downloads are disabled"), "{error}");
}

#[derive(Clone)]
struct TestCallback;

#[tinybus::interface(name = "org.example.Callback")]
impl TestCallback {
    async fn observe(&self) -> tinybus::Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TINYBUS_TEST_HOST_CALLBACK_MODULE to point at the built cdylib"]
async fn release_callback_resolution_uses_a_fresh_binding_for_each_rejected_candidate() {
    use super::load_first_admitted_with_host_callbacks;
    use crate::module::{HostCallbackEndpoint, ModuleRuntime, RuntimeNames};
    use std::sync::{Arc, Mutex};

    let fixture =
        std::path::PathBuf::from(std::env::var_os("TINYBUS_TEST_HOST_CALLBACK_MODULE").unwrap());
    let root = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(root.path()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let runtime = Arc::new(
        ModuleRuntime::start(
            &RuntimeNames {
                thread: "tinybus-callback-release-test".into(),
                worker: "tinybus-callback-release-worker".into(),
            },
            None,
        )
        .unwrap(),
    );
    let record = Box::leak(Box::new(crate::module::ReleaseRecord {
        id: "tinybus",
        bus_name: "org.example.EagerCallback",
        object_path: "/org/example/EagerCallback",
        version: "0.0.0",
        release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.0.0",
        assets: &[],
    }));

    let mut assets = Vec::new();
    for (host_key, archive_name, bytes) in [
        ("candidate-one", "one.tar.gz", b"archive-one".as_slice()),
        ("candidate-two", "two.tar.gz", b"archive-two".as_slice()),
    ] {
        let dir = artifact_dir(&root, record.id, record.version, host_key).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(archive_name), bytes).unwrap();
        let digest =
            crate::module::hash::file_hex(std::fs::File::open(dir.join(archive_name)).unwrap())
                .unwrap();
        let module = dir.join(format!(
            "libcandidate.{}",
            crate::module::cache::library_extension()
        ));
        if host_key == "candidate-one" {
            std::fs::write(&module, b"not a loadable module").unwrap();
        } else {
            std::fs::copy(&fixture, &module).unwrap();
            let library_hash =
                crate::module::hash::file_hex(std::fs::File::open(&module).unwrap()).unwrap();
            std::fs::write(
                dir.join("modules.toml"),
                format!(
                    "{:?} = {library_hash:?}\n",
                    module.file_name().unwrap().to_str().unwrap()
                ),
            )
            .unwrap();
        }
        assets.push(ReleaseAsset {
            host_key: Box::leak(host_key.to_owned().into_boxed_str()),
            archive: Box::leak(archive_name.to_owned().into_boxed_str()),
            sha256: Box::leak(digest.into_boxed_str()),
            library_sha256: None,
        });
    }

    let plan = ReleasePlan {
        id: record.id,
        version: record.version,
        release_url: record.release_url,
        assets: &assets,
        install_root: &root,
        bundled_root: None,
        allow_download: false,
    };
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let factory_runtime = Arc::clone(&runtime);
    let factory_attempts = Arc::clone(&attempts);
    let prepare = move |_attempt: usize| {
        let runtime = Arc::clone(&factory_runtime);
        let attempts = Arc::clone(&factory_attempts);
        async move {
            let binding = Arc::new(
                runtime
                    .host()
                    .prepare_host_callbacks(vec![HostCallbackEndpoint {
                        path: "/callback".parse().unwrap(),
                        interface: "org.example.Callback".parse().unwrap(),
                        members: vec!["Observe".parse().unwrap()],
                    }])
                    .await
                    .map_err(|error| error.to_string())?,
            );
            binding
                .serve_at("/callback".parse().unwrap(), TestCallback)
                .await
                .map_err(|error| error.to_string())?;
            attempts.lock().unwrap().push(Arc::clone(&binding));
            let config = serde_json::json!({ "host": binding.unique_name().as_str() });
            Ok((binding, config))
        }
    };

    let host = runtime.host();
    let (_info, selected) = load_first_admitted_with_host_callbacks(
        host,
        &plan,
        prepare,
        record.bus_name,
        record.object_path,
    )
    .await
    .unwrap();
    let bindings = attempts.lock().unwrap();
    assert_eq!(bindings.len(), 2);
    assert!(!host.retains_host_callback_binding(record.bus_name, &bindings[0]));
    assert!(host.retains_host_callback_binding(record.bus_name, &selected));
    assert!(Arc::ptr_eq(&bindings[1], &selected));
}

/// A marker-style bundle entry whose library and allowlist agree, so nothing
/// about its bytes is refused before the loader reaches the directory gate.
#[cfg(unix)]
fn marked_bundle(bundled: &std::path::Path) -> std::path::PathBuf {
    let dir = artifact_dir(bundled, "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(format!("{}.sha256", ASSET.archive)),
        format!("{}\n", ASSET.sha256),
    )
    .unwrap();
    let library = format!(
        "libdemo_module.{}",
        crate::module::cache::library_extension()
    );
    std::fs::write(dir.join(&library), b"not a real library").unwrap();
    let sha = crate::module::sha256_file(dir.join(&library)).unwrap();
    std::fs::write(
        dir.join("modules.toml"),
        format!("\"{library}\" = \"{sha}\"\n"),
    )
    .unwrap();
    dir
}

#[cfg(unix)]
#[test]
fn a_bundle_refused_only_for_its_location_falls_back_to_the_release_cache() {
    use std::os::unix::fs::PermissionsExt;

    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    let dir = marked_bundle(bundled.path());
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777)).unwrap();

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    // The release cache was consulted: with downloads off, that is a miss.
    assert!(error.contains("downloads are disabled"), "{error}");
    assert!(!error.contains("installer bundle"), "{error}");
}

#[cfg(unix)]
#[test]
fn a_bundle_refused_for_its_content_is_still_authoritative() {
    let host = ModuleHost::new(Broker::new());
    let bundled = tempfile::tempdir().unwrap();
    let user_cache = tempfile::tempdir().unwrap();
    // A private directory: the gate admits the location, then the loader
    // refuses bytes that are not a library.
    marked_bundle(bundled.path());

    let assets = [ASSET];
    let error = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), Some(bundled.path())),
        &serde_json::json!({}),
    )
    .unwrap_err();

    assert!(error.contains("installer bundle"), "{error}");
    assert!(!user_cache.path().join("demo").exists());
}

#[cfg(unix)]
#[test]
fn the_release_cache_is_repaired_to_owner_only_write_before_it_is_used() {
    use std::os::unix::fs::PermissionsExt;

    let host = ModuleHost::new(Broker::new());
    let user_cache = tempfile::tempdir().unwrap();
    let dir = artifact_dir(user_cache.path(), "demo", "1.0.0", ASSET.host_key).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    // What an earlier launch under umask 002 left behind.
    for directory in [user_cache.path().to_path_buf(), dir.clone()] {
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o775)).unwrap();
    }

    let assets = [ASSET];
    let _ = load_first_admitted(
        &host,
        &plan(&assets, user_cache.path(), None),
        &serde_json::json!({}),
    );

    for directory in [user_cache.path().to_path_buf(), dir] {
        let mode = std::fs::metadata(&directory).unwrap().permissions().mode();
        assert_eq!(mode & 0o022, 0, "{} is {mode:o}", directory.display());
    }
}
