use super::*;

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires TINYBUS_TEST_MODULE to point at the built module_clock cdylib"]
async fn the_first_strict_call_prepares_a_real_lazy_resolver_module_without_a_probe() {
    use crate::module::{ModuleAccess, ModuleCallError, ModuleClient, ProxyFailure};
    use std::sync::atomic::AtomicUsize;

    let library_source = PathBuf::from(std::env::var_os("TINYBUS_TEST_MODULE").unwrap());
    let install = tempfile::tempdir_in(std::env::current_dir().unwrap()).unwrap();
    let host_key = crate::module::platform::host_candidates()
        .into_iter()
        .next()
        .expect("test host has a published module platform key");
    let module_id: &'static str = Box::leak("resolver-attested-lazy".to_string().into_boxed_str());
    let archive_name: &'static str =
        Box::leak("clock-attested-test.tar.gz".to_string().into_boxed_str());
    let version = "0.1.7";
    let bus_name = "ai.tinyhumans.openhuman.Clock";
    let object_path = "/ai/tinyhumans/openhuman/Clock";
    let host_key: &'static str = Box::leak(host_key.into_boxed_str());
    let library_name = library_source
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap()
        .to_string();
    let cache_dir =
        crate::module::artifact_dir(install.path(), module_id, version, host_key).unwrap();
    let extraction = cache_dir.join("assets");
    std::fs::create_dir_all(&extraction).unwrap();
    let library_path = extraction.join(&library_name);
    std::fs::copy(&library_source, &library_path).unwrap();
    let library_sha256 = crate::module::sha256_file(&library_path).unwrap();
    std::fs::write(
        extraction.join("modules.toml"),
        format!("{library_name:?} = \"{library_sha256}\"\n"),
    )
    .unwrap();

    let archive_path = cache_dir.join(archive_name);
    let encoder = flate2::write::GzEncoder::new(
        std::fs::File::create(&archive_path).unwrap(),
        flate2::Compression::default(),
    );
    let mut archive = tar::Builder::new(encoder);
    for (name, bytes) in [
        (
            format!("assets/{library_name}"),
            std::fs::read(&library_path).unwrap(),
        ),
        (
            "assets/modules.toml".to_string(),
            std::fs::read(extraction.join("modules.toml")).unwrap(),
        ),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive.append_data(&mut header, name, &bytes[..]).unwrap();
    }
    archive.into_inner().unwrap().finish().unwrap();
    let archive_sha256 = crate::module::sha256_file(&archive_path).unwrap();
    crate::module::cache::write_digest_marker(&cache_dir, archive_name, &archive_sha256).unwrap();
    assert!(
        crate::module::cache::find_verified(&cache_dir, archive_name, Some(&archive_sha256))
            .is_some(),
        "fixture archive must be recognized by the release cache"
    );

    let assets = Box::leak(
        vec![crate::module::ReleaseAsset {
            host_key,
            archive: archive_name,
            sha256: Box::leak(archive_sha256.into_boxed_str()),
            library_sha256: Some(Box::leak(library_sha256.into_boxed_str())),
        }]
        .into_boxed_slice(),
    );
    let record = Box::leak(Box::new(ReleaseRecord {
        id: module_id,
        bus_name,
        object_path,
        version,
        release_url: "https://github.com/tinyhumansai/tinybus/releases/tag/v0.1.7",
        assets,
    }));
    let fixture = fixture(
        record,
        true,
        HashMap::from([(module_id.to_string(), ModuleSource::Pinned)]),
        Some(install.path().to_path_buf()),
    );

    struct CountedResolver {
        resolver: Resolver,
        preparations: Arc<AtomicUsize>,
    }

    impl ModuleAccess for CountedResolver {
        fn find(&self, id: &str) -> Option<&'static ReleaseRecord> {
            self.resolver.find(id)
        }

        fn enabled(&self) -> bool {
            self.resolver.enabled()
        }

        fn reporter(&self) -> &FailureReporter {
            self.resolver.reporter()
        }

        fn ensure_loaded<'a>(
            &'a self,
            id: &'a str,
        ) -> crate::module::AccessFuture<'a, Result<(), String>> {
            Box::pin(self.resolver.ensure_loaded(id))
        }

        fn prepare_attested<'a>(
            &'a self,
            id: &'a str,
        ) -> crate::module::AccessFuture<'a, Result<(), String>> {
            self.preparations.fetch_add(1, Ordering::SeqCst);
            Box::pin(self.resolver.prepare_attested(id))
        }

        fn proxy<'a>(
            &'a self,
            bus_name: &'a str,
            object_path: &'a str,
        ) -> crate::module::AccessFuture<'a, Result<crate::Proxy, ProxyFailure>> {
            Box::pin(async move {
                self.resolver
                    .runtime()
                    .await
                    .map_err(|_| ProxyFailure::Transport)?
                    .proxy(bus_name, object_path)
                    .map_err(|_| ProxyFailure::Contract)
            })
        }
    }

    let preparations = Arc::new(AtomicUsize::new(0));
    let client = ModuleClient::new(CountedResolver {
        resolver: fixture.resolver.clone(),
        preparations: preparations.clone(),
    });
    let response: std::result::Result<String, ModuleCallError> =
        client.call_attested(record.id, "Now", ()).await;
    let load_error = fixture.resolver.ensure_loaded(record.id).await.err();
    let diagnostic_host = crate::module::ModuleHost::new(crate::Broker::new());
    let direct_error = diagnostic_host
        .load_github_release_cached(
            &crate::module::CachedRelease {
                release_url: record.release_url,
                asset_name: archive_name,
                expected_sha256: Some(assets[0].sha256),
                expected_library_sha256: assets[0].library_sha256,
                cache_dir: &cache_dir,
                allow_download: false,
            },
            serde_json::json!({}),
        )
        .err()
        .map(|error| error.to_string());
    assert!(
        response.is_ok(),
        "{response:?}; preparations={}, state={:?}, load_error={load_error:?}, direct_error={direct_error:?}, config_calls={}, runtime_calls={}",
        preparations.load(Ordering::SeqCst),
        fixture.resolver.state_of(record.id),
        fixture.config_calls.load(Ordering::SeqCst),
        fixture.runtime_calls.load(Ordering::SeqCst),
    );
    assert_eq!(preparations.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.resolver.state_of(record.id), Availability::Ready);
}
