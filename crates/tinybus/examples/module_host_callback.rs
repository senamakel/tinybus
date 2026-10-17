//! Actual eager native fixture calling its fixed host during setup before READY.
use serde::Deserialize;
use tinybus::{Connection, Error, Result};

#[derive(Deserialize)]
struct Config {
    host: tinybus::BusName,
    #[serde(default)]
    fail: bool,
    #[serde(default = "confidential_by_default")]
    confidential: bool,
}

fn confidential_by_default() -> bool {
    true
}
async fn setup(connection: Connection, config: Config) -> Result<()> {
    async {
        if config.fail {
            return Err(Error::failed("fixture initialization rejected"));
        }
        let proxy = connection.proxy(config.host.as_str(), "/callback", "org.example.Callback")?;
        let _: tinybus::BusName = if config.confidential {
            proxy.call_confidential("Observe", ()).await?
        } else {
            proxy.call("Observe", ()).await?
        };
        connection.request_name("org.example.EagerCallback").await
    }
    .await
}

tinybus_module::module_export! {
    setup = setup, config = Config,
    worker_threads = 1,
    provides = ["org.example.EagerCallback"],
    methods = ["Observe"], signals = [], requires = [], optional = [], lazy = false,
}
