//! Explicit connection inputs and evidence for one maintenance operation.
use crate::{
    failure::{self, Result},
    observation::{Observation, Phase},
};
use sqlx_core::connection::ConnectOptions;
use sqlx_doris_core::{DorisConnectOptions, DorisConnection};
#[cfg(feature = "cli-support")]
use std::time::Duration;
use std::{ops::Deref, path::PathBuf};

/// URLs are supplied by the caller and are never discovered from process state.
pub struct ConnectionSettings {
    pub database_url: Option<String>,
    pub fallback_url: Option<String>,
    pub identity_file: Option<PathBuf>,
    pub replication_num: u16,
    pub lock_timeout: u64,
    pub ddl_timeout: u64,
}

pub struct Context {
    pub connection: ConnectionSettings,
    pub observation: Observation,
}

impl Deref for Context {
    type Target = ConnectionSettings;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl Context {
    pub fn url(&self) -> Result<String> {
        if let Some(path) = &self.identity_file {
            if self.database_url.is_some() || self.fallback_url.is_some() {
                return Err(failure::fault(
                    2,
                    "do not pass administrator/database URLs alongside --identity-file; launch the runner without DATABASE_URL, DORIS_TEST_URL or --database-url",
                ));
            }
            #[cfg(feature = "maintenance")]
            return crate::maintenance::identity::credential_url(path).map_err(|error| {
                failure::wrap(2, "invalid private runner credential input", error)
            });
            #[cfg(not(feature = "maintenance"))]
            let _ = path;
            #[cfg(not(feature = "maintenance"))]
            return Err(failure::fault(
                2,
                "managed credentials require maintenance support",
            ));
        }
        let url = self
            .database_url
            .clone()
            .or_else(|| self.fallback_url.clone())
            .ok_or_else(|| {
                failure::fault(2, "set DATABASE_URL or pass --database-url=doris://...")
            })?;
        url.parse::<DorisConnectOptions>()
            .map_err(|_| failure::fault(2, "invalid Doris URL"))?;
        Ok(url)
    }

    pub fn options(&self, database: Option<&str>) -> Result<DorisConnectOptions> {
        if self.replication_num == 0
            || self.lock_timeout > 3600
            || !(1..=3600).contains(&self.ddl_timeout)
        {
            return Err(failure::fault(
                2,
                "invalid migration replicas or operation timeouts",
            ));
        }
        let options = self
            .url()?
            .parse::<DorisConnectOptions>()
            .map_err(|_| failure::fault(2, "invalid Doris connection options"))?
            .disable_statement_logging();
        #[cfg(feature = "cli-support")]
        let options = options
            .migration_replication(self.replication_num)
            .migration_lock_timeout(Duration::from_secs(self.lock_timeout))
            .migration_timeout(Duration::from_secs(self.ddl_timeout));
        Ok(match database {
            Some(database) => options.database(database),
            None => options,
        })
    }

    pub async fn connect(&self) -> Result<DorisConnection> {
        self.observation.phase(Phase::Read, "doris_connection");
        Ok(self.options(None)?.connect().await?)
    }
}
