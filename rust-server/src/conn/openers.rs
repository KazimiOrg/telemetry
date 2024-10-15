use super::*;
use crate::conn::{JsonFiles, Postgres};
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::fs;
use std::path::PathBuf;
use tokio_postgres::NoTls;
use tracing::{debug, error, warn};

pub trait StorageOpen {
    async fn open(self) -> Result<Box<dyn Connection + Send>>;
}

pub struct SqliteOpen {
    pub custom_schema_path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
}

impl StorageOpen for SqliteOpen {
    async fn open(self) -> Result<Box<dyn Connection + Send>> {
        let db_path = self
            .db_path
            .clone()
            .unwrap_or_else(|| "telemetry.sqlite.db".to_owned().into());
        let schema_contents = match self.custom_schema_path {
            Some(custom_schema_path) => fs::read_to_string(custom_schema_path)?,
            None => include_str!("../../sql/sqlite.sql").to_owned(),
        };
        let mut conn = rusqlite::Connection::open(db_path)?;
        conn.pragma_update(None, "foreign_keys", "on")?;
        if !conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))? {
            warn!("foreign keys not enabled");
        }
        let tx = conn.transaction()?;
        let user_version: u64 = tx.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if user_version == 0 {
            tx.execute_batch(&schema_contents)?;
            tx.pragma_update(None, "user_version", 1)?;
        }
        tx.commit()?;
        Ok(Box::new(conn))
    }
}

pub struct JsonFilesOpen {}

impl StorageOpen for JsonFilesOpen {
    async fn open(self) -> Result<Box<dyn Connection + Send>> {
        let streams = JsonFileWriter::new("streams".to_owned()).context("opening streams")?;
        let events = JsonFileWriter::new("events".to_owned()).context("opening events")?;
        Ok(Box::new(JsonFiles { streams, events }))
    }
}

pub(crate) struct PostgresOpener {
    pub custom_schema_path: Option<PathBuf>,
    pub dbconnstring: String,
    pub tls_root_cert_path: Option<PathBuf>,
    // TODO: Extract this from the connection string.
    pub use_tls: bool,
}

impl StorageOpen for PostgresOpener {
    async fn open(self) -> Result<Box<dyn Connection + Send>> {
        let PostgresOpener {
            use_tls,
            tls_root_cert_path,
            dbconnstring,
            custom_schema_path,
        } = self;
        Ok({
            let client = match use_tls {
                false => {
                    debug!("Initializing postgres storage without TLS");
                    let (client, conn) = tokio_postgres::connect(&dbconnstring, NoTls).await?;
                    tokio::spawn(async move {
                        if let Err(err) = conn.await {
                            error!(%err, "postgres connection failed");
                        }
                    });
                    client
                }
                true => {
                    let mut builder = TlsConnector::builder();
                    if let Some(tls_root_cert_path) = tls_root_cert_path {
                        debug!("Adding TLS root cert from {}", tls_root_cert_path.display());
                        let cert = fs::read(tls_root_cert_path)?;
                        let cert = Certificate::from_pem(&cert)?;
                        builder.add_root_certificate(cert);
                    }
                    let connector = builder.build()?;
                    let connector = MakeTlsConnector::new(connector);
                    let (client, conn) = tokio_postgres::connect(&dbconnstring, connector).await?;
                    tokio::spawn(async move {
                        if let Err(err) = conn.await {
                            error!(%err, "postgres connection failed");
                        }
                    });
                    client
                }
            };
            let schema_contents = match custom_schema_path {
                Some(custom_schema_path) => fs::read_to_string(custom_schema_path)?,
                None => include_str!("../../sql/postgres.sql").to_owned(),
            };
            client.batch_execute(&schema_contents).await?;
            Box::new(Postgres { client })
        })
    }
}
