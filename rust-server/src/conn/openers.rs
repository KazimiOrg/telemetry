use super::*;
use crate::conn::{JsonFiles, Postgres};
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
                    // XXX <16-10-2024,afjoseph> The tokio-postgres crate doesn't officially
                    // support rustls (https://github.com/sfackler/rust-postgres/issues/421), but,
                    // as of today, [tokio-postgres-rustls](https://github.com/jbg/tokio-postgres-rustls)
                    // crate provides bindings for tokio-postgres using rustls and it works so far.
                    // The reason we're not using the official TLS bindings for tokio-postgres
                    // (i.e., openssl and native-tls) is that both require openssl, which is a
                    // massive pain-in-the-ass to cross-compile, especially using the Cross
                    // project (see https://github.com/cross-rs/cross/wiki/Recipes#openssl).
                    //
                    // If rustls ever breaks here, we can use postgres-native-tls and vendor
                    // openssl (see link above).
                    let mut roots = rustls::RootCertStore::empty();
                    // Load the platform's root certificates into the store
                    for cert in rustls_native_certs::load_native_certs()
                        .expect("could not load platform certs")
                    {
                        roots.add(cert).unwrap();
                    }
                    // Load the user's root certificates into the store, if any
                    if let Some(tls_root_cert_path) = tls_root_cert_path {
                        debug!("Adding TLS root cert from {}", tls_root_cert_path.display());
                        let cert_bytes = fs::read(tls_root_cert_path)?;
                        let cert = rustls_pki_types::CertificateDer::from_slice(&cert_bytes[..]);
                        roots.add(cert).unwrap();
                    }
                    let (client, conn) = tokio_postgres::connect(
                        &dbconnstring,
                        tokio_postgres_rustls::MakeRustlsConnect::new(
                            rustls::ClientConfig::builder()
                                .with_root_certificates(roots)
                                .with_no_client_auth(),
                        ),
                    )
                    .await?;
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
