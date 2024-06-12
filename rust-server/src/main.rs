use axum::http::StatusCode;
use futures::StreamExt;
use std::sync::Arc;

use tracing::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let sqlite_conn = tokio::sync::Mutex::new(rusqlite::Connection::open("telemetry.db")?);
    let server = Arc::new(Server { sqlite_conn });
    let app = axum::Router::new().route(
        "/",
        axum::routing::post(|body| async move { server.submit(body).await }),
    );
    // This is just the OTLP/HTTP port, because if we're using this we're probably not using OTLP.
    let listener = tokio::net::TcpListener::bind("[::]:4318").await?;
    Ok(axum::serve(listener, app).await?)
}

struct Server {
    sqlite_conn: tokio::sync::Mutex<rusqlite::Connection>,
}

impl Server {
    async fn submit(&self, req: axum::http::Request<axum::body::Body>) -> StatusCode {
        let mut body_data_stream = req.into_body().into_data_stream();
        let mut bytes = vec![];
        while let Some(result) = body_data_stream.next().await {
            let new_bytes = match result {
                Err(err) => {
                    error!(?err, "error in body data stream");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
                Ok(ok) => ok,
            };
            bytes.extend_from_slice(&new_bytes);
            // Iterate through JSON objects without allocating anything.
            let mut json_stream_deserializer =
                serde_json::Deserializer::from_slice(bytes.as_slice())
                    .into_iter::<serde::de::IgnoredAny>();
            let mut last_offset = 0;
            while let Some(result) = json_stream_deserializer.next() {
                match result {
                    Err(err) if err.is_eof() => {
                        // We need more data for a complete object.
                        break;
                    }
                    Err(err) => {
                        error!(?err, "error deserializing json value");
                        return StatusCode::BAD_REQUEST;
                    }
                    Ok(serde::de::IgnoredAny) => {
                        let value_end_offset = json_stream_deserializer.byte_offset();
                        let payload = &bytes[last_offset..value_end_offset];
                        // sqlite needs to be given text.
                        let payload = std::str::from_utf8(payload).unwrap();
                        if let Err(err) = self
                            .sqlite_conn
                            .lock()
                            .await
                            .execute("insert into data (payload) values (jsonb(?))", &[payload])
                        {
                            error!(payload, ?err, "error inserting payload into store");
                            return StatusCode::INTERNAL_SERVER_ERROR;
                        }
                        last_offset = value_end_offset;
                    }
                }
            }
        }
        StatusCode::OK
    }
}
