mod openers;
pub use openers::*;

use super::*;
use axum::async_trait;
use chrono::Utc;
use rand::random;
use serde_json::json;
use tempfile::NamedTempFile;
use tokio_postgres::Client;

#[async_trait]
pub(crate) trait Connection: Send {
    async fn new_stream(&mut self, headers: SerializedHeaders) -> Result<StreamId>;
    async fn insert_event(
        &mut self,
        stream_id: StreamId,
        stream_event_index: StreamEventIndex,
        // TODO: Could use payload type here to let implementation decide what to do.
        payload: &str,
    ) -> Result<()>;
    // Write stuff to disk
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    // Make data available to observers.
    async fn commit(&mut self) -> Result<()> {
        Ok(())
    }
    /// Whether sigint should be hooked to trigger a commit. Some storage types buffer output and
    /// need to be committed to ensure observability of data up to the point of the commit.
    fn commit_on_sigint(&self) -> bool {
        false
    }
}

pub struct Postgres {
    client: Client,
}

#[async_trait]
impl Connection for Postgres {
    async fn new_stream(&mut self, headers_value: SerializedHeaders) -> Result<StreamId> {
        let stmt = self
            .client
            .prepare(
                "INSERT INTO streams (headers, start_datetime) VALUES ($1, NOW()) RETURNING stream_id",
            )
            .await?;
        let stream_id: i32 = self
            .client
            .query_one(&stmt, &[&headers_value])
            .await?
            .get(0);
        Ok(StreamId(stream_id as u32))
    }

    async fn insert_event(
        &mut self,
        stream_id: StreamId,
        stream_event_index: StreamEventIndex,
        payload: &str,
    ) -> Result<()> {
        let payload_value: serde_json::Value = serde_json::from_str(payload)?;
        let stmt = self
            .client
            .prepare(
                "INSERT INTO events (insert_datetime, stream_event_index, payload, stream_id) VALUES (NOW(), $1, $2, $3)",
            )
            .await?;
        self.client
            .execute(
                &stmt,
                &[
                    &(stream_event_index as i32),
                    &payload_value,
                    &(stream_id.0 as i32),
                ],
            )
            .await?;
        Ok(())
    }
}

struct JsonFileWriter {
    w: Option<zstd::Encoder<'static, NamedTempFile>>,
    table: String,
}

impl JsonFileWriter {
    fn take(&mut self) -> Self {
        Self {
            w: self.w.take(),
            table: std::mem::take(&mut self.table),
        }
    }
    fn new(table: String) -> Result<Self> {
        Ok(Self { w: None, table })
    }
    /// Flushes the compressed stream but keeps the file open for the next stream.
    fn flush(&mut self) -> Result<()> {
        if let Some(file) = self.finish_stream()? {
            self.w = Some(Self::new_encoder(file)?)
        }
        Ok(())
    }
    fn finish_file(&mut self) -> Result<()> {
        self.finish_stream()?;
        Ok(())
    }
    fn finish_stream(&mut self) -> Result<Option<NamedTempFile>> {
        let Some(w) = self.w.take() else {
            return Ok(None);
        };
        Ok(Some(w.finish()?))
    }
    fn new_encoder(file: NamedTempFile) -> Result<zstd::Encoder<'static, NamedTempFile>> {
        Ok(zstd::Encoder::new(file, 0)?)
    }
    fn open(&mut self) -> Result<()> {
        self.finish_file()?;
        let dir_path = "json_files";
        std::fs::create_dir_all(dir_path)?;
        let temp_file = tempfile::Builder::new()
            .prefix(&format!("{}.file.", self.table))
            .append(true)
            .suffix(".json.zst")
            .keep(true)
            .tempfile_in(dir_path)
            .context("opening temp file")?;
        self.w = Some(Self::new_encoder(temp_file)?);
        Ok(())
    }
    fn write(&mut self) -> Result<impl Write + '_> {
        if self.w.is_none() {
            self.open()?;
        }
        Ok(self.w.as_mut().unwrap())
    }
}

impl Drop for JsonFileWriter {
    fn drop(&mut self) {
        self.finish_file().unwrap();
    }
}

#[async_trait]
impl Connection for rusqlite::Connection {
    async fn new_stream(&mut self, headers_value: SerializedHeaders) -> Result<StreamId> {
        Ok(self.query_row(
            "\
            insert into streams\
                (headers, start_datetime)\
                values (jsonb(?), datetime('now'))\
                returning stream_id",
            rusqlite::params![headers_value],
            |row| row.get(0),
        )?)
    }
    async fn insert_event(
        &mut self,
        stream_id: StreamId,
        _stream_event_index: StreamEventIndex,
        payload: &str,
    ) -> Result<()> {
        self.execute(
            "\
            insert into events (insert_datetime, payload, stream_id) \
            values (datetime('now'), jsonb(?), ?)",
            rusqlite::params![payload, stream_id],
        )?;
        Ok(())
    }
}

pub struct JsonFiles {
    streams: JsonFileWriter,
    events: JsonFileWriter,
}

impl JsonFiles {
    fn take(&mut self) -> Self {
        Self {
            streams: self.streams.take(),
            events: self.events.take(),
        }
    }
}

fn json_datetime_now() -> serde_json::Value {
    json!(Utc::now().to_rfc3339())
}

#[async_trait]
impl Connection for JsonFiles {
    async fn new_stream(&mut self, headers: SerializedHeaders) -> Result<StreamId> {
        let stream_id: StreamId = StreamId(random());
        let start_datetime = Utc::now().to_rfc3339();
        let json_value = json!({
            "stream_id": stream_id.0,
            "start_datetime": start_datetime,
            "headers": headers,
        });
        let mut writer = self.streams.write()?;
        serde_json::to_writer(&mut writer, &json_value)?;
        writer.write_all(b"\n")?;
        Ok(stream_id)
    }

    async fn insert_event(
        &mut self,
        stream_id: StreamId,
        stream_event_index: StreamEventIndex,
        payload: &str,
    ) -> Result<()> {
        let payload_value: serde_json::Value = serde_json::from_str(payload)?;
        let line_json = json!({
            "insert_datetime": json_datetime_now(),
            "stream_id": stream_id.0,
            "stream_event_index": stream_event_index,
            "payload": payload_value,
        });
        let mut writer = self.events.write()?;
        serde_json::to_writer(&mut writer, &line_json)?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.streams.flush()?;
        self.events.flush()?;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        self.streams.finish_file()?;
        self.events.finish_file()?;
        Ok(())
    }

    fn commit_on_sigint(&self) -> bool {
        true
    }
}

impl Drop for JsonFiles {
    fn drop(&mut self) {
        let mut conn = self.take();
        tokio::spawn(async move { log_commit(&mut conn).await.unwrap() });
    }
}
