use std::{path::Path, pin::Pin};

use futures::{Stream, StreamExt};
use reqwest::multipart;
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncSeekExt, SeekFrom};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::{
    common::types::ChatId,
    errors::{PentaractError, PentaractResult},
    services::storage_workers_scheduler::StorageWorkersScheduler,
};

use super::schemas::{DownloadBodySchema, UploadBodySchema, UploadSchema};

pub struct TelegramBotApi<'t> {
    base_url: &'t str,
    scheduler: StorageWorkersScheduler<'t>,
}

impl<'t> TelegramBotApi<'t> {
    pub fn new(base_url: &'t str, scheduler: StorageWorkersScheduler<'t>) -> Self {
        Self {
            base_url,
            scheduler,
        }
    }

    pub async fn upload(
        &self,
        file: &[u8],
        chat_id: ChatId,
        storage_id: Uuid,
    ) -> PentaractResult<UploadSchema> {
        tracing::debug!(
            "[TELEGRAM API] Uploading chunk: chat_id={}, file_size={}",
            chat_id,
            file.len()
        );

        if chat_id < 0 && chat_id > -10000000000 {
            tracing::info!(
                "[TELEGRAM API] Using regular group (chat_id={}). If bot can't find the chat, \
                make sure the bot is added and has permissions.",
                chat_id
            );
        }

        let token = self.scheduler.get_token(storage_id).await?;
        let url = self.build_url("", "sendDocument", token);

        let file_part = multipart::Part::bytes(file.to_vec()).file_name("pentaract_chunk.bin");
        let form = multipart::Form::new()
            .text("chat_id", chat_id.to_string())
            .part("document", file_part);

        let response = reqwest::Client::new()
            .post(url)
            .multipart(form)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unable to read error body".to_string());
            tracing::error!(
                "[TELEGRAM API] Upload failed: status={}, response={}",
                status,
                error_text
            );
            return Err(PentaractError::TelegramAPIError(format!(
                "Status {}: {}",
                status,
                error_text
            )));
        }

        match response.json::<UploadBodySchema>().await {
            Ok(body) => Ok(body.result.document),
            Err(e) => {
                tracing::error!("[TELEGRAM API] Failed to parse response: {}", e);
                Err(e.into())
            }
        }
    }

    /// Upload a part of a file from disk without buffering it fully in RAM.
    ///
    /// `offset` and `len` define the slice of the file to upload.
    pub async fn upload_file_part(
        &self,
        file_path: &Path,
        offset: u64,
        len: u64,
        chat_id: ChatId,
        storage_id: Uuid,
    ) -> PentaractResult<UploadSchema> {
        tracing::debug!(
            "[TELEGRAM API] Uploading chunk from disk: path={:?}, offset={}, len={}, chat_id={}",
            file_path,
            offset,
            len,
            chat_id
        );

        let token = self.scheduler.get_token(storage_id).await?;
        let url = self.build_url("", "sendDocument", token);

        let mut file = tokio::fs::File::open(file_path).await.map_err(|_| PentaractError::Unknown)?;
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|_| PentaractError::Unknown)?;
        let reader = file.take(len);
        let stream = ReaderStream::new(reader);
        let body = reqwest::Body::wrap_stream(stream);

        let part = multipart::Part::stream_with_length(body, len)
            .file_name("pentaract_chunk.bin");

        let form = multipart::Form::new()
            .text("chat_id", chat_id.to_string())
            .part("document", part);

        let response = reqwest::Client::new()
            .post(url)
            .multipart(form)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unable to read error body".to_string());
            tracing::error!(
                "[TELEGRAM API] Upload failed: status={}, response={}",
                status,
                error_text
            );
            return Err(PentaractError::TelegramAPIError(format!(
                "Status {}: {}",
                status, error_text
            )));
        }

        response
            .json::<UploadBodySchema>()
            .await
            .map(|body| body.result.document)
            .map_err(|e| {
                tracing::error!("[TELEGRAM API] Failed to parse response: {}", e);
                e.into()
            })
    }

    pub async fn download(
        &self,
        telegram_file_id: &str,
        storage_id: Uuid,
    ) -> PentaractResult<Vec<u8>> {
        // getting file path
        let token = self.scheduler.get_token(storage_id).await?;
        let url = self.build_url("", "getFile", token);
        // TODO: add retries with their number taking from env
        let body: DownloadBodySchema = reqwest::Client::new()
            .get(url)
            .query(&[("file_id", telegram_file_id)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        // Local Bot API returns an absolute filesystem path.
        if body.result.file_path.starts_with('/') {
            // Security: only allow reading from the expected local-bot-api directory.
            if !body
                .result
                .file_path
                .starts_with("/var/lib/telegram-bot-api/")
            {
                return Err(PentaractError::TelegramAPIError(
                    "Unexpected local file_path from telegram-bot-api".to_string(),
                ));
            }

            let bytes = tokio::fs::read(&body.result.file_path).await.map_err(|e| {
                PentaractError::TelegramAPIError(format!(
                    "Failed to read local bot api file: {}",
                    e
                ))
            })?;
            return Ok(bytes);
        }

        // downloading the file itself
        let token = self.scheduler.get_token(storage_id).await?;
        let url = self.build_url("file/", &body.result.file_path, token);
        let file = reqwest::Client::new()
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await
            .map(|file| file.to_vec())?;

        Ok(file)
    }

    /// Download file bytes as a stream (does not buffer whole chunk in RAM).
    pub async fn download_stream(
        &self,
        telegram_file_id: &str,
        storage_id: Uuid,
    ) -> PentaractResult<Pin<Box<dyn Stream<Item = Result<tokio_util::bytes::Bytes, PentaractError>> + Send>>> {
        // getting file path
        let token = self.scheduler.get_token(storage_id).await?;
        let url = self.build_url("", "getFile", token);

        let body: DownloadBodySchema = reqwest::Client::new()
            .get(url)
            .query(&[("file_id", telegram_file_id)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        // Local Bot API returns an absolute filesystem path.
        if body.result.file_path.starts_with('/') {
            if !body
                .result
                .file_path
                .starts_with("/var/lib/telegram-bot-api/")
            {
                return Err(PentaractError::TelegramAPIError(
                    "Unexpected local file_path from telegram-bot-api".to_string(),
                ));
            }

            let file = tokio::fs::File::open(&body.result.file_path).await.map_err(|e| {
                PentaractError::TelegramAPIError(format!(
                    "Failed to open local bot api file: {}",
                    e
                ))
            })?;
            let stream = ReaderStream::new(file).map(|res| {
                res.map_err(|e| {
                    PentaractError::TelegramAPIError(format!(
                        "Failed to read local bot api file: {}",
                        e
                    ))
                })
            });
            return Ok(Box::pin(stream));
        }

        // downloading the file itself
        let token = self.scheduler.get_token(storage_id).await?;
        let url = self.build_url("file/", &body.result.file_path, token);

        let response = reqwest::Client::new()
            .get(url)
            .send()
            .await?
            .error_for_status()?;

        let stream = response
            .bytes_stream()
            .map(|res| res.map_err(PentaractError::from));

        Ok(Box::pin(stream))
    }

    /// Taking token by a value to force dropping it so it can be used only once
    #[inline]
    fn build_url(&self, pre: &str, relative: &str, token: String) -> String {
        format!("{}/{pre}bot{token}/{relative}", self.base_url)
    }
}
