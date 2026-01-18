use std::time::Instant;

use reqwest::multipart;
use serde_json::json;
use uuid::Uuid;

use crate::{
    common::types::ChatId, errors::PentaractResult,
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

    /// Masks the bot token in URL for safe logging
    fn mask_url(&self, url: &str) -> String {
        // Replace bot token with ***
        if let Some(bot_idx) = url.find("/bot") {
            if let Some(slash_idx) = url[bot_idx + 4..].find('/') {
                let masked = format!(
                    "{}/bot***{}",
                    &url[..bot_idx],
                    &url[bot_idx + 4 + slash_idx..]
                );
                return masked;
            }
        }
        url.to_string()
    }

    pub async fn upload(
        &self,
        file: &[u8],
        chat_id: ChatId,
        storage_id: Uuid,
    ) -> PentaractResult<UploadSchema> {
        let original_chat_id = chat_id;
        let chat_id = {
            // inserting 100 between minus sign and chat id
            // cause telegram devs are complete retards and it works this way only
            //
            // https://stackoverflow.com/a/65965402/12255756

            let n = chat_id.abs().checked_ilog10().unwrap_or(0) + 1;
            chat_id - (100 * ChatId::from(10).pow(n))
        };

        let token = self.scheduler.get_token(storage_id).await?;
        let url = self.build_url("", "sendDocument", token);
        let masked_url = self.mask_url(&url);

        let file_part = multipart::Part::bytes(file.to_vec()).file_name("pentaract_chunk.bin");
        let form = multipart::Form::new()
            .text("chat_id", chat_id.to_string())
            .part("document", file_part);

        let start = Instant::now();
        let response = reqwest::Client::new()
            .post(&url)
            .multipart(form)
            .send()
            .await?;
        let elapsed_ms = start.elapsed().as_millis() as u64;

        let status = response.status();

        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default();
            tracing::error!(
                target: "http_outbound",
                "{}",
                json!({
                    "status": status.as_u16(),
                    "method": "POST",
                    "url": masked_url,
                    "body": {
                        "chat_id": chat_id,
                        "original_chat_id": original_chat_id,
                        "file_size_bytes": file.len(),
                        "storage_id": storage_id.to_string()
                    },
                    "response": error_body,
                    "elapsed_ms": elapsed_ms
                })
            );
            return Err(crate::errors::PentaractError::TelegramAPIError(
                format!("{}: {}", status, error_body)
            ));
        }

        let result = response.json::<UploadBodySchema>().await?;
        
        tracing::info!(
            target: "http_outbound",
            "{}",
            json!({
                "status": status.as_u16(),
                "method": "POST",
                "url": masked_url,
                "body": {
                    "chat_id": chat_id,
                    "original_chat_id": original_chat_id,
                    "file_size_bytes": file.len(),
                    "storage_id": storage_id.to_string()
                },
                "response": {
                    "telegram_file_id": result.result.document.file_id
                },
                "elapsed_ms": elapsed_ms
            })
        );

        Ok(result.result.document)
    }

    pub async fn download(
        &self,
        telegram_file_id: &str,
        storage_id: Uuid,
    ) -> PentaractResult<Vec<u8>> {
        // Step 1: Get file path from Telegram
        let token = self.scheduler.get_token(storage_id).await?;
        let url = self.build_url("", "getFile", token);
        let masked_url = self.mask_url(&url);

        // TODO: add retries with their number taking from env
        let start = Instant::now();
        let response = reqwest::Client::new()
            .get(&url)
            .query(&[("file_id", telegram_file_id)])
            .send()
            .await?;
        let elapsed_ms = start.elapsed().as_millis() as u64;

        let status = response.status();

        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default();
            tracing::error!(
                target: "http_outbound",
                "{}",
                json!({
                    "status": status.as_u16(),
                    "method": "GET",
                    "url": format!("{}?file_id={}", masked_url, telegram_file_id),
                    "body": null,
                    "response": error_body,
                    "elapsed_ms": elapsed_ms
                })
            );
            return Err(crate::errors::PentaractError::TelegramAPIError(
                format!("{}: {}", status, error_body)
            ));
        }

        let body: DownloadBodySchema = response.json().await?;
        
        tracing::info!(
            target: "http_outbound",
            "{}",
            json!({
                "status": status.as_u16(),
                "method": "GET",
                "url": format!("{}?file_id={}", masked_url, telegram_file_id),
                "body": null,
                "response": {
                    "file_path": body.result.file_path,
                    "file_size": body.result.file_size
                },
                "elapsed_ms": elapsed_ms
            })
        );

        // Step 2: Download the file itself
        let token = self.scheduler.get_token(storage_id).await?;
        let url = self.build_url("file/", &body.result.file_path, token);
        let masked_url = self.mask_url(&url);

        let start = Instant::now();
        let response = reqwest::get(&url).await?;
        let elapsed_ms = start.elapsed().as_millis() as u64;

        let status = response.status();
        if !status.is_success() {
            let error_body = response.text().await.unwrap_or_default();
            tracing::error!(
                target: "http_outbound",
                "{}",
                json!({
                    "status": status.as_u16(),
                    "method": "GET",
                    "url": masked_url,
                    "body": null,
                    "response": error_body,
                    "elapsed_ms": elapsed_ms
                })
            );
            return Err(crate::errors::PentaractError::TelegramAPIError(
                format!("{}: {}", status, error_body)
            ));
        }

        let file = response.bytes().await.map(|file| file.to_vec())?;

        tracing::info!(
            target: "http_outbound",
            "{}",
            json!({
                "status": status.as_u16(),
                "method": "GET",
                "url": masked_url,
                "body": null,
                "response": {
                    "downloaded_bytes": file.len()
                },
                "elapsed_ms": elapsed_ms
            })
        );

        Ok(file)
    }

    /// Taking token by a value to force dropping it so it can be used only once
    #[inline]
    fn build_url(&self, pre: &str, relative: &str, token: String) -> String {
        format!("{}/{pre}bot{token}/{relative}", self.base_url)
    }
}
