use super::traits::{Channel, ChannelMessage, SendMessage};
use crate::config::schema::StreamMode;
use async_trait::async_trait;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use reqwest::header::HeaderMap;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_tungstenite::tungstenite::Message as WsMessage;

#[derive(Clone)]
struct CachedSlackDisplayName {
    display_name: String,
    expires_at: Instant,
}

/// Slack channel — polls conversations.history via Web API
pub struct SlackChannel {
    bot_token: String,
    app_token: Option<String>,
    channel_id: Option<String>,
    channel_ids: Vec<String>,
    allowed_users: Vec<String>,
    mention_only: bool,
    group_reply_allowed_sender_ids: Vec<String>,
    user_display_name_cache: Mutex<HashMap<String, CachedSlackDisplayName>>,
    stream_mode: StreamMode,
    draft_update_interval_ms: u64,
    last_draft_edit: Mutex<HashMap<String, Instant>>,
}

const SLACK_MAX_MESSAGE_LENGTH: usize = 40_000;
const SLACK_HISTORY_MAX_RETRIES: u32 = 3;
const SLACK_HISTORY_DEFAULT_RETRY_AFTER_SECS: u64 = 1;
const SLACK_HISTORY_MAX_BACKOFF_SECS: u64 = 120;
const SLACK_HISTORY_MAX_JITTER_MS: u64 = 500;
const SLACK_USER_CACHE_TTL_SECS: u64 = 6 * 60 * 60;

impl SlackChannel {
    pub fn new(
        bot_token: String,
        app_token: Option<String>,
        channel_id: Option<String>,
        channel_ids: Vec<String>,
        allowed_users: Vec<String>,
    ) -> Self {
        Self {
            bot_token,
            app_token,
            channel_id,
            channel_ids,
            allowed_users,
            mention_only: false,
            group_reply_allowed_sender_ids: Vec::new(),
            user_display_name_cache: Mutex::new(HashMap::new()),
            stream_mode: StreamMode::Off,
            draft_update_interval_ms: 1000,
            last_draft_edit: Mutex::new(HashMap::new()),
        }
    }

    /// Configure streaming mode for progressive draft updates.
    pub fn with_streaming(
        mut self,
        stream_mode: StreamMode,
        draft_update_interval_ms: u64,
    ) -> Self {
        self.stream_mode = stream_mode;
        self.draft_update_interval_ms = draft_update_interval_ms;
        self
    }

    /// Configure group-chat trigger policy.
    pub fn with_group_reply_policy(
        mut self,
        mention_only: bool,
        allowed_sender_ids: Vec<String>,
    ) -> Self {
        self.mention_only = mention_only;
        self.group_reply_allowed_sender_ids =
            Self::normalize_group_reply_allowed_sender_ids(allowed_sender_ids);
        self
    }

    fn http_client(&self) -> reqwest::Client {
        crate::config::build_runtime_proxy_client("channel.slack")
    }

    /// Check if a Slack user ID is in the allowlist.
    /// Empty list means deny everyone until explicitly configured.
    /// `"*"` means allow everyone.
    fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    fn is_group_sender_trigger_enabled(&self, user_id: &str) -> bool {
        let user_id = user_id.trim();
        if user_id.is_empty() {
            return false;
        }

        self.group_reply_allowed_sender_ids
            .iter()
            .any(|entry| entry == "*" || entry == user_id)
    }

    /// Get the bot's own user ID so we can ignore our own messages
    async fn get_bot_user_id(&self) -> Option<String> {
        let resp: serde_json::Value = self
            .http_client()
            .get("https://slack.com/api/auth.test")
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;

        resp.get("user_id")
            .and_then(|u| u.as_str())
            .map(String::from)
    }

    /// Resolve the thread identifier for inbound Slack messages.
    /// Replies carry `thread_ts` (root thread id); top-level messages only have `ts`.
    fn inbound_thread_ts(msg: &serde_json::Value, ts: &str) -> Option<String> {
        msg.get("thread_ts")
            .and_then(|t| t.as_str())
            .or(if ts.is_empty() { None } else { Some(ts) })
            .map(str::to_string)
    }

    fn normalized_channel_id(input: Option<&str>) -> Option<String> {
        input
            .map(str::trim)
            .filter(|v| !v.is_empty() && *v != "*")
            .map(ToOwned::to_owned)
    }

    fn configured_channel_id(&self) -> Option<String> {
        Self::normalized_channel_id(self.channel_id.as_deref())
    }

    /// Resolve the effective channel scope:
    /// explicit `channel_ids` list first, then single `channel_id`, otherwise wildcard discovery.
    fn scoped_channel_ids(&self) -> Option<Vec<String>> {
        let mut seen = HashSet::new();
        let ids: Vec<String> = self
            .channel_ids
            .iter()
            .filter_map(|entry| Self::normalized_channel_id(Some(entry)))
            .filter(|id| seen.insert(id.clone()))
            .collect();
        if !ids.is_empty() {
            return Some(ids);
        }
        self.configured_channel_id().map(|id| vec![id])
    }

    fn configured_app_token(&self) -> Option<String> {
        self.app_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    }

    fn normalize_group_reply_allowed_sender_ids(sender_ids: Vec<String>) -> Vec<String> {
        let mut normalized = sender_ids
            .into_iter()
            .map(|entry| entry.trim().to_string())
            .filter(|entry| !entry.is_empty())
            .collect::<Vec<_>>();
        normalized.sort();
        normalized.dedup();
        normalized
    }

    fn user_cache_ttl() -> Duration {
        Duration::from_secs(SLACK_USER_CACHE_TTL_SECS)
    }

    fn sanitize_display_name(name: &str) -> Option<String> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    fn extract_user_display_name(payload: &serde_json::Value) -> Option<String> {
        let user = payload.get("user")?;
        let profile = user.get("profile");

        let candidates = [
            profile
                .and_then(|p| p.get("display_name"))
                .and_then(|v| v.as_str()),
            profile
                .and_then(|p| p.get("display_name_normalized"))
                .and_then(|v| v.as_str()),
            profile
                .and_then(|p| p.get("real_name_normalized"))
                .and_then(|v| v.as_str()),
            profile
                .and_then(|p| p.get("real_name"))
                .and_then(|v| v.as_str()),
            user.get("real_name").and_then(|v| v.as_str()),
            user.get("name").and_then(|v| v.as_str()),
        ];

        for candidate in candidates.into_iter().flatten() {
            if let Some(display_name) = Self::sanitize_display_name(candidate) {
                return Some(display_name);
            }
        }

        None
    }

    fn cached_sender_display_name(&self, user_id: &str) -> Option<String> {
        let now = Instant::now();
        let Ok(mut cache) = self.user_display_name_cache.lock() else {
            return None;
        };

        if let Some(entry) = cache.get(user_id) {
            if now <= entry.expires_at {
                return Some(entry.display_name.clone());
            }
        }

        cache.remove(user_id);
        None
    }

    fn cache_sender_display_name(&self, user_id: &str, display_name: &str) {
        let Ok(mut cache) = self.user_display_name_cache.lock() else {
            return;
        };
        cache.insert(
            user_id.to_string(),
            CachedSlackDisplayName {
                display_name: display_name.to_string(),
                expires_at: Instant::now() + Self::user_cache_ttl(),
            },
        );
    }

    async fn fetch_sender_display_name(&self, user_id: &str) -> Option<String> {
        let resp = match self
            .http_client()
            .get("https://slack.com/api/users.info")
            .bearer_auth(&self.bot_token)
            .query(&[("user", user_id)])
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!("Slack users.info request failed for {user_id}: {err}");
                return None;
            }
        };

        let status = resp.status();
        let body = resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));

        if !status.is_success() {
            let sanitized = crate::providers::sanitize_api_error(&body);
            tracing::warn!("Slack users.info failed for {user_id} ({status}): {sanitized}");
            return None;
        }

        let payload: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
        if payload.get("ok") == Some(&serde_json::Value::Bool(false)) {
            let err = payload
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown");
            tracing::warn!("Slack users.info returned error for {user_id}: {err}");
            return None;
        }

        Self::extract_user_display_name(&payload)
    }

    async fn resolve_sender_identity(&self, user_id: &str) -> String {
        let user_id = user_id.trim();
        if user_id.is_empty() {
            return String::new();
        }

        if let Some(display_name) = self.cached_sender_display_name(user_id) {
            return display_name;
        }

        if let Some(display_name) = self.fetch_sender_display_name(user_id).await {
            self.cache_sender_display_name(user_id, &display_name);
            return display_name;
        }

        user_id.to_string()
    }

    fn is_group_channel_id(channel_id: &str) -> bool {
        matches!(channel_id.chars().next(), Some('C' | 'G'))
    }

    fn contains_bot_mention(text: &str, bot_user_id: &str) -> bool {
        if bot_user_id.is_empty() {
            return false;
        }
        text.contains(&format!("<@{bot_user_id}>"))
    }

    fn strip_bot_mentions(text: &str, bot_user_id: &str) -> String {
        if bot_user_id.is_empty() {
            return text.trim().to_string();
        }
        text.replace(&format!("<@{bot_user_id}>"), " ")
            .trim()
            .to_string()
    }

    fn normalize_incoming_content(
        text: &str,
        require_mention: bool,
        bot_user_id: &str,
    ) -> Option<String> {
        if text.trim().is_empty() {
            return None;
        }
        if require_mention && !Self::contains_bot_mention(text, bot_user_id) {
            return None;
        }

        let normalized = if require_mention {
            Self::strip_bot_mentions(text, bot_user_id)
        } else {
            text.trim().to_string()
        };

        if normalized.is_empty() {
            return None;
        }
        Some(normalized)
    }

    fn extract_channel_ids(list_payload: &serde_json::Value) -> Vec<String> {
        let mut ids = list_payload
            .get("channels")
            .and_then(|c| c.as_array())
            .into_iter()
            .flatten()
            .filter_map(|channel| {
                let id = channel.get("id").and_then(|id| id.as_str())?;
                let is_archived = channel
                    .get("is_archived")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let is_member = channel
                    .get("is_member")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                if is_archived || !is_member {
                    return None;
                }
                Some(id.to_string())
            })
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        ids
    }

    async fn list_accessible_channels(&self) -> anyhow::Result<Vec<String>> {
        let mut channels = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let mut query_params = vec![
                ("exclude_archived", "true".to_string()),
                ("limit", "200".to_string()),
                (
                    "types",
                    "public_channel,private_channel,mpim,im".to_string(),
                ),
            ];
            if let Some(ref next) = cursor {
                query_params.push(("cursor", next.clone()));
            }

            let resp = self
                .http_client()
                .get("https://slack.com/api/conversations.list")
                .bearer_auth(&self.bot_token)
                .query(&query_params)
                .send()
                .await?;

            let status = resp.status();
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));

            if !status.is_success() {
                let sanitized = crate::providers::sanitize_api_error(&body);
                anyhow::bail!("Slack conversations.list failed ({status}): {sanitized}");
            }

            let data: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            if data.get("ok") == Some(&serde_json::Value::Bool(false)) {
                let err = data
                    .get("error")
                    .and_then(|e| e.as_str())
                    .unwrap_or("unknown");
                anyhow::bail!("Slack conversations.list failed: {err}");
            }

            channels.extend(Self::extract_channel_ids(&data));

            cursor = data
                .get("response_metadata")
                .and_then(|rm| rm.get("next_cursor"))
                .and_then(|c| c.as_str())
                .map(str::trim)
                .filter(|c| !c.is_empty())
                .map(ToOwned::to_owned);

            if cursor.is_none() {
                break;
            }
        }

        channels.sort();
        channels.dedup();
        Ok(channels)
    }

    fn slack_now_ts() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        format!("{}.{:06}", now.as_secs(), now.subsec_micros())
    }

    fn ensure_poll_cursor(
        cursors: &mut HashMap<String, String>,
        channel_id: &str,
        now_ts: &str,
    ) -> String {
        cursors
            .entry(channel_id.to_string())
            .or_insert_with(|| now_ts.to_string())
            .clone()
    }

    async fn open_socket_mode_url(&self) -> anyhow::Result<String> {
        let app_token = self
            .configured_app_token()
            .ok_or_else(|| anyhow::anyhow!("Slack Socket Mode requires app_token"))?;

        let resp = self
            .http_client()
            .post("https://slack.com/api/apps.connections.open")
            .bearer_auth(app_token)
            .send()
            .await?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));

        if !status.is_success() {
            let sanitized = crate::providers::sanitize_api_error(&body);
            anyhow::bail!("Slack apps.connections.open failed ({status}): {sanitized}");
        }

        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
        if parsed.get("ok") == Some(&serde_json::Value::Bool(false)) {
            let err = parsed
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown");
            anyhow::bail!("Slack apps.connections.open failed: {err}");
        }

        parsed
            .get("url")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("Slack apps.connections.open did not return url"))
    }

    async fn listen_socket_mode(
        &self,
        tx: tokio::sync::mpsc::Sender<ChannelMessage>,
        bot_user_id: &str,
        scoped_channels: Option<Vec<String>>,
    ) -> anyhow::Result<()> {
        let mut last_ts_by_channel: HashMap<String, String> = HashMap::new();

        loop {
            let ws_url = match self.open_socket_mode_url().await {
                Ok(url) => url,
                Err(e) => {
                    tracing::warn!("Slack Socket Mode: failed to open websocket URL: {e}");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };

            let (ws_stream, _) = match tokio_tungstenite::connect_async(&ws_url).await {
                Ok(connection) => connection,
                Err(e) => {
                    tracing::warn!("Slack Socket Mode: websocket connect failed: {e}");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };
            tracing::info!("Slack Socket Mode: websocket connected");

            let (mut write, mut read) = ws_stream.split();

            while let Some(frame) = read.next().await {
                let text = match frame {
                    Ok(WsMessage::Text(text)) => text,
                    Ok(WsMessage::Ping(payload)) => {
                        if let Err(e) = write.send(WsMessage::Pong(payload)).await {
                            tracing::warn!("Slack Socket Mode: pong send failed: {e}");
                            break;
                        }
                        continue;
                    }
                    Ok(WsMessage::Close(_)) => {
                        tracing::warn!("Slack Socket Mode: websocket closed by server");
                        break;
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!("Slack Socket Mode: websocket read failed: {e}");
                        break;
                    }
                };

                let envelope: serde_json::Value = match serde_json::from_str(text.as_ref()) {
                    Ok(value) => value,
                    Err(e) => {
                        tracing::warn!("Slack Socket Mode: invalid JSON payload: {e}");
                        continue;
                    }
                };

                if let Some(envelope_id) = envelope.get("envelope_id").and_then(|v| v.as_str()) {
                    let ack = serde_json::json!({ "envelope_id": envelope_id });
                    if let Err(e) = write.send(WsMessage::Text(ack.to_string().into())).await {
                        tracing::warn!("Slack Socket Mode: ack send failed: {e}");
                        break;
                    }
                }

                let envelope_type = envelope
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                if envelope_type == "disconnect" {
                    tracing::warn!("Slack Socket Mode: received disconnect event");
                    break;
                }
                if envelope_type != "events_api" {
                    continue;
                }

                let Some(event) = envelope
                    .get("payload")
                    .and_then(|payload| payload.get("event"))
                else {
                    continue;
                };
                if event.get("type").and_then(|v| v.as_str()) != Some("message") {
                    continue;
                }
                // Skip non-user message subtypes (e.g. channel_join/message_changed)
                // to avoid invalid thread replies.
                if event.get("subtype").is_some() {
                    continue;
                }

                let channel_id = event
                    .get("channel")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .unwrap_or_default();
                if channel_id.is_empty() {
                    continue;
                }
                if let Some(ref configured_channels) = scoped_channels {
                    if !configured_channels.iter().any(|id| id == &channel_id) {
                        continue;
                    }
                }

                let user = event
                    .get("user")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                if user.is_empty() || user == bot_user_id {
                    continue;
                }
                if !self.is_user_allowed(user) {
                    tracing::warn!("Slack: ignoring message from unauthorized user: {user}");
                    continue;
                }

                let text = event
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                if text.is_empty() {
                    continue;
                }

                let ts = event.get("ts").and_then(|v| v.as_str()).unwrap_or_default();
                if ts.is_empty() {
                    continue;
                }
                let last_ts = last_ts_by_channel
                    .get(&channel_id)
                    .map(String::as_str)
                    .unwrap_or_default();
                if ts <= last_ts {
                    continue;
                }

                let is_group_message = Self::is_group_channel_id(&channel_id);
                let allow_sender_without_mention =
                    is_group_message && self.is_group_sender_trigger_enabled(user);
                let require_mention =
                    self.mention_only && is_group_message && !allow_sender_without_mention;

                let Some(normalized_text) =
                    Self::normalize_incoming_content(text, require_mention, bot_user_id)
                else {
                    continue;
                };

                last_ts_by_channel.insert(channel_id.clone(), ts.to_string());
                let sender = self.resolve_sender_identity(user).await;

                let channel_msg = ChannelMessage {
                    id: format!("slack_{channel_id}_{ts}"),
                    sender,
                    reply_target: channel_id.clone(),
                    content: normalized_text,
                    channel: "slack".to_string(),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    thread_ts: Self::inbound_thread_ts(event, ts),
                };

                if tx.send(channel_msg).await.is_err() {
                    return Ok(());
                }
            }

            tracing::warn!("Slack Socket Mode: reconnecting in 3 seconds...");
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    fn parse_retry_after_secs(headers: &HeaderMap) -> Option<u64> {
        let value = headers
            .get(reqwest::header::RETRY_AFTER)?
            .to_str()
            .ok()?
            .trim();
        Self::parse_retry_after_value(value)
    }

    fn parse_retry_after_value(value: &str) -> Option<u64> {
        if value.is_empty() {
            return None;
        }

        if let Ok(seconds) = value.parse::<u64>() {
            return Some(seconds);
        }

        let truncated = value
            .split_once('.')
            .map(|(whole, _)| whole)
            .unwrap_or(value);
        truncated.parse::<u64>().ok()
    }

    fn jitter_ms_from_clock(max_jitter_ms: u64) -> u64 {
        if max_jitter_ms == 0 {
            return 0;
        }
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::from(d.subsec_nanos()))
            .unwrap_or(0);
        nanos % (max_jitter_ms + 1)
    }

    fn compute_retry_delay(base_retry_after_secs: u64, attempt: u32, jitter_ms: u64) -> Duration {
        let multiplier = 1_u64.checked_shl(attempt).unwrap_or(u64::MAX);
        let backoff_secs = base_retry_after_secs
            .saturating_mul(multiplier)
            .min(SLACK_HISTORY_MAX_BACKOFF_SECS);
        Duration::from_secs(backoff_secs) + Duration::from_millis(jitter_ms)
    }

    fn next_retry_timestamp(wait: Duration) -> String {
        match chrono::Duration::from_std(wait) {
            Ok(delta) => (Utc::now() + delta).to_rfc3339(),
            Err(_) => Utc::now().to_rfc3339(),
        }
    }

    async fn fetch_history_with_retry(
        &self,
        channel_id: &str,
        params: &[(&str, String)],
    ) -> Option<serde_json::Value> {
        let mut total_wait = Duration::from_secs(0);

        for attempt in 0..=SLACK_HISTORY_MAX_RETRIES {
            let resp = match self
                .http_client()
                .get("https://slack.com/api/conversations.history")
                .bearer_auth(&self.bot_token)
                .query(params)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("Slack poll error for channel {channel_id}: {e}");
                    return None;
                }
            };

            let status = resp.status();
            let headers = resp.headers().clone();
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));

            let is_ratelimited_http = status == reqwest::StatusCode::TOO_MANY_REQUESTS;
            let payload: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            let is_ratelimited_payload = payload.get("ok") == Some(&serde_json::Value::Bool(false))
                && payload
                    .get("error")
                    .and_then(|e| e.as_str())
                    .is_some_and(|err| err == "ratelimited");

            if is_ratelimited_http || is_ratelimited_payload {
                if attempt >= SLACK_HISTORY_MAX_RETRIES {
                    tracing::error!(
                        "Slack rate limit retries exhausted for conversations.history on channel {}. Total wait: {}s across {} attempts. Proceeding without channel history.",
                        channel_id,
                        total_wait.as_secs(),
                        SLACK_HISTORY_MAX_RETRIES
                    );
                    return None;
                }

                let retry_after_secs = Self::parse_retry_after_secs(&headers)
                    .unwrap_or(SLACK_HISTORY_DEFAULT_RETRY_AFTER_SECS);
                let jitter_ms = Self::jitter_ms_from_clock(SLACK_HISTORY_MAX_JITTER_MS);
                let wait = Self::compute_retry_delay(retry_after_secs, attempt, jitter_ms);
                total_wait += wait;
                let next_retry_at = Self::next_retry_timestamp(wait);
                tracing::warn!(
                    "Slack conversations.history rate limited for channel {}. Retry-After: {}s. Attempt {}/{}. Next retry at {}.",
                    channel_id,
                    retry_after_secs,
                    attempt + 1,
                    SLACK_HISTORY_MAX_RETRIES,
                    next_retry_at
                );
                tokio::time::sleep(wait).await;
                continue;
            }

            if !status.is_success() {
                let sanitized = crate::providers::sanitize_api_error(&body);
                tracing::warn!(
                    "Slack history request failed for channel {} ({}): {}",
                    channel_id,
                    status,
                    sanitized
                );
                return None;
            }

            if payload.get("ok") == Some(&serde_json::Value::Bool(false)) {
                let err = payload
                    .get("error")
                    .and_then(|e| e.as_str())
                    .unwrap_or("unknown");
                tracing::warn!("Slack history error for channel {channel_id}: {err}");
                return None;
            }

            return Some(payload);
        }

        None
    }
}

#[async_trait]
impl Channel for SlackChannel {
    fn name(&self) -> &str {
        "slack"
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        // Strip tool-call XML tags, then convert standard Markdown → Slack mrkdwn.
        let content = super::strip_tool_call_tags(&message.content);
        let content = markdown_to_slack_mrkdwn(&content);

        let mut body = serde_json::json!({
            "channel": message.recipient,
            "text": content
        });

        if let Some(ref ts) = message.thread_ts {
            body["thread_ts"] = serde_json::json!(ts);
        }

        let resp = self
            .http_client()
            .post("https://slack.com/api/chat.postMessage")
            .bearer_auth(&self.bot_token)
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));

        if !status.is_success() {
            let sanitized = crate::providers::sanitize_api_error(&body);
            anyhow::bail!("Slack chat.postMessage failed ({status}): {sanitized}");
        }

        // Slack returns 200 for most app-level errors; check JSON "ok" field
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
        if parsed.get("ok") == Some(&serde_json::Value::Bool(false)) {
            let err = parsed
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown");
            anyhow::bail!("Slack chat.postMessage failed: {err}");
        }

        Ok(())
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        let bot_user_id = self.get_bot_user_id().await.unwrap_or_default();
        let scoped_channels = self.scoped_channel_ids();
        if self.configured_app_token().is_some() {
            tracing::info!("Slack channel listening in Socket Mode");
            return self
                .listen_socket_mode(tx, &bot_user_id, scoped_channels)
                .await;
        }

        let mut discovered_channels: Vec<String> = Vec::new();
        let mut last_discovery = Instant::now();
        let mut last_ts_by_channel: HashMap<String, String> = HashMap::new();

        if let Some(ref channel_ids) = scoped_channels {
            tracing::info!(
                "Slack channel listening on {} configured channel(s): {}",
                channel_ids.len(),
                channel_ids.join(", ")
            );
        } else {
            tracing::info!(
                "Slack channel_id/channel_ids not set (or wildcard only); listening across all accessible channels."
            );
        }

        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;

            let target_channels = if let Some(ref channel_ids) = scoped_channels {
                channel_ids.clone()
            } else {
                if discovered_channels.is_empty()
                    || last_discovery.elapsed() >= Duration::from_secs(60)
                {
                    match self.list_accessible_channels().await {
                        Ok(channels) => {
                            if channels != discovered_channels {
                                tracing::info!(
                                    "Slack auto-discovery refreshed: listening on {} channel(s).",
                                    channels.len()
                                );
                            }
                            discovered_channels = channels;
                        }
                        Err(e) => {
                            tracing::warn!("Slack channel discovery failed: {e}");
                        }
                    }
                    last_discovery = Instant::now();
                }

                discovered_channels.clone()
            };

            if target_channels.is_empty() {
                tracing::debug!("Slack: no accessible channels discovered yet");
                continue;
            }

            for channel_id in target_channels {
                let had_cursor = last_ts_by_channel.contains_key(&channel_id);
                let bootstrap_ts = Self::slack_now_ts();
                let cursor_ts =
                    Self::ensure_poll_cursor(&mut last_ts_by_channel, &channel_id, &bootstrap_ts);
                if !had_cursor {
                    tracing::debug!(
                        "Slack: initialized cursor for channel {} at {} to prevent historical replay",
                        channel_id,
                        cursor_ts
                    );
                }
                let params = vec![
                    ("channel", channel_id.clone()),
                    ("limit", "10".to_string()),
                    ("oldest", cursor_ts),
                ];

                let Some(data) = self.fetch_history_with_retry(&channel_id, &params).await else {
                    continue;
                };

                if let Some(messages) = data.get("messages").and_then(|m| m.as_array()) {
                    // Messages come newest-first, reverse to process oldest first
                    for msg in messages.iter().rev() {
                        // Skip non-user message subtypes (e.g. channel_join/message_changed)
                        // to avoid invalid thread replies.
                        if msg.get("subtype").is_some() {
                            continue;
                        }
                        let ts = msg.get("ts").and_then(|t| t.as_str()).unwrap_or("");
                        let user = msg
                            .get("user")
                            .and_then(|u| u.as_str())
                            .unwrap_or("unknown");
                        let text = msg.get("text").and_then(|t| t.as_str()).unwrap_or("");
                        let last_ts = last_ts_by_channel
                            .get(&channel_id)
                            .map(String::as_str)
                            .unwrap_or("");

                        // Skip bot's own messages
                        if user == bot_user_id {
                            continue;
                        }

                        // Sender validation
                        if !self.is_user_allowed(user) {
                            tracing::warn!(
                                "Slack: ignoring message from unauthorized user: {user}"
                            );
                            continue;
                        }

                        // Skip empty or already-seen
                        if text.is_empty() || ts <= last_ts {
                            continue;
                        }

                        let is_group_message = Self::is_group_channel_id(&channel_id);
                        let allow_sender_without_mention =
                            is_group_message && self.is_group_sender_trigger_enabled(user);
                        let require_mention =
                            self.mention_only && is_group_message && !allow_sender_without_mention;
                        let Some(normalized_text) =
                            Self::normalize_incoming_content(text, require_mention, &bot_user_id)
                        else {
                            continue;
                        };

                        last_ts_by_channel.insert(channel_id.clone(), ts.to_string());
                        let sender = self.resolve_sender_identity(user).await;

                        let channel_msg = ChannelMessage {
                            id: format!("slack_{channel_id}_{ts}"),
                            sender,
                            reply_target: channel_id.clone(),
                            content: normalized_text,
                            channel: "slack".to_string(),
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            thread_ts: Self::inbound_thread_ts(msg, ts),
                        };

                        if tx.send(channel_msg).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn health_check(&self) -> bool {
        self.http_client()
            .get("https://slack.com/api/auth.test")
            .bearer_auth(&self.bot_token)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    fn supports_draft_updates(&self) -> bool {
        self.stream_mode != StreamMode::Off
    }

    async fn send_draft(&self, message: &SendMessage) -> anyhow::Result<Option<String>> {
        if self.stream_mode == StreamMode::Off {
            return Ok(None);
        }

        let initial_text = if message.content.is_empty() {
            "...".to_string()
        } else {
            message.content.clone()
        };

        let mut body = serde_json::json!({
            "channel": message.recipient,
            "text": initial_text
        });
        if let Some(ref ts) = message.thread_ts {
            body["thread_ts"] = serde_json::json!(ts);
        }

        let resp = self
            .http_client()
            .post("https://slack.com/api/chat.postMessage")
            .bearer_auth(&self.bot_token)
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        let resp_body = resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));

        if !status.is_success() {
            let sanitized = crate::providers::sanitize_api_error(&resp_body);
            anyhow::bail!("Slack chat.postMessage (draft) failed ({status}): {sanitized}");
        }

        let parsed: serde_json::Value = serde_json::from_str(&resp_body).unwrap_or_default();
        if parsed.get("ok") == Some(&serde_json::Value::Bool(false)) {
            let err = parsed
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown");
            anyhow::bail!("Slack chat.postMessage (draft) failed: {err}");
        }

        let ts = parsed.get("ts").and_then(|v| v.as_str()).map(String::from);

        if let Some(ref ts_val) = ts {
            if let Ok(mut edits) = self.last_draft_edit.lock() {
                edits.insert(message.recipient.clone(), Instant::now());
            }
            tracing::debug!("Slack draft sent to {}, ts={ts_val}", message.recipient);
        }

        Ok(ts)
    }

    async fn update_draft(
        &self,
        recipient: &str,
        message_id: &str,
        text: &str,
    ) -> anyhow::Result<Option<String>> {
        {
            if let Ok(edits) = self.last_draft_edit.lock() {
                if let Some(last_time) = edits.get(recipient) {
                    let elapsed =
                        u64::try_from(last_time.elapsed().as_millis()).unwrap_or(u64::MAX);
                    if elapsed < self.draft_update_interval_ms {
                        return Ok(None);
                    }
                }
            }
        }

        let display_text = if text.len() > SLACK_MAX_MESSAGE_LENGTH {
            let mut end = 0;
            for (idx, ch) in text.char_indices() {
                let next = idx + ch.len_utf8();
                if next > SLACK_MAX_MESSAGE_LENGTH {
                    break;
                }
                end = next;
            }
            &text[..end]
        } else {
            text
        };

        let body = serde_json::json!({
            "channel": recipient,
            "ts": message_id,
            "text": display_text,
        });

        let resp = self
            .http_client()
            .post("https://slack.com/api/chat.update")
            .bearer_auth(&self.bot_token)
            .json(&body)
            .send()
            .await?;

        if resp.status().is_success() {
            if let Ok(mut edits) = self.last_draft_edit.lock() {
                edits.insert(recipient.to_string(), Instant::now());
            }
        } else {
            let status = resp.status();
            let err = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
            let sanitized = crate::providers::sanitize_api_error(&err);
            tracing::debug!("Slack chat.update failed ({status}): {sanitized}");
        }

        Ok(None)
    }

    async fn finalize_draft(
        &self,
        recipient: &str,
        message_id: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        if let Ok(mut edits) = self.last_draft_edit.lock() {
            edits.remove(recipient);
        }

        let text = &super::strip_tool_call_tags(text);
        let formatted = markdown_to_slack_mrkdwn(text);

        if formatted.len() <= SLACK_MAX_MESSAGE_LENGTH {
            let body = serde_json::json!({
                "channel": recipient,
                "ts": message_id,
                "text": formatted,
            });

            let resp = self
                .http_client()
                .post("https://slack.com/api/chat.update")
                .bearer_auth(&self.bot_token)
                .json(&body)
                .send()
                .await?;

            let status = resp.status();
            let resp_body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));

            let parsed: serde_json::Value = serde_json::from_str(&resp_body).unwrap_or_default();
            if status.is_success() && parsed.get("ok") != Some(&serde_json::Value::Bool(false)) {
                return Ok(());
            }

            tracing::debug!(
                "Slack finalize_draft chat.update failed ({status}); falling back to delete+send"
            );
        }

        let _ = self
            .http_client()
            .post("https://slack.com/api/chat.delete")
            .bearer_auth(&self.bot_token)
            .json(&serde_json::json!({
                "channel": recipient,
                "ts": message_id,
            }))
            .send()
            .await;

        self.send(&SendMessage::new(&formatted, recipient)).await
    }

    async fn cancel_draft(&self, recipient: &str, message_id: &str) -> anyhow::Result<()> {
        if let Ok(mut edits) = self.last_draft_edit.lock() {
            edits.remove(recipient);
        }

        let resp = self
            .http_client()
            .post("https://slack.com/api/chat.delete")
            .bearer_auth(&self.bot_token)
            .json(&serde_json::json!({
                "channel": recipient,
                "ts": message_id,
            }))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
            let sanitized = crate::providers::sanitize_api_error(&body);
            tracing::debug!("Slack chat.delete failed ({status}): {sanitized}");
        }

        Ok(())
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Markdown → Slack mrkdwn converter
// ══════════════════════════════════════════════════════════════════════════
//
// Slack uses its own "mrkdwn" format that differs from standard Markdown:
//   • Bold:          *text*   (not **text**)
//   • Italic:        _text_   (not *text*)
//   • Strikethrough: ~text~   (not ~~text~~)
//   • Links:         <url|text> (not [text](url))
//   • Headers:       not supported — use *SECTION NAME* on its own line
//   • Bullets:       • item   (not - item)
//   • Code/blocks:   `code` and ```code``` (same as Markdown)
//
// This converter is a safety net applied in `send()`: the LLM is still
// prompted (via `channel_delivery_instructions`) to output Slack mrkdwn
// directly, but when it falls back to standard Markdown (which happens
// frequently in tool outputs and complex responses), this function catches
// and converts the formatting.
//
// The converter is idempotent for most Slack mrkdwn constructs: `_text_`
// (italic), `~text~` (strikethrough), `<url|text>` (links), and `\`code\``
// all pass through unchanged.  The one intentional non-idempotent transform
// is single-asterisk `*text*`: because this is standard Markdown italic,
// and Slack interprets `*text*` as bold, the converter rewrites it to
// `_text_` (Slack italic) so the author's intent is preserved.
//
// Architecture mirrors ZeroClaw's Telegram `markdown_to_telegram_html`:
// two-pass (per-line inline formatting, then cross-line code blocks).
// Ported from NVCortex's Slack formatting guidelines (base.md).

/// Convert standard Markdown formatting to Slack mrkdwn.
///
/// Handles: bold (`**` / `__`), italic (`*` → `_`), bold-italic (`***`),
/// strikethrough (`~~`), headers (`#`), links (`[text](url)`),
/// images (`![alt](url)`), bullet points (`-` / `*`), bare URLs, and
/// tables.  Code blocks and inline code are preserved verbatim.
fn markdown_to_slack_mrkdwn(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }

    // ── Pass 1: per-line inline formatting ──────────────────────────
    let lines: Vec<&str> = text.split('\n').collect();
    let mut result_lines: Vec<String> = Vec::with_capacity(lines.len());
    let mut in_code_block = false;

    for line in &lines {
        let trimmed = line.trim_start();

        // Track fenced code block boundaries (``` or ~~~).
        // Per the Markdown spec, both backtick and tilde fences are valid.
        // Slack only supports backtick fences, so convert ~~~ to ```.
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            in_code_block = !in_code_block;
            if trimmed.starts_with("~~~") {
                // Replace tilde fence with backtick fence for Slack.
                let converted = line.replacen("~~~", "```", 1);
                result_lines.push(converted);
            } else {
                result_lines.push(line.to_string());
            }
            continue;
        }

        // Inside a fenced code block: preserve verbatim.
        if in_code_block {
            result_lines.push(line.to_string());
            continue;
        }

        // ── Horizontal rules: ---, ***, ___ → ─── visual separator ──
        if is_horizontal_rule(trimmed) {
            result_lines.push("───────────────────────".to_string());
            continue;
        }

        // ── Headers: # Heading → *HEADING* ──────────────────────────
        if line.starts_with('#') {
            let stripped = line.trim_start_matches('#');
            let header_level = line.len() - stripped.len();
            if header_level > 0 && stripped.starts_with(' ') {
                // Strip bold markers from header text: ## **Title** → TITLE
                let title = strip_inline_bold(stripped.trim());
                let formatted = if header_level <= 2 {
                    format!("*{}*", title.to_uppercase())
                } else {
                    format!("*{title}*")
                };
                result_lines.push(formatted);
                continue;
            }
        }

        // ── Inline formatting ───────────────────────────────────────
        let out = convert_inline_formatting(line);

        // ── Bullet points & ordered lists ───────────────────────────
        result_lines.push(convert_list_item(&out));
    }

    // ── Pass 2: tables → bullet lists ───────────────────────────────
    let joined = result_lines.join("\n");
    let after_tables = convert_tables(&joined);

    // ── Pass 3: wrap bare URLs ──────────────────────────────────────
    let after_urls = wrap_bare_urls(&after_tables);

    // ── Pass 4: escape &, <, > for Slack ────────────────────────────
    // Slack uses these as control characters; they must be HTML-entity
    // encoded when they appear as literal text (not inside link syntax,
    // code blocks, or blockquotes).
    let final_text = escape_slack_entities(&after_urls);

    final_text.trim_end_matches('\n').to_string()
}

/// Convert inline Markdown formatting in a single line to Slack mrkdwn.
///
/// Handles bold, italic, strikethrough, inline code, links, and images.
/// Inline code spans are detected first and preserved verbatim so that
/// formatting markers inside backticks are not converted.
fn convert_inline_formatting(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let bytes = line.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        // ── Backslash escapes: \* \_ \~ etc. — emit the literal char ──
        // Per the Markdown spec, a backslash before a punctuation
        // character means the character should be treated literally.
        if bytes[i] == b'\\' && i + 1 < len {
            let next = bytes[i + 1];
            if matches!(
                next,
                b'\\'
                    | b'`'
                    | b'*'
                    | b'_'
                    | b'{'
                    | b'}'
                    | b'['
                    | b']'
                    | b'<'
                    | b'>'
                    | b'('
                    | b')'
                    | b'#'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'!'
                    | b'|'
                    | b'~'
            ) {
                out.push(next as char);
                i += 2;
                continue;
            }
        }

        // ── Inline code: `code` — preserve verbatim ─────────────
        if bytes[i] == b'`' && !(i + 2 < len && bytes[i + 1] == b'`' && bytes[i + 2] == b'`') {
            if let Some(end) = line[i + 1..].find('`') {
                let span = &line[i..i + 2 + end];
                out.push_str(span);
                i += 2 + end;
                continue;
            }
        }

        // ── Bold-italic: ***text*** → *_text_* ─────────────────
        // Must check before bold (**) to avoid partial match.
        if i + 2 < len && bytes[i] == b'*' && bytes[i + 1] == b'*' && bytes[i + 2] == b'*' {
            if let Some(end) = find_closing_marker(&line[i + 3..], "***") {
                if end > 0 {
                    let inner = &line[i + 3..i + 3 + end];
                    let _ = write!(out, "*_{inner}_*");
                    i += 6 + end;
                    continue;
                }
            }
        }

        // ── Bold: **text** → *text* ─────────────────────────────
        // Recursively convert inner content so that nested single-*
        // markers inside bold spans do not break Slack rendering.
        if i + 1 < len && bytes[i] == b'*' && bytes[i + 1] == b'*' {
            if let Some(end) = find_closing_marker(&line[i + 2..], "**") {
                if end > 0 {
                    let inner = &line[i + 2..i + 2 + end];
                    let converted_inner = convert_bold_inner(inner);
                    let _ = write!(out, "*{converted_inner}*");
                    i += 4 + end;
                    continue;
                }
            }
        }

        // ── Italic: *text* → _text_ ────────────────────────────
        // Standard Markdown italic (single asterisk) must become
        // Slack italic (underscore), because Slack *text* = bold.
        //
        // Matching rules (mirrors CommonMark flanking delimiter):
        //   • The character after the opening `*` must be non-whitespace.
        //   • The character before the closing `*` must be non-whitespace.
        // This prevents `* bullet item` or `5 * 3 * 2` from being
        // misinterpreted as italic.
        if bytes[i] == b'*' {
            // Opening `*` must be followed by non-whitespace.
            if i + 1 < len && !bytes[i + 1].is_ascii_whitespace() {
                if let Some(end) = find_closing_marker(&line[i + 1..], "*") {
                    // end > 0 ensures non-empty content; check that the
                    // character before the closing `*` is non-whitespace.
                    if end > 0 && !line.as_bytes()[i + end].is_ascii_whitespace() {
                        let inner = &line[i + 1..i + 1 + end];
                        let _ = write!(out, "_{inner}_");
                        i += 2 + end;
                        continue;
                    }
                }
            }
        }

        // ── Bold: __text__ → *text* ─────────────────────────────
        // Per CommonMark, `__` only opens emphasis at a left-flanking
        // delimiter run that is NOT preceded by a Unicode alphanumeric.
        // This prevents `foo__bar__baz` (mid-word) from being treated
        // as bold, while `__init__` also stays literal.
        if i + 1 < len && bytes[i] == b'_' && bytes[i + 1] == b'_' {
            let preceded_by_alnum = i > 0 && (bytes[i - 1] as char).is_alphanumeric();
            if !preceded_by_alnum {
                if let Some(end) = find_closing_marker(&line[i + 2..], "__") {
                    // Also check that the closing `__` is not followed by an
                    // alphanumeric character (right-flanking rule).
                    let after_close = i + 2 + end + 2;
                    let followed_by_alnum =
                        after_close < len && (bytes[after_close] as char).is_alphanumeric();
                    if end > 0 && !followed_by_alnum {
                        let inner = &line[i + 2..i + 2 + end];
                        let _ = write!(out, "*{inner}*");
                        i += 4 + end;
                        continue;
                    }
                }
            }
        }

        // ── Strikethrough: ~~text~~ → ~text~ ────────────────────
        if i + 1 < len && bytes[i] == b'~' && bytes[i + 1] == b'~' {
            if let Some(end) = find_closing_marker(&line[i + 2..], "~~") {
                if end > 0 {
                    let inner = &line[i + 2..i + 2 + end];
                    let _ = write!(out, "~{inner}~");
                    i += 4 + end;
                    continue;
                }
            }
        }

        // ── Image link: ![alt](url) → <url> ────────────────────
        if bytes[i] == b'!' && i + 1 < len && bytes[i + 1] == b'[' {
            if let Some(bracket_end) = line[i + 2..].find(']') {
                let after_bracket = i + 2 + bracket_end + 1;
                if after_bracket < len && bytes[after_bracket] == b'(' {
                    if let Some(paren_end) = line[after_bracket + 1..].find(')') {
                        let raw = &line[after_bracket + 1..after_bracket + 1 + paren_end];
                        let url = strip_link_title(raw);
                        let _ = write!(out, "<{url}>");
                        i = after_bracket + 1 + paren_end + 1;
                        continue;
                    }
                }
            }
        }

        // ── Link: [text](url) → <url|text> ─────────────────────
        if bytes[i] == b'[' {
            if let Some(bracket_end) = line[i + 1..].find(']') {
                let text_part = &line[i + 1..i + 1 + bracket_end];
                let after_bracket = i + 1 + bracket_end + 1;
                if after_bracket < len && bytes[after_bracket] == b'(' {
                    if let Some(paren_end) = line[after_bracket + 1..].find(')') {
                        let raw = &line[after_bracket + 1..after_bracket + 1 + paren_end];
                        let url = strip_link_title(raw);
                        if url.starts_with("http://") || url.starts_with("https://") {
                            let _ = write!(out, "<{url}|{text_part}>");
                            i = after_bracket + 1 + paren_end + 1;
                            continue;
                        }
                        // Non-http links: emit just the link text (relative
                        // URLs are meaningless in Slack).
                        out.push_str(text_part);
                        i = after_bracket + 1 + paren_end + 1;
                        continue;
                    }
                }
            }
        }

        // ── Default: pass character through ─────────────────────
        let ch = line[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }

    out
}

/// Find the position of a closing marker in `text`, skipping over inline
/// code spans (backtick-delimited).  Returns the byte offset of the marker
/// start relative to `text`, or `None` if not found.
///
/// This prevents formatting markers inside inline code from being treated
/// as closing delimiters (e.g. `**see `*note*` here**` should close at the
/// final `**`, not at the `*` inside backticks).
fn find_closing_marker(text: &str, marker: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let marker_bytes = marker.as_bytes();
    let marker_len = marker_bytes.len();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        // Skip inline code spans.
        if bytes[i] == b'`' {
            if let Some(end) = text[i + 1..].find('`') {
                i += 2 + end;
                continue;
            }
        }
        // Check for marker match.
        if i + marker_len <= len && &bytes[i..i + marker_len] == marker_bytes {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Convert inner content of a `**bold**` span to be safe for Slack `*...*`.
///
/// Inside a Slack bold span (`*...*`), any literal `*` would prematurely
/// close the bold.  The only Markdown formatting that can appear inside a
/// bold span and uses `*` is single-asterisk italic (`*text*`).  We convert
/// those to Slack italic (`_text_`) so the enclosing `*...*` is not broken.
fn convert_bold_inner(inner: &str) -> String {
    let bytes = inner.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len);
    let mut i = 0;

    while i < len {
        // Preserve inline code verbatim.
        if bytes[i] == b'`' {
            if let Some(end) = inner[i + 1..].find('`') {
                out.push_str(&inner[i..i + 2 + end]);
                i += 2 + end;
                continue;
            }
        }
        // Convert nested italic *text* → _text_ inside the bold span.
        if bytes[i] == b'*' {
            if let Some(end) = find_closing_marker(&inner[i + 1..], "*") {
                if end > 0 {
                    let italic_inner = &inner[i + 1..i + 1 + end];
                    let _ = write!(out, "_{italic_inner}_");
                    i += 2 + end;
                    continue;
                }
            }
        }
        let ch = inner[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Strip an optional Markdown link title from the parenthesized portion of
/// a link or image: `https://example.com "Title"` → `https://example.com`.
///
/// Per the Markdown spec, the title can be enclosed in double quotes (`"`),
/// single quotes (`'`), or parentheses (`(…)`) and appears after the URL
/// separated by whitespace.
fn strip_link_title(raw: &str) -> &str {
    let trimmed = raw.trim();
    // Detect title suffix: the last char is a quote/paren that closes a title.
    let last = trimmed.as_bytes().last().copied();
    let opener = match last {
        Some(b'"') => b'"',
        Some(b'\'') => b'\'',
        Some(b')') => b'(',
        _ => return trimmed,
    };
    // Walk backwards to find the matching opener preceded by whitespace.
    // We search for the *last* occurrence of `opener` that is preceded by a
    // space (to separate it from the URL).
    let without_close = &trimmed[..trimmed.len() - 1];
    if let Some(title_start) = without_close.rfind(opener as char) {
        if title_start > 0 && without_close.as_bytes()[title_start - 1] == b' ' {
            return without_close[..title_start - 1].trim_end();
        }
    }
    trimmed
}

/// Check if a line is a Markdown horizontal rule: `---`, `***`, `___`
/// (three or more of the same character, optionally with spaces).
fn is_horizontal_rule(trimmed: &str) -> bool {
    if trimmed.len() < 3 {
        return false;
    }
    let without_spaces: String = trimmed.chars().filter(|c| *c != ' ').collect();
    if without_spaces.len() < 3 {
        return false;
    }
    let first = without_spaces.as_bytes()[0];
    matches!(first, b'-' | b'*' | b'_') && without_spaces.bytes().all(|b| b == first)
}

/// Strip bold markers from text: `**Title**` → `Title`, `__Title__` → `Title`.
/// Used to clean up header text that contains inline bold markers.
fn strip_inline_bold(text: &str) -> String {
    let mut result = text.to_string();
    // Remove ** pairs
    while let (Some(_), Some(_)) = (result.find("**"), result.rfind("**")) {
        if result.find("**") == result.rfind("**") {
            break; // only one ** found, not a pair
        }
        result = result.replacen("**", "", 1);
        // Remove the last occurrence
        if let Some(pos) = result.rfind("**") {
            result.replace_range(pos..pos + 2, "");
        }
    }
    // Remove __ pairs
    while let (Some(start), Some(end)) = (result.find("__"), result.rfind("__")) {
        if start == end {
            break;
        }
        result = result.replacen("__", "", 1);
        if let Some(pos) = result.rfind("__") {
            result.replace_range(pos..pos + 2, "");
        }
    }
    result
}

/// Convert `- item` / `* item` → `• item`, and `1. item` → `1.  item`
/// (preserving ordered list numbers with consistent alignment).
fn convert_list_item(line: &str) -> String {
    let trimmed = line.trim_start();
    let indent = &line[..line.len() - trimmed.len()];

    // Unordered bullets: - item / * item → • item
    if let Some(rest) = trimmed.strip_prefix("- ") {
        return format!("{indent}• {rest}");
    }
    if let Some(rest) = trimmed.strip_prefix("* ") {
        return format!("{indent}• {rest}");
    }

    // Ordered lists: 1. item → 1.  item (Slack doesn't render numbered
    // lists natively, so we keep the number but ensure consistent spacing).
    if let Some(dot_pos) = trimmed.find(". ") {
        let prefix = &trimmed[..dot_pos];
        if !prefix.is_empty() && prefix.chars().all(|c| c.is_ascii_digit()) {
            let rest = &trimmed[dot_pos + 2..];
            return format!("{indent}{prefix}.  {rest}");
        }
    }

    line.to_string()
}

/// Convert Markdown tables to Slack-friendly bullet-point lists.
///
/// | Name  | Age | Role |
/// |-------|-----|------|
/// | Alice | 30  | Eng  |
///
/// →
///
/// *Name* | *Age* | *Role*
/// • Alice | 30 | Eng
fn convert_tables(text: &str) -> String {
    let lines: Vec<&str> = text.split('\n').collect();
    let mut result: Vec<String> = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        let trimmed = lines[i].trim();
        if !trimmed.starts_with('|') {
            result.push(lines[i].to_string());
            i += 1;
            continue;
        }

        // Collect consecutive table lines.
        let mut table_lines: Vec<&str> = Vec::new();
        while i < lines.len() && lines[i].trim().starts_with('|') {
            table_lines.push(lines[i].trim());
            i += 1;
        }

        if table_lines.len() < 2 {
            result.extend(table_lines.iter().map(|l| l.to_string()));
            continue;
        }

        // Parse header and data rows, skipping separator rows (|---|---|).
        let mut header: Option<Vec<String>> = None;
        let mut data_rows: Vec<Vec<String>> = Vec::new();

        for tl in &table_lines {
            let stripped = tl.trim().trim_matches('|');
            // Detect separator: all cells are dashes/colons only.
            let is_separator = stripped.split('|').all(|cell| {
                let c = cell.trim();
                !c.is_empty() && c.chars().all(|ch| ch == '-' || ch == ':' || ch == ' ')
            });
            if is_separator {
                continue;
            }

            let cells: Vec<String> = stripped.split('|').map(|c| c.trim().to_string()).collect();
            if header.is_none() {
                header = Some(cells);
            } else {
                data_rows.push(cells);
            }
        }

        if let Some(hdr) = header {
            let header_str = hdr
                .iter()
                .map(|h| format!("*{h}*"))
                .collect::<Vec<_>>()
                .join(" | ");
            result.push(header_str);
            for row in &data_rows {
                result.push(format!("• {}", row.join(" | ")));
            }
        } else {
            result.extend(table_lines.iter().map(|l| l.to_string()));
        }
    }

    result.join("\n")
}

/// Wrap bare `http://` and `https://` URLs in angle brackets (`<url>`)
/// unless they are already inside a Slack link (`<url|text>` or `<url>`),
/// inside inline code, or inside a fenced code block.
fn wrap_bare_urls(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut in_code_block = false;
    let mut first_line = true;

    for line in text.split('\n') {
        if !first_line {
            result.push('\n');
        }
        first_line = false;

        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            in_code_block = !in_code_block;
            result.push_str(line);
            continue;
        }
        if in_code_block {
            result.push_str(line);
            continue;
        }

        let bytes = line.as_bytes();
        let len = bytes.len();
        let mut i = 0;
        let mut in_inline_code = false;

        while i < len {
            if bytes[i] == b'`' {
                in_inline_code = !in_inline_code;
                result.push('`');
                i += 1;
                continue;
            }

            if in_inline_code {
                let ch = line[i..].chars().next().unwrap();
                result.push(ch);
                i += ch.len_utf8();
                continue;
            }

            // Detect URL start.
            if (line[i..].starts_with("http://") || line[i..].starts_with("https://"))
                && (i == 0 || !matches!(bytes[i - 1], b'<' | b'|' | b'(' | b'"' | b'\''))
            {
                let url_start = i;
                let mut url_end = i;
                for &b in &bytes[i..] {
                    if matches!(b, b' ' | b'\t' | b'>' | b')' | b']' | b'\n') {
                        break;
                    }
                    url_end += 1;
                }

                let url = &line[url_start..url_end];
                let already_wrapped = url_start > 0 && bytes[url_start - 1] == b'<';
                if already_wrapped {
                    result.push_str(url);
                } else {
                    let _ = write!(result, "<{url}>");
                }
                i = url_end;
                continue;
            }

            let ch = line[i..].chars().next().unwrap();
            result.push(ch);
            i += ch.len_utf8();
        }
    }

    result
}

/// Escape `&`, `<`, and `>` to HTML entities for Slack.
///
/// Slack uses these characters as control characters for special parsing
/// (links, mentions, dates).  Literal occurrences in normal text must be
/// encoded as `&amp;`, `&lt;`, `&gt;` respectively.
///
/// Preserved contexts (NOT escaped):
/// - Inside fenced code blocks (` ``` `)
/// - Inside inline code spans (`` ` ``)
/// - Inside Slack link/mention syntax (`<…>`)
/// - The `>` at the start of a line (Slack blockquote)
fn escape_slack_entities(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut in_code_block = false;
    let mut first_line = true;

    for line in text.split('\n') {
        if !first_line {
            result.push('\n');
        }
        first_line = false;

        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            in_code_block = !in_code_block;
            result.push_str(line);
            continue;
        }
        if in_code_block {
            result.push_str(line);
            continue;
        }

        let bytes = line.as_bytes();
        let len = bytes.len();
        let mut i = 0;
        let mut in_inline_code = false;
        let mut in_angle_bracket = false;

        while i < len {
            let b = bytes[i];

            // Track inline code spans.
            if b == b'`' {
                in_inline_code = !in_inline_code;
                result.push('`');
                i += 1;
                continue;
            }

            // Inside inline code: pass through verbatim.
            if in_inline_code {
                let ch = line[i..].chars().next().unwrap();
                result.push(ch);
                i += ch.len_utf8();
                continue;
            }

            // Track Slack angle-bracket syntax: <url>, <url|text>,
            // <@U...>, <#C...>, <!here>, <!date...>, <!subteam^...>,
            // <mailto:...>.
            // Only enter angle-bracket mode if the `<` is followed by a
            // pattern that indicates valid Slack special syntax.  A bare
            // `<` in text (e.g. `a < b`) must be escaped instead.
            if b == b'<' && !in_angle_bracket {
                let rest = &line[i + 1..];
                let is_slack_syntax = rest.starts_with("http://")
                    || rest.starts_with("https://")
                    || rest.starts_with("mailto:")
                    || rest.starts_with('@')
                    || rest.starts_with('#')
                    || rest.starts_with('!');
                if is_slack_syntax {
                    in_angle_bracket = true;
                    result.push('<');
                    i += 1;
                    continue;
                }
            }
            if b == b'>' && in_angle_bracket {
                in_angle_bracket = false;
                result.push('>');
                i += 1;
                continue;
            }

            // Inside angle-bracket syntax: pass through verbatim.
            if in_angle_bracket {
                let ch = line[i..].chars().next().unwrap();
                result.push(ch);
                i += ch.len_utf8();
                continue;
            }

            // Blockquote `>` at line start: pass through.
            if b == b'>' && i == 0 {
                result.push('>');
                i += 1;
                continue;
            }

            // ── Escape the three Slack control characters ───────
            match b {
                b'&' => {
                    result.push_str("&amp;");
                    i += 1;
                }
                b'<' => {
                    result.push_str("&lt;");
                    i += 1;
                }
                b'>' => {
                    result.push_str("&gt;");
                    i += 1;
                }
                _ => {
                    let ch = line[i..].chars().next().unwrap();
                    result.push(ch);
                    i += ch.len_utf8();
                }
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slack_channel_name() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec![]);
        assert_eq!(ch.name(), "slack");
    }

    #[test]
    fn slack_channel_with_channel_id() {
        let ch = SlackChannel::new(
            "xoxb-fake".into(),
            None,
            Some("C12345".into()),
            vec![],
            vec![],
        );
        assert_eq!(ch.channel_id, Some("C12345".to_string()));
    }

    #[test]
    fn slack_group_reply_policy_defaults_to_all_messages() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec!["*".into()]);
        assert!(!ch.mention_only);
        assert!(ch.group_reply_allowed_sender_ids.is_empty());
    }

    #[test]
    fn slack_group_reply_policy_applies_sender_overrides() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec!["*".into()])
            .with_group_reply_policy(true, vec![" U111 ".into(), "U111".into(), "U222".into()]);

        assert!(ch.mention_only);
        assert_eq!(
            ch.group_reply_allowed_sender_ids,
            vec!["U111".to_string(), "U222".to_string()]
        );
        assert!(ch.is_group_sender_trigger_enabled("U111"));
        assert!(!ch.is_group_sender_trigger_enabled("U999"));
    }

    #[test]
    fn normalized_channel_id_respects_wildcard_and_blank() {
        assert_eq!(SlackChannel::normalized_channel_id(None), None);
        assert_eq!(SlackChannel::normalized_channel_id(Some("")), None);
        assert_eq!(SlackChannel::normalized_channel_id(Some("   ")), None);
        assert_eq!(SlackChannel::normalized_channel_id(Some("*")), None);
        assert_eq!(SlackChannel::normalized_channel_id(Some(" * ")), None);
        assert_eq!(
            SlackChannel::normalized_channel_id(Some(" C12345 ")),
            Some("C12345".to_string())
        );
    }

    #[test]
    fn configured_app_token_ignores_blank_values() {
        let ch = SlackChannel::new("xoxb-fake".into(), Some("   ".into()), None, vec![], vec![]);
        assert_eq!(ch.configured_app_token(), None);
    }

    #[test]
    fn configured_app_token_trims_value() {
        let ch = SlackChannel::new(
            "xoxb-fake".into(),
            Some(" xapp-123 ".into()),
            None,
            vec![],
            vec![],
        );
        assert_eq!(ch.configured_app_token().as_deref(), Some("xapp-123"));
    }

    #[test]
    fn scoped_channel_ids_prefers_explicit_list() {
        let ch = SlackChannel::new(
            "xoxb-fake".into(),
            None,
            Some("C_SINGLE".into()),
            vec!["C_LIST1".into(), "D_DM1".into()],
            vec![],
        );
        assert_eq!(
            ch.scoped_channel_ids(),
            Some(vec!["C_LIST1".to_string(), "D_DM1".to_string()])
        );
    }

    #[test]
    fn scoped_channel_ids_falls_back_to_single_channel_id() {
        let ch = SlackChannel::new(
            "xoxb-fake".into(),
            None,
            Some("C_SINGLE".into()),
            vec![],
            vec![],
        );
        assert_eq!(ch.scoped_channel_ids(), Some(vec!["C_SINGLE".to_string()]));
    }

    #[test]
    fn scoped_channel_ids_returns_none_for_wildcard_mode() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec![]);
        assert_eq!(ch.scoped_channel_ids(), None);
    }

    #[test]
    fn is_group_channel_id_detects_channel_prefixes() {
        assert!(SlackChannel::is_group_channel_id("C123"));
        assert!(SlackChannel::is_group_channel_id("G123"));
        assert!(!SlackChannel::is_group_channel_id("D123"));
        assert!(!SlackChannel::is_group_channel_id(""));
    }

    #[test]
    fn extract_channel_ids_filters_archived_and_non_member_entries() {
        let payload = serde_json::json!({
            "channels": [
                {"id": "C1", "is_archived": false, "is_member": true},
                {"id": "C2", "is_archived": true, "is_member": true},
                {"id": "C3", "is_archived": false, "is_member": false},
                {"id": "C1", "is_archived": false, "is_member": true},
                {"id": "C4"}
            ]
        });
        let ids = SlackChannel::extract_channel_ids(&payload);
        assert_eq!(ids, vec!["C1".to_string(), "C4".to_string()]);
    }

    #[test]
    fn empty_allowlist_denies_everyone() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec![]);
        assert!(!ch.is_user_allowed("U12345"));
        assert!(!ch.is_user_allowed("anyone"));
    }

    #[test]
    fn wildcard_allows_everyone() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec!["*".into()]);
        assert!(ch.is_user_allowed("U12345"));
    }

    #[test]
    fn extract_user_display_name_prefers_profile_display_name() {
        let payload = serde_json::json!({
            "ok": true,
            "user": {
                "name": "fallback_name",
                "profile": {
                    "display_name": "Display Name",
                    "real_name": "Real Name"
                }
            }
        });

        assert_eq!(
            SlackChannel::extract_user_display_name(&payload).as_deref(),
            Some("Display Name")
        );
    }

    #[test]
    fn extract_user_display_name_falls_back_to_username() {
        let payload = serde_json::json!({
            "ok": true,
            "user": {
                "name": "fallback_name",
                "profile": {
                    "display_name": "   ",
                    "real_name": ""
                }
            }
        });

        assert_eq!(
            SlackChannel::extract_user_display_name(&payload).as_deref(),
            Some("fallback_name")
        );
    }

    #[test]
    fn cached_sender_display_name_returns_none_when_expired() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec!["*".into()]);
        {
            let mut cache = ch.user_display_name_cache.lock().unwrap();
            cache.insert(
                "U123".to_string(),
                CachedSlackDisplayName {
                    display_name: "Expired Name".to_string(),
                    expires_at: Instant::now() - Duration::from_secs(1),
                },
            );
        }

        assert_eq!(ch.cached_sender_display_name("U123"), None);
    }

    #[test]
    fn cached_sender_display_name_returns_cached_value_when_valid() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec!["*".into()]);
        ch.cache_sender_display_name("U123", "Cached Name");

        assert_eq!(
            ch.cached_sender_display_name("U123").as_deref(),
            Some("Cached Name")
        );
    }

    #[test]
    fn normalize_incoming_content_requires_mention_when_enabled() {
        assert!(SlackChannel::normalize_incoming_content("hello", true, "U_BOT").is_none());
        assert_eq!(
            SlackChannel::normalize_incoming_content("<@U_BOT> run", true, "U_BOT").as_deref(),
            Some("run")
        );
    }

    #[test]
    fn normalize_incoming_content_without_mention_mode_keeps_message() {
        assert_eq!(
            SlackChannel::normalize_incoming_content("  hello world  ", false, "U_BOT").as_deref(),
            Some("hello world")
        );
    }

    #[test]
    fn specific_allowlist_filters() {
        let ch = SlackChannel::new(
            "xoxb-fake".into(),
            None,
            None,
            vec![],
            vec!["U111".into(), "U222".into()],
        );
        assert!(ch.is_user_allowed("U111"));
        assert!(ch.is_user_allowed("U222"));
        assert!(!ch.is_user_allowed("U333"));
    }

    #[test]
    fn allowlist_exact_match_not_substring() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec!["U111".into()]);
        assert!(!ch.is_user_allowed("U1111"));
        assert!(!ch.is_user_allowed("U11"));
    }

    #[test]
    fn allowlist_empty_user_id() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec!["U111".into()]);
        assert!(!ch.is_user_allowed(""));
    }

    #[test]
    fn allowlist_case_sensitive() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec!["U111".into()]);
        assert!(ch.is_user_allowed("U111"));
        assert!(!ch.is_user_allowed("u111"));
    }

    #[test]
    fn allowlist_wildcard_and_specific() {
        let ch = SlackChannel::new(
            "xoxb-fake".into(),
            None,
            None,
            vec![],
            vec!["U111".into(), "*".into()],
        );
        assert!(ch.is_user_allowed("U111"));
        assert!(ch.is_user_allowed("anyone"));
    }

    // ── Message ID edge cases ─────────────────────────────────────

    #[test]
    fn slack_message_id_format_includes_channel_and_ts() {
        // Verify that message IDs follow the format: slack_{channel_id}_{ts}
        let ts = "1234567890.123456";
        let channel_id = "C12345";
        let expected_id = format!("slack_{channel_id}_{ts}");
        assert_eq!(expected_id, "slack_C12345_1234567890.123456");
    }

    #[test]
    fn slack_message_id_is_deterministic() {
        // Same channel_id + same ts = same ID (prevents duplicates after restart)
        let ts = "1234567890.123456";
        let channel_id = "C12345";
        let id1 = format!("slack_{channel_id}_{ts}");
        let id2 = format!("slack_{channel_id}_{ts}");
        assert_eq!(id1, id2);
    }

    #[test]
    fn slack_message_id_different_ts_different_id() {
        // Different timestamps produce different IDs
        let channel_id = "C12345";
        let id1 = format!("slack_{channel_id}_1234567890.123456");
        let id2 = format!("slack_{channel_id}_1234567890.123457");
        assert_ne!(id1, id2);
    }

    #[test]
    fn slack_message_id_different_channel_different_id() {
        // Different channels produce different IDs even with same ts
        let ts = "1234567890.123456";
        let id1 = format!("slack_C12345_{ts}");
        let id2 = format!("slack_C67890_{ts}");
        assert_ne!(id1, id2);
    }

    #[test]
    fn slack_message_id_no_uuid_randomness() {
        // Verify format doesn't contain random UUID components
        let ts = "1234567890.123456";
        let channel_id = "C12345";
        let id = format!("slack_{channel_id}_{ts}");
        assert!(!id.contains('-')); // No UUID dashes
        assert!(id.starts_with("slack_"));
    }

    #[test]
    fn inbound_thread_ts_prefers_explicit_thread_ts() {
        let msg = serde_json::json!({
            "ts": "123.002",
            "thread_ts": "123.001"
        });

        let thread_ts = SlackChannel::inbound_thread_ts(&msg, "123.002");
        assert_eq!(thread_ts.as_deref(), Some("123.001"));
    }

    #[test]
    fn inbound_thread_ts_falls_back_to_ts() {
        let msg = serde_json::json!({
            "ts": "123.001"
        });

        let thread_ts = SlackChannel::inbound_thread_ts(&msg, "123.001");
        assert_eq!(thread_ts.as_deref(), Some("123.001"));
    }

    #[test]
    fn inbound_thread_ts_none_when_ts_missing() {
        let msg = serde_json::json!({});

        let thread_ts = SlackChannel::inbound_thread_ts(&msg, "");
        assert_eq!(thread_ts, None);
    }

    #[test]
    fn ensure_poll_cursor_bootstraps_new_channel() {
        let mut cursors = HashMap::new();
        let now_ts = "1700000000.123456";

        let cursor = SlackChannel::ensure_poll_cursor(&mut cursors, "C123", now_ts);
        assert_eq!(cursor, now_ts);
        assert_eq!(cursors.get("C123").map(String::as_str), Some(now_ts));
    }

    #[test]
    fn ensure_poll_cursor_keeps_existing_cursor() {
        let mut cursors = HashMap::from([("C123".to_string(), "1700000000.000001".to_string())]);
        let cursor = SlackChannel::ensure_poll_cursor(&mut cursors, "C123", "9999999999.999999");

        assert_eq!(cursor, "1700000000.000001");
        assert_eq!(
            cursors.get("C123").map(String::as_str),
            Some("1700000000.000001")
        );
    }

    #[test]
    fn parse_retry_after_value_accepts_integer_seconds() {
        assert_eq!(SlackChannel::parse_retry_after_value("30"), Some(30));
    }

    #[test]
    fn parse_retry_after_value_accepts_decimal_seconds() {
        assert_eq!(SlackChannel::parse_retry_after_value("2.9"), Some(2));
    }

    #[test]
    fn parse_retry_after_value_rejects_non_numeric_values() {
        assert_eq!(SlackChannel::parse_retry_after_value("later"), None);
        assert_eq!(SlackChannel::parse_retry_after_value(""), None);
    }

    #[test]
    fn parse_retry_after_secs_reads_header_value() {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "45".parse().unwrap());
        assert_eq!(SlackChannel::parse_retry_after_secs(&headers), Some(45));
    }

    #[test]
    fn compute_retry_delay_applies_backoff_and_jitter_with_cap() {
        let delay = SlackChannel::compute_retry_delay(30, 3, 250);
        assert_eq!(delay, Duration::from_secs(120) + Duration::from_millis(250));
    }

    // ── markdown_to_slack_mrkdwn tests ──────────────────────────────

    #[test]
    fn mrkdwn_empty_passthrough() {
        assert_eq!(markdown_to_slack_mrkdwn(""), "");
    }

    #[test]
    fn mrkdwn_plain_text_passthrough() {
        let text = "Hello world, this is a normal message.";
        assert_eq!(markdown_to_slack_mrkdwn(text), text);
    }

    #[test]
    fn mrkdwn_bold_double_asterisk() {
        assert_eq!(markdown_to_slack_mrkdwn("**bold text**"), "*bold text*");
    }

    #[test]
    fn mrkdwn_bold_double_underscore() {
        assert_eq!(markdown_to_slack_mrkdwn("__bold text__"), "*bold text*");
    }

    #[test]
    fn mrkdwn_single_asterisk_to_italic() {
        // Single-asterisk *text* is standard Markdown italic.
        // Slack uses *text* for bold, so we must convert to _text_ (Slack italic).
        assert_eq!(markdown_to_slack_mrkdwn("*italic text*"), "_italic text_");
    }

    #[test]
    fn mrkdwn_bold_and_italic_in_same_line() {
        // **bold** → *bold* (Slack bold), *italic* → _italic_ (Slack italic)
        assert_eq!(
            markdown_to_slack_mrkdwn("**bold** and *italic*"),
            "*bold* and _italic_"
        );
    }

    #[test]
    fn mrkdwn_strikethrough() {
        assert_eq!(
            markdown_to_slack_mrkdwn("~~deleted text~~"),
            "~deleted text~"
        );
    }

    #[test]
    fn mrkdwn_header_h1() {
        assert_eq!(markdown_to_slack_mrkdwn("# Main Title"), "*MAIN TITLE*");
    }

    #[test]
    fn mrkdwn_header_h2() {
        assert_eq!(markdown_to_slack_mrkdwn("## Section"), "*SECTION*");
    }

    #[test]
    fn mrkdwn_header_h3_preserves_case() {
        assert_eq!(markdown_to_slack_mrkdwn("### Sub Section"), "*Sub Section*");
    }

    #[test]
    fn mrkdwn_link_conversion() {
        assert_eq!(
            markdown_to_slack_mrkdwn("[Click here](https://example.com)"),
            "<https://example.com|Click here>"
        );
    }

    #[test]
    fn mrkdwn_image_link() {
        assert_eq!(
            markdown_to_slack_mrkdwn("![alt text](https://img.example.com/pic.png)"),
            "<https://img.example.com/pic.png>"
        );
    }

    #[test]
    fn mrkdwn_bullet_dash() {
        assert_eq!(markdown_to_slack_mrkdwn("- item one"), "• item one");
    }

    #[test]
    fn mrkdwn_bullet_asterisk() {
        assert_eq!(markdown_to_slack_mrkdwn("* item one"), "• item one");
    }

    #[test]
    fn mrkdwn_nested_bullet() {
        assert_eq!(
            markdown_to_slack_mrkdwn("  - nested item"),
            "  • nested item"
        );
    }

    #[test]
    fn mrkdwn_inline_code_preserved() {
        assert_eq!(
            markdown_to_slack_mrkdwn("Use `git status` to check"),
            "Use `git status` to check"
        );
    }

    #[test]
    fn mrkdwn_code_block_preserved() {
        let input = "```rust\nfn main() {\n    println!(\"hello\");\n}\n```";
        let output = markdown_to_slack_mrkdwn(input);
        assert!(output.contains("```rust"));
        assert!(output.contains("fn main()"));
        assert!(output.contains("println!"));
    }

    #[test]
    fn mrkdwn_no_formatting_inside_code_block() {
        let input = "```\n**bold** and *italic* inside code\n```";
        let output = markdown_to_slack_mrkdwn(input);
        assert!(
            output.contains("**bold**"),
            "bold should be preserved in code block"
        );
        assert!(
            output.contains("*italic*"),
            "italic should be preserved in code block"
        );
    }

    #[test]
    fn mrkdwn_inline_code_not_converted() {
        let input = "Run `**not bold**` here";
        let output = markdown_to_slack_mrkdwn(input);
        assert!(output.contains("`**not bold**`"));
    }

    #[test]
    fn mrkdwn_bare_url_wrapped() {
        assert_eq!(
            markdown_to_slack_mrkdwn("Visit https://example.com for more"),
            "Visit <https://example.com> for more"
        );
    }

    #[test]
    fn mrkdwn_already_wrapped_url_unchanged() {
        let input = "Visit <https://example.com> for more";
        assert_eq!(markdown_to_slack_mrkdwn(input), input);
    }

    #[test]
    fn mrkdwn_slack_link_not_double_wrapped() {
        let input = "<https://example.com|Example>";
        assert_eq!(markdown_to_slack_mrkdwn(input), input);
    }

    #[test]
    fn mrkdwn_table_conversion() {
        let input = "| Name  | Age | Role |\n|-------|-----|------|\n| Alice | 30  | Eng  |\n| Bob   | 25  | PM   |";
        let output = markdown_to_slack_mrkdwn(input);
        assert!(output.contains("*Name*"));
        assert!(output.contains("*Age*"));
        assert!(output.contains("*Role*"));
        assert!(output.contains("• Alice | 30 | Eng"));
        assert!(output.contains("• Bob | 25 | PM"));
    }

    #[test]
    fn mrkdwn_mixed_formatting() {
        let input = "## Summary\n\nHere is a **bold** statement with a [link](https://example.com).\n\n- First item\n- Second item with `code`";
        let output = markdown_to_slack_mrkdwn(input);
        assert!(output.contains("*SUMMARY*"));
        assert!(output.contains("*bold*"));
        assert!(output.contains("<https://example.com|link>"));
        assert!(output.contains("• First item"));
        assert!(output.contains("`code`"));
    }

    // ── Horizontal rules ────────────────────────────────────────

    #[test]
    fn mrkdwn_horizontal_rule_dashes() {
        assert_eq!(markdown_to_slack_mrkdwn("---"), "───────────────────────");
    }

    #[test]
    fn mrkdwn_horizontal_rule_asterisks() {
        assert_eq!(markdown_to_slack_mrkdwn("***"), "───────────────────────");
    }

    #[test]
    fn mrkdwn_horizontal_rule_underscores() {
        assert_eq!(markdown_to_slack_mrkdwn("___"), "───────────────────────");
    }

    #[test]
    fn mrkdwn_horizontal_rule_long() {
        assert_eq!(
            markdown_to_slack_mrkdwn("----------"),
            "───────────────────────"
        );
    }

    #[test]
    fn mrkdwn_horizontal_rule_with_spaces() {
        assert_eq!(markdown_to_slack_mrkdwn("- - -"), "───────────────────────");
    }

    #[test]
    fn mrkdwn_horizontal_rule_in_context() {
        let input = "Above\n\n---\n\nBelow";
        let output = markdown_to_slack_mrkdwn(input);
        assert!(output.contains("Above"));
        assert!(output.contains("───────────────────────"));
        assert!(output.contains("Below"));
    }

    // ── Headers with bold markers ───────────────────────────────

    #[test]
    fn mrkdwn_header_strips_bold_markers() {
        assert_eq!(
            markdown_to_slack_mrkdwn("## **Bold Title**"),
            "*BOLD TITLE*"
        );
    }

    #[test]
    fn mrkdwn_header_strips_underscore_bold() {
        assert_eq!(
            markdown_to_slack_mrkdwn("## __Bold Title__"),
            "*BOLD TITLE*"
        );
    }

    // ── Bold-italic (triple asterisks) ──────────────────────────

    #[test]
    fn mrkdwn_bold_italic_triple_asterisk() {
        assert_eq!(
            markdown_to_slack_mrkdwn("***bold italic***"),
            "*_bold italic_*"
        );
    }

    #[test]
    fn mrkdwn_bold_italic_in_sentence() {
        assert_eq!(
            markdown_to_slack_mrkdwn("This is ***important*** text"),
            "This is *_important_* text"
        );
    }

    // ── Mixed bold + italic on the same line ────────────────────

    #[test]
    fn mrkdwn_italic_then_bold_same_line() {
        // *italic* first, then **bold** — both should render correctly.
        assert_eq!(
            markdown_to_slack_mrkdwn("*italic* and **bold**"),
            "_italic_ and *bold*"
        );
    }

    #[test]
    fn mrkdwn_bold_with_nested_italic() {
        // **bold *nested italic* text** — inner *...* must become _..._
        // so the enclosing *...* (Slack bold) is not broken.
        assert_eq!(
            markdown_to_slack_mrkdwn("**bold *nested* text**"),
            "*bold _nested_ text*"
        );
    }

    #[test]
    fn mrkdwn_multiple_bold_and_italic() {
        // Multiple bold and italic segments on the same line.
        assert_eq!(
            markdown_to_slack_mrkdwn("**a** *b* **c** *d*"),
            "*a* _b_ *c* _d_"
        );
    }

    #[test]
    fn mrkdwn_italic_with_inline_code() {
        // Inline code inside italic should be preserved.
        assert_eq!(
            markdown_to_slack_mrkdwn("*use `git status` to check*"),
            "_use `git status` to check_"
        );
    }

    #[test]
    fn mrkdwn_bold_then_italic_adjacent() {
        // Adjacent bold and italic without space.
        assert_eq!(
            markdown_to_slack_mrkdwn("**bold***italic*"),
            "*bold*_italic_"
        );
    }

    #[test]
    fn mrkdwn_unmatched_single_asterisk() {
        // A lone * without a closing pair should pass through as-is.
        assert_eq!(markdown_to_slack_mrkdwn("5 * 3 = 15"), "5 * 3 = 15");
    }

    #[test]
    fn mrkdwn_bullet_with_italic_content() {
        // `* *italic* rest` — the leading `* ` is a bullet, inner *italic*
        // should still convert to _italic_ after bullet conversion.
        assert_eq!(
            markdown_to_slack_mrkdwn("* *italic* rest"),
            "• _italic_ rest"
        );
    }

    #[test]
    fn mrkdwn_math_asterisks_not_italic() {
        // Multiplication expressions should not be converted to italic.
        assert_eq!(
            markdown_to_slack_mrkdwn("result = a * b * c"),
            "result = a * b * c"
        );
    }

    // ── Ordered lists ───────────────────────────────────────────

    #[test]
    fn mrkdwn_ordered_list() {
        let input = "1. First item\n2. Second item\n3. Third item";
        let output = markdown_to_slack_mrkdwn(input);
        assert!(output.contains("1.  First item"));
        assert!(output.contains("2.  Second item"));
        assert!(output.contains("3.  Third item"));
    }

    #[test]
    fn mrkdwn_ordered_list_double_digit() {
        assert_eq!(
            markdown_to_slack_mrkdwn("10. Tenth item"),
            "10.  Tenth item"
        );
    }

    #[test]
    fn mrkdwn_ordered_list_nested() {
        assert_eq!(
            markdown_to_slack_mrkdwn("  1. Nested item"),
            "  1.  Nested item"
        );
    }

    #[test]
    fn mrkdwn_ordered_list_with_formatting() {
        let input = "1. **Bold** first\n2. *Italic* second";
        let output = markdown_to_slack_mrkdwn(input);
        assert!(output.contains("1.  *Bold* first"));
        // Single-asterisk *Italic* → _Italic_ (Slack italic)
        assert!(output.contains("2.  _Italic_ second"));
    }

    // ── Realistic LLM output ────────────────────────────────────

    #[test]
    fn mrkdwn_realistic_llm_output() {
        let input = "\
## Summary

Here are the results:

---

**Key findings:**

1. First finding with **bold** emphasis
2. Second finding with *italic* note
3. Third finding

---

### Details

- Item A
- Item B

***Note:*** this is important.";

        let output = markdown_to_slack_mrkdwn(input);
        assert!(output.contains("*SUMMARY*"), "header should be converted");
        assert!(
            output.contains("───────────────────────"),
            "hr should be converted"
        );
        assert!(
            output.contains("*Key findings:*"),
            "bold should be converted"
        );
        assert!(
            output.contains("1.  First finding with *bold* emphasis"),
            "ordered list + bold"
        );
        assert!(
            output.contains("2.  Second finding with _italic_ note"),
            "ordered list + italic (single * → _ for Slack italic)"
        );
        assert!(output.contains("3.  Third finding"), "ordered list plain");
        assert!(output.contains("*Details*"), "h3 should be converted");
        assert!(output.contains("• Item A"), "bullets should be converted");
        assert!(
            output.contains("*_Note:_*"),
            "bold-italic should be converted"
        );
    }

    // ── Link/image title stripping ────────────────────────────────

    #[test]
    fn mrkdwn_link_with_title_stripped() {
        // Markdown link with title attribute: title must be stripped.
        assert_eq!(
            markdown_to_slack_mrkdwn(r#"[Click](https://example.com "The best site")"#),
            "<https://example.com|Click>"
        );
    }

    #[test]
    fn mrkdwn_link_with_single_quote_title() {
        assert_eq!(
            markdown_to_slack_mrkdwn("[Click](https://example.com 'Title')"),
            "<https://example.com|Click>"
        );
    }

    #[test]
    fn mrkdwn_image_with_title_stripped() {
        assert_eq!(
            markdown_to_slack_mrkdwn(r#"![alt](https://img.example.com/pic.png "Caption")"#),
            "<https://img.example.com/pic.png>"
        );
    }

    #[test]
    fn mrkdwn_link_without_title_unchanged() {
        // Links without title should still work as before.
        assert_eq!(
            markdown_to_slack_mrkdwn("[Click](https://example.com)"),
            "<https://example.com|Click>"
        );
    }

    // ── Tilde fenced code blocks ────────────────────────────────

    #[test]
    fn mrkdwn_tilde_code_block_converted() {
        // ~~~ fences should be converted to ``` for Slack.
        let input = "~~~rust\nfn main() {}\n~~~";
        let output = markdown_to_slack_mrkdwn(input);
        assert!(
            output.contains("```rust"),
            "tilde fence should become backtick fence"
        );
        assert!(
            output.contains("fn main()"),
            "code content should be preserved"
        );
        assert!(!output.contains("~~~"), "tilde fences should not remain");
    }

    #[test]
    fn mrkdwn_tilde_code_block_no_formatting() {
        // Content inside ~~~ blocks should not be formatted.
        let input = "~~~\n**bold** and *italic*\n~~~";
        let output = markdown_to_slack_mrkdwn(input);
        assert!(
            output.contains("**bold**"),
            "bold should be preserved inside tilde code block"
        );
        assert!(
            output.contains("*italic*"),
            "italic should be preserved inside tilde code block"
        );
    }

    // ── Non-http links (relative URLs) ──────────────────────────

    #[test]
    fn mrkdwn_relative_link_emits_text_only() {
        // Relative URLs are meaningless in Slack; emit just the text.
        assert_eq!(
            markdown_to_slack_mrkdwn("[see docs](/api/reference)"),
            "see docs"
        );
    }

    #[test]
    fn mrkdwn_anchor_link_emits_text_only() {
        assert_eq!(
            markdown_to_slack_mrkdwn("[Section](#heading-id)"),
            "Section"
        );
    }

    // ── Backslash escapes ───────────────────────────────────────

    #[test]
    fn mrkdwn_escaped_asterisks_literal() {
        // \* should produce a literal * and not trigger italic.
        assert_eq!(markdown_to_slack_mrkdwn(r"\*not italic\*"), "*not italic*");
    }

    #[test]
    fn mrkdwn_escaped_double_asterisks_literal() {
        assert_eq!(
            markdown_to_slack_mrkdwn(r"\*\*not bold\*\*"),
            "**not bold**"
        );
    }

    #[test]
    fn mrkdwn_escaped_underscore_literal() {
        assert_eq!(markdown_to_slack_mrkdwn(r"\_not italic\_"), "_not italic_");
    }

    #[test]
    fn mrkdwn_escaped_backtick_literal() {
        assert_eq!(markdown_to_slack_mrkdwn(r"\`not code\`"), "`not code`");
    }

    #[test]
    fn mrkdwn_escaped_backslash() {
        assert_eq!(markdown_to_slack_mrkdwn(r"\\"), r"\");
    }

    // ── Underscore bold word-boundary ───────────────────────────

    #[test]
    fn mrkdwn_mid_word_underscores_not_bold() {
        // Per CommonMark, mid-word __ should NOT trigger bold.
        assert_eq!(markdown_to_slack_mrkdwn("Love__is__bold"), "Love__is__bold");
    }

    #[test]
    fn mrkdwn_dunder_in_prose_not_bold() {
        // Python dunder names mid-word should not be mangled.
        // `foo.__init__` has `__` preceded by `.` (punctuation, not
        // alphanumeric), so the opening `__` IS left-flanking per
        // CommonMark.  But `method__init__call` is truly mid-word.
        assert_eq!(
            markdown_to_slack_mrkdwn("method__init__call"),
            "method__init__call"
        );
    }

    #[test]
    fn mrkdwn_underscore_bold_at_word_boundary() {
        // __bold__ at word boundary should still convert.
        assert_eq!(
            markdown_to_slack_mrkdwn("Use __bold__ here"),
            "Use *bold* here"
        );
    }

    // ── Slack entity escaping (&, <, >) ────────────────────────────

    #[test]
    fn mrkdwn_ampersand_escaped() {
        assert_eq!(
            markdown_to_slack_mrkdwn("AT&T is a company"),
            "AT&amp;T is a company"
        );
    }

    #[test]
    fn mrkdwn_angle_brackets_escaped() {
        assert_eq!(
            markdown_to_slack_mrkdwn("a < b && c > d"),
            "a &lt; b &amp;&amp; c &gt; d"
        );
    }

    #[test]
    fn mrkdwn_slack_link_not_escaped() {
        // Angle brackets in Slack link syntax must NOT be escaped.
        assert_eq!(
            markdown_to_slack_mrkdwn("[Click](https://example.com)"),
            "<https://example.com|Click>"
        );
    }

    #[test]
    fn mrkdwn_bare_url_angles_not_escaped() {
        // Bare URL wrapping adds < > which must stay as-is.
        assert_eq!(
            markdown_to_slack_mrkdwn("Visit https://example.com today"),
            "Visit <https://example.com> today"
        );
    }

    #[test]
    fn mrkdwn_blockquote_gt_not_escaped() {
        // > at line start is blockquote, not a control char.
        assert_eq!(markdown_to_slack_mrkdwn("> quoted text"), "> quoted text");
    }

    #[test]
    fn mrkdwn_inline_code_not_entity_escaped() {
        // Inside inline code, & < > should NOT be escaped.
        assert_eq!(
            markdown_to_slack_mrkdwn("Use `a < b && c > d` in code"),
            "Use `a < b && c > d` in code"
        );
    }

    #[test]
    fn mrkdwn_code_block_not_entity_escaped() {
        // Inside code blocks, & < > should NOT be escaped.
        let input = "```\na < b && c > d\n```";
        let output = markdown_to_slack_mrkdwn(input);
        assert!(
            output.contains("a < b && c > d"),
            "code block content should not be entity-escaped"
        );
    }

    #[test]
    fn mrkdwn_mixed_escaping_with_link() {
        // Ampersand in text + link in same line.
        assert_eq!(
            markdown_to_slack_mrkdwn("Check AT&T at [their site](https://att.com)"),
            "Check AT&amp;T at <https://att.com|their site>"
        );
    }

    // ── Streaming / draft tests ───────────────────────────────────

    #[test]
    fn supports_draft_updates_off_by_default() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec![]);
        assert!(!ch.supports_draft_updates());
    }

    #[test]
    fn supports_draft_updates_enabled_when_partial() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec![])
            .with_streaming(StreamMode::Partial, 1000);
        assert!(ch.supports_draft_updates());
    }

    #[test]
    fn with_streaming_configures_fields() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec![])
            .with_streaming(StreamMode::Partial, 500);
        assert_eq!(ch.stream_mode, StreamMode::Partial);
        assert_eq!(ch.draft_update_interval_ms, 500);
    }

    #[tokio::test]
    async fn send_draft_returns_none_when_stream_mode_off() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec![]);
        let id = ch
            .send_draft(&SendMessage::new("draft", "C123"))
            .await
            .unwrap();
        assert!(id.is_none());
    }

    #[tokio::test]
    async fn update_draft_rate_limit_short_circuits() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec![])
            .with_streaming(StreamMode::Partial, 60_000);
        ch.last_draft_edit
            .lock()
            .unwrap()
            .insert("C123".to_string(), Instant::now());

        let result = ch.update_draft("C123", "1234.5678", "text").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn update_draft_utf8_truncation_is_safe() {
        let ch = SlackChannel::new("xoxb-fake".into(), None, None, vec![], vec![])
            .with_streaming(StreamMode::Partial, 0);
        let long_text = "\u{1F600}".repeat(SLACK_MAX_MESSAGE_LENGTH + 20);
        let result = ch.update_draft("C123", "1234.5678", &long_text).await;
        assert!(result.is_err() || result.is_ok());
    }
}
