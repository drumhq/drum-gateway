use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Body,
    extract::{FromRequestParts, Request, State, WebSocketUpgrade, ws::Message},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD},
};
use futures_util::{SinkExt, StreamExt};
use nostr::Event;
use rand::RngCore;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{env, net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite};

const RELAY_SERVICE: &str = "shared-relay";
const BLOSSOM_SERVICE: &str = "shared-blossom";
const NIP42_KIND: u16 = 22_242;
const BLOSSOM_AUTH_KIND: u16 = 24_242;
const BUILD_SHA: &str = match option_env!("DRUM_GATEWAY_BUILD_SHA") {
    Some(value) => value,
    None => "development",
};

#[derive(Clone)]
struct GatewayState {
    config: Arc<GatewayConfig>,
    client: reqwest::Client,
}

struct GatewayConfig {
    bind_addr: SocketAddr,
    api_origin: String,
    shared_secret: String,
    relay_hostname: String,
    relay_public_url: String,
    relay_upstream_ws: String,
    relay_upstream_http: String,
    blossom_hostname: String,
    blossom_upstream_http: String,
    blossom_max_write_bytes: i64,
}

impl GatewayConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            bind_addr: env::var("GATEWAY_BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:3100".to_string())
                .parse()
                .context("GATEWAY_BIND_ADDR must be a socket address")?,
            api_origin: required_env("DRUM_API_ORIGIN")?
                .trim_end_matches('/')
                .to_string(),
            shared_secret: required_env("GATEWAY_SHARED_SECRET")?,
            relay_hostname: env::var("RELAY_HOSTNAME")
                .unwrap_or_else(|_| "relay.drum.dev".to_string()),
            relay_public_url: env::var("RELAY_PUBLIC_URL")
                .unwrap_or_else(|_| "wss://relay.drum.dev/".to_string()),
            relay_upstream_ws: required_env("RELAY_UPSTREAM_WS")?,
            relay_upstream_http: required_env("RELAY_UPSTREAM_HTTP")?,
            blossom_hostname: env::var("BLOSSOM_HOSTNAME")
                .unwrap_or_else(|_| "media.drum.dev".to_string()),
            blossom_upstream_http: required_env("BLOSSOM_UPSTREAM_HTTP")?,
            blossom_max_write_bytes: positive_i64_env("BLOSSOM_MAX_WRITE_BYTES", "25000000")?,
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthorizationRequest<'a> {
    service: &'a str,
    action: &'a str,
    public_key: Option<&'a str>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuthorizationResponse {
    authorized: bool,
    account_identity_id: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RecordNotePublicationRequest<'a> {
    public_key: &'a str,
    event_id: &'a str,
}

enum NotePublicationError {
    LimitReached,
    Unavailable,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReserveUploadRequest<'a> {
    account_identity_id: &'a str,
    requested_bytes: i64,
    sha256: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReserveUploadResponse {
    reservation_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CommitUploadRequest<'a> {
    sha256: &'a str,
    size_bytes: i64,
    mime_type: &'a str,
    public_url: &'a str,
    uploaded_at: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseOwnerRequest<'a> {
    account_identity_id: &'a str,
    sha256: &'a str,
}

#[derive(Deserialize)]
struct BlobDescriptor {
    sha256: String,
    size: i64,
    #[serde(rename = "type")]
    mime_type: String,
    url: String,
    uploaded: i64,
}

enum ReserveError {
    UploadLimitReached,
    StorageLimitReached,
    Unavailable,
}

#[derive(Deserialize)]
struct ApiErrorResponse {
    error: ApiErrorDetails,
}

#[derive(Deserialize)]
struct ApiErrorDetails {
    code: String,
}

impl GatewayState {
    async fn authorize(
        &self,
        service: &str,
        action: &str,
        pubkey: Option<&str>,
    ) -> AuthorizationResponse {
        let request = self
            .client
            .post(format!(
                "{}/v1/internal/nostr/authorize",
                self.config.api_origin
            ))
            .bearer_auth(&self.config.shared_secret)
            .json(&AuthorizationRequest {
                service,
                action,
                public_key: pubkey,
            })
            .timeout(Duration::from_secs(5))
            .send()
            .await;

        match request {
            Ok(response) if response.status().is_success() => response
                .json::<AuthorizationResponse>()
                .await
                .unwrap_or_else(|_| AuthorizationResponse::denied()),
            Ok(response) => {
                tracing::warn!(status = %response.status(), service, action, "authorization API rejected request");
                AuthorizationResponse::denied()
            }
            Err(error) => {
                tracing::warn!(%error, service, action, "authorization API unavailable");
                AuthorizationResponse::denied()
            }
        }
    }

    async fn record_note_publication(
        &self,
        public_key: &str,
        event_id: &str,
    ) -> Result<(), NotePublicationError> {
        let response = self
            .client
            .post(format!(
                "{}/v1/internal/nostr/note-publications",
                self.config.api_origin
            ))
            .bearer_auth(&self.config.shared_secret)
            .json(&RecordNotePublicationRequest {
                public_key,
                event_id,
            })
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(|_| NotePublicationError::Unavailable)?;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            return Err(NotePublicationError::LimitReached);
        }
        if !response.status().is_success() {
            return Err(NotePublicationError::Unavailable);
        }
        Ok(())
    }

    async fn reserve_blossom_upload(
        &self,
        account_identity_id: &str,
        requested_bytes: i64,
        sha256: Option<&str>,
    ) -> Result<ReserveUploadResponse, ReserveError> {
        let response = self
            .client
            .post(format!(
                "{}/v1/internal/blossom/reservations",
                self.config.api_origin
            ))
            .bearer_auth(&self.config.shared_secret)
            .json(&ReserveUploadRequest {
                account_identity_id,
                requested_bytes,
                sha256,
            })
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(|_| ReserveError::Unavailable)?;
        if response.status() == StatusCode::PAYLOAD_TOO_LARGE {
            let code = response
                .json::<ApiErrorResponse>()
                .await
                .ok()
                .map(|body| body.error.code);
            return Err(match code.as_deref() {
                Some("blossom_upload_limit_reached") => ReserveError::UploadLimitReached,
                _ => ReserveError::StorageLimitReached,
            });
        }
        if !response.status().is_success() {
            return Err(ReserveError::Unavailable);
        }
        response.json().await.map_err(|_| ReserveError::Unavailable)
    }

    async fn commit_blossom_upload(
        &self,
        reservation_id: &str,
        descriptor: &BlobDescriptor,
    ) -> bool {
        self.retry_internal_request(|| {
            self.client
                .post(format!(
                    "{}/v1/internal/blossom/reservations/{reservation_id}/commit",
                    self.config.api_origin
                ))
                .bearer_auth(&self.config.shared_secret)
                .json(&CommitUploadRequest {
                    sha256: &descriptor.sha256,
                    size_bytes: descriptor.size,
                    mime_type: &descriptor.mime_type,
                    public_url: &descriptor.url,
                    uploaded_at: descriptor.uploaded,
                })
        })
        .await
    }

    async fn cancel_blossom_reservation(&self, reservation_id: &str) {
        let _ = self
            .client
            .delete(format!(
                "{}/v1/internal/blossom/reservations/{reservation_id}",
                self.config.api_origin
            ))
            .bearer_auth(&self.config.shared_secret)
            .timeout(Duration::from_secs(5))
            .send()
            .await;
    }

    async fn release_blossom_owner(&self, account_identity_id: &str, sha256: &str) -> bool {
        self.retry_internal_request(|| {
            self.client
                .post(format!(
                    "{}/v1/internal/blossom/owners/release",
                    self.config.api_origin
                ))
                .bearer_auth(&self.config.shared_secret)
                .json(&ReleaseOwnerRequest {
                    account_identity_id,
                    sha256,
                })
        })
        .await
    }

    async fn retry_internal_request<F>(&self, make_request: F) -> bool
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        for attempt in 0..3 {
            if make_request()
                .timeout(Duration::from_secs(5))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return true;
            }
            if attempt < 2 {
                tokio::time::sleep(Duration::from_millis(50 * (attempt + 1))).await;
            }
        }
        false
    }
}

impl AuthorizationResponse {
    fn denied() -> Self {
        Self {
            authorized: false,
            account_identity_id: None,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nostr_gateway=info".into()),
        )
        .json()
        .init();
    let config = Arc::new(GatewayConfig::from_env()?);
    let bind_addr = config.bind_addr;
    let state = GatewayState {
        config,
        client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
    };
    let app = Router::new()
        .route("/health", get(health))
        .fallback(route_request)
        .with_state(state);
    let listener = TcpListener::bind(bind_addr).await?;
    tracing::info!(%bind_addr, "Nostr gateway listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> impl IntoResponse {
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({ "status": "ok", "build": BUILD_SHA })),
    )
}

async fn route_request(State(state): State<GatewayState>, request: Request) -> Response {
    let hostname = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(':').next())
        .unwrap_or_default();

    if hostname == state.config.relay_hostname {
        return relay_request(state, request).await;
    }
    if hostname == state.config.blossom_hostname {
        return blossom_request(state, request).await;
    }
    (StatusCode::NOT_FOUND, "unknown Drum service").into_response()
}

async fn relay_request(state: GatewayState, request: Request) -> Response {
    if is_websocket_upgrade(request.headers()) {
        let (mut parts, _body) = request.into_parts();
        return match WebSocketUpgrade::from_request_parts(&mut parts, &state).await {
            Ok(upgrade) => upgrade
                .on_upgrade(move |socket| relay_session(socket, state))
                .into_response(),
            Err(rejection) => rejection.into_response(),
        };
    }

    if is_relay_information_request(&request) {
        return proxy_relay_information(request, &state.config.relay_upstream_http, &state.client)
            .await;
    }

    proxy_http(request, &state.config.relay_upstream_http, &state.client).await
}

fn is_relay_information_request(request: &Request) -> bool {
    request.method() == Method::GET
        && request.uri().path() == "/"
        && request
            .headers()
            .get(header::ACCEPT)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value.split(',').any(|media_type| {
                    media_type
                        .split(';')
                        .next()
                        .is_some_and(|value| value.trim() == "application/nostr+json")
                })
            })
}

fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
}

async fn relay_session(mut client_socket: axum::extract::ws::WebSocket, state: GatewayState) {
    let Ok((upstream_socket, _)) = connect_async(&state.config.relay_upstream_ws).await else {
        let _ = client_socket
            .send(Message::Text(
                json!(["NOTICE", "relay temporarily unavailable"])
                    .to_string()
                    .into(),
            ))
            .await;
        return;
    };
    let (mut upstream_tx, mut upstream_rx) = upstream_socket.split();
    let mut challenge_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut challenge_bytes);
    let challenge = hex::encode(challenge_bytes);
    if client_socket
        .send(Message::Text(json!(["AUTH", challenge]).to_string().into()))
        .await
        .is_err()
    {
        return;
    }

    let public_read = state
        .authorize(RELAY_SERVICE, "read", None)
        .await
        .authorized;
    let mut authenticated_pubkey: Option<String> = None;
    loop {
        tokio::select! {
            incoming = client_socket.recv() => {
                let Some(Ok(message)) = incoming else { break };
                match message {
                    Message::Text(text) => {
                        let value = serde_json::from_str::<Value>(&text).ok();
                        let command = value.as_ref()
                            .and_then(Value::as_array)
                            .and_then(|items| items.first())
                            .and_then(Value::as_str);
                        if command == Some("AUTH") {
                            let event_value = value.as_ref()
                                .and_then(Value::as_array)
                                .and_then(|items| items.get(1));
                            let Some((event_id, pubkey)) = event_value
                                .and_then(|value| verify_relay_auth(value, &challenge, &state.config.relay_public_url).ok())
                            else {
                                let _ = send_relay_ok(&mut client_socket, "", false, "auth-required: invalid NIP-42 event").await;
                                continue;
                            };
                            if state.authorize(RELAY_SERVICE, "read", Some(&pubkey)).await.authorized {
                                authenticated_pubkey = Some(pubkey);
                                let _ = send_relay_ok(&mut client_socket, &event_id, true, "").await;
                            } else {
                                let _ = send_relay_ok(&mut client_socket, &event_id, false, "restricted: public key is not approved").await;
                            }
                            continue;
                        }

                        if !can_attempt_relay_command(
                            command,
                            public_read,
                            authenticated_pubkey.is_some(),
                        ) {
                            let _ = client_socket.send(Message::Text(
                                auth_required_response().to_string().into(),
                            )).await;
                            continue;
                        }

                        if command == Some("EVENT") {
                            let event_value = value.as_ref()
                                .and_then(Value::as_array)
                                .and_then(|items| items.get(1));
                            let event_id = event_value
                                .and_then(|event| event.get("id"))
                                .and_then(Value::as_str)
                                .unwrap_or_default();
                            if event_value.is_some_and(is_protected_event)
                                && authenticated_pubkey.is_none()
                            {
                                let _ = send_relay_ok(
                                    &mut client_socket,
                                    event_id,
                                    false,
                                    "auth-required: protected events require NIP-42",
                                )
                                .await;
                                continue;
                            }
                            let Some(event) = event_value.and_then(|value| {
                                verify_relay_event(value, authenticated_pubkey.as_deref()).ok()
                            }) else {
                                let _ = send_relay_ok(&mut client_socket, event_id, false, "invalid: event signature or author").await;
                                continue;
                            };
                            let event_id = event.id.to_hex();
                            let event_pubkey = event.pubkey.to_hex();
                            if !state.authorize(RELAY_SERVICE, "write", Some(&event_pubkey)).await.authorized {
                                let _ = send_relay_ok(&mut client_socket, &event_id, false, "restricted: event author is not approved").await;
                                continue;
                            }
                            if event.kind.as_u16() == 1 {
                                match state.record_note_publication(&event_pubkey, &event_id).await {
                                    Ok(()) => {}
                                    Err(NotePublicationError::LimitReached) => {
                                        let _ = send_relay_ok(&mut client_socket, &event_id, false, "rate-limited: publishing temporarily unavailable").await;
                                        continue;
                                    }
                                    Err(NotePublicationError::Unavailable) => {
                                        let _ = send_relay_ok(&mut client_socket, &event_id, false, "error: publishing temporarily unavailable").await;
                                        continue;
                                    }
                                }
                            }
                        }

                        if upstream_tx.send(tungstenite::Message::Text(text.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Message::Binary(bytes) => {
                        if upstream_tx.send(tungstenite::Message::Binary(bytes)).await.is_err() { break; }
                    }
                    Message::Ping(bytes) => {
                        if upstream_tx.send(tungstenite::Message::Ping(bytes)).await.is_err() { break; }
                    }
                    Message::Pong(bytes) => {
                        if upstream_tx.send(tungstenite::Message::Pong(bytes)).await.is_err() { break; }
                    }
                    Message::Close(_) => break,
                }
            }
            incoming = upstream_rx.next() => {
                let Some(Ok(message)) = incoming else { break };
                let outgoing = match message {
                    tungstenite::Message::Text(text) => Message::Text(text.to_string().into()),
                    tungstenite::Message::Binary(bytes) => Message::Binary(bytes),
                    tungstenite::Message::Ping(bytes) => Message::Ping(bytes),
                    tungstenite::Message::Pong(bytes) => Message::Pong(bytes),
                    tungstenite::Message::Close(_) => break,
                    tungstenite::Message::Frame(_) => continue,
                };
                if client_socket.send(outgoing).await.is_err() { break; }
            }
        }
    }
}

async fn send_relay_ok(
    socket: &mut axum::extract::ws::WebSocket,
    event_id: &str,
    accepted: bool,
    message: &str,
) -> Result<(), axum::Error> {
    socket
        .send(Message::Text(
            json!(["OK", event_id, accepted, message])
                .to_string()
                .into(),
        ))
        .await
}

fn verify_relay_auth(value: &Value, challenge: &str, relay_url: &str) -> Result<(String, String)> {
    let event: Event = serde_json::from_value(value.clone())?;
    event.verify()?;
    if event.kind.as_u16() != NIP42_KIND || !event.content.is_empty() {
        anyhow::bail!("invalid NIP-42 event kind or content");
    }
    let tags = value
        .get("tags")
        .and_then(Value::as_array)
        .context("missing tags")?;
    if !has_json_tag(tags, "challenge", challenge) || !has_relay_tag(tags, relay_url) {
        anyhow::bail!("NIP-42 event is not bound to this session");
    }
    let now = nostr::Timestamp::now().as_secs();
    let created_at = event.created_at.as_secs();
    if created_at > now + 60 || now.saturating_sub(created_at) > 600 {
        anyhow::bail!("stale NIP-42 event");
    }
    Ok((event.id.to_hex(), event.pubkey.to_hex()))
}

fn has_json_tag(tags: &[Value], name: &str, expected: &str) -> bool {
    tags.iter().any(|tag| {
        tag.as_array().is_some_and(|items| {
            items.first().and_then(Value::as_str) == Some(name)
                && items.get(1).and_then(Value::as_str) == Some(expected)
        })
    })
}

fn has_relay_tag(tags: &[Value], expected: &str) -> bool {
    tags.iter().any(|tag| {
        tag.as_array().is_some_and(|items| {
            items.first().and_then(Value::as_str) == Some("relay")
                && items
                    .get(1)
                    .and_then(Value::as_str)
                    .is_some_and(|relay| relay_urls_match(relay, expected))
        })
    })
}

fn relay_urls_match(left: &str, right: &str) -> bool {
    left.trim_end_matches('/') == right.trim_end_matches('/')
}

fn is_protected_event(value: &Value) -> bool {
    value
        .get("tags")
        .and_then(Value::as_array)
        .is_some_and(|tags| {
            tags.iter().any(|tag| {
                tag.as_array().is_some_and(|items| {
                    items.len() == 1 && items.first().and_then(Value::as_str) == Some("-")
                })
            })
        })
}

fn verify_relay_event(value: &Value, authenticated_pubkey: Option<&str>) -> Result<Event> {
    let event: Event = serde_json::from_value(value.clone())?;
    event.verify()?;
    if authenticated_pubkey.is_some_and(|pubkey| pubkey != event.pubkey.to_hex()) {
        anyhow::bail!("event author does not match the authenticated public key");
    }
    Ok(event)
}

fn can_attempt_relay_command(
    command: Option<&str>,
    public_read: bool,
    authenticated: bool,
) -> bool {
    if command == Some("EVENT") {
        true
    } else {
        public_read || authenticated
    }
}

fn auth_required_response() -> Value {
    json!(["NOTICE", "auth-required: authenticate with NIP-42"])
}

async fn blossom_request(state: GatewayState, request: Request) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let is_upload = method == Method::PUT && path == "/upload";
    let is_upload_preflight = method == Method::HEAD && path == "/upload";
    let is_mirror = method == Method::PUT && path == "/mirror";
    let is_media = (method == Method::PUT || method == Method::HEAD) && path == "/media";
    let delete_hash = (method == Method::DELETE)
        .then(|| blossom_hash_from_path(&path))
        .flatten();
    let expected_action = if is_upload || is_upload_preflight || is_mirror {
        Some("upload")
    } else if delete_hash.is_some() {
        Some("delete")
    } else if is_media {
        Some("media")
    } else {
        None
    };
    let header_hash = request
        .headers()
        .get("x-sha-256")
        .and_then(|value| value.to_str().ok())
        .filter(|value| valid_sha256(value));
    let expected_hash = delete_hash.as_deref().or(header_hash);
    let requires_hash = is_mirror || delete_hash.is_some() || is_media;
    let write = method != Method::GET && method != Method::HEAD;
    let requires_authorization = expected_action.is_some() || (write && path != "/report");
    let authorization = if requires_authorization {
        match blossom_authorization(
            request.headers(),
            expected_action,
            &state.config.blossom_hostname,
            expected_hash,
            requires_hash,
        ) {
            Ok(authorization) => Some(authorization),
            Err(message) => return (StatusCode::UNAUTHORIZED, message).into_response(),
        }
    } else {
        None
    };
    let action = if requires_authorization {
        "write"
    } else {
        "read"
    };
    let decision = state
        .authorize(
            BLOSSOM_SERVICE,
            action,
            authorization
                .as_ref()
                .map(|authorization| authorization.public_key.as_str()),
        )
        .await;
    if !decision.authorized {
        return (StatusCode::FORBIDDEN, "Nostr public key is not approved").into_response();
    }

    if is_upload_preflight {
        let Some(account_identity_id) = decision.account_identity_id.as_deref() else {
            return (
                StatusCode::FORBIDDEN,
                "Nostr identity is not linked to an account",
            )
                .into_response();
        };
        let Some(requested_bytes) = request
            .headers()
            .get("x-content-length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|length| *length >= 0)
        else {
            return (StatusCode::LENGTH_REQUIRED, "X-Content-Length is required").into_response();
        };
        match state
            .reserve_blossom_upload(account_identity_id, requested_bytes, header_hash)
            .await
        {
            Ok(reservation) => {
                state
                    .cancel_blossom_reservation(&reservation.reservation_id)
                    .await;
            }
            Err(error) => return blossom_reservation_error(error),
        }
    }

    if is_upload || is_mirror {
        let Some(account_identity_id) = decision.account_identity_id.as_deref() else {
            return (
                StatusCode::FORBIDDEN,
                "Nostr identity is not linked to an account",
            )
                .into_response();
        };
        let requested_bytes = if is_upload {
            let Some(length) = request
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<i64>().ok())
                .filter(|length| *length >= 0)
            else {
                return (StatusCode::LENGTH_REQUIRED, "Content-Length is required").into_response();
            };
            length
        } else {
            state.config.blossom_max_write_bytes
        };
        let expected_hash = request
            .headers()
            .get("x-sha-256")
            .and_then(|value| value.to_str().ok())
            .filter(|value| valid_sha256(value))
            .or_else(|| {
                authorization
                    .as_ref()
                    .and_then(|authorization| authorization.hashes.first().map(String::as_str))
            });
        let reservation = match state
            .reserve_blossom_upload(account_identity_id, requested_bytes, expected_hash)
            .await
        {
            Ok(reservation) => reservation,
            Err(error) => return blossom_reservation_error(error),
        };
        let upstream =
            match send_http(request, &state.config.blossom_upstream_http, &state.client).await {
                Ok(response) => response,
                Err(error) => {
                    tracing::warn!(%error, "Blossom upstream request failed");
                    state
                        .cancel_blossom_reservation(&reservation.reservation_id)
                        .await;
                    return (StatusCode::BAD_GATEWAY, "service temporarily unavailable")
                        .into_response();
                }
            };
        let buffered = match buffer_upstream_response(upstream).await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(%error, "Blossom upstream response failed");
                state
                    .cancel_blossom_reservation(&reservation.reservation_id)
                    .await;
                return (StatusCode::BAD_GATEWAY, "service temporarily unavailable")
                    .into_response();
            }
        };
        if !buffered.status.is_success() {
            state
                .cancel_blossom_reservation(&reservation.reservation_id)
                .await;
            return buffered.into_response();
        }
        let descriptor = match serde_json::from_slice::<BlobDescriptor>(&buffered.body) {
            Ok(descriptor)
                if valid_blob_descriptor(&descriptor) && descriptor.size <= requested_bytes =>
            {
                descriptor
            }
            _ => {
                state
                    .cancel_blossom_reservation(&reservation.reservation_id)
                    .await;
                return (StatusCode::BAD_GATEWAY, "Invalid Blossom upload response")
                    .into_response();
            }
        };
        if !state
            .commit_blossom_upload(&reservation.reservation_id, &descriptor)
            .await
        {
            state
                .cancel_blossom_reservation(&reservation.reservation_id)
                .await;
            tracing::error!(
                reservation_id = %reservation.reservation_id,
                sha256 = %descriptor.sha256,
                "Blossom upload succeeded but accounting commit failed"
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Storage accounting unavailable",
            )
                .into_response();
        }
        return buffered.into_response();
    }

    let upstream = match send_http(request, &state.config.blossom_upstream_http, &state.client)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, "upstream request failed");
            return (StatusCode::BAD_GATEWAY, "service temporarily unavailable").into_response();
        }
    };
    if upstream.status().is_success()
        && let (Some(account_identity_id), Some(sha256)) = (
            decision.account_identity_id.as_deref(),
            delete_hash.as_deref(),
        )
        && !state
            .release_blossom_owner(account_identity_id, sha256)
            .await
    {
        tracing::error!(
            account_identity_id,
            sha256,
            "Blossom delete accounting failed"
        );
    }
    stream_upstream_response(upstream)
}

fn blossom_reservation_error(error: ReserveError) -> Response {
    match error {
        ReserveError::UploadLimitReached => (
            StatusCode::PAYLOAD_TOO_LARGE,
            "Upload exceeds the account plan's file-size limit",
        )
            .into_response(),
        ReserveError::StorageLimitReached => (
            StatusCode::PAYLOAD_TOO_LARGE,
            "Account Blossom storage limit reached",
        )
            .into_response(),
        ReserveError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Storage accounting unavailable",
        )
            .into_response(),
    }
}

struct BlossomAuthorization {
    public_key: String,
    hashes: Vec<String>,
}

fn blossom_authorization(
    headers: &HeaderMap,
    expected_action: Option<&str>,
    expected_server: &str,
    expected_hash: Option<&str>,
    require_hash: bool,
) -> Result<BlossomAuthorization, &'static str> {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or("missing Blossom Nostr authorization")?;
    let mut parts = authorization.split_whitespace();
    let scheme = parts.next().ok_or("missing Blossom Nostr authorization")?;
    let encoded = parts.next().ok_or("missing Blossom Nostr authorization")?;
    if !scheme.eq_ignore_ascii_case("nostr") || parts.next().is_some() {
        return Err("invalid Blossom authorization scheme");
    }
    let payload =
        decode_blossom_authorization(encoded).ok_or("invalid Blossom authorization encoding")?;
    let event: Event =
        serde_json::from_slice(&payload).map_err(|_| "invalid Blossom authorization event")?;
    event
        .verify()
        .map_err(|_| "invalid Blossom authorization signature")?;
    if event.kind.as_u16() != BLOSSOM_AUTH_KIND {
        return Err("invalid Blossom authorization kind");
    }
    let now = nostr::Timestamp::now().as_secs();
    if event.created_at.as_secs() > now.saturating_add(60) {
        return Err("Blossom authorization was created too far in the future");
    }
    let value: Value =
        serde_json::from_slice(&payload).map_err(|_| "invalid Blossom authorization event")?;
    let tags = value
        .get("tags")
        .and_then(Value::as_array)
        .ok_or("missing Blossom authorization tags")?;
    let expiration = tags
        .iter()
        .find_map(|tag| {
            let items = tag.as_array()?;
            (items.first()?.as_str()? == "expiration")
                .then(|| items.get(1)?.as_str()?.parse::<u64>().ok())
                .flatten()
        })
        .ok_or("missing Blossom authorization expiration")?;
    if expiration <= now {
        return Err("expired Blossom authorization");
    }
    let action = tags.iter().find_map(|tag| {
        let items = tag.as_array()?;
        (items.first()?.as_str()? == "t").then(|| items.get(1)?.as_str())?
    });
    let action = action.ok_or("missing Blossom authorization action")?;
    if expected_action.is_some_and(|expected| action != expected) {
        return Err("Blossom authorization action does not match request");
    }
    let servers: Vec<&str> = tags
        .iter()
        .filter_map(|tag| {
            let items = tag.as_array()?;
            (items.first()?.as_str()? == "server").then(|| items.get(1)?.as_str())?
        })
        .collect();
    if !servers.is_empty()
        && !servers
            .iter()
            .any(|server| blossom_server_matches(server, expected_server))
    {
        return Err("Blossom authorization server does not match request");
    }
    let hashes = tags
        .iter()
        .filter_map(|tag| {
            let items = tag.as_array()?;
            let hash = items
                .first()
                .and_then(Value::as_str)
                .is_some_and(|name| name == "x")
                .then(|| items.get(1)?.as_str())??;
            valid_sha256(hash).then(|| hash.to_string())
        })
        .collect::<Vec<_>>();
    if require_hash && hashes.is_empty() {
        return Err("missing Blossom authorization hash");
    }
    if expected_hash
        .is_some_and(|expected| !hashes.is_empty() && !hashes.iter().any(|hash| hash == expected))
    {
        return Err("Blossom authorization hash does not match request");
    }
    Ok(BlossomAuthorization {
        public_key: event.pubkey.to_hex(),
        hashes,
    })
}

fn decode_blossom_authorization(encoded: &str) -> Option<Vec<u8>> {
    [&URL_SAFE_NO_PAD, &URL_SAFE, &STANDARD_NO_PAD, &STANDARD]
        .into_iter()
        .find_map(|engine| engine.decode(encoded).ok())
}

fn blossom_server_matches(server: &str, expected_server: &str) -> bool {
    fn hostname(value: &str) -> Option<String> {
        if value.contains("://") {
            reqwest::Url::parse(value)
                .ok()?
                .host_str()
                .map(str::to_owned)
        } else {
            value
                .split('/')
                .next()?
                .split(':')
                .next()
                .map(str::to_owned)
        }
    }

    hostname(server).is_some_and(|server_host| {
        hostname(expected_server)
            .is_some_and(|expected_host| server_host.eq_ignore_ascii_case(&expected_host))
    })
}

async fn proxy_http(request: Request, upstream: &str, client: &reqwest::Client) -> Response {
    match send_http(request, upstream, client).await {
        Ok(response) => stream_upstream_response(response),
        Err(error) => {
            tracing::warn!(%error, "upstream request failed");
            (StatusCode::BAD_GATEWAY, "service temporarily unavailable").into_response()
        }
    }
}

async fn proxy_relay_information(
    request: Request,
    upstream: &str,
    client: &reqwest::Client,
) -> Response {
    let upstream = match send_http(request, upstream, client).await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, "relay information request failed");
            return (StatusCode::BAD_GATEWAY, "service temporarily unavailable").into_response();
        }
    };
    let buffered = match buffer_upstream_response(upstream).await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, "relay information response failed");
            return (StatusCode::BAD_GATEWAY, "service temporarily unavailable").into_response();
        }
    };
    if !buffered.status.is_success() {
        return buffered.into_response();
    }
    let Some(body) = rewrite_relay_information(&buffered.body) else {
        return buffered.into_response();
    };
    relay_information_response(buffered.status, &buffered.headers, body)
}

fn relay_information_response(status: StatusCode, headers: &HeaderMap, body: Vec<u8>) -> Response {
    let content_length = body.len();
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    copy_response_headers(headers, response.headers_mut());
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/nostr+json"),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        header::HeaderValue::from_str(&content_length.to_string()).unwrap(),
    );
    response
}

fn rewrite_relay_information(body: &[u8]) -> Option<Vec<u8>> {
    let mut information: Value = serde_json::from_slice(body).ok()?;
    let object = information.as_object_mut()?;
    let supported_nips = object
        .entry("supported_nips")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()?;
    if !supported_nips.iter().any(|nip| nip.as_u64() == Some(42)) {
        supported_nips.push(Value::from(42));
        supported_nips.sort_by_key(|nip| nip.as_u64().unwrap_or(u64::MAX));
    }
    let limitation = object
        .entry("limitation")
        .or_insert_with(|| Value::Object(Default::default()))
        .as_object_mut()?;
    limitation.insert("restricted_writes".to_string(), Value::Bool(true));
    serde_json::to_vec(&information).ok()
}

async fn send_http(
    request: Request,
    upstream: &str,
    client: &reqwest::Client,
) -> Result<reqwest::Response, reqwest::Error> {
    let (parts, body) = request.into_parts();
    let path = parts
        .uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/");
    let url = format!("{}{}", upstream.trim_end_matches('/'), path);
    let method = Method::from_bytes(parts.method.as_str().as_bytes()).unwrap_or(Method::GET);
    let forwards_body = method_forwards_body(&method);
    let mut builder = client.request(method, url);
    if forwards_body {
        builder = builder.body(reqwest::Body::wrap_stream(body.into_data_stream()));
    }
    for (name, value) in &parts.headers {
        if !is_hop_by_hop(name.as_str())
            && name != header::HOST
            && (forwards_body || name != header::CONTENT_LENGTH)
        {
            builder = builder.header(name, value);
        }
    }
    builder.send().await
}

fn method_forwards_body(method: &Method) -> bool {
    method != Method::GET && method != Method::HEAD
}

fn stream_upstream_response(response: reqwest::Response) -> Response {
    let status = response.status();
    let headers = response.headers().clone();
    let mut outgoing = Response::new(Body::from_stream(response.bytes_stream()));
    *outgoing.status_mut() = status;
    for (name, value) in headers {
        if let Some(name) = name
            && !is_hop_by_hop(name.as_str())
        {
            outgoing.headers_mut().append(name, value);
        }
    }
    outgoing
}

struct BufferedUpstreamResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl BufferedUpstreamResponse {
    fn into_response(self) -> Response {
        let mut outgoing = Response::new(Body::from(self.body));
        *outgoing.status_mut() = self.status;
        copy_response_headers(&self.headers, outgoing.headers_mut());
        outgoing
    }
}

async fn buffer_upstream_response(
    response: reqwest::Response,
) -> Result<BufferedUpstreamResponse, reqwest::Error> {
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await?.to_vec();
    Ok(BufferedUpstreamResponse {
        status,
        headers,
        body,
    })
}

fn copy_response_headers(source: &HeaderMap, destination: &mut HeaderMap) {
    for (name, value) in source {
        if !is_hop_by_hop(name.as_str()) {
            destination.append(name, value.clone());
        }
    }
}

fn blossom_hash_from_path(path: &str) -> Option<String> {
    let value = path.strip_prefix('/')?.split('.').next()?;
    valid_sha256(value).then(|| value.to_string())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|character| character.is_ascii_hexdigit() && !character.is_ascii_uppercase())
}

fn valid_blob_descriptor(descriptor: &BlobDescriptor) -> bool {
    valid_sha256(&descriptor.sha256)
        && descriptor.size >= 0
        && descriptor.uploaded >= 0
        && !descriptor.mime_type.trim().is_empty()
        && descriptor.mime_type.len() <= 255
        && descriptor.mime_type.contains('/')
        && descriptor.url.len() <= 2_048
        && reqwest::Url::parse(&descriptor.url)
            .ok()
            .is_some_and(|url| matches!(url.scheme(), "http" | "https") && url.host().is_some())
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} must be set"))
}

fn positive_i64_env(name: &str, default: &str) -> Result<i64> {
    let value = env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<i64>()
        .with_context(|| format!("{name} must be a valid i64"))?;
    if value <= 0 {
        anyhow::bail!("{name} must be greater than zero");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag};

    #[test]
    fn verifies_bound_nip42_event() {
        let challenge = "challenge";
        let relay = "wss://relay.drum.dev";
        let event = EventBuilder::new(Kind::Authentication, "")
            .tags([
                Tag::parse(["relay", relay]).unwrap(),
                Tag::parse(["challenge", challenge]).unwrap(),
            ])
            .sign_with_keys(&Keys::generate())
            .unwrap();
        let value = serde_json::to_value(event).unwrap();

        assert!(verify_relay_auth(&value, challenge, "wss://relay.drum.dev/").is_ok());
        assert!(verify_relay_auth(&value, "another", "wss://relay.drum.dev/").is_err());
        assert!(verify_relay_auth(&value, challenge, "wss://another-relay.example/").is_err());
    }

    #[test]
    fn signed_events_can_attempt_publish_without_nip42() {
        assert!(can_attempt_relay_command(Some("REQ"), true, false));
        assert!(can_attempt_relay_command(Some("CLOSE"), true, false));
        assert!(can_attempt_relay_command(Some("EVENT"), true, false));
        assert!(can_attempt_relay_command(Some("EVENT"), true, true));
        assert!(!can_attempt_relay_command(Some("REQ"), false, false));
    }

    #[test]
    fn verifies_signed_event_without_nip42() {
        let event = EventBuilder::new(Kind::TextNote, "hello")
            .sign_with_keys(&Keys::generate())
            .unwrap();
        let value = serde_json::to_value(event).unwrap();

        assert!(verify_relay_event(&value, None).is_ok());
    }

    #[test]
    fn rejects_tampered_event_without_nip42() {
        let event = EventBuilder::new(Kind::TextNote, "hello")
            .sign_with_keys(&Keys::generate())
            .unwrap();
        let mut value = serde_json::to_value(event).unwrap();
        value["content"] = json!("tampered");

        assert!(verify_relay_event(&value, None).is_err());
    }

    #[test]
    fn identifies_nip70_protected_events() {
        assert!(is_protected_event(&json!({ "tags": [["-"]] })));
        assert!(!is_protected_event(&json!({ "tags": [] })));
        assert!(!is_protected_event(
            &json!({ "tags": [["-", "unexpected"]] })
        ));
    }

    #[test]
    fn authenticated_event_author_must_match_session() {
        let author = Keys::generate();
        let another_author = Keys::generate();
        let event = EventBuilder::new(Kind::TextNote, "hello")
            .sign_with_keys(&author)
            .unwrap();
        let value = serde_json::to_value(event).unwrap();

        assert!(verify_relay_event(&value, Some(&author.public_key().to_hex())).is_ok());
        assert!(verify_relay_event(&value, Some(&another_author.public_key().to_hex())).is_err());
    }

    #[test]
    fn relay_information_advertises_nip42_and_restricted_writes() {
        let rewritten = rewrite_relay_information(
            br#"{"name":"Drum Relay","supported_nips":[1,11],"limitation":{"payment_required":false}}"#,
        )
        .unwrap();
        let information: Value = serde_json::from_slice(&rewritten).unwrap();

        assert_eq!(information["name"], "Drum Relay");
        assert_eq!(information["supported_nips"], json!([1, 11, 42]));
        assert_eq!(information["limitation"]["payment_required"], false);
        assert_eq!(information["limitation"]["restricted_writes"], true);
    }

    #[test]
    fn rewritten_relay_information_uses_its_new_content_length() {
        let original = br#"{"supported_nips":[1]}"#;
        let rewritten = rewrite_relay_information(original).unwrap();
        let original_length = original.len().to_string();
        let rewritten_length = rewritten.len().to_string();

        assert_ne!(original_length, rewritten_length);
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_LENGTH,
            header::HeaderValue::from_str(&original_length).unwrap(),
        );
        let response = relay_information_response(StatusCode::OK, &headers, rewritten);

        assert_eq!(
            response.headers().get(header::CONTENT_LENGTH).unwrap(),
            rewritten_length.as_str()
        );
    }

    #[test]
    fn bodyless_proxy_methods_do_not_forward_streaming_bodies() {
        assert!(!method_forwards_body(&Method::GET));
        assert!(!method_forwards_body(&Method::HEAD));
        assert!(method_forwards_body(&Method::POST));
        assert!(method_forwards_body(&Method::PUT));
        assert!(method_forwards_body(&Method::DELETE));
    }

    #[test]
    fn extracts_lowercase_hashes_from_paths() {
        let hash = "a".repeat(64);
        assert_eq!(blossom_hash_from_path(&format!("/{hash}.jpg")), Some(hash));
        assert_eq!(blossom_hash_from_path("/not-a-hash"), None);
    }

    #[test]
    fn verifies_url_safe_blossom_authorization_and_request_bindings() {
        let keys = Keys::generate();
        let hash = "b".repeat(64);
        let expiration = (nostr::Timestamp::now().as_secs() + 60).to_string();
        let event = EventBuilder::new(Kind::Custom(BLOSSOM_AUTH_KIND), "Authorize upload")
            .tags([
                Tag::parse(["t", "upload"]).unwrap(),
                Tag::parse(["expiration", expiration.as_str()]).unwrap(),
                Tag::parse(["x", hash.as_str()]).unwrap(),
                Tag::parse(["server", "https://media.drum.dev/upload"]).unwrap(),
            ])
            .sign_with_keys(&keys)
            .unwrap();
        let encoded = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&event).unwrap());
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("nostr {encoded}").parse().unwrap(),
        );

        let authorization = blossom_authorization(
            &headers,
            Some("upload"),
            "media.drum.dev",
            Some(&hash),
            true,
        )
        .unwrap();
        assert_eq!(authorization.public_key, keys.public_key().to_hex());
        assert_eq!(authorization.hashes, vec![hash]);

        assert!(
            blossom_authorization(&headers, Some("delete"), "media.drum.dev", None, false,)
                .is_err()
        );
        assert!(
            blossom_authorization(&headers, Some("upload"), "another.example", None, false,)
                .is_err()
        );
        assert!(
            blossom_authorization(
                &headers,
                Some("upload"),
                "media.drum.dev",
                Some(&"c".repeat(64)),
                true,
            )
            .is_err()
        );
    }

    #[test]
    fn accepts_legacy_standard_base64_blossom_authorization() {
        let expiration = (nostr::Timestamp::now().as_secs() + 60).to_string();
        let event = EventBuilder::new(Kind::Custom(BLOSSOM_AUTH_KIND), "Authorize upload")
            .tags([
                Tag::parse(["t", "upload"]).unwrap(),
                Tag::parse(["expiration", expiration.as_str()]).unwrap(),
            ])
            .sign_with_keys(&Keys::generate())
            .unwrap();
        let encoded = STANDARD.encode(serde_json::to_vec(&event).unwrap());
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Nostr {encoded}").parse().unwrap(),
        );

        assert!(
            blossom_authorization(&headers, Some("upload"), "media.drum.dev", None, false,).is_ok()
        );
    }

    #[test]
    fn validates_gallery_metadata_from_blob_descriptors() {
        let descriptor = BlobDescriptor {
            sha256: "d".repeat(64),
            size: 42,
            mime_type: "image/webp".to_string(),
            url: format!("https://cdn.drum.dev/{}.webp", "d".repeat(64)),
            uploaded: 1_700_000_000,
        };
        assert!(valid_blob_descriptor(&descriptor));

        let invalid = BlobDescriptor {
            url: "file:///tmp/blob.webp".to_string(),
            ..descriptor
        };
        assert!(!valid_blob_descriptor(&invalid));
    }
}
