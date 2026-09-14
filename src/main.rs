use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Body,
    extract::{FromRequestParts, Request, State, WebSocketUpgrade, ws::Message},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::STANDARD};
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
                .unwrap_or_else(|_| "blossom.drum.dev".to_string()),
            blossom_upstream_http: required_env("BLOSSOM_UPSTREAM_HTTP")?,
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

#[derive(Deserialize)]
struct AuthorizationResponse {
    authorized: bool,
}

impl GatewayState {
    async fn authorize(&self, service: &str, action: &str, pubkey: Option<&str>) -> bool {
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
                .map(|response| response.authorized)
                .unwrap_or(false),
            Ok(response) => {
                tracing::warn!(status = %response.status(), service, action, "authorization API rejected request");
                false
            }
            Err(error) => {
                tracing::warn!(%error, service, action, "authorization API unavailable");
                false
            }
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
        .route("/health", get(|| async { Json(json!({ "status": "ok" })) }))
        .fallback(route_request)
        .with_state(state);
    let listener = TcpListener::bind(bind_addr).await?;
    tracing::info!(%bind_addr, "Nostr gateway listening");
    axum::serve(listener, app).await?;
    Ok(())
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

    proxy_http(request, &state.config.relay_upstream_http, &state.client).await
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
                            if state.authorize(RELAY_SERVICE, "read", Some(&pubkey)).await {
                                authenticated_pubkey = Some(pubkey);
                                let _ = send_relay_ok(&mut client_socket, &event_id, true, "").await;
                            } else {
                                let _ = send_relay_ok(&mut client_socket, &event_id, false, "restricted: public key is not approved").await;
                            }
                            continue;
                        }

                        let Some(authenticated) = authenticated_pubkey.as_deref() else {
                            let _ = client_socket.send(Message::Text(
                                json!(["NOTICE", "auth-required: authenticate with NIP-42"])
                                    .to_string().into()
                            )).await;
                            continue;
                        };

                        if command == Some("EVENT") {
                            let event_value = value.as_ref()
                                .and_then(Value::as_array)
                                .and_then(|items| items.get(1));
                            let event = event_value.and_then(|value| serde_json::from_value::<Event>(value.clone()).ok());
                            let valid = event.as_ref().is_some_and(|event| {
                                event.verify().is_ok() && event.pubkey.to_hex() == authenticated
                            });
                            let event_id = event.as_ref().map(|event| event.id.to_hex()).unwrap_or_default();
                            if !valid || !state.authorize(RELAY_SERVICE, "write", Some(authenticated)).await {
                                let _ = send_relay_ok(&mut client_socket, &event_id, false, "restricted: event author is not approved").await;
                                continue;
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
    if !has_json_tag(tags, "challenge", challenge) || !has_json_tag(tags, "relay", relay_url) {
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

async fn blossom_request(state: GatewayState, request: Request) -> Response {
    let write = !matches!(
        *request.method(),
        axum::http::Method::GET | axum::http::Method::HEAD
    );
    let public_key = if write {
        match blossom_authorization_pubkey(request.headers()) {
            Ok(public_key) => Some(public_key),
            Err(message) => return (StatusCode::UNAUTHORIZED, message).into_response(),
        }
    } else {
        None
    };
    let action = if write { "write" } else { "read" };
    if !state
        .authorize(BLOSSOM_SERVICE, action, public_key.as_deref())
        .await
    {
        return (StatusCode::FORBIDDEN, "Nostr public key is not approved").into_response();
    }
    proxy_http(request, &state.config.blossom_upstream_http, &state.client).await
}

fn blossom_authorization_pubkey(headers: &HeaderMap) -> Result<String, &'static str> {
    let encoded = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Nostr "))
        .ok_or("missing Blossom Nostr authorization")?;
    let payload = STANDARD
        .decode(encoded)
        .map_err(|_| "invalid Blossom authorization encoding")?;
    let event: Event =
        serde_json::from_slice(&payload).map_err(|_| "invalid Blossom authorization event")?;
    event
        .verify()
        .map_err(|_| "invalid Blossom authorization signature")?;
    if event.kind.as_u16() != BLOSSOM_AUTH_KIND {
        return Err("invalid Blossom authorization kind");
    }
    let value: Value =
        serde_json::from_slice(&payload).map_err(|_| "invalid Blossom authorization event")?;
    let expiration = value
        .get("tags")
        .and_then(Value::as_array)
        .and_then(|tags| {
            tags.iter().find_map(|tag| {
                let items = tag.as_array()?;
                (items.first()?.as_str()? == "expiration")
                    .then(|| items.get(1)?.as_str()?.parse::<u64>().ok())
                    .flatten()
            })
        })
        .ok_or("missing Blossom authorization expiration")?;
    if expiration <= nostr::Timestamp::now().as_secs() {
        return Err("expired Blossom authorization");
    }
    Ok(event.pubkey.to_hex())
}

async fn proxy_http(request: Request, upstream: &str, client: &reqwest::Client) -> Response {
    let (parts, body) = request.into_parts();
    let path = parts
        .uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/");
    let url = format!("{}{}", upstream.trim_end_matches('/'), path);
    let method = Method::from_bytes(parts.method.as_str().as_bytes()).unwrap_or(Method::GET);
    let mut builder = client
        .request(method, url)
        .body(reqwest::Body::wrap_stream(body.into_data_stream()));
    for (name, value) in &parts.headers {
        if !is_hop_by_hop(name.as_str()) && name != header::HOST {
            builder = builder.header(name, value);
        }
    }
    let response = match builder.send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, "upstream request failed");
            return (StatusCode::BAD_GATEWAY, "service temporarily unavailable").into_response();
        }
    };
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

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag};

    #[test]
    fn verifies_bound_nip42_event() {
        let challenge = "challenge";
        let relay = "wss://relay.drum.dev/";
        let event = EventBuilder::new(Kind::Authentication, "")
            .tags([
                Tag::parse(["relay", relay]).unwrap(),
                Tag::parse(["challenge", challenge]).unwrap(),
            ])
            .sign_with_keys(&Keys::generate())
            .unwrap();
        let value = serde_json::to_value(event).unwrap();

        assert!(verify_relay_auth(&value, challenge, relay).is_ok());
        assert!(verify_relay_auth(&value, "another", relay).is_err());
    }
}
