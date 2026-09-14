# Drum Gateway

Drum Gateway is the public authorization boundary for Drum's Nostr relay and
Blossom services. It runs behind Cloudflare and Caddy, verifies signed Nostr
requests, asks the Drum API whether the signing public key has an active grant,
and proxies approved traffic to private Vultr VPC origins.

## Enforcement

- Relay WebSocket sessions require NIP-42 authentication.
- Relay events must be valid, authored by the authenticated key, and have an
  active `shared-relay` write grant.
- Blossom uploads, mirrors, and deletes require a valid kind `24242`
  authorization event and an active `shared-blossom` write grant.
- Blossom downloads remain public so media works in ordinary Nostr clients.
- Authorization fails closed when the Drum API is unavailable.

The gateway receives only a machine-to-machine API secret. It does not receive
Postgres, Tigris, login-session, or Nostr private-key credentials.

## Development

Copy `.env.example` to `.env`, replace its values, then run:

```sh
cargo run
```

Verify changes with:

```sh
cargo fmt --check
cargo check --all-targets
cargo test
```
