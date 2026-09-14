# Drum Gateway Agent Guide

## Scope

- This repository is only the public Nostr/Blossom authorization gateway.
- Do not add database credentials, user sessions, Tigris credentials, or Nostr
  private keys. Authorization decisions come from the Drum API.
- Fail closed when authentication, signature verification, or the API check is
  unavailable.

## Security invariants

- Relay clients authenticate with NIP-42 before subscriptions or publishing.
- Published events must be signed by the authenticated public key.
- Blossom write requests require a valid kind 24242 authorization event.
- Never log authorization headers, signed event bodies, or the shared secret.
- Relay and Blossom upstreams are private VPC HTTP/WebSocket addresses only.

## Verification

Run `cargo fmt --check`, `cargo check --all-targets`, and `cargo test` before
hand-off.
