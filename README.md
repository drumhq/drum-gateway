# Drum Gateway

Drum Gateway is the public authorization boundary for Drum's Nostr relay and
Blossom services. It runs behind Cloudflare and Caddy, verifies signed Nostr
requests, asks the Drum API whether the signing public key has an active grant,
and proxies approved traffic to private Vultr VPC origins.

## Enforcement

- Relay subscriptions are public. Clients may publish signed events directly
  or authenticate first with NIP-42.
- Relay events must have a valid signature and an active `shared-relay` write
  grant for the event author's public key. For NIP-42 sessions, the
  authenticated key must also match the event author.
- NIP-70 protected events still require NIP-42 authentication so they cannot
  be replayed by a third party.
- Blossom uploads, mirrors, and deletes require a valid kind `24242`
  authorization event and an active `shared-blossom` write grant.
- Blossom uploads and mirrors reserve account storage before reaching the
  upstream service, then commit the returned hash and size to account-level
  usage. Deletes release ownership after the upstream accepts them.
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

## Shared gateway deployment

Pushes to `main` run `.github/workflows/deploy-shared.yml`. The workflow tests
the gateway and builds the release binary inside Debian Bookworm, matching the
gateway operating system. It publishes the binary as a public, immutable,
commit-specific GitHub prerelease, verifies the SHA-256 checksum on the VM, and
installs the gateway as a hardened systemd service. Vultr user-data is removed
after the deployment attempt.

Configure these repository Actions secrets:

- `VULTR_API_KEY`: the deployment API key.
- `GATEWAY_SHARED_SECRET`: the Drum API machine-to-machine secret.

Configure one repository Actions variable:

- `DRUM_API_ORIGIN`: the Drum API used for authorization and accounting.

The gateway receives the shared API secret and a public, commit-specific binary
URL. It does not receive GitHub credentials.
