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

`.github/workflows/deploy-shared.yml` tests the gateway and builds the release
binary inside Debian Bookworm, matching the gateway operating system. It
publishes both a public, immutable,
commit-specific prerelease and checksum-verified stable deployment assets, then
reboots the existing Vultr VM. A one-shot systemd updater installs the stable
asset during boot before the gateway starts.

Deployments do not reinstall the VM. Caddy, `/var/lib/caddy`, certificates,
gateway configuration, and the operating system remain intact. The updater
runs only during boot and does not poll GitHub.

Configure these repository Actions secrets:

- `VULTR_API_KEY`: the deployment API key.

The normal deployment workflow does not need `GATEWAY_SHARED_SECRET` or
`DRUM_API_ORIGIN`; they remain in the VM's protected gateway environment file.
The VM downloads public release assets and receives no GitHub credentials.

The one-time `Recover shared gateway` workflow rebuilds an existing VM with a
static Cloudflare Origin CA certificate. It requires the existing recovery
values plus the `CLOUDFLARE_ORIGIN_CERT` and `CLOUDFLARE_ORIGIN_KEY` repository
secrets, and only runs when manually dispatched with `REBUILD`. Its recovery
user-data is cleared after every attempt. Normal deployments never read these
certificate secrets or modify Caddy.

For an existing VM created before the updater was introduced, push these
changes and run `Recover shared gateway` once. The normal deployment workflow
is manual-only during this migration so pushing the recovery code cannot reboot
or reinstall the VM. Automatic push deployments can be enabled after recovery.
