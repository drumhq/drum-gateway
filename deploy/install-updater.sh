#!/bin/sh
set -eu

install -d -m 0755 /usr/local/libexec

cat > /usr/local/libexec/update-drum-gateway <<'EOF'
#!/bin/sh
set -eu

artifact_base=https://github.com/drumhq/drum-gateway/releases/download/gateway-shared
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT

curl --fail --location --retry 5 --retry-all-errors \
  --header 'Cache-Control: no-cache' \
  --output "$work_dir/drum-gateway-linux-amd64" \
  "$artifact_base/drum-gateway-linux-amd64"
curl --fail --location --retry 5 --retry-all-errors \
  --header 'Cache-Control: no-cache' \
  --output "$work_dir/drum-gateway-linux-amd64.sha256" \
  "$artifact_base/drum-gateway-linux-amd64.sha256"

(cd "$work_dir" && sha256sum --check --status drum-gateway-linux-amd64.sha256)

if [ -x /usr/local/bin/drum-gateway ] && \
  cmp --silent "$work_dir/drum-gateway-linux-amd64" /usr/local/bin/drum-gateway; then
  exit 0
fi

install -m 0755 -o root -g root \
  "$work_dir/drum-gateway-linux-amd64" /usr/local/bin/drum-gateway
EOF
chmod 0755 /usr/local/libexec/update-drum-gateway

cat > /etc/systemd/system/drum-gateway-update.service <<'EOF'
[Unit]
Description=Install the published Drum gateway release
After=network-online.target
Wants=network-online.target
Before=drum-gateway.service

[Service]
Type=oneshot
ExecStart=/usr/local/libexec/update-drum-gateway

[Install]
WantedBy=multi-user.target
EOF

install -d -m 0755 /etc/systemd/system/drum-gateway.service.d
cat > /etc/systemd/system/drum-gateway.service.d/update.conf <<'EOF'
[Unit]
After=drum-gateway-update.service
Wants=drum-gateway-update.service
EOF

systemctl daemon-reload
systemctl enable drum-gateway-update.service
