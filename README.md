# Telegram MCP Bridge

[![CI](https://github.com/ph4ntom-rev/telegram-mcp-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/ph4ntom-rev/telegram-mcp-bridge/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-3776AB.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A low-latency, durable bridge between the Telegram Bot API and MCP clients such as Codex, ChatGPT desktop/CLI, or any other compatible agent. The project does not include its own LLM; it gives an agent a deliberately restricted toolset for receiving, acknowledging, and sending Telegram messages.

## Why two operating modes?

| Profile | MCP transport | Telegram ingress | Intended use |
|---|---|---|---|
| Local (recommended for Codex) | `stdio` | Long polling | Minimal attack surface, no listening port, and near-immediate event delivery |
| Server | Stateless Streamable HTTP | HTTPS webhook | An always-on remote service behind a TLS reverse proxy |

SQLite in WAL mode is the source of truth. A webhook returns `200` only after commit, and long polling advances its offset only after a durable insert. The bridge uses a `claim → lease → ack` workflow. Incoming events are delivered at least once, with repeatedly unacknowledged events retained in quarantine for operator review.

> MCP cannot wake a stopped Codex task by itself. While an agent is running, it can wait for events with `telegram_wait_updates`. An always-on agent loop or scheduled task is required for autonomous 24/7 responses.

## Quick local setup on Windows

1. Create a bot with `@BotFather` and store the token securely.
2. Clone the repository, create a virtual environment, and install the hash-locked dependencies:

   ```powershell
   git clone https://github.com/ph4ntom-rev/telegram-mcp-bridge.git
   Set-Location telegram-mcp-bridge
   py -m venv .venv
   .\.venv\Scripts\python.exe -m pip install --require-hashes -r requirements.lock
   .\.venv\Scripts\python.exe -m pip install --no-deps -e .
   Copy-Item .env.example .env
   ```

3. Set `TELEGRAM_BOT_TOKEN` and the numeric allowlists in `.env`. `TELEGRAM_ALLOWED_USER_IDS` authorizes inbound actors. `TELEGRAM_ALLOWED_CHAT_IDS` authorizes both inbound chats and outbound destinations. If both lists are populated, an event must satisfy both. Usernames are never used for authorization.
4. If the bot already has a webhook, explicitly disable it without dropping pending updates:

   ```powershell
   .\.venv\Scripts\telegram-mcp.exe webhook delete --env-file .env
   ```

5. Verify storage and Telegram connectivity:

   ```powershell
   .\.venv\Scripts\telegram-mcp.exe doctor --env-file .env
   ```

6. Add the stdio server in Codex through Settings → MCP servers → Add server, using absolute paths:

   ```text
   command: C:\absolute\path\telegram-mcp-bridge\.venv\Scripts\telegram-mcp.exe
   arguments: serve --transport stdio --env-file C:\absolute\path\telegram-mcp-bridge\.env
   ```

   An equivalent `config.toml` entry is provided in [`codex-config.toml.example`](codex-config.toml.example).

After restarting Codex, the tools appear under the `telegram` MCP server.

## MCP tools

- `telegram_wait_updates` atomically leases the next batch of events and can wait for up to 30 seconds.
- `telegram_ack_updates` acknowledges events only with the correct lease token.
- `telegram_release_updates` releases a batch after a temporary failure.
- `telegram_reply` replies to a stored event and is safer than copying a raw `chat_id`.
- `telegram_send_text` sends only to chats explicitly present in `TELEGRAM_ALLOWED_CHAT_IDS`.
- `telegram_edit_text`, `telegram_delete_message`, `telegram_typing`, and `telegram_answer_callback` provide bounded message operations.
- `telegram_peek_updates`, `telegram_list_chats`, and `telegram_bridge_status` are diagnostic read-only tools.

Every logical send requires an `idempotency_key`, for example `reply:824991:0`. Reusing the same key with the same payload returns the stored result; reusing it with a different payload is rejected.

## Delivery guarantees

| Boundary | Guarantee |
|---|---|
| Telegram → durable inbox | Effectively once per `(bot_key, update_id)` |
| Durable inbox → MCP consumer | At least once until a valid acknowledgement |
| MCP → Telegram `sendMessage` | Cached idempotency when the outcome is known; `uncertain` after an ambiguous network outcome |

The Telegram Bot API does not accept idempotency keys. If Telegram receives a message but the TCP response is lost, the bridge cannot distinguish success from failure. It records the operation as `uncertain` and does **not** retry automatically, preventing hidden duplicates.

## Server and webhook profile

1. Generate two independent secrets; do not reuse the bot token:

   ```powershell
   py -c "import secrets; print(secrets.token_urlsafe(32)); print(secrets.token_urlsafe(32))"
   ```

2. Configure `.env`:

   ```dotenv
   BRIDGE_TRANSPORT=http
   TELEGRAM_INGRESS_MODE=webhook
   MCP_AUTH_TOKEN=<first secret, at least 32 characters>
   TELEGRAM_WEBHOOK_SECRET=<second secret>
   PUBLIC_BASE_URL=https://bridge.example.com
   MCP_HOST=127.0.0.1
   MCP_ALLOWED_HOSTS=bridge.example.com
   ```

3. Run the service behind a TLS reverse proxy, then register the webhook:

   ```powershell
   .\.venv\Scripts\telegram-mcp.exe serve --transport http --env-file .env
   .\.venv\Scripts\telegram-mcp.exe webhook set --env-file .env
   ```

The MCP endpoint is `https://bridge.example.com/mcp`; the webhook is `https://bridge.example.com/telegram/webhook`; health endpoints are `/healthz` and `/readyz`. For remote MCP, Codex supplies `Authorization: Bearer ...` through `bearer_token_env_var`.

Production requirements include TLS 1.2+, one process worker per SQLite file, a local non-NFS disk, an explicit Host allowlist, filtering of Telegram's official source CIDRs at the reverse proxy, and backups through the SQLite Online Backup API. Replace SQLite with a server database and distributed queue before scaling horizontally.

### Docker Compose

After completing `.env`, start the server profile with:

```powershell
docker compose up --build -d
```

The container is unprivileged, its root filesystem is read-only, and the database is stored in a dedicated volume. Port 8000 is published only on `127.0.0.1`; an external TLS reverse proxy must sit in front of it. Do not run multiple replicas against one SQLite volume.

## Security boundaries

- Ingress and outbound operations enforce numeric allowlists. An empty ingress allowlist fails closed unless `TELEGRAM_ALLOW_ALL=true` is explicitly set.
- An incoming event never grants outbound access. Even with `TELEGRAM_ALLOW_ALL=true`, outbound sending remains blocked until the chat ID is explicitly listed in `TELEGRAM_ALLOWED_CHAT_IDS`.
- There is no generic `call_api`, arbitrary URL fetch, SQL execution, or caller-supplied filesystem path.
- Attachments are exposed only as validated metadata and opaque Telegram `file_id` values; the bridge intentionally does not download arbitrary files.
- Plain text is the default. Formatted messages longer than 4096 characters are rejected to avoid corrupting Telegram entities.
- The webhook validates `X-Telegram-Bot-Api-Secret-Token` with a constant-time comparison and limits the request body to 1 MiB.
- The bot token appears in Telegram API URLs, so exceptions, responses, and access logs are explicitly redacted.
- Telegram text is marked `untrusted_content=true`; it must not control secrets, allowlists, system policy, or unrelated agent actions.
- HTTP MCP always requires a bearer token and enables Host/Origin protection.

See [`SECURITY.md`](SECURITY.md) for the operational security guide and [`AUDIT.md`](AUDIT.md) for the threat model, verified properties, and residual limitations.

## Verification

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\mypy.exe src
.\.venv\Scripts\pytest.exe --cov
.\.venv\Scripts\bandit.exe -c pyproject.toml -q -r src
.\.venv\Scripts\pip-audit.exe --require-hashes -r requirements.lock
```

GitHub Actions checks Linux on Python 3.10/3.12 and Windows/macOS on Python 3.12. It also builds the production Docker image and tests its installed package with networking disabled. Runtime dependencies are installed with mandatory SHA-256 verification from `requirements.lock`.

Run the local synthetic benchmark without contacting Telegram:

```powershell
.\.venv\Scripts\python.exe benchmarks\latency.py
```

## Configuration and operations

Every configuration variable and its secure default is documented in [`.env.example`](.env.example). Never pass secrets as process arguments or commit `.env`. Terminal outbox records (`sent`, `dead`, and `uncertain`) form the idempotency ledger; monitor disk usage and do not delete them without explicitly accepting the loss of replay protection.

Use [`OPERATIONS.md`](OPERATIONS.md) for quarantine review, manual resolution of uncertain deliveries, verified backups and schema-2 migration. Operator queue commands run offline and never send Telegram messages. Credential rotation and incident response remain documented in [`SECURITY.md`](SECURITY.md); verified behavior and remaining limitations are in [`AUDIT.md`](AUDIT.md).
