# Security Guide

This bridge passes untrusted Telegram content to an MCP-enabled agent. Security depends on the code, correct allowlists, secure credential storage, and a properly configured network boundary.

## Secrets

- `TELEGRAM_BOT_TOKEN`, `MCP_AUTH_TOKEN`, and `TELEGRAM_WEBHOOK_SECRET` must be independent random values.
- Store them in a secret manager or an owner-only local `.env`. Never expose a token in process arguments, reverse-proxy URLs, issues, or logs.
- For the HTTP profile, use at least 32 random URL-safe characters for each service secret.
- Never authorize by Telegram username; use numeric user and chat IDs only.

If the bot token is exposed, revoke it immediately through `@BotFather`, issue a new token, stop the old bridge, update the secret, and start a new instance. If an MCP or webhook secret is exposed, replace that value and restart the bridge and reverse proxy. The existing database may be retained: deduplication is namespaced by a short irreversible SHA-256 token digest, not the token itself.

## Authorization model

- `TELEGRAM_ALLOWED_USER_IDS` authorizes inbound actors.
- `TELEGRAM_ALLOWED_CHAT_IDS` authorizes inbound chats and is the only source of outbound permissions.
- If both lists are configured, an inbound event must satisfy both.
- `TELEGRAM_ALLOW_ALL=true` opens ingress only; it does not grant permission to send to arbitrary chats.
- An inbound event never adds its chat to the durable outbound ACL. When a chat ID is removed from configuration, restart atomically revokes it and moves unfinished sends for that chat to a safe terminal state.

Grant the agent only the MCP access it needs. Telegram text is marked `untrusted_content`; do not allow it to change system policy, secrets, allowlists, or initiate unrelated external actions without a separate agent policy.

## Local profile

The preferred Codex setup is MCP over `stdio` with Telegram long polling. It makes outbound HTTPS connections only and opens no listening port. The `.env`, database directory, and backups should belong to one OS account. Use `umask 077` on POSIX; on Windows, restrict directory ACLs to the service account.

## HTTP and webhook profile

A reverse proxy is required in front of the application:

1. Use TLS 1.2+ with a valid public certificate.
2. Filter current Telegram source CIDRs according to the [official webhook guide](https://core.telegram.org/bots/webhooks), while the application validates `X-Telegram-Bot-Api-Secret-Token`.
3. Configure an explicit `Host` allowlist and an `Origin` allowlist if MCP is called from a browser environment.
4. Limit request bodies to no more than `BRIDGE_MAX_UPDATE_BYTES` and enforce reasonable connection and request limits at the edge.
5. Expose the MCP endpoint only with `Authorization: Bearer MCP_AUTH_TOKEN`.
6. Run only one worker/process per SQLite file. Multiple replicas require a server database and distributed queue.

Do not expose the built-in Uvicorn server directly to the internet. `docker-compose.yml` intentionally publishes the application port on loopback only.

## Database, backup, and recovery

- Keep SQLite on a local disk. NFS/SMB and sharing one volume between multiple processes are unsupported.
- The bridge uses WAL and `synchronous=FULL`. Do not copy the `.sqlite3` file alone while the service is running; use the SQLite Online Backup API or briefly stop the service and preserve a consistent file set.
- Encrypt backups and test restoration. The database contains complete message text and opaque Telegram `file_id` values.
- Acknowledged inbox rows are removed according to the retention policy. Outbox tombstones intentionally remain as an idempotency ledger; monitor volume size and free disk space.
- When the queue is full, the webhook returns non-2xx and polling does not advance its offset, allowing Telegram to retry. Free disk or queue capacity without manually deleting the active database.

## Ambiguous delivery

The Telegram Bot API does not accept idempotency keys. If a connection ends after the request body may have been transmitted, the bridge records `uncertain` and does not retry automatically. An operator must inspect the chat and then either keep the record or explicitly send again with a new key. Automatically retrying an `uncertain` operation can create duplicates.

## Updates

Before updating:

1. Review the MCP SDK and Telegram Bot API release notes.
2. Rebuild `requirements.lock` with hashes in a trusted environment.
3. Run the test suite, Ruff, mypy, Bandit, and `pip-audit`.
4. Create a consistent database backup.
5. Restart only one replica; startup recovery marks an unfinished network send as `uncertain`.

Detailed verified properties and residual risks are listed in [`AUDIT.md`](AUDIT.md).
