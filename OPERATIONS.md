# Queue recovery and backups

## Upgrade

Stop the bridge and take a complete SQLite backup before upgrading an existing database. The first open migrates schema 1 to schema 2 with an additive quarantine table in a transaction. IDs, offsets, authorizations and outbox fingerprints remain intact. Older bridge versions refuse schema 2; do not change `user_version` manually or run the old worker alongside the upgrade.

## Poison inbox events

An event is quarantined after `BRIDGE_MAX_DELIVERY_ATTEMPTS` unacknowledged deliveries (default 5, range 1–100). Release quarantines an exhausted event immediately. An expired lease is checked before the next claim; a live lease is never taken away. Quarantine survives restarts and is excluded from normal claims, so later events can proceed.

Quarantined records still count toward queue capacity and are not automatically purged. Status exposes `inbox.quarantined` and `attention_required` counts. This bounds retained poison data and makes an eventual full queue visible rather than silently losing events. Delivery remains at-least-once; workers must tolerate duplicates and acknowledge successfully processed events.

Stop the bridge before these operator commands. They hold the database instance lock, refuse an active worker, and do not start ingress, startup recovery or a Telegram network client. Use the same environment file and bot token as the service; records are scoped to that token's namespace.

```text
telegram-mcp queue inspect --env-file /private/bridge.env --limit 50
telegram-mcp queue requeue 17 --env-file /private/bridge.env
telegram-mcp queue discard 17 --env-file /private/bridge.env
```

Replace 17 with an ID from inspection. Inspect returns bounded IDs, attempt counts, timestamps and outbound fingerprints, without message bodies or lease tokens. Investigate the consumer failure before requeueing; it resets that event's attempt budget. Discard marks it acknowledged for the ordinary retention policy. Neither operation sends a Telegram message. Counts of operator actions are retained in storage counters.

## Uncertain outbound delivery

An interrupted request may have reached Telegram even when its response was lost. Such an outbox record remains `uncertain` and is not automatically retried. Inspect the destination chat independently. If the exact message is present, record its Telegram message ID:

```text
telegram-mcp queue resolve-outbox 24 --message-id 391 --env-file /private/bridge.env
```

If the intended outcome is to stop pursuing this delivery:

```text
telegram-mcp queue resolve-outbox 24 --discard --env-file /private/bridge.env
```

These commands only record the operator's decision. They do not verify Telegram remotely and do not send anything. Both retain the idempotency key and fingerprint. Reusing that key returns the terminal result; it does not create a second send. Only this bot's currently uncertain record may be resolved. There is deliberately no automatic or operator CLI shortcut that blindly resends uncertain content.

## Backup and restore

```text
telegram-mcp queue backup /private/backups/bridge-2026-09-06.sqlite3 --env-file /private/bridge.env
```

The parent directory must already exist. The command creates a new file, refuses to overwrite any existing destination, uses SQLite's backup API, and runs integrity and foreign-key checks. The snapshot includes **all bot namespaces**, inbound data, quarantine, poll offsets, authorizations, counters and the complete outbound idempotency ledger. Store it like message data: private and encrypted where appropriate. POSIX creation uses mode 0600; on Windows use a directory with an appropriate private ACL. Backups do not contain environment-file credentials.

To restore, stop every worker, preserve the current database and its WAL/SHM files, and place a verified snapshot under a **new database path**. Point `BRIDGE_DATABASE_PATH` at that new path. Do not copy only the main file out of a running WAL database or replace a live file. Keep the original bot credential/namespace. First inspect the restored queue offline, then restart one worker. Startup moves inherited sending records to uncertain, not automatic retry.

Restoring an older snapshot also restores its older offset and idempotency ledger: deliveries after the snapshot may be missing from that ledger. Reconcile them against Telegram before resuming sends. No backup can guarantee exactly-once delivery for events that happened after it was taken.

Use the existing `BRIDGE_RETENTION_DAYS` policy for acknowledged inbox records. Quarantine requires explicit resolution; terminal outbox records remain retained to protect idempotency. Schedule backups and monitor disk usage outside the bridge. Deleting the outbound ledger or changing the bot token changes the deduplication boundary.

## Verification

Local checks include the original suite plus queue starvation/restart tests, stale lease protection, bot-scoped resolutions, migration from a frozen version-1 schema, complete snapshot restore, SQLite FULL rollback and abrupt subprocess termination with live inbox/outbox leases. The FULL test limits a disposable database's page count; it does not fill the host disk. No real Telegram credentials or messages are used.

CI runs Python 3.10/3.12 on Linux, Python 3.12 on Windows and macOS, and builds the production Docker image. The container smoke test runs as its non-root user with `--network none`; it checks HTTP readiness and MCP authentication, durable quarantine and snapshot restore using the installed package. A live Telegram integration and long-duration operational soak remain separate release checks.
