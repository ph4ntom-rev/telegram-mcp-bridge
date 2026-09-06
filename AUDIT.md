# Engineering Audit Report

Snapshot date: **2026-08-17**

Version: **1.0.0**

Scope: Python source, MCP surface, Telegram transport, SQLite lifecycle, HTTP/webhook boundary, CLI, configuration, dependencies, container, and documentation.

This is an engineering security and reliability audit with independent code review and adversarial testing. It is not a certified external penetration test.

## Review scope

The architecture was checked against current primary sources:

- Codex supports local MCP over `stdio` and remote Streamable HTTP: [OpenAI Codex MCP](https://developers.openai.com/codex/mcp).
- The implementation uses the stable Python MCP SDK v2 and stateless HTTP: [official MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk).
- Update semantics, long polling, webhook secrets, and Bot API methods were checked against the [Telegram Bot API](https://core.telegram.org/bots/api), [webhook guide](https://core.telegram.org/bots/webhooks), and [FAQ/rate limits](https://core.telegram.org/bots/faq).
- WAL, the single-writer model, and durability trade-offs were checked against the [SQLite WAL documentation](https://www.sqlite.org/wal.html).

The selected low-latency local profile is `stdio + long polling`: no public port, a persistent HTTP/2 client, and a Telegram long poll of up to 50 seconds that returns immediately when an event arrives. The server profile uses stateless Streamable HTTP and a webhook behind a TLS proxy.

## Threat model

The audit covered these adversaries and failure modes:

- unknown Telegram users/chats, username impersonation, and events with conflicting nested chat IDs;
- prompt injection and oversized or malformed JSON/Unicode payloads;
- duplicate, conflicting, and out-of-order `update_id` values;
- forged webhooks, weak secrets, DNS rebinding, invalid Host/Origin headers, and body exhaustion;
- unauthorized MCP clients and attempts to call arbitrary Bot API methods, URLs, or paths;
- bot-token leakage through URLs, exception chains, access logs, or Telegram error descriptions;
- timeouts, resets, 429 responses, ambiguous send outcomes, and unsafe mutation retries;
- crashes or cancellation during SQLite transactions, HTTP sends, shutdown, or leases;
- concurrent claims, restart with unexpired leases, and ACL revocation during a send;
- queue/disk exhaustion and unbounded idempotency-ledger growth.

## Verified properties

| Property | Implementation and verification |
|---|---|
| Durable ingress | A webhook returns 2xx only after commit; the polling offset advances in the same transaction as the insert. |
| Deduplication | `(bot_key, update_id)` is the primary key. Identical payloads are duplicates; a different fingerprint is recorded as a conflict without replacing the original. |
| Ordering | The consumer cursor is the local monotonic `event_id`, not Telegram's `update_id`, which need not remain sequential. |
| Consumer delivery | Atomic claim, opaque batch token, lease, and idempotent acknowledgement provide at-least-once delivery until ack. |
| Outbound delivery | Explicit chat ACL, payload fingerprint, and mandatory idempotency key. Replaying a known `sent` record makes no second HTTP request. |
| Ambiguous send | A transport failure after request transmission begins, or restart during an active send, moves the record to `uncertain` without a hidden retry. |
| Concurrency | One SQLite writer, `BEGIN IMMEDIATE`, WAL, and busy timeout. Only one worker can win an exact-ID outbox claim. |
| Access revocation | ACL replacement/revocation atomically blocks new claims and quarantines existing outbox records. Ingress never expands the outbound ACL. |
| Secret handling | Token redaction covers URLs, exceptions, raw patterns, and server responses; HTTP access logging is disabled. |
| HTTP boundary | Bearer authentication, stateless MCP, Host/Origin protection, constant-time webhook-secret checks, strict JSON, and a byte limit. |
| API surface | No generic `call_api`, arbitrary URL/file download, SQL, or caller-supplied filesystem path. |

## Defects reproduced and fixed

The review reproduced and closed the following issues:

1. A bot token could reach an exception cause or Telegram error description; errors are now sanitized without preserving a dangerous chain.
2. A malformed item in a successful `getUpdates` response could be silently discarded; the entire batch is now rejected and the offset remains unchanged.
3. Unsafe edit/delete/webhook/callback mutations could be retried after an ambiguous outcome; retries are now limited to safe cases.
4. A malformed HTTP 2xx send response was treated as not sent; after transmission starts, it is now `uncertain`.
5. A large `retry_after` did not update the global cooldown; all 429 responses now penalize the rate limiter.
6. Several current Telegram Bot API update variants did not expose actor/chat data; the parser and adversarial matrix were expanded.
7. Empty allowlists could fail open, while OR semantics weakened two configured boundaries; the policy is now deny-by-default and intersects populated lists.
8. An authorized inbound user could permanently authorize an arbitrary outbound chat; durable write ACL entries now come only from explicit configuration.
9. A chat removed from the environment retained stale SQLite permissions; startup now performs atomic replacement and revocation.
10. Revoked pending/sending outbox rows could consume the bounded queue; they now move atomically to `dead` or `uncertain`.
11. Cancellation while waiting for `BEGIN IMMEDIATE` could leave an orphan transaction, and cancellation of `close()` could strand a worker thread. Both lifecycle paths are shielded and covered by failpoint tests.
12. Restart left unexpired inbox/outbox leases stuck; startup immediately releases inherited inbox leases and quarantines inherited sends.
13. The final `message_id` could be lost after a long lease expired; the exact current token remains the owner until atomic recovery/reclaim.
14. The webhook route remained active in polling/disabled mode; it is now mounted only in webhook mode, and every configured secret is validated.
15. `doctor` ran mutable crash recovery next to a live server; diagnostics now open only the resources required to read status and do not mutate leases or ACLs.
16. A second process sharing the same database could run crash recovery against the live first process; a cross-platform advisory lock is now acquired before open/recovery and released only after cancellation-safe shutdown.
17. JSON `NaN`/`Infinity`, duplicate keys, reserved webhook paths, unsafe host/auth characters, and relative database paths now receive strict validation.
18. Text splitting could break a Unicode grapheme; plain-text chunking is tested with emoji, ZWJ, combining sequences, and multiple scripts.

## Verification

An independent pre-release verification was repeated on **2026-09-01** with Python 3.10.6. Results matched the original audit. CI for Python 3.10/3.12 and a secret scan of the publishable tree were also added.

The final clean-room results were:

```text
pytest --cov --cov-branch: 201 passed, branch coverage 85.50%
ruff check .: clean
ruff format --check .: clean
mypy --strict src/telegram_mcp: clean (15 modules)
pip check: clean
pip-audit --require-hashes -r requirements.lock: no known vulnerabilities
bandit -c pyproject.toml -r src: no unsuppressed findings; generated-placeholder SQL reviewed manually
wheel build + install in fresh venv + CLI/import smoke: passed
Docker Compose YAML/config: parsed; image build not performed because Docker was unavailable in the audit environment
```

The test matrix covers deduplication/conflicts, two SQLite connections, 20 concurrent exact-ID claims, expired/restarted leases, cancellation failpoints, ACL revocation, idempotency conflict/replay, ambiguous sends, 429 cooldown, Telegram schema variants, hostile JSON/Unicode, webhook auth/mode, MCP auth/metadata, runtime lifecycle, CLI, and log redaction.

### Queue operations update (2026-09-06)

Local Windows/Python 3.10.6 verification after the schema-2 changes: 217 tests pass with 85.99% combined statement/branch coverage. Lint, formatting, strict types, environment consistency, Bandit and the hash-locked dependency audit pass. The additional tests cover poison-event quarantine without queue starvation, two-connection claims, live/stale leases, bot-scoped manual outcomes, full backup restoration, failed/cancelled backups, version-1 migration, SQLite FULL rollback and abrupt subprocess termination. No real Telegram messages are sent.

The production-image smoke script also passes locally against the installed dependencies, checking HTTP MCP authentication, lifecycle, quarantine and snapshot restore. Docker itself is unavailable locally; a new CI job builds the production image and runs that script as the image's non-root user with networking disabled. The matrix now includes Windows and macOS. See [operational procedures](OPERATIONS.md).

### Synthetic latency

Windows, SQLite `WAL + synchronous=FULL`, 500 iterations, excluding Telegram network and model latency:

| Operation | p50 | p95 | p99 | Mean |
|---|---:|---:|---:|---:|
| Durable ingest | 1.515 ms | 1.856 ms | 1.974 ms | 1.566 ms |
| Claim + acknowledgement | 2.619 ms | 3.249 ms | 3.599 ms | 2.724 ms |
| Full durable round trip | 4.168 ms | 5.018 ms | 5.436 ms | 4.290 ms |

This measures only the bridge's own overhead. Actual response time includes Telegram transport and model work. Long polling returns immediately when an update arrives rather than waiting for the timeout.

## Residual limitations

- Absolute exactly-once delivery is impossible without an idempotency primitive from Telegram. An `uncertain` record requires human or agent inspection.
- MCP is passive and cannot wake a stopped Codex task. An always-on agent loop is required for 24/7 operation.
- The SQLite profile supports one process on a local disk. Horizontal scaling requires a different durable coordination layer.
- Outbox tombstones are not deleted automatically because doing so would allow old idempotency keys to send again. Disk monitoring and a deliberate retention policy are required.
- Repeatedly unacknowledged events enter durable quarantine after a bounded delivery budget. They still consume queue capacity and require explicit operator requeue or discard.
- Attachments are available only as validated metadata and opaque `file_id` values; a download pipeline is intentionally outside this release.
- TLS, Telegram source-CIDR filtering, DDoS protection, and backup scheduling remain infrastructure responsibilities.
- Tests and static analysis cannot prove the absence of every defect. Repeat the audit after significant SDK or Bot API updates.
