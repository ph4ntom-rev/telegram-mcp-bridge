
        CREATE TABLE IF NOT EXISTS inbox_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_key TEXT NOT NULL,
            update_id INTEGER NOT NULL CHECK(update_id >= 0),
            payload_json TEXT NOT NULL,
            payload_hash TEXT NOT NULL CHECK(length(payload_hash) = 64),
            chat_id INTEGER,
            state TEXT NOT NULL DEFAULT 'queued'
                CHECK(state IN ('queued', 'leased', 'acked')),
            received_at REAL NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
            lease_token TEXT,
            lease_until REAL,
            acked_at REAL,
            UNIQUE(bot_key, update_id),
            CHECK(
                (state = 'leased' AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
                OR (state <> 'leased' AND lease_token IS NULL AND lease_until IS NULL)
            )
        );
        CREATE INDEX IF NOT EXISTS inbox_claim_idx
            ON inbox_events(bot_key, event_id)
            WHERE state IN ('queued', 'leased');
        CREATE INDEX IF NOT EXISTS inbox_purge_idx ON inbox_events(state, acked_at);

        CREATE TABLE IF NOT EXISTS poll_offsets (
            bot_key TEXT PRIMARY KEY,
            next_offset INTEGER NOT NULL CHECK(next_offset >= 0),
            updated_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chat_authorizations (
            bot_key TEXT NOT NULL,
            chat_id INTEGER NOT NULL,
            alias TEXT,
            can_read INTEGER NOT NULL CHECK(can_read IN (0, 1)),
            can_write INTEGER NOT NULL CHECK(can_write IN (0, 1)),
            updated_at REAL NOT NULL,
            PRIMARY KEY(bot_key, chat_id),
            UNIQUE(bot_key, alias)
        );

        CREATE TABLE IF NOT EXISTS outbox (
            outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_key TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            fingerprint TEXT NOT NULL CHECK(length(fingerprint) = 64),
            chat_id INTEGER NOT NULL,
            method TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL
                CHECK(status IN ('pending', 'sending', 'retry', 'uncertain', 'sent', 'dead')),
            attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
            next_attempt_at REAL NOT NULL,
            lease_token TEXT,
            lease_until REAL,
            telegram_message_id INTEGER,
            last_error TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            sent_at REAL,
            UNIQUE(bot_key, idempotency_key),
            CHECK(
                (status = 'sending' AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
                OR (status <> 'sending' AND lease_token IS NULL AND lease_until IS NULL)
            )
        );
        CREATE INDEX IF NOT EXISTS outbox_due_idx
            ON outbox(next_attempt_at, created_at, outbox_id)
            WHERE status IN ('pending', 'retry');
        CREATE INDEX IF NOT EXISTS outbox_bot_due_idx
            ON outbox(bot_key, next_attempt_at, created_at, outbox_id)
            WHERE status IN ('pending', 'retry');

        CREATE TABLE IF NOT EXISTS storage_counters (
            bot_key TEXT NOT NULL,
            name TEXT NOT NULL,
            value INTEGER NOT NULL DEFAULT 0 CHECK(value >= 0),
            PRIMARY KEY(bot_key, name)
        );

        PRAGMA user_version=1;
        