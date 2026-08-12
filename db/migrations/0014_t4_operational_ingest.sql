-- Operational, append-only Plus500 Futures / T4 ingest and replay contract.
-- Raw bridge envelopes are retained verbatim as base64 plus their SHA-256 digest.

CREATE TABLE crypto_agent.t4_ingestion_batches (
    t4_batch_id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id               bigint NOT NULL REFERENCES crypto_agent.data_sources(source_id),
    market_id               bigint NOT NULL REFERENCES crypto_agent.markets(market_id),
    logical_symbol          text NOT NULL CHECK (logical_symbol IN ('BTC/USD', 'ETH/USD')),
    contract_id             text NOT NULL CHECK (contract_id ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{2,127}$'),
    contract_expires_at     timestamptz NOT NULL,
    contract_roll_at        timestamptz NOT NULL,
    contract_selection      text NOT NULL CHECK (contract_selection IN ('front_month', 'rolled')),
    rolled_from_contract_id text,
    interval_seconds        integer NOT NULL CHECK (interval_seconds > 0),
    requested_as_of         timestamptz NOT NULL,
    bridge_schema_version   integer NOT NULL CHECK (bridge_schema_version = 2),
    reference_price         numeric NOT NULL CHECK (reference_price >= 0),
    reference_event_time    timestamptz NOT NULL,
    reference_available_at  timestamptz NOT NULL,
    reference_ingested_at   timestamptz NOT NULL,
    raw_payload_base64      text NOT NULL CHECK (btrim(raw_payload_base64) <> ''),
    raw_payload_hash        crypto_agent.sha256_hex NOT NULL UNIQUE,
    record_count            integer NOT NULL CHECK (record_count > 0),
    status                  text NOT NULL CHECK (status = 'completed'),
    observed_at             timestamptz NOT NULL,
    available_at            timestamptz NOT NULL,
    ingested_at             timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (requested_as_of < contract_roll_at AND contract_roll_at < contract_expires_at),
    CHECK (reference_event_time <= reference_available_at
           AND reference_available_at <= reference_ingested_at),
    CHECK (
        (contract_selection = 'front_month' AND rolled_from_contract_id IS NULL)
        OR
        (contract_selection = 'rolled'
         AND rolled_from_contract_id IS NOT NULL
         AND rolled_from_contract_id <> contract_id
         AND rolled_from_contract_id ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{2,127}$')
    ),
    CHECK (observed_at <= available_at AND available_at <= ingested_at),
    UNIQUE (t4_batch_id, source_id, market_id, logical_symbol, contract_id)
);

CREATE TABLE crypto_agent.t4_canonical_candles (
    t4_candle_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    t4_batch_id         bigint NOT NULL,
    source_id           bigint NOT NULL,
    market_id           bigint NOT NULL,
    logical_symbol      text NOT NULL,
    contract_id         text NOT NULL,
    interval_seconds    integer NOT NULL CHECK (interval_seconds > 0),
    open_time           timestamptz NOT NULL,
    close_time          timestamptz NOT NULL,
    open_price          numeric NOT NULL CHECK (open_price >= 0),
    high_price          numeric NOT NULL CHECK (high_price >= 0),
    low_price           numeric NOT NULL CHECK (low_price >= 0),
    close_price         numeric NOT NULL CHECK (close_price >= 0),
    base_volume         numeric NOT NULL CHECK (base_volume >= 0),
    available_at        timestamptz NOT NULL,
    provider_ingested_at timestamptz NOT NULL,
    content_hash        crypto_agent.sha256_hex NOT NULL,
    FOREIGN KEY (t4_batch_id, source_id, market_id, logical_symbol, contract_id)
        REFERENCES crypto_agent.t4_ingestion_batches (
            t4_batch_id, source_id, market_id, logical_symbol, contract_id
        ),
    CHECK (open_time < close_time),
    CHECK (high_price >= GREATEST(open_price, close_price, low_price)),
    CHECK (low_price <= LEAST(open_price, close_price, high_price)),
    CHECK (close_time <= available_at AND available_at <= provider_ingested_at),
    UNIQUE (t4_batch_id, open_time),
    UNIQUE (t4_batch_id, content_hash)
);

CREATE INDEX t4_batches_replay_cutoff_idx
ON crypto_agent.t4_ingestion_batches (
    logical_symbol, interval_seconds, available_at DESC, t4_batch_id DESC
);

CREATE INDEX t4_candles_replay_cutoff_idx
ON crypto_agent.t4_canonical_candles (
    logical_symbol, interval_seconds, open_time DESC, available_at DESC
);

CREATE TRIGGER t4_ingestion_batches_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.t4_ingestion_batches
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_ingestion_batches_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.t4_ingestion_batches
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_canonical_candles_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.t4_canonical_candles
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_canonical_candles_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.t4_canonical_candles
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

COMMENT ON TABLE crypto_agent.t4_ingestion_batches IS
    'Exact, immutable T4 bridge envelopes. Hash uniqueness makes ingest idempotent.';

COMMENT ON TABLE crypto_agent.t4_canonical_candles IS
    'Normalized T4 futures candles used by deterministic point-in-time replay.';
