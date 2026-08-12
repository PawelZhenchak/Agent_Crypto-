-- Immutable futures microstructure evidence for bridge schema v3.
-- Historical schema-v2 batches remain replayable, but have no row in these tables
-- and therefore fail closed in the 0.6.0 futures-analysis gate.

ALTER TABLE crypto_agent.t4_ingestion_batches
    DROP CONSTRAINT t4_ingestion_batches_bridge_schema_version_check;

ALTER TABLE crypto_agent.t4_ingestion_batches
    ADD CONSTRAINT t4_ingestion_batches_bridge_schema_version_check
    CHECK (bridge_schema_version IN (2, 3));

CREATE TABLE crypto_agent.t4_futures_snapshots (
    t4_snapshot_id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    t4_batch_id               bigint NOT NULL UNIQUE,
    source_id                 bigint NOT NULL,
    market_id                 bigint NOT NULL,
    logical_symbol            text NOT NULL CHECK (logical_symbol IN ('BTC/USD', 'ETH/USD')),
    contract_id               text NOT NULL CHECK (contract_id ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{2,127}$'),
    session_status            text NOT NULL CHECK (session_status IN ('OPEN', 'CLOSED', 'HALTED')),
    is_full_snapshot          boolean NOT NULL CHECK (is_full_snapshot),
    observed_at               timestamptz NOT NULL,
    available_at              timestamptz NOT NULL,
    provider_ingested_at      timestamptz NOT NULL,
    basis_reference_symbol    text NOT NULL,
    basis_reference_type      text NOT NULL CHECK (basis_reference_type = 'index'),
    basis_reference_source    text NOT NULL
        CHECK (basis_reference_source = 'plus500_t4_index_v1'),
    basis_reference_price     numeric NOT NULL CHECK (basis_reference_price > 0),
    basis_observed_at         timestamptz NOT NULL,
    basis_available_at        timestamptz NOT NULL,
    basis_ingested_at         timestamptz NOT NULL,
    content_hash              crypto_agent.sha256_hex NOT NULL,
    FOREIGN KEY (t4_batch_id, source_id, market_id, logical_symbol, contract_id)
        REFERENCES crypto_agent.t4_ingestion_batches (
            t4_batch_id, source_id, market_id, logical_symbol, contract_id
        ),
    CHECK (basis_reference_symbol = logical_symbol),
    CHECK (observed_at <= available_at AND available_at <= provider_ingested_at),
    CHECK (basis_observed_at <= basis_available_at
           AND basis_available_at <= basis_ingested_at)
);

CREATE TABLE crypto_agent.t4_orderbook_levels (
    t4_snapshot_id       bigint NOT NULL
        REFERENCES crypto_agent.t4_futures_snapshots(t4_snapshot_id),
    side                  text NOT NULL CHECK (side IN ('bid', 'ask')),
    level_no              integer NOT NULL CHECK (level_no BETWEEN 1 AND 50),
    price                 numeric NOT NULL CHECK (price > 0),
    quantity              numeric NOT NULL CHECK (quantity >= 0),
    content_hash          crypto_agent.sha256_hex NOT NULL,
    PRIMARY KEY (t4_snapshot_id, side, level_no),
    UNIQUE (t4_snapshot_id, content_hash)
);

CREATE TABLE crypto_agent.t4_contract_transition_evidence (
    t4_transition_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    t4_batch_id           bigint NOT NULL UNIQUE,
    source_id             bigint NOT NULL,
    market_id             bigint NOT NULL,
    logical_symbol        text NOT NULL,
    from_contract_id      text NOT NULL CHECK (from_contract_id ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{2,127}$'),
    to_contract_id        text NOT NULL CHECK (to_contract_id ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{2,127}$'),
    price_type            text NOT NULL CHECK (price_type = 'mid'),
    from_price            numeric NOT NULL CHECK (from_price > 0),
    to_price              numeric NOT NULL CHECK (to_price > 0),
    evidence_source       text NOT NULL
        CHECK (evidence_source = 'plus500_t4_futures_v1'),
    observed_at           timestamptz NOT NULL,
    available_at          timestamptz NOT NULL,
    provider_ingested_at  timestamptz NOT NULL,
    content_hash          crypto_agent.sha256_hex NOT NULL,
    FOREIGN KEY (t4_batch_id, source_id, market_id, logical_symbol, to_contract_id)
        REFERENCES crypto_agent.t4_ingestion_batches (
            t4_batch_id, source_id, market_id, logical_symbol, contract_id
        ),
    CHECK (from_contract_id <> to_contract_id),
    CHECK (observed_at <= available_at AND available_at <= provider_ingested_at)
);

CREATE INDEX t4_futures_snapshots_pit_idx
ON crypto_agent.t4_futures_snapshots (
    logical_symbol, available_at DESC, provider_ingested_at DESC, t4_snapshot_id DESC
);

CREATE INDEX t4_contract_transitions_pit_idx
ON crypto_agent.t4_contract_transition_evidence (
    logical_symbol, available_at DESC, provider_ingested_at DESC, t4_transition_id DESC
);

CREATE TRIGGER t4_futures_snapshots_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.t4_futures_snapshots
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_futures_snapshots_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.t4_futures_snapshots
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_orderbook_levels_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.t4_orderbook_levels
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_orderbook_levels_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.t4_orderbook_levels
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_contract_transition_evidence_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.t4_contract_transition_evidence
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_contract_transition_evidence_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.t4_contract_transition_evidence
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

COMMENT ON TABLE crypto_agent.t4_futures_snapshots IS
    'Exact schema-v3 T4 order-book and typed basis evidence bound to one raw batch.';

COMMENT ON TABLE crypto_agent.t4_contract_transition_evidence IS
    'Synchronized old/new contract prices used only for an attested controlled roll.';
