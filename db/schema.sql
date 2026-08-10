-- Crypto Agent: point-in-time data model (PostgreSQL 15+)
-- No extension is required. All timestamps use TIMESTAMPTZ and are interpreted in UTC.

BEGIN;

CREATE SCHEMA IF NOT EXISTS crypto_agent;
SET LOCAL search_path TO crypto_agent, public;
SET LOCAL TIME ZONE 'UTC';

CREATE DOMAIN sha256_hex AS text
    CHECK (VALUE ~ '^[0-9a-f]{64}$');

CREATE OR REPLACE FUNCTION forbid_append_only_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; % is forbidden', TG_TABLE_NAME, TG_OP
        USING ERRCODE = '55000';
END;
$$;

-- ---------------------------------------------------------------------------
-- Sources and ingestion lineage
-- ---------------------------------------------------------------------------

CREATE TABLE data_sources (
    source_id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_key          text NOT NULL UNIQUE,
    display_name        text NOT NULL,
    source_kind         text NOT NULL,
    trust_tier          smallint NOT NULL DEFAULT 3 CHECK (trust_tier BETWEEN 1 AND 5),
    homepage_url        text,
    description         text,
    registry_version    text NOT NULL,
    config_hash         sha256_hex NOT NULL,
    observed_at         timestamptz NOT NULL,
    available_at        timestamptz NOT NULL,
    ingested_at         timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (btrim(source_key) <> ''),
    CHECK (btrim(source_kind) <> ''),
    CHECK (btrim(registry_version) <> ''),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE data_source_versions (
    data_source_version_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id              bigint NOT NULL REFERENCES data_sources(source_id),
    source_version         text NOT NULL,
    terms_version          text,
    schema_contract        jsonb NOT NULL DEFAULT '{}'::jsonb,
    observed_at            timestamptz NOT NULL,
    available_at           timestamptz NOT NULL,
    ingested_at            timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash           sha256_hex NOT NULL,
    UNIQUE (source_id, source_version, content_hash),
    CHECK (btrim(source_version) <> ''),
    CHECK (jsonb_typeof(schema_contract) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE ingestion_batches (
    batch_id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    batch_key            text NOT NULL,
    source_version       text NOT NULL,
    extractor_version    text NOT NULL,
    status               text NOT NULL CHECK (status IN ('completed', 'partial', 'failed', 'quarantined')),
    record_count         bigint CHECK (record_count IS NULL OR record_count >= 0),
    error_count          bigint CHECK (error_count IS NULL OR error_count >= 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash         sha256_hex NOT NULL,
    metadata             jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (source_id, batch_key),
    UNIQUE (batch_id, source_id),
    CHECK (btrim(batch_key) <> ''),
    CHECK (btrim(source_version) <> ''),
    CHECK (btrim(extractor_version) <> ''),
    CHECK (jsonb_typeof(metadata) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

-- ---------------------------------------------------------------------------
-- Point-in-time reference data: exchanges, assets, identifiers and markets
-- ---------------------------------------------------------------------------

CREATE TABLE exchanges (
    exchange_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    exchange_key         text NOT NULL UNIQUE,
    venue_kind           text NOT NULL,
    mic                  text,
    lei                  text,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id             bigint,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (btrim(exchange_key) <> ''),
    CHECK (btrim(venue_kind) <> ''),
    CHECK (btrim(source_record_key) <> ''),
    CHECK (btrim(source_version) <> ''),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE exchange_versions (
    exchange_version_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    exchange_id          bigint NOT NULL REFERENCES exchanges(exchange_id),
    legal_name           text,
    display_name         text NOT NULL,
    jurisdiction_code    text,
    operational_status   text NOT NULL,
    effective_from       timestamptz,
    effective_to         timestamptz,
    attributes           jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id             bigint,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (effective_to IS NULL OR effective_from IS NULL OR effective_from < effective_to),
    CHECK (jsonb_typeof(attributes) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE assets (
    asset_id                     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset_key                    text NOT NULL UNIQUE,
    asset_type                   text NOT NULL,
    chain_namespace              text,
    chain_reference              text,
    contract_address_normalized  text,
    contract_address_display     text,
    source_id                    bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key            text NOT NULL,
    source_version               text NOT NULL,
    revision_no                  integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at                  timestamptz NOT NULL,
    available_at                 timestamptz NOT NULL,
    ingested_at                  timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id                     bigint,
    content_hash                 sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (btrim(asset_key) <> ''),
    CHECK (btrim(asset_type) <> ''),
    CHECK ((chain_namespace IS NULL) = (chain_reference IS NULL)),
    CHECK (contract_address_normalized IS NULL OR chain_namespace IS NOT NULL),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE asset_versions (
    asset_version_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset_id             bigint NOT NULL REFERENCES assets(asset_id),
    display_name         text NOT NULL,
    canonical_symbol     text NOT NULL,
    decimals             integer CHECK (decimals IS NULL OR decimals BETWEEN 0 AND 255),
    lifecycle_status     text NOT NULL,
    issuer_name          text,
    effective_from       timestamptz,
    effective_to         timestamptz,
    attributes           jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id             bigint,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (effective_to IS NULL OR effective_from IS NULL OR effective_from < effective_to),
    CHECK (jsonb_typeof(attributes) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE asset_symbols (
    asset_symbol_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset_id             bigint NOT NULL REFERENCES assets(asset_id),
    exchange_id          bigint REFERENCES exchanges(exchange_id),
    symbol_namespace     text NOT NULL,
    symbol               text NOT NULL,
    effective_from       timestamptz,
    effective_to         timestamptz,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id             bigint,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (btrim(symbol_namespace) <> ''),
    CHECK (btrim(symbol) <> ''),
    CHECK (effective_to IS NULL OR effective_from IS NULL OR effective_from < effective_to),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE markets (
    market_id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    market_key           text NOT NULL UNIQUE,
    exchange_id          bigint NOT NULL REFERENCES exchanges(exchange_id),
    base_asset_id        bigint NOT NULL REFERENCES assets(asset_id),
    quote_asset_id       bigint NOT NULL REFERENCES assets(asset_id),
    settlement_asset_id  bigint REFERENCES assets(asset_id),
    instrument_type      text NOT NULL,
    expiry_at            timestamptz,
    strike_price         numeric,
    option_side          text CHECK (option_side IS NULL OR option_side IN ('call', 'put')),
    contract_multiplier  numeric CHECK (contract_multiplier IS NULL OR contract_multiplier > 0),
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id             bigint,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (base_asset_id <> quote_asset_id),
    CHECK (strike_price IS NULL OR strike_price >= 0),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE market_versions (
    market_version_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    market_id            bigint NOT NULL REFERENCES markets(market_id),
    lifecycle_status     text NOT NULL,
    price_tick           numeric CHECK (price_tick IS NULL OR price_tick > 0),
    quantity_step        numeric CHECK (quantity_step IS NULL OR quantity_step > 0),
    minimum_quantity     numeric CHECK (minimum_quantity IS NULL OR minimum_quantity >= 0),
    minimum_notional     numeric CHECK (minimum_notional IS NULL OR minimum_notional >= 0),
    fee_schedule         jsonb NOT NULL DEFAULT '{}'::jsonb,
    effective_from       timestamptz,
    effective_to         timestamptz,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id             bigint,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (jsonb_typeof(fee_schedule) = 'object'),
    CHECK (effective_to IS NULL OR effective_from IS NULL OR effective_from < effective_to),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE market_symbols (
    market_symbol_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    market_id            bigint NOT NULL REFERENCES markets(market_id),
    symbol_namespace     text NOT NULL,
    symbol               text NOT NULL,
    effective_from       timestamptz,
    effective_to         timestamptz,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id             bigint,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (btrim(symbol_namespace) <> ''),
    CHECK (btrim(symbol) <> ''),
    CHECK (effective_to IS NULL OR effective_from IS NULL OR effective_from < effective_to),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE INDEX exchanges_pit_idx ON exchanges (exchange_key, available_at DESC, ingested_at DESC);
CREATE INDEX exchange_versions_pit_idx ON exchange_versions (exchange_id, available_at DESC, ingested_at DESC, revision_no DESC);
CREATE INDEX assets_chain_contract_idx ON assets (chain_namespace, chain_reference, contract_address_normalized);
CREATE INDEX assets_pit_idx ON assets (asset_key, available_at DESC, ingested_at DESC);
CREATE INDEX asset_versions_pit_idx ON asset_versions (asset_id, available_at DESC, ingested_at DESC, revision_no DESC);
CREATE INDEX asset_symbols_lookup_idx ON asset_symbols (symbol_namespace, lower(symbol), available_at DESC, ingested_at DESC);
CREATE INDEX markets_assets_idx ON markets (base_asset_id, quote_asset_id, instrument_type);
CREATE INDEX markets_pit_idx ON markets (exchange_id, available_at DESC, ingested_at DESC);
CREATE INDEX market_versions_pit_idx ON market_versions (market_id, available_at DESC, ingested_at DESC, revision_no DESC);
CREATE INDEX market_symbols_lookup_idx ON market_symbols (symbol_namespace, symbol, available_at DESC, ingested_at DESC);

-- ---------------------------------------------------------------------------
-- Market data
-- ---------------------------------------------------------------------------

CREATE TABLE candles (
    candle_id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    market_id            bigint NOT NULL REFERENCES markets(market_id),
    interval_seconds     integer NOT NULL CHECK (interval_seconds > 0),
    open_time            timestamptz NOT NULL,
    close_time           timestamptz NOT NULL,
    open_price           numeric NOT NULL CHECK (open_price >= 0),
    high_price           numeric NOT NULL CHECK (high_price >= 0),
    low_price            numeric NOT NULL CHECK (low_price >= 0),
    close_price          numeric NOT NULL CHECK (close_price >= 0),
    base_volume          numeric CHECK (base_volume IS NULL OR base_volume >= 0),
    quote_volume         numeric CHECK (quote_volume IS NULL OR quote_volume >= 0),
    trade_count          bigint CHECK (trade_count IS NULL OR trade_count >= 0),
    is_final             boolean NOT NULL DEFAULT true,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id             bigint,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (open_time < close_time),
    CHECK (high_price >= GREATEST(open_price, close_price, low_price)),
    CHECK (low_price <= LEAST(open_price, close_price, high_price)),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE trades (
    trade_id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    market_id             bigint NOT NULL REFERENCES markets(market_id),
    venue_trade_id        text,
    price                 numeric NOT NULL CHECK (price >= 0),
    quantity              numeric NOT NULL CHECK (quantity > 0),
    quote_quantity        numeric CHECK (quote_quantity IS NULL OR quote_quantity >= 0),
    aggressor_side        text CHECK (aggressor_side IS NULL OR aggressor_side IN ('buy', 'sell', 'unknown')),
    is_liquidation        boolean,
    source_sequence       numeric,
    source_id             bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key     text NOT NULL,
    source_version        text NOT NULL,
    revision_no           integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at           timestamptz NOT NULL,
    available_at          timestamptz NOT NULL,
    ingested_at           timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id              bigint,
    content_hash          sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE orderbook_snapshots (
    orderbook_snapshot_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    market_id             bigint NOT NULL REFERENCES markets(market_id),
    venue_sequence        numeric,
    depth                 integer NOT NULL CHECK (depth > 0),
    checksum              text,
    is_full_snapshot      boolean NOT NULL DEFAULT true,
    source_id             bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key     text NOT NULL,
    source_version        text NOT NULL,
    revision_no           integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at           timestamptz NOT NULL,
    available_at          timestamptz NOT NULL,
    ingested_at           timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id              bigint,
    content_hash          sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE orderbook_levels (
    orderbook_snapshot_id bigint NOT NULL REFERENCES orderbook_snapshots(orderbook_snapshot_id),
    side                  text NOT NULL CHECK (side IN ('bid', 'ask')),
    level_no              integer NOT NULL CHECK (level_no > 0),
    price                 numeric NOT NULL CHECK (price > 0),
    quantity              numeric NOT NULL CHECK (quantity >= 0),
    order_count           integer CHECK (order_count IS NULL OR order_count >= 0),
    PRIMARY KEY (orderbook_snapshot_id, side, level_no)
);

CREATE TABLE derivatives_metrics (
    derivatives_metric_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    market_id             bigint NOT NULL REFERENCES markets(market_id),
    metric_name           text NOT NULL,
    metric_value          numeric NOT NULL,
    unit                  text NOT NULL,
    window_seconds        integer CHECK (window_seconds IS NULL OR window_seconds > 0),
    maturity_at           timestamptz,
    dimensions            jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id             bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key     text NOT NULL,
    source_version        text NOT NULL,
    revision_no           integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at           timestamptz NOT NULL,
    available_at          timestamptz NOT NULL,
    ingested_at           timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id              bigint,
    content_hash          sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (btrim(metric_name) <> ''),
    CHECK (btrim(unit) <> ''),
    CHECK (jsonb_typeof(dimensions) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE INDEX candles_series_idx ON candles (market_id, interval_seconds, open_time DESC);
CREATE INDEX candles_pit_idx ON candles (market_id, interval_seconds, available_at DESC, ingested_at DESC, observed_at DESC);
CREATE INDEX candles_ingested_brin_idx ON candles USING brin (ingested_at);
CREATE INDEX trades_series_idx ON trades (market_id, observed_at DESC);
CREATE INDEX trades_pit_idx ON trades (market_id, available_at DESC, ingested_at DESC, observed_at DESC);
CREATE INDEX trades_ingested_brin_idx ON trades USING brin (ingested_at);
CREATE INDEX orderbook_pit_idx ON orderbook_snapshots (market_id, available_at DESC, ingested_at DESC, observed_at DESC);
CREATE INDEX orderbook_ingested_brin_idx ON orderbook_snapshots USING brin (ingested_at);
CREATE INDEX derivatives_metric_pit_idx ON derivatives_metrics (market_id, metric_name, available_at DESC, ingested_at DESC, observed_at DESC);
CREATE INDEX derivatives_metric_ingested_brin_idx ON derivatives_metrics USING brin (ingested_at);

-- ---------------------------------------------------------------------------
-- On-chain, macro, documents/news and token supply events
-- ---------------------------------------------------------------------------

CREATE TABLE onchain_metrics (
    onchain_metric_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_namespace     text NOT NULL,
    chain_reference     text NOT NULL,
    entity_type         text NOT NULL,
    entity_key          text NOT NULL,
    asset_id            bigint REFERENCES assets(asset_id),
    metric_name         text NOT NULL,
    numeric_value       numeric,
    text_value          text,
    json_value          jsonb,
    unit                text,
    block_number        numeric CHECK (block_number IS NULL OR block_number >= 0),
    block_hash          text,
    finality_status     text,
    window_seconds      integer CHECK (window_seconds IS NULL OR window_seconds > 0),
    dimensions          jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id           bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key   text NOT NULL,
    source_version      text NOT NULL,
    revision_no         integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at         timestamptz NOT NULL,
    available_at        timestamptz NOT NULL,
    ingested_at         timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id            bigint,
    content_hash        sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (num_nonnulls(numeric_value, text_value, json_value) = 1),
    CHECK (json_value IS NULL OR jsonb_typeof(json_value) IN ('object', 'array', 'number', 'string', 'boolean')),
    CHECK (jsonb_typeof(dimensions) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE macro_series (
    macro_series_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    series_key          text NOT NULL UNIQUE,
    display_name        text NOT NULL,
    provider_series_key text NOT NULL,
    frequency           text NOT NULL,
    unit                text NOT NULL,
    seasonal_adjustment text,
    geography_code      text,
    source_id           bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key   text NOT NULL,
    source_version      text NOT NULL,
    revision_no         integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at         timestamptz NOT NULL,
    available_at        timestamptz NOT NULL,
    ingested_at         timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id            bigint,
    content_hash        sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE macro_observations (
    macro_observation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    macro_series_id      bigint NOT NULL REFERENCES macro_series(macro_series_id),
    period_start         timestamptz NOT NULL,
    period_end           timestamptz NOT NULL,
    release_at           timestamptz NOT NULL,
    vintage_at           timestamptz NOT NULL,
    numeric_value        numeric,
    text_value           text,
    observation_status   text NOT NULL DEFAULT 'published',
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id             bigint,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (period_start < period_end),
    CHECK (num_nonnulls(numeric_value, text_value) = 1),
    CHECK (release_at <= available_at),
    CHECK (vintage_at <= available_at),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE documents (
    document_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_family_key  text NOT NULL,
    document_type        text NOT NULL,
    publisher            text,
    author               text,
    title                text NOT NULL,
    canonical_url        text,
    language_code        text,
    published_at         timestamptz NOT NULL,
    effective_at         timestamptz,
    normalized_text      text,
    storage_uri          text,
    normalized_text_hash sha256_hex,
    attributes           jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id             bigint,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (btrim(document_family_key) <> ''),
    CHECK (btrim(document_type) <> ''),
    CHECK (normalized_text IS NOT NULL OR storage_uri IS NOT NULL),
    CHECK (jsonb_typeof(attributes) = 'object'),
    CHECK (published_at <= available_at),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE document_entities (
    document_entity_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_id         bigint NOT NULL REFERENCES documents(document_id),
    asset_id            bigint REFERENCES assets(asset_id),
    exchange_id         bigint REFERENCES exchanges(exchange_id),
    market_id           bigint REFERENCES markets(market_id),
    entity_key          text,
    relation_type       text NOT NULL,
    relevance_score     numeric CHECK (relevance_score IS NULL OR relevance_score BETWEEN 0 AND 1),
    extractor_version   text NOT NULL,
    source_id           bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key   text NOT NULL,
    source_version      text NOT NULL,
    revision_no         integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at         timestamptz NOT NULL,
    available_at        timestamptz NOT NULL,
    ingested_at         timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id            bigint,
    content_hash        sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (num_nonnulls(asset_id, exchange_id, market_id, entity_key) = 1),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE VIEW news_items AS
SELECT *
FROM documents
WHERE document_type = 'news';

CREATE TABLE token_unlocks (
    token_unlock_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset_id             bigint NOT NULL REFERENCES assets(asset_id),
    unlock_key           text NOT NULL,
    unlock_at            timestamptz NOT NULL,
    quantity             numeric CHECK (quantity IS NULL OR quantity >= 0),
    percentage_of_supply numeric CHECK (percentage_of_supply IS NULL OR percentage_of_supply BETWEEN 0 AND 100),
    supply_basis         text,
    beneficiary_category text,
    unlock_mechanism     text,
    confidence_status    text NOT NULL,
    attributes           jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id             bigint,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (quantity IS NOT NULL OR percentage_of_supply IS NOT NULL),
    CHECK (jsonb_typeof(attributes) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE INDEX onchain_metric_pit_idx ON onchain_metrics (chain_namespace, chain_reference, entity_key, metric_name, available_at DESC, ingested_at DESC);
CREATE INDEX onchain_metric_block_idx ON onchain_metrics (chain_namespace, chain_reference, block_number DESC);
CREATE INDEX onchain_metric_ingested_brin_idx ON onchain_metrics USING brin (ingested_at);
CREATE INDEX macro_observations_period_idx ON macro_observations (macro_series_id, period_end DESC, vintage_at DESC);
CREATE INDEX macro_observations_pit_idx ON macro_observations (macro_series_id, available_at DESC, ingested_at DESC);
CREATE INDEX documents_published_idx ON documents (document_type, published_at DESC);
CREATE INDEX documents_pit_idx ON documents (available_at DESC, ingested_at DESC, published_at DESC);
CREATE INDEX documents_family_idx ON documents (document_family_key, revision_no DESC, available_at DESC);
CREATE INDEX documents_hash_idx ON documents (content_hash);
CREATE INDEX document_entities_asset_idx ON document_entities (asset_id, available_at DESC) WHERE asset_id IS NOT NULL;
CREATE INDEX token_unlocks_calendar_idx ON token_unlocks (asset_id, unlock_at);
CREATE INDEX token_unlocks_pit_idx ON token_unlocks (asset_id, available_at DESC, ingested_at DESC, unlock_at);

-- ---------------------------------------------------------------------------
-- Data quality incidents use events instead of mutable status fields.
-- ---------------------------------------------------------------------------

CREATE TABLE data_quality_incidents (
    data_quality_incident_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    incident_key             text NOT NULL UNIQUE,
    severity                 text NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    dataset_name             text NOT NULL,
    rule_key                 text NOT NULL,
    affected_source_id       bigint REFERENCES data_sources(source_id),
    affected_record_key      text,
    affected_from            timestamptz,
    affected_to              timestamptz,
    detected_at              timestamptz NOT NULL,
    summary                  text NOT NULL,
    evidence                 jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id                bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key        text NOT NULL,
    source_version           text NOT NULL,
    revision_no              integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at              timestamptz NOT NULL,
    available_at             timestamptz NOT NULL,
    ingested_at              timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id                 bigint,
    content_hash             sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    FOREIGN KEY (batch_id, source_id) REFERENCES ingestion_batches(batch_id, source_id),
    CHECK (affected_to IS NULL OR affected_from IS NULL OR affected_from <= affected_to),
    CHECK (jsonb_typeof(evidence) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE data_quality_incident_events (
    data_quality_incident_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    data_quality_incident_id       bigint NOT NULL REFERENCES data_quality_incidents(data_quality_incident_id),
    event_type                     text NOT NULL CHECK (event_type IN ('opened', 'acknowledged', 'quarantined', 'resolved', 'reopened', 'commented')),
    event_at                       timestamptz NOT NULL,
    actor_key                      text NOT NULL,
    details                        jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id                      bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key              text NOT NULL,
    source_version                 text NOT NULL,
    revision_no                    integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at                    timestamptz NOT NULL,
    available_at                   timestamptz NOT NULL,
    ingested_at                    timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash                   sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (jsonb_typeof(details) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE INDEX dq_incidents_dataset_idx ON data_quality_incidents (dataset_name, detected_at DESC);
CREATE INDEX dq_incidents_source_idx ON data_quality_incidents (affected_source_id, detected_at DESC);
CREATE INDEX dq_incident_events_idx ON data_quality_incident_events (data_quality_incident_id, event_at);

-- ---------------------------------------------------------------------------
-- Reproducible research and explicit input manifests
-- ---------------------------------------------------------------------------

CREATE TABLE research_runs (
    research_run_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_key               text NOT NULL UNIQUE,
    run_kind              text NOT NULL,
    as_of_at              timestamptz NOT NULL,
    horizon_seconds       integer CHECK (horizon_seconds IS NULL OR horizon_seconds > 0),
    requested_at          timestamptz NOT NULL,
    requested_by          text NOT NULL,
    agent_version         text NOT NULL,
    model_provider        text,
    model_name            text,
    model_version         text,
    model_parameters      jsonb NOT NULL DEFAULT '{}'::jsonb,
    code_version          text NOT NULL,
    prompt_hash           sha256_hex NOT NULL,
    dataset_manifest_hash sha256_hex,
    trace_key             text,
    source_id             bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key     text NOT NULL,
    source_version        text NOT NULL,
    revision_no           integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at           timestamptz NOT NULL,
    available_at          timestamptz NOT NULL,
    ingested_at           timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash          sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (jsonb_typeof(model_parameters) = 'object'),
    CHECK (as_of_at <= requested_at),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE research_run_events (
    research_run_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    research_run_id       bigint NOT NULL REFERENCES research_runs(research_run_id),
    event_type            text NOT NULL CHECK (event_type IN ('queued', 'started', 'completed', 'failed', 'cancelled', 'timed_out')),
    event_at              timestamptz NOT NULL,
    details               jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id             bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key     text NOT NULL,
    source_version        text NOT NULL,
    revision_no           integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at           timestamptz NOT NULL,
    available_at          timestamptz NOT NULL,
    ingested_at           timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash          sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (jsonb_typeof(details) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE research_run_universe (
    research_run_universe_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    research_run_id          bigint NOT NULL REFERENCES research_runs(research_run_id),
    asset_id                 bigint REFERENCES assets(asset_id),
    market_id                bigint REFERENCES markets(market_id),
    selection_rank           integer CHECK (selection_rank IS NULL OR selection_rank > 0),
    inclusion_reason         text,
    source_id                bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key        text NOT NULL,
    source_version           text NOT NULL,
    revision_no              integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at              timestamptz NOT NULL,
    available_at             timestamptz NOT NULL,
    ingested_at              timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash             sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (num_nonnulls(asset_id, market_id) = 1),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE research_run_inputs (
    research_run_input_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    research_run_id       bigint NOT NULL REFERENCES research_runs(research_run_id),
    input_table           text NOT NULL,
    input_record_id       bigint NOT NULL,
    source_id             bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key     text NOT NULL,
    source_version        text NOT NULL,
    revision_no           integer NOT NULL CHECK (revision_no > 0),
    observed_at           timestamptz NOT NULL,
    available_at          timestamptz NOT NULL,
    ingested_at           timestamptz NOT NULL,
    content_hash          sha256_hex NOT NULL,
    captured_at           timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (research_run_id, input_table, input_record_id, content_hash),
    CHECK (btrim(input_table) <> ''),
    CHECK (input_record_id > 0),
    CHECK (observed_at <= available_at AND available_at <= ingested_at),
    CHECK (ingested_at <= captured_at)
);

CREATE TABLE research_artifacts (
    research_artifact_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    research_run_id      bigint NOT NULL REFERENCES research_runs(research_run_id),
    artifact_key         text NOT NULL,
    artifact_type        text NOT NULL,
    schema_version       text NOT NULL,
    artifact_json        jsonb,
    storage_uri          text,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (research_run_id, artifact_key, revision_no),
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (num_nonnulls(artifact_json, storage_uri) >= 1),
    CHECK (artifact_json IS NULL OR jsonb_typeof(artifact_json) IN ('object', 'array')),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE OR REPLACE FUNCTION enforce_research_input_cutoff()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    cutoff timestamptz;
BEGIN
    SELECT as_of_at INTO STRICT cutoff
    FROM research_runs
    WHERE research_run_id = NEW.research_run_id;

    IF NEW.observed_at > cutoff OR NEW.available_at > cutoff OR NEW.ingested_at > cutoff THEN
        RAISE EXCEPTION
            'research input crosses run cutoff: observed %, available %, ingested %, cutoff %',
            NEW.observed_at, NEW.available_at, NEW.ingested_at, cutoff
            USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER research_inputs_cutoff_guard
BEFORE INSERT ON research_run_inputs
FOR EACH ROW EXECUTE FUNCTION enforce_research_input_cutoff();

CREATE INDEX research_runs_asof_idx ON research_runs (as_of_at DESC, requested_at DESC);
CREATE INDEX research_events_run_idx ON research_run_events (research_run_id, event_at);
CREATE INDEX research_universe_run_idx ON research_run_universe (research_run_id, selection_rank);
CREATE UNIQUE INDEX research_universe_asset_unique_idx
    ON research_run_universe (research_run_id, asset_id)
    WHERE asset_id IS NOT NULL;
CREATE UNIQUE INDEX research_universe_market_unique_idx
    ON research_run_universe (research_run_id, market_id)
    WHERE market_id IS NOT NULL;
CREATE INDEX research_inputs_run_idx ON research_run_inputs (research_run_id, input_table, observed_at);
CREATE INDEX research_inputs_cutoff_idx ON research_run_inputs (research_run_id, available_at DESC, ingested_at DESC);
CREATE INDEX research_artifacts_run_idx ON research_artifacts (research_run_id, artifact_type);

-- ---------------------------------------------------------------------------
-- Deterministic risk plane, alerts and human approval
-- ---------------------------------------------------------------------------

CREATE TABLE risk_policies (
    risk_policy_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    policy_key           text NOT NULL,
    policy_version       text NOT NULL,
    effective_from       timestamptz NOT NULL,
    effective_to         timestamptz,
    policy_document      jsonb NOT NULL,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (policy_key, policy_version),
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (effective_to IS NULL OR effective_from < effective_to),
    CHECK (jsonb_typeof(policy_document) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE risk_assessments (
    risk_assessment_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    assessment_key       text NOT NULL UNIQUE,
    research_run_id      bigint NOT NULL REFERENCES research_runs(research_run_id),
    risk_policy_id       bigint NOT NULL REFERENCES risk_policies(risk_policy_id),
    asset_id             bigint REFERENCES assets(asset_id),
    market_id            bigint REFERENCES markets(market_id),
    horizon_seconds      integer NOT NULL CHECK (horizon_seconds > 0),
    decision             text NOT NULL CHECK (decision IN ('NO_SIGNAL', 'ALERT', 'PAPER_PROPOSAL', 'REJECTED')),
    thesis               text,
    counter_evidence     jsonb NOT NULL DEFAULT '[]'::jsonb,
    scenarios            jsonb NOT NULL DEFAULT '[]'::jsonb,
    invalidation_rules   jsonb NOT NULL DEFAULT '[]'::jsonb,
    regime_probabilities jsonb NOT NULL DEFAULT '{}'::jsonb,
    calibrated_score     numeric CHECK (calibrated_score IS NULL OR calibrated_score BETWEEN 0 AND 1),
    data_quality_score   numeric NOT NULL CHECK (data_quality_score BETWEEN 0 AND 1),
    expires_at           timestamptz NOT NULL,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (asset_id IS NOT NULL OR market_id IS NOT NULL),
    CHECK (jsonb_typeof(counter_evidence) = 'array'),
    CHECK (jsonb_typeof(scenarios) = 'array'),
    CHECK (jsonb_typeof(invalidation_rules) = 'array'),
    CHECK (jsonb_typeof(regime_probabilities) = 'object'),
    CHECK (expires_at > observed_at),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE risk_assessment_flags (
    risk_assessment_flag_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    risk_assessment_id      bigint NOT NULL REFERENCES risk_assessments(risk_assessment_id),
    flag_key                text NOT NULL,
    severity                text NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    hard_veto               boolean NOT NULL DEFAULT false,
    details                 jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id               bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key       text NOT NULL,
    source_version          text NOT NULL,
    revision_no             integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at             timestamptz NOT NULL,
    available_at            timestamptz NOT NULL,
    ingested_at             timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash            sha256_hex NOT NULL,
    UNIQUE (risk_assessment_id, flag_key),
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (jsonb_typeof(details) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE alerts (
    alert_id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alert_key             text NOT NULL UNIQUE,
    research_run_id       bigint REFERENCES research_runs(research_run_id),
    risk_assessment_id    bigint REFERENCES risk_assessments(risk_assessment_id),
    asset_id              bigint REFERENCES assets(asset_id),
    market_id             bigint REFERENCES markets(market_id),
    alert_type            text NOT NULL,
    severity              text NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    title                 text NOT NULL,
    message               text NOT NULL,
    dedupe_key            text NOT NULL,
    expires_at            timestamptz,
    payload               jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id             bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key     text NOT NULL,
    source_version        text NOT NULL,
    revision_no           integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at           timestamptz NOT NULL,
    available_at          timestamptz NOT NULL,
    ingested_at           timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash          sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK (expires_at IS NULL OR expires_at > observed_at),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE alert_events (
    alert_event_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alert_id             bigint NOT NULL REFERENCES alerts(alert_id),
    event_type           text NOT NULL CHECK (event_type IN ('created', 'delivered', 'acknowledged', 'expired', 'delivery_failed')),
    event_at             timestamptz NOT NULL,
    channel              text,
    actor_key            text,
    details              jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (jsonb_typeof(details) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE approval_requests (
    approval_request_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    approval_key         text NOT NULL UNIQUE,
    target_type          text NOT NULL,
    target_key           text NOT NULL,
    research_run_id      bigint REFERENCES research_runs(research_run_id),
    risk_assessment_id   bigint REFERENCES risk_assessments(risk_assessment_id),
    requested_by         text NOT NULL,
    requested_at         timestamptz NOT NULL,
    expires_at           timestamptz NOT NULL,
    request_payload      jsonb NOT NULL,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (jsonb_typeof(request_payload) = 'object'),
    CHECK (requested_at < expires_at),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE approval_decisions (
    approval_decision_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    approval_request_id  bigint NOT NULL UNIQUE REFERENCES approval_requests(approval_request_id),
    decision             text NOT NULL CHECK (decision IN ('approved', 'rejected', 'expired', 'cancelled')),
    decided_by           text NOT NULL,
    decided_at           timestamptz NOT NULL,
    rationale            text,
    decision_payload     jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (jsonb_typeof(decision_payload) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE INDEX risk_policy_pit_idx ON risk_policies (policy_key, effective_from DESC, available_at DESC, ingested_at DESC);
CREATE INDEX risk_assessments_run_idx ON risk_assessments (research_run_id, observed_at DESC);
CREATE INDEX risk_assessments_scope_idx ON risk_assessments (asset_id, market_id, expires_at);
CREATE INDEX risk_flags_veto_idx ON risk_assessment_flags (risk_assessment_id, hard_veto) WHERE hard_veto;
CREATE INDEX alerts_scope_idx ON alerts (asset_id, market_id, observed_at DESC);
CREATE INDEX alerts_dedupe_idx ON alerts (dedupe_key, observed_at DESC);
CREATE INDEX alert_events_idx ON alert_events (alert_id, event_at);
CREATE INDEX approval_requests_target_idx ON approval_requests (target_type, target_key, requested_at DESC);

-- ---------------------------------------------------------------------------
-- Paper/shadow trading ledger. Orders never change state in-place.
-- ---------------------------------------------------------------------------

CREATE TABLE paper_accounts (
    paper_account_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_key         text NOT NULL UNIQUE,
    mode                text NOT NULL CHECK (mode IN ('paper', 'shadow', 'canary_simulation')),
    base_asset_id       bigint NOT NULL REFERENCES assets(asset_id),
    starting_balance    numeric NOT NULL CHECK (starting_balance >= 0),
    simulation_version  text NOT NULL,
    opened_at           timestamptz NOT NULL,
    source_id           bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key   text NOT NULL,
    source_version      text NOT NULL,
    revision_no         integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at         timestamptz NOT NULL,
    available_at        timestamptz NOT NULL,
    ingested_at         timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash        sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE paper_orders (
    paper_order_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_key               text NOT NULL UNIQUE,
    paper_account_id        bigint NOT NULL REFERENCES paper_accounts(paper_account_id),
    market_id               bigint NOT NULL REFERENCES markets(market_id),
    research_run_id         bigint REFERENCES research_runs(research_run_id),
    risk_assessment_id      bigint NOT NULL REFERENCES risk_assessments(risk_assessment_id),
    approval_request_id     bigint REFERENCES approval_requests(approval_request_id),
    requires_human_approval boolean NOT NULL DEFAULT true,
    side                    text NOT NULL CHECK (side IN ('buy', 'sell')),
    order_type              text NOT NULL CHECK (order_type IN ('market', 'limit', 'stop', 'stop_limit')),
    quantity                numeric NOT NULL CHECK (quantity > 0),
    limit_price             numeric CHECK (limit_price IS NULL OR limit_price > 0),
    stop_price              numeric CHECK (stop_price IS NULL OR stop_price > 0),
    time_in_force           text NOT NULL CHECK (time_in_force IN ('GTC', 'IOC', 'FOK', 'DAY')),
    submitted_at            timestamptz NOT NULL,
    expires_at              timestamptz,
    client_tags             jsonb NOT NULL DEFAULT '{}'::jsonb,
    simulation_version      text NOT NULL,
    source_id               bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key       text NOT NULL,
    source_version          text NOT NULL,
    revision_no             integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at             timestamptz NOT NULL,
    available_at            timestamptz NOT NULL,
    ingested_at             timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash            sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK ((order_type IN ('limit', 'stop_limit')) = (limit_price IS NOT NULL)),
    CHECK ((order_type IN ('stop', 'stop_limit')) = (stop_price IS NOT NULL)),
    CHECK (expires_at IS NULL OR submitted_at < expires_at),
    CHECK (NOT requires_human_approval OR approval_request_id IS NOT NULL),
    CHECK (jsonb_typeof(client_tags) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE paper_order_events (
    paper_order_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    paper_order_id       bigint NOT NULL REFERENCES paper_orders(paper_order_id),
    event_type           text NOT NULL CHECK (event_type IN (
                              'submitted', 'accepted', 'rejected', 'partially_filled',
                              'filled', 'cancel_requested', 'cancelled', 'expired'
                          )),
    event_at             timestamptz NOT NULL,
    reason_code          text,
    details              jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (jsonb_typeof(details) = 'object'),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE paper_fills (
    paper_fill_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fill_key             text NOT NULL UNIQUE,
    paper_order_id       bigint NOT NULL REFERENCES paper_orders(paper_order_id),
    fill_at              timestamptz NOT NULL,
    price                numeric NOT NULL CHECK (price > 0),
    quantity             numeric NOT NULL CHECK (quantity > 0),
    fee_amount           numeric NOT NULL DEFAULT 0 CHECK (fee_amount >= 0),
    fee_asset_id         bigint REFERENCES assets(asset_id),
    liquidity_role       text CHECK (liquidity_role IS NULL OR liquidity_role IN ('maker', 'taker')),
    reference_price      numeric CHECK (reference_price IS NULL OR reference_price > 0),
    slippage_bps         numeric,
    latency_ms           integer CHECK (latency_ms IS NULL OR latency_ms >= 0),
    fill_model_version   text NOT NULL,
    source_id            bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key    text NOT NULL,
    source_version       text NOT NULL,
    revision_no          integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at          timestamptz NOT NULL,
    available_at         timestamptz NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash         sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK ((fee_amount = 0) OR fee_asset_id IS NOT NULL),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TABLE paper_position_snapshots (
    paper_position_snapshot_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_key               text NOT NULL UNIQUE,
    paper_account_id           bigint NOT NULL REFERENCES paper_accounts(paper_account_id),
    asset_id                   bigint REFERENCES assets(asset_id),
    market_id                  bigint REFERENCES markets(market_id),
    position_at                timestamptz NOT NULL,
    quantity                   numeric NOT NULL,
    average_entry_price        numeric CHECK (average_entry_price IS NULL OR average_entry_price >= 0),
    mark_price                 numeric CHECK (mark_price IS NULL OR mark_price >= 0),
    realized_pnl               numeric NOT NULL DEFAULT 0,
    unrealized_pnl             numeric NOT NULL DEFAULT 0,
    margin_used                numeric CHECK (margin_used IS NULL OR margin_used >= 0),
    valuation_model_version    text NOT NULL,
    source_id                  bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key          text NOT NULL,
    source_version             text NOT NULL,
    revision_no                integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at                timestamptz NOT NULL,
    available_at               timestamptz NOT NULL,
    ingested_at                timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash               sha256_hex NOT NULL,
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (num_nonnulls(asset_id, market_id) = 1),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE OR REPLACE FUNCTION enforce_paper_order_approval()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    approved boolean;
BEGIN
    IF NOT NEW.requires_human_approval THEN
        RETURN NEW;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM approval_requests ar
        JOIN approval_decisions ad USING (approval_request_id)
        WHERE ar.approval_request_id = NEW.approval_request_id
          AND ad.decision = 'approved'
          AND ad.decided_at <= NEW.submitted_at
          AND ar.expires_at >= NEW.submitted_at
          AND ad.available_at <= NEW.submitted_at
          AND ad.ingested_at <= NEW.submitted_at
    ) INTO approved;

    IF NOT approved THEN
        RAISE EXCEPTION 'paper order % has no valid point-in-time human approval', NEW.order_key
            USING ERRCODE = '42501';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER paper_order_approval_guard
BEFORE INSERT ON paper_orders
FOR EACH ROW EXECUTE FUNCTION enforce_paper_order_approval();

CREATE INDEX paper_orders_account_idx ON paper_orders (paper_account_id, submitted_at DESC);
CREATE INDEX paper_orders_market_idx ON paper_orders (market_id, submitted_at DESC);
CREATE INDEX paper_order_events_idx ON paper_order_events (paper_order_id, event_at);
CREATE INDEX paper_fills_order_idx ON paper_fills (paper_order_id, fill_at);
CREATE INDEX paper_fills_time_idx ON paper_fills (fill_at DESC);
CREATE INDEX paper_positions_account_idx ON paper_position_snapshots (paper_account_id, position_at DESC);
CREATE INDEX paper_positions_scope_idx ON paper_position_snapshots (asset_id, market_id, position_at DESC);

-- ---------------------------------------------------------------------------
-- Tamper-evident audit trail. Hashes are calculated by the application.
-- ---------------------------------------------------------------------------

CREATE TABLE audit_log (
    audit_log_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream_key          text NOT NULL,
    sequence_no         bigint NOT NULL CHECK (sequence_no > 0),
    actor_type          text NOT NULL,
    actor_key           text NOT NULL,
    action              text NOT NULL,
    entity_type         text NOT NULL,
    entity_key          text NOT NULL,
    request_key         text,
    trace_key           text,
    research_run_id     bigint REFERENCES research_runs(research_run_id),
    details             jsonb NOT NULL DEFAULT '{}'::jsonb,
    previous_entry_hash sha256_hex,
    entry_hash          sha256_hex NOT NULL,
    source_id           bigint NOT NULL REFERENCES data_sources(source_id),
    source_record_key   text NOT NULL,
    source_version      text NOT NULL,
    revision_no         integer NOT NULL DEFAULT 1 CHECK (revision_no > 0),
    observed_at         timestamptz NOT NULL,
    available_at        timestamptz NOT NULL,
    ingested_at         timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash        sha256_hex NOT NULL,
    UNIQUE (stream_key, sequence_no),
    UNIQUE (entry_hash),
    UNIQUE (source_id, source_record_key, revision_no),
    CHECK (jsonb_typeof(details) = 'object'),
    CHECK ((sequence_no = 1 AND previous_entry_hash IS NULL) OR
           (sequence_no > 1 AND previous_entry_hash IS NOT NULL)),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE OR REPLACE FUNCTION enforce_audit_hash_chain()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_previous sha256_hex;
BEGIN
    IF NEW.sequence_no = 1 THEN
        RETURN NEW;
    END IF;

    SELECT entry_hash INTO expected_previous
    FROM audit_log
    WHERE stream_key = NEW.stream_key
      AND sequence_no = NEW.sequence_no - 1;

    IF expected_previous IS NULL OR expected_previous <> NEW.previous_entry_hash THEN
        RAISE EXCEPTION 'invalid audit hash chain for stream %, sequence %',
            NEW.stream_key, NEW.sequence_no
            USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER audit_hash_chain_guard
BEFORE INSERT ON audit_log
FOR EACH ROW EXECUTE FUNCTION enforce_audit_hash_chain();

CREATE INDEX audit_entity_idx ON audit_log (entity_type, entity_key, observed_at DESC);
CREATE INDEX audit_trace_idx ON audit_log (trace_key, observed_at) WHERE trace_key IS NOT NULL;
CREATE INDEX audit_run_idx ON audit_log (research_run_id, observed_at) WHERE research_run_id IS NOT NULL;
CREATE INDEX audit_ingested_brin_idx ON audit_log USING brin (ingested_at);

-- ---------------------------------------------------------------------------
-- Make base tables append-only. Corrections are new revisions or event rows.
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'data_sources', 'data_source_versions', 'ingestion_batches',
        'exchanges', 'exchange_versions', 'assets', 'asset_versions', 'asset_symbols',
        'markets', 'market_versions', 'market_symbols',
        'candles', 'trades', 'orderbook_snapshots', 'orderbook_levels', 'derivatives_metrics',
        'onchain_metrics', 'macro_series', 'macro_observations', 'documents',
        'document_entities', 'token_unlocks',
        'data_quality_incidents', 'data_quality_incident_events',
        'research_runs', 'research_run_events', 'research_run_universe',
        'research_run_inputs', 'research_artifacts',
        'risk_policies', 'risk_assessments', 'risk_assessment_flags',
        'alerts', 'alert_events', 'approval_requests', 'approval_decisions',
        'paper_accounts', 'paper_orders', 'paper_order_events', 'paper_fills',
        'paper_position_snapshots', 'audit_log'
    ]
    LOOP
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I.%I '
            'FOR EACH ROW EXECUTE FUNCTION %I.forbid_append_only_change()',
            table_name || '_append_only_row_guard', 'crypto_agent', table_name, 'crypto_agent'
        );
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE TRUNCATE ON %I.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION %I.forbid_append_only_change()',
            table_name || '_append_only_truncate_guard', 'crypto_agent', table_name, 'crypto_agent'
        );
    END LOOP;
END;
$$;

COMMENT ON SCHEMA crypto_agent IS
'Append-only, point-in-time store for crypto research, risk review and paper trading.';
COMMENT ON COLUMN candles.observed_at IS 'Time represented/observed by the source, never the database receipt time.';
COMMENT ON COLUMN candles.available_at IS 'Earliest verified time this revision was obtainable from the source.';
COMMENT ON COLUMN candles.ingested_at IS 'Time this exact revision entered the agent data boundary.';
COMMENT ON COLUMN macro_observations.vintage_at IS 'Provider vintage/revision timestamp; later vintages remain separate rows.';
COMMENT ON COLUMN research_runs.as_of_at IS 'Hard knowledge cutoff. Every research_run_inputs timestamp must be <= this value.';
COMMENT ON TABLE research_run_inputs IS 'Immutable manifest of exact source revisions used by a run.';
COMMENT ON TABLE paper_order_events IS 'Event-sourced paper order lifecycle; paper_orders is never updated.';
COMMENT ON TABLE audit_log IS 'Append-only hash chain. entry_hash is computed from canonical payload plus previous_entry_hash.';

COMMIT;
