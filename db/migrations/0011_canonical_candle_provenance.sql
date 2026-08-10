-- V1.1: immutable venue bindings, receipt lineage and canonical candle provenance.
-- No BEGIN/COMMIT: the application runner wraps this file in one transaction and
-- records its checksum only after every statement succeeds.

CREATE TABLE IF NOT EXISTS crypto_agent.schema_migrations (
    version         text PRIMARY KEY,
    name            text NOT NULL,
    checksum_sha256 crypto_agent.sha256_hex NOT NULL,
    applied_at      timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (btrim(version) <> ''),
    CHECK (btrim(name) <> '')
);

-- Explicitly binds a market-data feed to one venue-specific market and to the
-- normalized symbol returned by the adapter. A different venue necessarily uses
-- a different market_id; equivalence is checked through the markets asset scope.
CREATE TABLE crypto_agent.market_data_source_bindings (
    binding_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id        bigint NOT NULL REFERENCES crypto_agent.data_sources(source_id),
    market_id        bigint NOT NULL REFERENCES crypto_agent.markets(market_id),
    canonical_symbol text NOT NULL,
    venue_symbol     text NOT NULL,
    observed_at      timestamptz NOT NULL,
    available_at     timestamptz NOT NULL,
    ingested_at      timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash     crypto_agent.sha256_hex NOT NULL,
    UNIQUE (source_id, market_id),
    CHECK (btrim(canonical_symbol) <> ''),
    CHECK (btrim(venue_symbol) <> ''),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

-- A canonical series points at a synthetic/derived market. Raw members may have
-- different market_id values but must match this market's base, quote and type.
CREATE TABLE crypto_agent.canonical_candle_series (
    canonical_series_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    series_key          text NOT NULL UNIQUE,
    canonical_market_id bigint NOT NULL REFERENCES crypto_agent.markets(market_id),
    canonical_source_id bigint NOT NULL REFERENCES crypto_agent.data_sources(source_id),
    risk_policy_id      bigint NOT NULL
        REFERENCES crypto_agent.risk_policies(risk_policy_id),
    canonical_symbol    text NOT NULL,
    interval_seconds    integer NOT NULL CHECK (interval_seconds > 0),
    algorithm_version   text NOT NULL,
    policy_hash         crypto_agent.sha256_hex NOT NULL,
    observed_at         timestamptz NOT NULL,
    available_at        timestamptz NOT NULL,
    ingested_at         timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash        crypto_agent.sha256_hex NOT NULL,
    UNIQUE (
        canonical_market_id, canonical_source_id, risk_policy_id, interval_seconds,
        algorithm_version, policy_hash
    ),
    CHECK (btrim(series_key) <> ''),
    CHECK (btrim(canonical_symbol) <> ''),
    CHECK (
        algorithm_version = 'cross_exchange_spot_consensus_v1'
    ),
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

-- Preserves both the adapter's receipt time and the later durable-store boundary.
-- Re-fetches are separate immutable receipts even when candle bytes are unchanged.
CREATE TABLE crypto_agent.source_candle_receipts (
    source_candle_receipt_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    candle_id                bigint NOT NULL REFERENCES crypto_agent.candles(candle_id),
    provider_ingested_at     timestamptz NOT NULL,
    received_at              timestamptz NOT NULL,
    cutoff_as_of             timestamptz NOT NULL,
    receipt_hash             crypto_agent.sha256_hex NOT NULL UNIQUE,
    CHECK (provider_ingested_at <= cutoff_as_of AND cutoff_as_of <= received_at)
);

-- One immutable computation manifest binds every derived row to its algorithm,
-- policy, exact cutoff, fixed input window, diagnostics and payload hashes.
-- Canonical candles deliberately leave candles.base_volume NULL: the consensus
-- volume is a dimensionless ratio and is stored here with an explicit unit.
CREATE TABLE crypto_agent.canonical_candle_manifests (
    canonical_candle_id bigint PRIMARY KEY REFERENCES crypto_agent.candles(candle_id),
    canonical_series_id bigint NOT NULL
        REFERENCES crypto_agent.canonical_candle_series(canonical_series_id),
    risk_policy_id      bigint NOT NULL REFERENCES crypto_agent.risk_policies(risk_policy_id),
    algorithm_version   text NOT NULL,
    policy_hash         crypto_agent.sha256_hex NOT NULL,
    consensus_window_size integer NOT NULL CHECK (consensus_window_size = 120),
    cutoff_as_of        timestamptz NOT NULL,
    inputs_hash         crypto_agent.sha256_hex NOT NULL,
    computed_payload_hash crypto_agent.sha256_hex NOT NULL,
    normalized_volume   numeric NOT NULL CHECK (normalized_volume >= 0),
    normalized_volume_unit text NOT NULL
        CHECK (normalized_volume_unit = 'dimensionless_ratio_to_source_median'),
    diagnostics_document jsonb NOT NULL
        CHECK (jsonb_typeof(diagnostics_document) = 'object'),
    evidence_hash       crypto_agent.sha256_hex NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (btrim(algorithm_version) <> '')
);

CREATE TABLE crypto_agent.canonical_candle_provenance (
    canonical_candle_id bigint NOT NULL
        REFERENCES crypto_agent.candles(candle_id),
    source_candle_id    bigint NOT NULL
        REFERENCES crypto_agent.candles(candle_id),
    input_role          text NOT NULL CHECK (input_role IN ('observation', 'context')),
    linked_at           timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (canonical_candle_id, source_candle_id),
    CHECK (canonical_candle_id <> source_candle_id)
);

CREATE INDEX market_data_source_bindings_market_idx
ON crypto_agent.market_data_source_bindings (market_id, source_id);
CREATE INDEX canonical_candle_series_scope_idx
ON crypto_agent.canonical_candle_series (
    canonical_market_id, interval_seconds, available_at DESC, ingested_at DESC
);
CREATE INDEX source_candle_receipts_candle_idx
ON crypto_agent.source_candle_receipts (candle_id, received_at DESC);
CREATE INDEX canonical_candle_manifests_series_idx
ON crypto_agent.canonical_candle_manifests (canonical_series_id, canonical_candle_id);
CREATE INDEX canonical_candle_provenance_source_idx
ON crypto_agent.canonical_candle_provenance (source_candle_id, canonical_candle_id);

CREATE OR REPLACE FUNCTION crypto_agent.enforce_market_data_source_binding()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_symbol text;
BEGIN
    SELECT base_version.canonical_symbol || '/' || quote_version.canonical_symbol
    INTO expected_symbol
    FROM crypto_agent.markets market
    JOIN LATERAL (
        SELECT canonical_symbol
        FROM crypto_agent.asset_versions
        WHERE asset_id = market.base_asset_id
          AND available_at <= NEW.available_at
          AND ingested_at <= NEW.ingested_at
        ORDER BY available_at DESC, ingested_at DESC, revision_no DESC
        LIMIT 1
    ) base_version ON true
    JOIN LATERAL (
        SELECT canonical_symbol
        FROM crypto_agent.asset_versions
        WHERE asset_id = market.quote_asset_id
          AND available_at <= NEW.available_at
          AND ingested_at <= NEW.ingested_at
        ORDER BY available_at DESC, ingested_at DESC, revision_no DESC
        LIMIT 1
    ) quote_version ON true
    WHERE market.market_id = NEW.market_id;

    IF expected_symbol IS NULL OR expected_symbol <> NEW.canonical_symbol THEN
        RAISE EXCEPTION 'market-data binding canonical symbol does not match market assets'
            USING ERRCODE = '23514';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM crypto_agent.market_symbols symbol
        WHERE symbol.market_id = NEW.market_id
          AND symbol.symbol = NEW.venue_symbol
          AND symbol.available_at <= NEW.available_at
          AND symbol.ingested_at <= NEW.ingested_at
    ) THEN
        RAISE EXCEPTION 'market-data binding venue symbol is not registered for market'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION crypto_agent.enforce_canonical_candle_series()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    expected_symbol text;
BEGIN
    SELECT base_version.canonical_symbol || '/' || quote_version.canonical_symbol
    INTO expected_symbol
    FROM crypto_agent.markets market
    JOIN LATERAL (
        SELECT canonical_symbol
        FROM crypto_agent.asset_versions
        WHERE asset_id = market.base_asset_id
          AND available_at <= NEW.available_at
          AND ingested_at <= NEW.ingested_at
        ORDER BY available_at DESC, ingested_at DESC, revision_no DESC
        LIMIT 1
    ) base_version ON true
    JOIN LATERAL (
        SELECT canonical_symbol
        FROM crypto_agent.asset_versions
        WHERE asset_id = market.quote_asset_id
          AND available_at <= NEW.available_at
          AND ingested_at <= NEW.ingested_at
        ORDER BY available_at DESC, ingested_at DESC, revision_no DESC
        LIMIT 1
    ) quote_version ON true
    WHERE market.market_id = NEW.canonical_market_id;

    IF expected_symbol IS NULL OR expected_symbol <> NEW.canonical_symbol THEN
        RAISE EXCEPTION 'canonical series symbol does not match market assets'
            USING ERRCODE = '23514';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM crypto_agent.risk_policies policy
        WHERE policy.risk_policy_id = NEW.risk_policy_id
          AND policy.content_hash = NEW.policy_hash
          AND policy.available_at <= NEW.available_at
          AND policy.ingested_at <= NEW.ingested_at
          AND policy.effective_from <= NEW.observed_at
          AND (policy.effective_to IS NULL OR policy.effective_to > NEW.observed_at)
          AND jsonb_typeof(policy.policy_document->'policy_id') = 'string'
          AND btrim(policy.policy_document->>'policy_id') <> ''
          AND jsonb_typeof(policy.policy_document->'mode') = 'string'
          AND policy.policy_document->>'mode' = 'V1_READ_ONLY'
          AND jsonb_typeof(policy.policy_document->'execution_enabled') = 'boolean'
          AND (policy.policy_document->>'execution_enabled')::boolean IS FALSE
          AND jsonb_typeof(policy.policy_document->'leverage_allowed') = 'boolean'
          AND (policy.policy_document->>'leverage_allowed')::boolean IS FALSE
          AND jsonb_typeof(policy.policy_document->'martingale_allowed') = 'boolean'
          AND (policy.policy_document->>'martingale_allowed')::boolean IS FALSE
          AND jsonb_typeof(
              policy.policy_document->'exchange_credentials_allowed'
          ) = 'boolean'
          AND (
              policy.policy_document->>'exchange_credentials_allowed'
          )::boolean IS FALSE
          AND jsonb_typeof(
              policy.policy_document->'human_approval_required_for_execution'
          ) = 'boolean'
          AND (
              policy.policy_document->>'human_approval_required_for_execution'
          )::boolean IS TRUE
          AND jsonb_typeof(policy.policy_document->'allowed_decisions') = 'array'
          AND jsonb_array_length(
              policy.policy_document->'allowed_decisions'
          ) = 2
          AND policy.policy_document->'allowed_decisions'
              @> '["NO_SIGNAL", "ALERT"]'::jsonb
          AND jsonb_typeof(policy.policy_document->'allowed_assets') = 'array'
          AND jsonb_array_length(policy.policy_document->'allowed_assets')
              BETWEEN 1 AND 20
          AND (policy.policy_document->'allowed_assets') ? NEW.canonical_symbol
          AND NOT EXISTS (
              SELECT 1
              FROM jsonb_array_elements(
                  policy.policy_document->'allowed_assets'
              ) AS asset(value)
              WHERE jsonb_typeof(asset.value) <> 'string'
                 OR btrim(asset.value #>> '{}') = ''
          )
          AND policy.policy_document->'allowed_intervals_minutes'
              = '[240, 1440, 10080]'::jsonb
          AND NEW.interval_seconds IN (14400, 86400, 604800)
          AND jsonb_typeof(policy.policy_document->'min_samples') = 'number'
          AND (policy.policy_document->>'min_samples') ~ '^(0|[1-9][0-9]*)$'
          AND (policy.policy_document->>'min_samples')::integer BETWEEN 60 AND 720
          AND jsonb_typeof(policy.policy_document->'report_ttl_seconds') = 'number'
          AND (policy.policy_document->>'report_ttl_seconds')
              ~ '^(0|[1-9][0-9]*)$'
          AND (policy.policy_document->>'report_ttl_seconds')::integer
              BETWEEN 60 AND 86400
          AND jsonb_typeof(policy.policy_document->'max_clock_skew_seconds') = 'number'
          AND (policy.policy_document->>'max_clock_skew_seconds')
              ~ '^(0|[1-9][0-9]*)$'
          AND (policy.policy_document->>'max_clock_skew_seconds')::integer
              BETWEEN 0 AND 300
          AND jsonb_typeof(policy.policy_document->'min_consensus_sources') = 'number'
          AND policy.policy_document->>'min_consensus_sources' = '2'
          AND jsonb_typeof(policy.policy_document->'min_consensus_overlap') = 'number'
          AND policy.policy_document->>'min_consensus_overlap' = '120'
          AND jsonb_typeof(policy.policy_document->'min_data_quality') = 'number'
          AND (policy.policy_document->>'min_data_quality')::numeric BETWEEN 0.85 AND 1
          AND jsonb_typeof(
              policy.policy_document->'max_staleness_multiplier'
          ) = 'number'
          AND (policy.policy_document->>'max_staleness_multiplier')::numeric
              BETWEEN 0.5 AND 1.25
          AND jsonb_typeof(policy.policy_document->'max_gap_multiplier') = 'number'
          AND (policy.policy_document->>'max_gap_multiplier')::numeric BETWEEN 1 AND 1.5
          AND jsonb_typeof(
              policy.policy_document->'max_annualized_volatility'
          ) = 'number'
          AND (policy.policy_document->>'max_annualized_volatility')::numeric
              BETWEEN 0.1 AND 2
          AND jsonb_typeof(
              policy.policy_document->'max_absolute_period_return'
          ) = 'number'
          AND (policy.policy_document->>'max_absolute_period_return')::numeric
              BETWEEN 0.01 AND 0.5
          AND jsonb_typeof(
              policy.policy_document->'max_cross_source_divergence_bps'
          ) = 'number'
          AND (policy.policy_document->>'max_cross_source_divergence_bps')::numeric
              BETWEEN 1 AND 100
          AND jsonb_typeof(
              policy.policy_document->'max_cross_source_ohlc_divergence_bps'
          ) = 'number'
          AND (policy.policy_document->>'max_cross_source_ohlc_divergence_bps')::numeric
              BETWEEN 1 AND 500
          AND jsonb_typeof(
              policy.policy_document->'max_cross_source_volume_zscore_delta'
          ) = 'number'
          AND (policy.policy_document->>'max_cross_source_volume_zscore_delta')::numeric
              BETWEEN 0.1 AND 3
          AND jsonb_typeof(
              policy.policy_document->'max_divergent_candle_fraction'
          ) = 'number'
          AND (policy.policy_document->>'max_divergent_candle_fraction')::numeric = 0
          AND jsonb_typeof(policy.policy_document->'max_market_price') = 'number'
          AND (policy.policy_document->>'max_market_price')::numeric
              BETWEEN 1000000 AND 1000000000
          AND jsonb_typeof(policy.policy_document->'max_base_volume') = 'number'
          AND (policy.policy_document->>'max_base_volume')::numeric
              BETWEEN 10000000 AND 1000000000000
    ) THEN
        RAISE EXCEPTION 'canonical series policy hash is not an active policy revision'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION crypto_agent.enforce_source_candle_receipt()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    candle_row crypto_agent.candles%ROWTYPE;
BEGIN
    SELECT * INTO STRICT candle_row
    FROM crypto_agent.candles
    WHERE candle_id = NEW.candle_id;

    IF candle_row.available_at > NEW.provider_ingested_at
       OR candle_row.ingested_at > NEW.received_at THEN
        RAISE EXCEPTION 'source candle receipt violates availability lineage'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION crypto_agent.enforce_canonical_candle_manifest()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    candle_row crypto_agent.candles%ROWTYPE;
    series_row crypto_agent.canonical_candle_series%ROWTYPE;
BEGIN
    SELECT * INTO STRICT candle_row
    FROM crypto_agent.candles
    WHERE candle_id = NEW.canonical_candle_id;

    SELECT * INTO STRICT series_row
    FROM crypto_agent.canonical_candle_series
    WHERE canonical_series_id = NEW.canonical_series_id;

    IF candle_row.market_id <> series_row.canonical_market_id
       OR candle_row.source_id <> series_row.canonical_source_id
       OR candle_row.interval_seconds <> series_row.interval_seconds
       OR NEW.risk_policy_id <> series_row.risk_policy_id
       OR NEW.algorithm_version <> series_row.algorithm_version
       OR NEW.policy_hash <> series_row.policy_hash
       OR NEW.consensus_window_size <> 120
       OR candle_row.available_at > NEW.cutoff_as_of
       OR NEW.cutoff_as_of > candle_row.ingested_at
       OR series_row.available_at > NEW.cutoff_as_of
       OR series_row.ingested_at > NEW.cutoff_as_of
       OR candle_row.base_volume IS NOT NULL
       OR NEW.normalized_volume_unit
          <> 'dimensionless_ratio_to_source_median'
       OR NOT EXISTS (
           SELECT 1
           FROM crypto_agent.risk_policies policy
           JOIN crypto_agent.markets market
             ON market.market_id = series_row.canonical_market_id
           JOIN crypto_agent.exchanges exchange
             ON exchange.exchange_id = market.exchange_id
           JOIN crypto_agent.data_sources source
             ON source.source_id = series_row.canonical_source_id
           WHERE policy.risk_policy_id = NEW.risk_policy_id
             AND policy.content_hash = NEW.policy_hash
             AND policy.available_at <= NEW.cutoff_as_of
             AND policy.ingested_at <= NEW.cutoff_as_of
             AND market.available_at <= NEW.cutoff_as_of
             AND market.ingested_at <= NEW.cutoff_as_of
             AND exchange.available_at <= NEW.cutoff_as_of
             AND exchange.ingested_at <= NEW.cutoff_as_of
             AND source.available_at <= NEW.cutoff_as_of
             AND source.ingested_at <= NEW.cutoff_as_of
             AND policy.effective_from <= NEW.cutoff_as_of
             AND (
                 policy.effective_to IS NULL
                 OR policy.effective_to > NEW.cutoff_as_of
             )
       ) THEN
        RAISE EXCEPTION 'canonical candle manifest does not match its series'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION crypto_agent.enforce_canonical_candle_provenance()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    canonical_row crypto_agent.candles%ROWTYPE;
    source_row    crypto_agent.candles%ROWTYPE;
    series_row    crypto_agent.canonical_candle_series%ROWTYPE;
    manifest_row  crypto_agent.canonical_candle_manifests%ROWTYPE;
    canonical_market_row crypto_agent.markets%ROWTYPE;
    source_market_row    crypto_agent.markets%ROWTYPE;
    manifest_created_at  timestamptz;
BEGIN
    SELECT c.* INTO STRICT canonical_row
    FROM crypto_agent.candles c
    JOIN crypto_agent.canonical_candle_manifests manifest
      ON manifest.canonical_candle_id = c.candle_id
    WHERE c.candle_id = NEW.canonical_candle_id;

    SELECT manifest.* INTO STRICT manifest_row
    FROM crypto_agent.canonical_candle_manifests manifest
    WHERE manifest.canonical_candle_id = NEW.canonical_candle_id;
    manifest_created_at := manifest_row.created_at;

    -- Freeze the exact input set with its immutable manifest. A later
    -- transaction must not append context or observation links while leaving
    -- the recorded inputs_hash unchanged.
    IF manifest_created_at <> CURRENT_TIMESTAMP THEN
        RAISE EXCEPTION 'canonical candle provenance set is already finalized'
            USING ERRCODE = '55000';
    END IF;

    SELECT series.* INTO STRICT series_row
    FROM crypto_agent.canonical_candle_manifests manifest
    JOIN crypto_agent.canonical_candle_series series
      ON series.canonical_series_id = manifest.canonical_series_id
    WHERE manifest.canonical_candle_id = NEW.canonical_candle_id;

    SELECT * INTO STRICT source_row
    FROM crypto_agent.candles
    WHERE candle_id = NEW.source_candle_id;

    SELECT * INTO STRICT canonical_market_row
    FROM crypto_agent.markets
    WHERE market_id = canonical_row.market_id;

    SELECT * INTO STRICT source_market_row
    FROM crypto_agent.markets
    WHERE market_id = source_row.market_id;

    IF canonical_row.source_id = source_row.source_id THEN
        RAISE EXCEPTION 'canonical and source candles must use independent data sources'
            USING ERRCODE = '23514';
    END IF;

    IF canonical_market_row.exchange_id = source_market_row.exchange_id THEN
        RAISE EXCEPTION 'canonical market must be separate from source venues'
            USING ERRCODE = '23514';
    END IF;

    IF canonical_market_row.base_asset_id <> source_market_row.base_asset_id
       OR canonical_market_row.quote_asset_id <> source_market_row.quote_asset_id
       OR canonical_market_row.instrument_type <> source_market_row.instrument_type
       OR canonical_row.interval_seconds <> source_row.interval_seconds THEN
        RAISE EXCEPTION 'canonical candle provenance asset/interval scope mismatch'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.input_role = 'observation'
       AND (canonical_row.open_time <> source_row.open_time
            OR canonical_row.close_time <> source_row.close_time) THEN
        RAISE EXCEPTION 'canonical observation time does not match source candle'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.input_role = 'context' AND source_row.close_time > canonical_row.open_time THEN
        RAISE EXCEPTION 'canonical context must precede its observation window'
            USING ERRCODE = '23514';
    END IF;

    IF NOT source_row.is_final THEN
        RAISE EXCEPTION 'canonical candle provenance must be final'
            USING ERRCODE = '23514';
    END IF;

    IF source_row.available_at > canonical_row.available_at
       OR source_row.ingested_at > canonical_row.ingested_at
       OR source_row.available_at > manifest_row.cutoff_as_of
       THEN
        RAISE EXCEPTION 'canonical candle predates its provenance'
            USING ERRCODE = '23514';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM crypto_agent.market_data_source_bindings binding
        WHERE binding.source_id = source_row.source_id
          AND binding.market_id = source_row.market_id
          AND binding.canonical_symbol = series_row.canonical_symbol
          AND binding.available_at <= canonical_row.available_at
          AND binding.ingested_at <= canonical_row.ingested_at
    ) OR NOT EXISTS (
        SELECT 1
        FROM crypto_agent.source_candle_receipts receipt
        WHERE receipt.candle_id = source_row.candle_id
          AND receipt.provider_ingested_at <= manifest_row.cutoff_as_of
          AND receipt.cutoff_as_of <= manifest_row.cutoff_as_of
          AND receipt.received_at <= canonical_row.ingested_at
    ) THEN
        RAISE EXCEPTION 'canonical provenance lacks verified binding or receipt'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.input_role = 'observation' AND EXISTS (
        SELECT 1
        FROM crypto_agent.canonical_candle_provenance existing_link
        JOIN crypto_agent.candles existing_source
          ON existing_source.candle_id = existing_link.source_candle_id
        JOIN crypto_agent.markets existing_market
          ON existing_market.market_id = existing_source.market_id
        WHERE existing_link.canonical_candle_id = NEW.canonical_candle_id
          AND existing_link.input_role = 'observation'
          AND (
              existing_source.source_id = source_row.source_id
              OR existing_market.exchange_id = source_market_row.exchange_id
          )
    ) THEN
        RAISE EXCEPTION 'canonical candle has duplicate source or venue provenance'
            USING ERRCODE = '23505';
    END IF;

    RETURN NEW;
END;
$$;

-- The row-level provenance guard validates each link independently. This deferred
-- guard validates the completed, DB-derived 2 x 120 input universe at commit.
CREATE OR REPLACE FUNCTION crypto_agent.enforce_canonical_exact_provenance()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    manifest_row          crypto_agent.canonical_candle_manifests%ROWTYPE;
    canonical_row         crypto_agent.candles%ROWTYPE;
    total_count           bigint;
    observation_count     bigint;
    context_count         bigint;
    distinct_source_count bigint;
    distinct_venue_count  bigint;
    source_keys           text[];
    venue_keys            text[];
    first_open_time       timestamptz;
    superseded_count      bigint;
    eligible_count        bigint;
    unlinked_count        bigint;
BEGIN
    SELECT * INTO STRICT manifest_row
    FROM crypto_agent.canonical_candle_manifests
    WHERE canonical_candle_id = NEW.canonical_candle_id;

    SELECT * INTO STRICT canonical_row
    FROM crypto_agent.candles
    WHERE candle_id = NEW.canonical_candle_id;

    first_open_time := canonical_row.open_time
        - make_interval(secs => canonical_row.interval_seconds * 119);

    SELECT COUNT(*),
           COUNT(*) FILTER (WHERE provenance.input_role = 'observation'),
           COUNT(*) FILTER (WHERE provenance.input_role = 'context'),
           COUNT(DISTINCT source.source_id),
           COUNT(DISTINCT market.exchange_id),
           ARRAY_AGG(DISTINCT data_source.source_key ORDER BY data_source.source_key),
           ARRAY_AGG(DISTINCT exchange.exchange_key ORDER BY exchange.exchange_key)
    INTO total_count, observation_count, context_count,
         distinct_source_count, distinct_venue_count, source_keys, venue_keys
    FROM crypto_agent.canonical_candle_provenance provenance
    JOIN crypto_agent.candles source
      ON source.candle_id = provenance.source_candle_id
    JOIN crypto_agent.markets market
      ON market.market_id = source.market_id
    JOIN crypto_agent.exchanges exchange
      ON exchange.exchange_id = market.exchange_id
    JOIN crypto_agent.data_sources data_source
      ON data_source.source_id = source.source_id
    WHERE provenance.canonical_candle_id = NEW.canonical_candle_id;

    IF total_count <> 240
       OR observation_count <> 2
       OR context_count <> 238
       OR distinct_source_count <> 2
       OR distinct_venue_count <> 2
       OR source_keys <> ARRAY[
           'coinbase_exchange_spot_rest_v1', 'kraken_spot_rest_v1'
       ]::text[]
       OR venue_keys <> ARRAY['coinbase', 'kraken']::text[] THEN
        RAISE EXCEPTION
            'canonical candle requires the exact approved 2 x 120 provenance set'
            USING ERRCODE = '23514';
    END IF;

    IF canonical_row.close_time
           <> canonical_row.open_time
              + make_interval(secs => canonical_row.interval_seconds)
       OR EXISTS (
            SELECT 1
            FROM crypto_agent.canonical_candle_provenance provenance
            JOIN crypto_agent.candles source
              ON source.candle_id = provenance.source_candle_id
            JOIN crypto_agent.markets market
              ON market.market_id = source.market_id
            JOIN crypto_agent.exchanges exchange
              ON exchange.exchange_id = market.exchange_id
            JOIN crypto_agent.data_sources data_source
              ON data_source.source_id = source.source_id
            WHERE provenance.canonical_candle_id = NEW.canonical_candle_id
              AND (
                  source.close_time
                      <> source.open_time
                         + make_interval(secs => canonical_row.interval_seconds)
                  OR source.open_time < first_open_time
                  OR source.open_time > canonical_row.open_time
                  OR mod(
                      extract(epoch FROM (source.open_time - first_open_time))::bigint,
                      canonical_row.interval_seconds
                  ) <> 0
                  OR (
                      source.open_time = canonical_row.open_time
                      AND provenance.input_role <> 'observation'
                  )
                  OR (
                      source.open_time < canonical_row.open_time
                      AND provenance.input_role <> 'context'
                  )
                  OR NOT (
                      (data_source.source_key = 'kraken_spot_rest_v1'
                       AND exchange.exchange_key = 'kraken')
                      OR
                      (data_source.source_key = 'coinbase_exchange_spot_rest_v1'
                       AND exchange.exchange_key = 'coinbase')
                  )
              )
       ) OR EXISTS (
            SELECT 1
            FROM crypto_agent.canonical_candle_provenance provenance
            JOIN crypto_agent.candles source
              ON source.candle_id = provenance.source_candle_id
            JOIN crypto_agent.data_sources data_source
              ON data_source.source_id = source.source_id
            WHERE provenance.canonical_candle_id = NEW.canonical_candle_id
            GROUP BY data_source.source_key
            HAVING COUNT(*) <> 120
                OR COUNT(DISTINCT source.open_time) <> 120
                OR COUNT(*) FILTER (
                    WHERE provenance.input_role = 'observation'
                ) <> 1
                OR COUNT(DISTINCT source.source_id) <> 1
                OR COUNT(DISTINCT source.market_id) <> 1
       ) THEN
        RAISE EXCEPTION
            'canonical provenance windows must be identical, aligned and contiguous'
            USING ERRCODE = '23514';
    END IF;

    SELECT COUNT(*) INTO superseded_count
    FROM crypto_agent.canonical_candle_provenance provenance
    JOIN crypto_agent.candles selected
      ON selected.candle_id = provenance.source_candle_id
    WHERE provenance.canonical_candle_id = NEW.canonical_candle_id
      AND EXISTS (
          SELECT 1
          FROM crypto_agent.candles newer
          WHERE newer.source_id = selected.source_id
            AND newer.source_record_key = selected.source_record_key
            AND newer.revision_no > selected.revision_no
            AND newer.is_final
            AND newer.available_at <= manifest_row.cutoff_as_of
            AND newer.ingested_at <= canonical_row.ingested_at
            AND EXISTS (
                SELECT 1
                FROM crypto_agent.source_candle_receipts receipt
                WHERE receipt.candle_id = newer.candle_id
                  AND receipt.provider_ingested_at <= manifest_row.cutoff_as_of
                  AND receipt.cutoff_as_of <= manifest_row.cutoff_as_of
                  AND receipt.received_at <= canonical_row.ingested_at
            )
      );

    IF superseded_count <> 0 THEN
        RAISE EXCEPTION 'canonical provenance contains a superseded raw revision'
            USING ERRCODE = '23514';
    END IF;

    WITH eligible_revisions AS (
        SELECT DISTINCT ON (candidate.source_id, candidate.source_record_key)
               candidate.candle_id
        FROM crypto_agent.candles candidate
        JOIN crypto_agent.markets market
          ON market.market_id = candidate.market_id
        JOIN crypto_agent.exchanges exchange
          ON exchange.exchange_id = market.exchange_id
        JOIN crypto_agent.data_sources data_source
          ON data_source.source_id = candidate.source_id
        JOIN crypto_agent.market_data_source_bindings binding
          ON binding.source_id = candidate.source_id
         AND binding.market_id = candidate.market_id
        JOIN crypto_agent.canonical_candle_series series
          ON series.canonical_series_id = manifest_row.canonical_series_id
        JOIN crypto_agent.markets canonical_market
          ON canonical_market.market_id = series.canonical_market_id
        WHERE candidate.interval_seconds = canonical_row.interval_seconds
          AND candidate.open_time >= first_open_time
          AND candidate.close_time <= canonical_row.close_time
          AND candidate.is_final
          AND market.base_asset_id = canonical_market.base_asset_id
          AND market.quote_asset_id = canonical_market.quote_asset_id
          AND market.instrument_type = canonical_market.instrument_type
          AND binding.canonical_symbol = series.canonical_symbol
          AND (
              (data_source.source_key = 'kraken_spot_rest_v1'
               AND exchange.exchange_key = 'kraken')
              OR
              (data_source.source_key = 'coinbase_exchange_spot_rest_v1'
               AND exchange.exchange_key = 'coinbase')
          )
          AND candidate.available_at <= manifest_row.cutoff_as_of
          AND candidate.ingested_at <= canonical_row.ingested_at
          AND binding.available_at <= manifest_row.cutoff_as_of
          AND binding.ingested_at <= canonical_row.ingested_at
          AND market.available_at <= manifest_row.cutoff_as_of
          AND market.ingested_at <= canonical_row.ingested_at
          AND exchange.available_at <= manifest_row.cutoff_as_of
          AND exchange.ingested_at <= canonical_row.ingested_at
          AND data_source.available_at <= manifest_row.cutoff_as_of
          AND data_source.ingested_at <= canonical_row.ingested_at
          AND EXISTS (
              SELECT 1 FROM crypto_agent.market_symbols symbol
              WHERE symbol.market_id = market.market_id
                AND symbol.symbol = binding.venue_symbol
                AND symbol.available_at <= manifest_row.cutoff_as_of
                AND symbol.ingested_at <= canonical_row.ingested_at
          )
          AND EXISTS (
              SELECT 1 FROM crypto_agent.source_candle_receipts receipt
              WHERE receipt.candle_id = candidate.candle_id
                AND receipt.provider_ingested_at <= manifest_row.cutoff_as_of
                AND receipt.cutoff_as_of <= manifest_row.cutoff_as_of
                AND receipt.received_at <= canonical_row.ingested_at
          )
        ORDER BY candidate.source_id, candidate.source_record_key,
                 candidate.revision_no DESC, candidate.available_at DESC,
                 candidate.ingested_at DESC, candidate.candle_id DESC
    )
    SELECT COUNT(*),
           COUNT(*) FILTER (WHERE provenance.source_candle_id IS NULL)
    INTO eligible_count, unlinked_count
    FROM eligible_revisions eligible
    LEFT JOIN crypto_agent.canonical_candle_provenance provenance
      ON provenance.canonical_candle_id = NEW.canonical_candle_id
     AND provenance.source_candle_id = eligible.candle_id
    ;

    IF eligible_count <> 240 OR unlinked_count <> 0 THEN
        RAISE EXCEPTION 'canonical provenance omits eligible raw input revisions'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER source_candle_receipt_integrity_guard
BEFORE INSERT ON crypto_agent.source_candle_receipts
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_source_candle_receipt();

CREATE TRIGGER market_data_source_binding_integrity_guard
BEFORE INSERT ON crypto_agent.market_data_source_bindings
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_market_data_source_binding();

CREATE TRIGGER canonical_candle_series_integrity_guard
BEFORE INSERT ON crypto_agent.canonical_candle_series
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_canonical_candle_series();

CREATE TRIGGER canonical_candle_manifest_integrity_guard
BEFORE INSERT ON crypto_agent.canonical_candle_manifests
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_canonical_candle_manifest();

CREATE TRIGGER canonical_candle_provenance_integrity_guard
BEFORE INSERT ON crypto_agent.canonical_candle_provenance
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_canonical_candle_provenance();

CREATE CONSTRAINT TRIGGER canonical_candle_exact_provenance_guard
AFTER INSERT ON crypto_agent.canonical_candle_manifests
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_canonical_exact_provenance();

CREATE CONSTRAINT TRIGGER canonical_candle_exact_provenance_link_guard
AFTER INSERT ON crypto_agent.canonical_candle_provenance
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_canonical_exact_provenance();

DO $$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'schema_migrations', 'market_data_source_bindings',
        'canonical_candle_series', 'source_candle_receipts',
        'canonical_candle_manifests', 'canonical_candle_provenance'
    ]
    LOOP
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I.%I '
            'FOR EACH ROW EXECUTE FUNCTION %I.forbid_append_only_change()',
            table_name || '_append_only_row_guard', 'crypto_agent', table_name,
            'crypto_agent'
        );
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE TRUNCATE ON %I.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION %I.forbid_append_only_change()',
            table_name || '_append_only_truncate_guard', 'crypto_agent', table_name,
            'crypto_agent'
        );
    END LOOP;
END;
$$;

COMMENT ON TABLE crypto_agent.market_data_source_bindings IS
'Immutable source-to-venue-market and normalized-symbol authorization.';
COMMENT ON TABLE crypto_agent.source_candle_receipts IS
'Exact adapter receipt and durable-store boundary for raw provider candles.';
COMMENT ON TABLE crypto_agent.canonical_candle_manifests IS
'Cutoff, fixed-window policy, dimensionless volume, diagnostics and evidence hashes.';
COMMENT ON TABLE crypto_agent.canonical_candle_provenance IS
'Exact immutable approved 2 x 120 raw inputs used by the canonical consensus.';
