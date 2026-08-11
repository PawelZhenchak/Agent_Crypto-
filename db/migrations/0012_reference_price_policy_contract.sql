-- V1.1: immutable two-venue one-minute reference-price snapshots.
-- Canonical 4h/1d/1w candles remain historical OHLC evidence and are not used
-- as a freshness proxy. The application runner wraps this file in a transaction.

CREATE TABLE crypto_agent.reference_price_manifests (
    reference_price_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reference_key      text NOT NULL UNIQUE,
    canonical_market_id bigint NOT NULL REFERENCES crypto_agent.markets(market_id),
    risk_policy_id     bigint NOT NULL REFERENCES crypto_agent.risk_policies(risk_policy_id),
    symbol             text NOT NULL,
    algorithm_version  text NOT NULL
        CHECK (algorithm_version = 'cross_exchange_reference_price_v1'),
    policy_hash        crypto_agent.sha256_hex NOT NULL,
    event_time         timestamptz NOT NULL,
    cutoff_as_of       timestamptz NOT NULL,
    evaluated_at       timestamptz NOT NULL,
    median_price       numeric NOT NULL CHECK (median_price > 0),
    pairwise_divergence_bps numeric NOT NULL
        CHECK (pairwise_divergence_bps BETWEEN 0 AND 100),
    max_deviation_from_median_bps numeric NOT NULL
        CHECK (max_deviation_from_median_bps BETWEEN 0 AND 50),
    inputs_hash         crypto_agent.sha256_hex NOT NULL,
    evidence_hash       crypto_agent.sha256_hex NOT NULL,
    available_at        timestamptz NOT NULL,
    ingested_at         timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash        crypto_agent.sha256_hex NOT NULL UNIQUE,
    CHECK (btrim(reference_key) <> ''),
    CHECK (btrim(symbol) <> ''),
    CHECK (
        ABS(pairwise_divergence_bps - 2 * max_deviation_from_median_bps)
            <= 0.000000000002
    ),
    CHECK (
        event_time <= cutoff_as_of
        AND cutoff_as_of <= evaluated_at
        AND event_time <= available_at
        AND available_at <= evaluated_at
        AND evaluated_at <= ingested_at
    ),
    CHECK (evaluated_at - event_time <= INTERVAL '300 seconds'),
    CHECK (
        date_trunc('minute', event_time AT TIME ZONE 'UTC')
            = event_time AT TIME ZONE 'UTC'
    )
);

CREATE TABLE crypto_agent.reference_price_provenance (
    reference_price_id bigint NOT NULL
        REFERENCES crypto_agent.reference_price_manifests(reference_price_id),
    source_candle_id   bigint NOT NULL REFERENCES crypto_agent.candles(candle_id),
    source_candle_receipt_id bigint NOT NULL
        REFERENCES crypto_agent.source_candle_receipts(source_candle_receipt_id),
    linked_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (reference_price_id, source_candle_id),
    UNIQUE (reference_price_id, source_candle_receipt_id)
);

CREATE INDEX reference_price_manifest_scope_idx
ON crypto_agent.reference_price_manifests (
    canonical_market_id, symbol, event_time DESC, evaluated_at DESC
);

CREATE INDEX reference_price_provenance_source_idx
ON crypto_agent.reference_price_provenance (
    source_candle_id, reference_price_id
);

-- A pre-0012 canonical series may remain readable for historical replay, but it
-- must not receive new canonical manifests under archived schema r1.
CREATE OR REPLACE FUNCTION crypto_agent.enforce_v1_canonical_manifest_policy()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    policy_document  jsonb;
    max_age_seconds  integer;
    median_deviation numeric;
BEGIN
    SELECT policy.policy_document
    INTO policy_document
    FROM crypto_agent.risk_policies policy
    WHERE policy.risk_policy_id = NEW.risk_policy_id
      AND policy.content_hash = NEW.policy_hash
      AND policy.available_at <= NEW.cutoff_as_of
      AND policy.ingested_at <= NEW.cutoff_as_of
      AND policy.effective_from <= NEW.cutoff_as_of
      AND (policy.effective_to IS NULL OR policy.effective_to > NEW.cutoff_as_of);

    IF policy_document IS NULL
       OR policy_document->>'policy_id' IS DISTINCT FROM
           'v1-read-only-2026-08-10-r2'
       OR jsonb_typeof(policy_document->'policy_schema_version')
           IS DISTINCT FROM 'number'
       OR policy_document->>'policy_schema_version' IS DISTINCT FROM '2'
       OR jsonb_typeof(policy_document->'max_reference_price_age_seconds')
           IS DISTINCT FROM 'number'
       OR (policy_document->>'max_reference_price_age_seconds')
           !~ '^(0|[1-9][0-9]*)$'
       OR jsonb_typeof(
           policy_document->'max_reference_price_deviation_from_median_bps'
       ) IS DISTINCT FROM 'number' THEN
        RAISE EXCEPTION 'new canonical manifests require the current schema-r2 policy'
            USING ERRCODE = '23514';
    END IF;

    BEGIN
        max_age_seconds := (
            policy_document->>'max_reference_price_age_seconds'
        )::integer;
        median_deviation := (
            policy_document->>'max_reference_price_deviation_from_median_bps'
        )::numeric;
    EXCEPTION
        WHEN invalid_text_representation OR numeric_value_out_of_range THEN
            RAISE EXCEPTION 'canonical manifest policy values are invalid'
                USING ERRCODE = '23514';
    END;

    IF max_age_seconds <> 300
       OR median_deviation < 1 OR median_deviation > 50 THEN
        RAISE EXCEPTION 'canonical manifest policy exceeds the V1 safety envelope'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

-- This guard is evaluated at manifest decision time, not at the historical
-- candle selection cutoff. It rejects archived schema-r1 policies for new writes.
CREATE OR REPLACE FUNCTION crypto_agent.enforce_v1_reference_price_manifest()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    policy_document     jsonb;
    canonical_symbol    text;
    max_age_seconds     integer;
    median_deviation    numeric;
    pairwise_limit      numeric;
    maximum_price       numeric;
BEGIN
    SELECT policy.policy_document,
           base_version.canonical_symbol || '/' || quote_version.canonical_symbol
    INTO policy_document, canonical_symbol
    FROM crypto_agent.markets market
    JOIN crypto_agent.exchanges exchange
      ON exchange.exchange_id = market.exchange_id
    JOIN crypto_agent.risk_policies policy
      ON policy.risk_policy_id = NEW.risk_policy_id
     AND policy.content_hash = NEW.policy_hash
    JOIN LATERAL (
        SELECT canonical_symbol
        FROM crypto_agent.asset_versions
        WHERE asset_id = market.base_asset_id
          AND available_at <= NEW.evaluated_at
          AND ingested_at <= NEW.evaluated_at
        ORDER BY available_at DESC, ingested_at DESC, revision_no DESC
        LIMIT 1
    ) base_version ON true
    JOIN LATERAL (
        SELECT canonical_symbol
        FROM crypto_agent.asset_versions
        WHERE asset_id = market.quote_asset_id
          AND available_at <= NEW.evaluated_at
          AND ingested_at <= NEW.evaluated_at
        ORDER BY available_at DESC, ingested_at DESC, revision_no DESC
        LIMIT 1
    ) quote_version ON true
    WHERE market.market_id = NEW.canonical_market_id
      AND market.available_at <= NEW.evaluated_at
      AND market.ingested_at <= NEW.evaluated_at
      AND exchange.available_at <= NEW.evaluated_at
      AND exchange.ingested_at <= NEW.evaluated_at
      AND policy.available_at <= NEW.evaluated_at
      AND policy.ingested_at <= NEW.evaluated_at
      AND policy.effective_from <= NEW.evaluated_at
      AND (policy.effective_to IS NULL OR policy.effective_to > NEW.evaluated_at);

    IF policy_document IS NULL OR canonical_symbol IS DISTINCT FROM NEW.symbol THEN
        RAISE EXCEPTION 'reference-price policy or canonical market is unavailable'
            USING ERRCODE = '23514';
    END IF;

    IF policy_document->>'policy_id' IS DISTINCT FROM
           'v1-read-only-2026-08-10-r2'
       OR jsonb_typeof(policy_document->'policy_schema_version')
           IS DISTINCT FROM 'number'
       OR policy_document->>'policy_schema_version' IS DISTINCT FROM '2'
       OR policy_document->>'mode' IS DISTINCT FROM 'V1_READ_ONLY'
       OR jsonb_typeof(policy_document->'execution_enabled')
           IS DISTINCT FROM 'boolean'
       OR (policy_document->>'execution_enabled')::boolean IS DISTINCT FROM false
       OR jsonb_typeof(policy_document->'min_consensus_sources')
           IS DISTINCT FROM 'number'
       OR policy_document->>'min_consensus_sources' IS DISTINCT FROM '2'
       OR jsonb_typeof(policy_document->'allowed_assets') IS DISTINCT FROM 'array'
       OR NOT (policy_document->'allowed_assets' ? NEW.symbol)
       OR jsonb_typeof(policy_document->'max_reference_price_age_seconds')
           IS DISTINCT FROM 'number'
       OR (policy_document->>'max_reference_price_age_seconds')
           !~ '^(0|[1-9][0-9]*)$'
       OR jsonb_typeof(
           policy_document->'max_reference_price_deviation_from_median_bps'
       ) IS DISTINCT FROM 'number'
       OR jsonb_typeof(policy_document->'max_cross_source_divergence_bps')
           IS DISTINCT FROM 'number'
       OR jsonb_typeof(policy_document->'max_market_price')
           IS DISTINCT FROM 'number' THEN
        RAISE EXCEPTION 'reference-price policy document is not current schema r2'
            USING ERRCODE = '23514';
    END IF;

    BEGIN
        max_age_seconds := (
            policy_document->>'max_reference_price_age_seconds'
        )::integer;
        median_deviation := (
            policy_document->>'max_reference_price_deviation_from_median_bps'
        )::numeric;
        pairwise_limit := (
            policy_document->>'max_cross_source_divergence_bps'
        )::numeric;
        maximum_price := (policy_document->>'max_market_price')::numeric;
    EXCEPTION
        WHEN invalid_text_representation OR numeric_value_out_of_range THEN
            RAISE EXCEPTION 'reference-price policy values are invalid'
                USING ERRCODE = '23514';
    END;

    IF max_age_seconds <> 300
       OR median_deviation < 1 OR median_deviation > 50
       OR pairwise_limit < 1 OR pairwise_limit > 2 * median_deviation
       OR maximum_price <= 0 THEN
        RAISE EXCEPTION 'reference-price policy values exceed the V1 safety envelope'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

-- Validate each source link independently. Receipt.cutoff_as_of is an
-- evaluation/eligibility boundary and therefore compares with evaluated_at;
-- manifest.cutoff_as_of only selects the latest closed minute.
CREATE OR REPLACE FUNCTION crypto_agent.enforce_v1_reference_price_provenance()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    manifest_row        crypto_agent.reference_price_manifests%ROWTYPE;
    source_row          crypto_agent.candles%ROWTYPE;
    receipt_row         crypto_agent.source_candle_receipts%ROWTYPE;
    source_market_row   crypto_agent.markets%ROWTYPE;
    canonical_market_row crypto_agent.markets%ROWTYPE;
    source_key          text;
    venue_key           text;
BEGIN
    SELECT * INTO STRICT manifest_row
    FROM crypto_agent.reference_price_manifests
    WHERE reference_price_id = NEW.reference_price_id;

    SELECT * INTO STRICT source_row
    FROM crypto_agent.candles
    WHERE candle_id = NEW.source_candle_id;

    SELECT * INTO STRICT receipt_row
    FROM crypto_agent.source_candle_receipts
    WHERE source_candle_receipt_id = NEW.source_candle_receipt_id
      AND candle_id = NEW.source_candle_id;

    SELECT * INTO STRICT source_market_row
    FROM crypto_agent.markets
    WHERE market_id = source_row.market_id;

    SELECT * INTO STRICT canonical_market_row
    FROM crypto_agent.markets
    WHERE market_id = manifest_row.canonical_market_id;

    SELECT source.source_key, exchange.exchange_key
    INTO source_key, venue_key
    FROM crypto_agent.data_sources source
    JOIN crypto_agent.exchanges exchange
      ON exchange.exchange_id = source_market_row.exchange_id
    WHERE source.source_id = source_row.source_id
      AND source.available_at <= manifest_row.evaluated_at
      AND source.ingested_at <= manifest_row.evaluated_at
      AND exchange.available_at <= manifest_row.evaluated_at
      AND exchange.ingested_at <= manifest_row.evaluated_at;

    IF source_key IS NULL
       OR NEW.linked_at > manifest_row.ingested_at
       OR NOT (
           (source_key = 'kraken_spot_rest_v1' AND venue_key = 'kraken')
           OR
           (source_key = 'coinbase_exchange_spot_rest_v1'
            AND venue_key = 'coinbase')
       )
       OR source_market_row.exchange_id = canonical_market_row.exchange_id
       OR source_market_row.base_asset_id <> canonical_market_row.base_asset_id
       OR source_market_row.quote_asset_id <> canonical_market_row.quote_asset_id
       OR source_market_row.instrument_type <> canonical_market_row.instrument_type
       OR source_row.interval_seconds <> 60
       OR source_row.open_time <> manifest_row.event_time - INTERVAL '60 seconds'
       OR source_row.close_time <> manifest_row.event_time
       OR date_trunc('minute', source_row.open_time AT TIME ZONE 'UTC')
           <> source_row.open_time AT TIME ZONE 'UTC'
       OR date_trunc('minute', source_row.close_time AT TIME ZONE 'UTC')
           <> source_row.close_time AT TIME ZONE 'UTC'
       OR NOT source_row.is_final
       OR source_row.close_time > source_row.available_at
       OR source_row.available_at > manifest_row.evaluated_at
       OR source_row.ingested_at > manifest_row.ingested_at
       OR source_row.close_price <= 0
       OR scale(source_row.close_price) > 18
       OR receipt_row.provider_ingested_at > manifest_row.evaluated_at
       OR receipt_row.cutoff_as_of > manifest_row.evaluated_at
       OR receipt_row.received_at > manifest_row.ingested_at
       OR NOT EXISTS (
           SELECT 1
           FROM crypto_agent.market_data_source_bindings binding
           WHERE binding.source_id = source_row.source_id
             AND binding.market_id = source_row.market_id
             AND binding.canonical_symbol = manifest_row.symbol
             AND binding.available_at <= manifest_row.evaluated_at
             AND binding.ingested_at <= manifest_row.evaluated_at
             AND EXISTS (
                 SELECT 1
                 FROM crypto_agent.market_symbols symbol
                 WHERE symbol.market_id = source_row.market_id
                   AND symbol.symbol = binding.venue_symbol
                   AND symbol.available_at <= manifest_row.evaluated_at
                   AND symbol.ingested_at <= manifest_row.evaluated_at
             )
       ) THEN
        RAISE EXCEPTION 'reference-price provenance is outside its PIT scope'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM crypto_agent.reference_price_provenance existing_link
        JOIN crypto_agent.candles existing_source
          ON existing_source.candle_id = existing_link.source_candle_id
        JOIN crypto_agent.markets existing_market
          ON existing_market.market_id = existing_source.market_id
        WHERE existing_link.reference_price_id = NEW.reference_price_id
          AND (
              existing_source.source_id = source_row.source_id
              OR existing_market.exchange_id = source_market_row.exchange_id
          )
    ) THEN
        RAISE EXCEPTION 'reference price has duplicate source or venue provenance'
            USING ERRCODE = '23505';
    END IF;

    RETURN NEW;
END;
$$;

-- Recompute the completed two-row snapshot and its exact eligible revision
-- universe at commit. This prevents caller-supplied medians, superseded raw
-- revisions, or incomplete provenance from becoming durable evidence.
CREATE OR REPLACE FUNCTION crypto_agent.enforce_v1_reference_price_exact_provenance()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    manifest_row          crypto_agent.reference_price_manifests%ROWTYPE;
    total_count           bigint;
    distinct_source_count bigint;
    distinct_venue_count  bigint;
    source_keys           text[];
    venue_keys            text[];
    minimum_price         numeric;
    maximum_price         numeric;
    derived_median        numeric;
    derived_pairwise      numeric;
    derived_max_deviation numeric;
    pairwise_numerator    numeric;
    max_deviation_numerator numeric;
    scaled_numerator      numeric;
    rounded_units         numeric;
    derived_available_at  timestamptz;
    policy_max_deviation  numeric;
    policy_max_price      numeric;
    eligible_count        bigint;
    unlinked_count        bigint;
BEGIN
    SELECT * INTO STRICT manifest_row
    FROM crypto_agent.reference_price_manifests
    WHERE reference_price_id = NEW.reference_price_id;

    SELECT COUNT(*),
           COUNT(DISTINCT candle.source_id),
           COUNT(DISTINCT market.exchange_id),
           ARRAY_AGG(DISTINCT source.source_key ORDER BY source.source_key),
           ARRAY_AGG(DISTINCT exchange.exchange_key ORDER BY exchange.exchange_key),
           MIN(candle.close_price), MAX(candle.close_price),
           MAX(candle.available_at)
    INTO total_count, distinct_source_count, distinct_venue_count,
         source_keys, venue_keys, minimum_price, maximum_price,
         derived_available_at
    FROM crypto_agent.reference_price_provenance provenance
    JOIN crypto_agent.candles candle
      ON candle.candle_id = provenance.source_candle_id
    JOIN crypto_agent.markets market ON market.market_id = candle.market_id
    JOIN crypto_agent.exchanges exchange
      ON exchange.exchange_id = market.exchange_id
    JOIN crypto_agent.data_sources source ON source.source_id = candle.source_id
    WHERE provenance.reference_price_id = NEW.reference_price_id;

    IF total_count IS DISTINCT FROM 2
       OR distinct_source_count IS DISTINCT FROM 2
       OR distinct_venue_count IS DISTINCT FROM 2
       OR source_keys IS DISTINCT FROM ARRAY[
           'coinbase_exchange_spot_rest_v1', 'kraken_spot_rest_v1'
       ]::text[]
       OR venue_keys IS DISTINCT FROM ARRAY['coinbase', 'kraken']::text[]
       OR minimum_price IS NULL OR minimum_price <= 0 THEN
        RAISE EXCEPTION 'reference price requires the exact approved 2 x 1 provenance set'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM crypto_agent.reference_price_provenance provenance
        JOIN crypto_agent.candles candle
          ON candle.candle_id = provenance.source_candle_id
        JOIN crypto_agent.markets market ON market.market_id = candle.market_id
        JOIN crypto_agent.exchanges exchange
          ON exchange.exchange_id = market.exchange_id
        JOIN crypto_agent.data_sources source ON source.source_id = candle.source_id
        WHERE provenance.reference_price_id = NEW.reference_price_id
          AND (
              candle.interval_seconds <> 60
              OR candle.open_time <> manifest_row.event_time - INTERVAL '60 seconds'
              OR candle.close_time <> manifest_row.event_time
              OR date_trunc('minute', candle.open_time AT TIME ZONE 'UTC')
                  <> candle.open_time AT TIME ZONE 'UTC'
              OR date_trunc('minute', candle.close_time AT TIME ZONE 'UTC')
                  <> candle.close_time AT TIME ZONE 'UTC'
              OR NOT (
                  (source.source_key = 'kraken_spot_rest_v1'
                   AND exchange.exchange_key = 'kraken')
                  OR
                  (source.source_key = 'coinbase_exchange_spot_rest_v1'
                   AND exchange.exchange_key = 'coinbase')
              )
          )
    ) THEN
        RAISE EXCEPTION 'reference-price source windows are not one aligned UTC minute'
            USING ERRCODE = '23514';
    END IF;

    WITH eligible_revisions AS (
        SELECT DISTINCT ON (candidate.source_id, candidate.source_record_key)
               candidate.candle_id, receipt.source_candle_receipt_id
        FROM crypto_agent.candles candidate
        JOIN crypto_agent.markets market ON market.market_id = candidate.market_id
        JOIN crypto_agent.exchanges exchange
          ON exchange.exchange_id = market.exchange_id
        JOIN crypto_agent.data_sources source ON source.source_id = candidate.source_id
        JOIN crypto_agent.market_data_source_bindings binding
          ON binding.source_id = candidate.source_id
         AND binding.market_id = candidate.market_id
        JOIN LATERAL (
            SELECT source_receipt.source_candle_receipt_id
            FROM crypto_agent.source_candle_receipts source_receipt
            WHERE source_receipt.candle_id = candidate.candle_id
              AND source_receipt.provider_ingested_at
                  <= manifest_row.evaluated_at
              AND source_receipt.cutoff_as_of <= manifest_row.evaluated_at
              AND source_receipt.received_at <= manifest_row.ingested_at
            ORDER BY source_receipt.provider_ingested_at DESC,
                     source_receipt.received_at DESC,
                     source_receipt.source_candle_receipt_id DESC
            LIMIT 1
        ) receipt ON true
        JOIN crypto_agent.markets canonical_market
          ON canonical_market.market_id = manifest_row.canonical_market_id
        WHERE candidate.interval_seconds = 60
          AND candidate.open_time = manifest_row.event_time - INTERVAL '60 seconds'
          AND candidate.close_time = manifest_row.event_time
          AND candidate.is_final
          AND market.base_asset_id = canonical_market.base_asset_id
          AND market.quote_asset_id = canonical_market.quote_asset_id
          AND market.instrument_type = canonical_market.instrument_type
          AND binding.canonical_symbol = manifest_row.symbol
          AND (
              (source.source_key = 'kraken_spot_rest_v1'
               AND exchange.exchange_key = 'kraken')
              OR
              (source.source_key = 'coinbase_exchange_spot_rest_v1'
               AND exchange.exchange_key = 'coinbase')
          )
          AND candidate.available_at <= manifest_row.evaluated_at
          AND candidate.ingested_at <= manifest_row.ingested_at
          AND binding.available_at <= manifest_row.evaluated_at
          AND binding.ingested_at <= manifest_row.evaluated_at
          AND market.available_at <= manifest_row.evaluated_at
          AND market.ingested_at <= manifest_row.evaluated_at
          AND exchange.available_at <= manifest_row.evaluated_at
          AND exchange.ingested_at <= manifest_row.evaluated_at
          AND source.available_at <= manifest_row.evaluated_at
          AND source.ingested_at <= manifest_row.evaluated_at
          AND EXISTS (
              SELECT 1
              FROM crypto_agent.market_symbols symbol
              WHERE symbol.market_id = market.market_id
                AND symbol.symbol = binding.venue_symbol
                AND symbol.available_at <= manifest_row.evaluated_at
                AND symbol.ingested_at <= manifest_row.evaluated_at
          )
        ORDER BY candidate.source_id, candidate.source_record_key,
                 candidate.revision_no DESC, candidate.available_at DESC,
                 candidate.ingested_at DESC, candidate.candle_id DESC
    )
    SELECT COUNT(*),
           COUNT(*) FILTER (WHERE provenance.source_candle_id IS NULL)
    INTO eligible_count, unlinked_count
    FROM eligible_revisions eligible
    LEFT JOIN crypto_agent.reference_price_provenance provenance
      ON provenance.reference_price_id = NEW.reference_price_id
     AND provenance.source_candle_id = eligible.candle_id
     AND provenance.source_candle_receipt_id
         = eligible.source_candle_receipt_id;

    IF eligible_count IS DISTINCT FROM 2 OR unlinked_count IS DISTINCT FROM 0 THEN
        RAISE EXCEPTION 'reference-price provenance omits an eligible raw revision'
            USING ERRCODE = '23514';
    END IF;

    SELECT (policy.policy_document
                ->>'max_reference_price_deviation_from_median_bps')::numeric,
           (policy.policy_document->>'max_market_price')::numeric
    INTO STRICT policy_max_deviation, policy_max_price
    FROM crypto_agent.risk_policies policy
    WHERE policy.risk_policy_id = manifest_row.risk_policy_id
      AND policy.content_hash = manifest_row.policy_hash;

    derived_median := (minimum_price + maximum_price) / 2;
    pairwise_numerator := ABS(maximum_price - minimum_price) * 10000;
    max_deviation_numerator := GREATEST(
        ABS(minimum_price - derived_median),
        ABS(maximum_price - derived_median)
    ) * 10000;

    -- Exact ROUND_HALF_UP of a non-negative rational to 12 decimal places.
    -- div/mod avoid PostgreSQL numeric division precision at the 50-bps edge.
    scaled_numerator := pairwise_numerator * 1000000000000;
    rounded_units := div(scaled_numerator, derived_median);
    IF 2 * mod(scaled_numerator, derived_median) >= derived_median THEN
        rounded_units := rounded_units + 1;
    END IF;
    derived_pairwise := rounded_units / 1000000000000;

    scaled_numerator := max_deviation_numerator * 1000000000000;
    rounded_units := div(scaled_numerator, derived_median);
    IF 2 * mod(scaled_numerator, derived_median) >= derived_median THEN
        rounded_units := rounded_units + 1;
    END IF;
    derived_max_deviation := rounded_units / 1000000000000;

    IF manifest_row.median_price IS DISTINCT FROM derived_median
       OR manifest_row.pairwise_divergence_bps IS DISTINCT FROM derived_pairwise
       OR manifest_row.max_deviation_from_median_bps
           IS DISTINCT FROM derived_max_deviation
       OR manifest_row.available_at IS DISTINCT FROM derived_available_at
       OR max_deviation_numerator
           > policy_max_deviation * derived_median
       OR maximum_price > policy_max_price
       OR manifest_row.event_time > manifest_row.cutoff_as_of
       OR manifest_row.cutoff_as_of > manifest_row.evaluated_at
       OR manifest_row.evaluated_at - manifest_row.event_time
           > INTERVAL '300 seconds' THEN
        RAISE EXCEPTION 'reference-price manifest differs from DB-derived 2 x 1 replay'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER reference_price_manifest_integrity_guard
BEFORE INSERT ON crypto_agent.reference_price_manifests
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_v1_reference_price_manifest();

CREATE TRIGGER canonical_candle_manifest_reference_price_policy_guard
BEFORE INSERT ON crypto_agent.canonical_candle_manifests
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_v1_canonical_manifest_policy();

CREATE TRIGGER reference_price_provenance_integrity_guard
BEFORE INSERT ON crypto_agent.reference_price_provenance
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_v1_reference_price_provenance();

CREATE CONSTRAINT TRIGGER reference_price_exact_provenance_guard
AFTER INSERT ON crypto_agent.reference_price_manifests
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_v1_reference_price_exact_provenance();

CREATE CONSTRAINT TRIGGER reference_price_exact_provenance_link_guard
AFTER INSERT ON crypto_agent.reference_price_provenance
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_v1_reference_price_exact_provenance();

DO $$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'reference_price_manifests', 'reference_price_provenance'
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

COMMENT ON TABLE crypto_agent.reference_price_manifests IS
'Immutable evaluated two-venue one-minute midpoint, divergence and hash manifest.';

COMMENT ON TABLE crypto_agent.reference_price_provenance IS
'Exact immutable approved 2 x 1 raw candle inputs for a reference-price manifest.';
