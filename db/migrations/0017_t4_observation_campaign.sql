-- Immutable evidence ledger for the V1 T4 read-only acceptance campaign.
-- A campaign report can pass only after at least 672 real elapsed hours.

-- Schema v5 separates the registry market key, T4 product ContractID and opaque
-- tradable MarketID. Historical v2-v4 rows deliberately keep the new columns NULL.
ALTER TABLE crypto_agent.t4_ingestion_batches
    DROP CONSTRAINT t4_ingestion_batches_bridge_schema_version_check;

ALTER TABLE crypto_agent.t4_ingestion_batches
    ADD CONSTRAINT t4_ingestion_batches_bridge_schema_version_check
    CHECK (bridge_schema_version IN (2, 3, 4, 5)),
    ADD COLUMN environment text,
    ADD COLUMN exchange_id text,
    ADD COLUMN active_market_id text,
    ADD COLUMN rolled_from_market_id text,
    ADD COLUMN basis_exchange_id text,
    ADD COLUMN basis_contract_id text,
    ADD COLUMN basis_market_id text;

DO $$
DECLARE
    constraint_name text;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'crypto_agent.t4_ingestion_batches'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%contract_id ~%'
    LOOP
        EXECUTE format(
            'ALTER TABLE crypto_agent.t4_ingestion_batches DROP CONSTRAINT %I',
            constraint_name
        );
    END LOOP;
END;
$$;

ALTER TABLE crypto_agent.t4_ingestion_batches
    ADD CONSTRAINT t4_ingestion_batches_product_contract_id_safe_check
    CHECK (
        contract_id = btrim(contract_id)
        AND contract_id <> ''
        AND octet_length(contract_id) <= 128
        AND contract_id !~ '[[:cntrl:]]'
    ),
    ADD CONSTRAINT t4_ingestion_batches_rolled_product_contract_id_safe_check
    CHECK (
        rolled_from_contract_id IS NULL
        OR (
            rolled_from_contract_id = btrim(rolled_from_contract_id)
            AND rolled_from_contract_id <> ''
            AND octet_length(rolled_from_contract_id) <= 128
            AND rolled_from_contract_id !~ '[[:cntrl:]]'
        )
    ),
    ADD CONSTRAINT t4_ingestion_batches_contract_selection_v5_check
    CHECK (
        (bridge_schema_version IN (2, 3, 4)
         AND (
             (contract_selection = 'front_month'
              AND rolled_from_contract_id IS NULL)
             OR
             (contract_selection = 'rolled'
              AND rolled_from_contract_id IS NOT NULL
              AND rolled_from_contract_id <> contract_id)
         ))
        OR
        (bridge_schema_version = 5 AND rolled_from_contract_id IS NULL)
    ),
    ADD CONSTRAINT t4_ingestion_batches_v5_identity_check
    CHECK (
        (bridge_schema_version IN (2, 3, 4)
         AND environment IS NULL
         AND exchange_id IS NULL
         AND active_market_id IS NULL
         AND rolled_from_market_id IS NULL
         AND basis_exchange_id IS NULL
         AND basis_contract_id IS NULL
         AND basis_market_id IS NULL)
        OR
        (bridge_schema_version = 5
         AND environment IS NOT NULL
         AND environment IN ('t4_simulator', 'live_t4')
         AND exchange_id IS NOT NULL
         AND exchange_id = btrim(exchange_id)
         AND exchange_id <> ''
         AND octet_length(exchange_id) <= 64
         AND exchange_id !~ '[[:cntrl:]]'
         AND active_market_id IS NOT NULL
         AND active_market_id = btrim(active_market_id)
         AND active_market_id <> ''
         AND octet_length(active_market_id) <= 256
         AND active_market_id !~ '[[:cntrl:]]'
         AND basis_exchange_id IS NOT NULL
         AND basis_exchange_id = btrim(basis_exchange_id)
         AND basis_exchange_id <> ''
         AND octet_length(basis_exchange_id) <= 64
         AND basis_exchange_id !~ '[[:cntrl:]]'
         AND basis_contract_id IS NOT NULL
         AND basis_contract_id = btrim(basis_contract_id)
         AND basis_contract_id <> ''
         AND octet_length(basis_contract_id) <= 128
         AND basis_contract_id !~ '[[:cntrl:]]'
         AND basis_market_id IS NOT NULL
         AND basis_market_id = btrim(basis_market_id)
         AND basis_market_id <> ''
         AND octet_length(basis_market_id) <= 256
         AND basis_market_id !~ '[[:cntrl:]]'
         AND basis_market_id <> active_market_id
         AND (
             (contract_selection = 'front_month'
              AND rolled_from_market_id IS NULL)
             OR
             (contract_selection = 'rolled'
              AND rolled_from_market_id = btrim(rolled_from_market_id)
              AND rolled_from_market_id <> ''
              AND octet_length(rolled_from_market_id) <= 256
              AND rolled_from_market_id !~ '[[:cntrl:]]'
              AND rolled_from_market_id <> active_market_id)
         ))
    );

ALTER TABLE crypto_agent.t4_canonical_candles
    RENAME COLUMN market_id TO registry_market_id;

ALTER TABLE crypto_agent.t4_canonical_candles
    ADD COLUMN market_id text,
    ADD CONSTRAINT t4_canonical_candles_market_id_safe_check
    CHECK (
        market_id IS NULL
        OR (
            market_id = btrim(market_id)
            AND market_id <> ''
            AND octet_length(market_id) <= 256
            AND market_id !~ '[[:cntrl:]]'
        )
    );

DO $$
DECLARE
    constraint_name text;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid IN (
            'crypto_agent.t4_futures_snapshots'::regclass,
            'crypto_agent.t4_contract_transition_evidence'::regclass
        )
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%contract_id ~%'
    LOOP
        IF EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = constraint_name
              AND conrelid = 'crypto_agent.t4_futures_snapshots'::regclass
        ) THEN
            EXECUTE format(
                'ALTER TABLE crypto_agent.t4_futures_snapshots DROP CONSTRAINT %I',
                constraint_name
            );
        ELSE
            EXECUTE format(
                'ALTER TABLE crypto_agent.t4_contract_transition_evidence '
                'DROP CONSTRAINT %I',
                constraint_name
            );
        END IF;
    END LOOP;
END;
$$;

ALTER TABLE crypto_agent.t4_futures_snapshots
    ADD COLUMN exchange_id text,
    ADD COLUMN product_contract_id text,
    ADD COLUMN active_market_id text,
    ADD COLUMN basis_exchange_id text,
    ADD COLUMN basis_product_contract_id text,
    ADD COLUMN basis_market_id text,
    ADD CONSTRAINT t4_futures_snapshots_legacy_contract_id_safe_check
    CHECK (
        contract_id = btrim(contract_id)
        AND contract_id <> ''
        AND octet_length(contract_id) <= 128
        AND contract_id !~ '[[:cntrl:]]'
    ),
    ADD CONSTRAINT t4_futures_snapshots_v5_identity_safe_check
    CHECK (
        (exchange_id IS NULL AND product_contract_id IS NULL
         AND active_market_id IS NULL AND basis_exchange_id IS NULL
         AND basis_product_contract_id IS NULL AND basis_market_id IS NULL)
        OR
        (exchange_id IS NOT NULL
         AND exchange_id = btrim(exchange_id)
         AND exchange_id <> ''
         AND octet_length(exchange_id) <= 64
         AND exchange_id !~ '[[:cntrl:]]'
         AND product_contract_id IS NOT NULL
         AND product_contract_id = btrim(product_contract_id)
         AND product_contract_id <> ''
         AND octet_length(product_contract_id) <= 128
         AND product_contract_id !~ '[[:cntrl:]]'
         AND active_market_id IS NOT NULL
         AND active_market_id = btrim(active_market_id)
         AND active_market_id <> ''
         AND octet_length(active_market_id) <= 256
         AND active_market_id !~ '[[:cntrl:]]'
         AND basis_exchange_id IS NOT NULL
         AND basis_exchange_id = btrim(basis_exchange_id)
         AND basis_exchange_id <> ''
         AND octet_length(basis_exchange_id) <= 64
         AND basis_exchange_id !~ '[[:cntrl:]]'
         AND basis_product_contract_id IS NOT NULL
         AND basis_product_contract_id = btrim(basis_product_contract_id)
         AND basis_product_contract_id <> ''
         AND octet_length(basis_product_contract_id) <= 128
         AND basis_product_contract_id !~ '[[:cntrl:]]'
         AND basis_market_id IS NOT NULL
         AND basis_market_id = btrim(basis_market_id)
         AND basis_market_id <> ''
         AND octet_length(basis_market_id) <= 256
         AND basis_market_id !~ '[[:cntrl:]]'
         AND basis_market_id <> active_market_id)
    );

ALTER TABLE crypto_agent.t4_contract_transition_evidence
    ADD COLUMN exchange_id text,
    ADD COLUMN product_contract_id text,
    ADD COLUMN from_market_id text,
    ADD COLUMN to_market_id text,
    ADD CONSTRAINT t4_contract_transition_from_contract_id_safe_check
    CHECK (
        from_contract_id = btrim(from_contract_id)
        AND from_contract_id <> ''
        AND octet_length(from_contract_id) <= 256
        AND from_contract_id !~ '[[:cntrl:]]'
    ),
    ADD CONSTRAINT t4_contract_transition_to_contract_id_safe_check
    CHECK (
        to_contract_id = btrim(to_contract_id)
        AND to_contract_id <> ''
        AND octet_length(to_contract_id) <= 256
        AND to_contract_id !~ '[[:cntrl:]]'
    ),
    ADD CONSTRAINT t4_contract_transition_v5_identity_safe_check
    CHECK (
        (exchange_id IS NULL AND product_contract_id IS NULL
         AND from_market_id IS NULL AND to_market_id IS NULL)
        OR
        (exchange_id IS NOT NULL
         AND exchange_id = btrim(exchange_id)
         AND exchange_id <> ''
         AND octet_length(exchange_id) <= 64
         AND exchange_id !~ '[[:cntrl:]]'
         AND product_contract_id IS NOT NULL
         AND product_contract_id = btrim(product_contract_id)
         AND product_contract_id <> ''
         AND octet_length(product_contract_id) <= 128
         AND product_contract_id !~ '[[:cntrl:]]'
         AND from_market_id IS NOT NULL
         AND from_market_id = btrim(from_market_id)
         AND from_market_id <> ''
         AND octet_length(from_market_id) <= 256
         AND from_market_id !~ '[[:cntrl:]]'
         AND to_market_id IS NOT NULL
         AND to_market_id = btrim(to_market_id)
         AND to_market_id <> ''
         AND octet_length(to_market_id) <= 256
         AND to_market_id !~ '[[:cntrl:]]'
         AND from_market_id <> to_market_id)
    );

DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT conname INTO constraint_name
    FROM pg_constraint
    WHERE conrelid = 'crypto_agent.t4_contract_transition_evidence'::regclass
      AND contype = 'f'
      AND confrelid = 'crypto_agent.t4_ingestion_batches'::regclass
    LIMIT 1;
    IF constraint_name IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE crypto_agent.t4_contract_transition_evidence '
            'DROP CONSTRAINT %I',
            constraint_name
        );
    END IF;
END;
$$;

ALTER TABLE crypto_agent.t4_contract_transition_evidence
    ADD CONSTRAINT t4_contract_transition_batch_fk
    FOREIGN KEY (t4_batch_id)
    REFERENCES crypto_agent.t4_ingestion_batches(t4_batch_id);

CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_schema_v5_evidence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    batch crypto_agent.t4_ingestion_batches%ROWTYPE;
BEGIN
    SELECT * INTO batch
    FROM crypto_agent.t4_ingestion_batches
    WHERE t4_batch_id = NEW.t4_batch_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'T4 ingestion batch is missing' USING ERRCODE = '23503';
    END IF;

    IF TG_TABLE_NAME = 't4_canonical_candles' THEN
        IF (batch.bridge_schema_version = 5
            AND (NEW.market_id IS NULL OR NEW.market_id = ''))
           OR (batch.bridge_schema_version < 5 AND NEW.market_id IS NOT NULL) THEN
            RAISE EXCEPTION 'candle MarketID does not match bridge schema'
                USING ERRCODE = '22023';
        END IF;
    ELSIF TG_TABLE_NAME = 't4_futures_snapshots' THEN
        IF batch.bridge_schema_version = 5 THEN
            IF NEW.exchange_id IS DISTINCT FROM batch.exchange_id
               OR NEW.product_contract_id IS DISTINCT FROM batch.contract_id
               OR NEW.active_market_id IS DISTINCT FROM batch.active_market_id
               OR NEW.basis_exchange_id IS DISTINCT FROM batch.basis_exchange_id
               OR NEW.basis_product_contract_id IS DISTINCT FROM batch.basis_contract_id
               OR NEW.basis_market_id IS DISTINCT FROM batch.basis_market_id THEN
                RAISE EXCEPTION 'snapshot T4 identity does not match its batch'
                    USING ERRCODE = '22023';
            END IF;
        ELSIF NEW.exchange_id IS NOT NULL OR NEW.product_contract_id IS NOT NULL
              OR NEW.active_market_id IS NOT NULL OR NEW.basis_exchange_id IS NOT NULL
              OR NEW.basis_product_contract_id IS NOT NULL
              OR NEW.basis_market_id IS NOT NULL THEN
            RAISE EXCEPTION 'legacy snapshot cannot contain schema-v5 identity'
                USING ERRCODE = '22023';
        END IF;
    ELSE
        IF batch.bridge_schema_version = 5 THEN
            IF NEW.exchange_id IS DISTINCT FROM batch.exchange_id
               OR NEW.product_contract_id IS DISTINCT FROM batch.contract_id
               OR NEW.to_market_id IS DISTINCT FROM batch.active_market_id
               OR NEW.from_market_id IS DISTINCT FROM batch.rolled_from_market_id THEN
                RAISE EXCEPTION 'transition T4 identity does not match its batch'
                    USING ERRCODE = '22023';
            END IF;
        ELSIF NEW.exchange_id IS NOT NULL OR NEW.product_contract_id IS NOT NULL
              OR NEW.from_market_id IS NOT NULL OR NEW.to_market_id IS NOT NULL
              OR NEW.to_contract_id IS DISTINCT FROM batch.contract_id THEN
            RAISE EXCEPTION 'legacy transition identity does not match its batch'
                USING ERRCODE = '22023';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER t4_canonical_candles_schema_v5_guard
BEFORE INSERT ON crypto_agent.t4_canonical_candles
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_t4_schema_v5_evidence();

CREATE TRIGGER t4_futures_snapshots_schema_v5_guard
BEFORE INSERT ON crypto_agent.t4_futures_snapshots
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_t4_schema_v5_evidence();

CREATE TRIGGER t4_contract_transitions_schema_v5_guard
BEFORE INSERT ON crypto_agent.t4_contract_transition_evidence
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_t4_schema_v5_evidence();

CREATE INDEX t4_ingestion_batches_t4_identity_idx
ON crypto_agent.t4_ingestion_batches (
    environment, exchange_id, contract_id, active_market_id, available_at DESC
)
WHERE bridge_schema_version = 5;

CREATE INDEX t4_canonical_candles_market_id_idx
ON crypto_agent.t4_canonical_candles (market_id, open_time DESC)
WHERE market_id IS NOT NULL;

CREATE TABLE crypto_agent.t4_observation_campaigns (
    campaign_id                 uuid PRIMARY KEY,
    environment                 text NOT NULL CHECK (environment = 'live_t4'),
    started_at                  timestamptz NOT NULL,
    planned_ends_at             timestamptz NOT NULL,
    cycle_interval_seconds      integer NOT NULL CHECK (
        cycle_interval_seconds BETWEEN 60 AND 86400
    ),
    observation_policy_id       text NOT NULL,
    observation_policy_hash     crypto_agent.sha256_hex NOT NULL,
    code_commit_hash            crypto_agent.sha256_hex NOT NULL,
    t4_protocol_commit_hash     crypto_agent.sha256_hex NOT NULL,
    runtime_config_hash         crypto_agent.sha256_hex NOT NULL,
    scope_manifest              jsonb NOT NULL,
    scope_manifest_hash         crypto_agent.sha256_hex NOT NULL,
    frozen_baseline_hash        crypto_agent.sha256_hex NOT NULL UNIQUE,
    read_only                   boolean NOT NULL CHECK (read_only),
    execution_enabled           boolean NOT NULL CHECK (NOT execution_enabled),
    content_hash                crypto_agent.sha256_hex NOT NULL UNIQUE,
    created_at                  timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (planned_ends_at >= started_at + interval '672 hours'),
    CHECK (
        btrim(observation_policy_id) <> ''
        AND length(observation_policy_id) <= 128
    ),
    CHECK (
        jsonb_typeof(scope_manifest) = 'array'
        AND jsonb_array_length(scope_manifest) > 0
    )
);

CREATE TABLE crypto_agent.t4_observation_research_inputs (
    observation_research_input_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    campaign_id                 uuid NOT NULL REFERENCES
        crypto_agent.t4_observation_campaigns(campaign_id),
    scope_key                   text NOT NULL,
    sequence_no                 integer NOT NULL CHECK (sequence_no > 0),
    expected_at                 timestamptz NOT NULL,
    t4_batch_id                 bigint NOT NULL UNIQUE REFERENCES
        crypto_agent.t4_ingestion_batches(t4_batch_id),
    research_run_id             bigint NOT NULL UNIQUE REFERENCES
        crypto_agent.research_runs(research_run_id),
    raw_payload_hash            crypto_agent.sha256_hex NOT NULL,
    analysis_input_hash         crypto_agent.sha256_hex NOT NULL,
    trace_id                    uuid NOT NULL UNIQUE,
    content_hash                crypto_agent.sha256_hex NOT NULL UNIQUE,
    created_at                  timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (campaign_id, scope_key, sequence_no),
    UNIQUE (
        campaign_id, scope_key, sequence_no, expected_at, t4_batch_id,
        research_run_id, raw_payload_hash, analysis_input_hash, trace_id
    ),
    CHECK (btrim(scope_key) <> '' AND length(scope_key) <= 256)
);

CREATE TABLE crypto_agent.t4_observation_cycles (
    observation_cycle_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    campaign_id                 uuid NOT NULL REFERENCES
        crypto_agent.t4_observation_campaigns(campaign_id),
    scope_key                   text NOT NULL,
    sequence_no                 integer NOT NULL CHECK (sequence_no > 0),
    expected_at                 timestamptz NOT NULL,
    started_at                  timestamptz,
    finished_at                 timestamptz,
    outcome                     text NOT NULL CHECK (
        outcome IN ('success', 'failure', 'missed')
    ),
    error_code                  text,
    gap_explanation_code        text,
    t4_batch_id                 bigint REFERENCES
        crypto_agent.t4_ingestion_batches(t4_batch_id),
    research_run_id             bigint REFERENCES crypto_agent.research_runs(research_run_id),
    analysis_input_hash         crypto_agent.sha256_hex,
    bridge_schema_version       smallint,
    raw_payload_hash            crypto_agent.sha256_hex,
    bridge_rtt_milliseconds     integer CHECK (bridge_rtt_milliseconds >= 0),
    trace_id                    uuid,
    read_only                   boolean NOT NULL CHECK (read_only),
    execution_enabled           boolean NOT NULL CHECK (NOT execution_enabled),
    previous_cycle_hash         crypto_agent.sha256_hex,
    content_hash                crypto_agent.sha256_hex NOT NULL UNIQUE,
    created_at                  timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (campaign_id, scope_key, sequence_no),
    CHECK (btrim(scope_key) <> '' AND length(scope_key) <= 256),
    CHECK (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'),
    CHECK (
        gap_explanation_code IS NULL
        OR gap_explanation_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    ),
    CHECK (
        (sequence_no = 1 AND previous_cycle_hash IS NULL)
        OR (sequence_no > 1 AND previous_cycle_hash IS NOT NULL)
    ),
    CHECK (
        (outcome = 'success'
         AND started_at IS NOT NULL
         AND finished_at IS NOT NULL
         AND started_at <= finished_at
         AND error_code IS NULL
         AND gap_explanation_code IS NULL
         AND t4_batch_id IS NOT NULL
         AND research_run_id IS NOT NULL
         AND analysis_input_hash IS NOT NULL
         AND bridge_schema_version IS NOT NULL
         AND bridge_schema_version > 0
         AND raw_payload_hash IS NOT NULL
         AND bridge_rtt_milliseconds IS NOT NULL
         AND trace_id IS NOT NULL)
        OR
        (outcome = 'failure'
         AND started_at IS NOT NULL
         AND finished_at IS NOT NULL
         AND started_at <= finished_at
         AND error_code IS NOT NULL
         AND t4_batch_id IS NULL
         AND research_run_id IS NULL
         AND analysis_input_hash IS NULL
         AND bridge_schema_version IS NULL
         AND raw_payload_hash IS NULL
         AND bridge_rtt_milliseconds IS NULL
         AND trace_id IS NOT NULL)
        OR
        (outcome = 'missed'
         AND started_at IS NULL
         AND finished_at IS NULL
         AND error_code IS NULL
         AND t4_batch_id IS NULL
         AND research_run_id IS NULL
         AND analysis_input_hash IS NULL
         AND bridge_schema_version IS NULL
         AND raw_payload_hash IS NULL
         AND bridge_rtt_milliseconds IS NULL
         AND trace_id IS NULL
         AND gap_explanation_code IS NULL)
    ),
    FOREIGN KEY (
        campaign_id, scope_key, sequence_no, expected_at, t4_batch_id,
        research_run_id, raw_payload_hash, analysis_input_hash, trace_id
    ) REFERENCES crypto_agent.t4_observation_research_inputs (
        campaign_id, scope_key, sequence_no, expected_at, t4_batch_id,
        research_run_id, raw_payload_hash, analysis_input_hash, trace_id
    )
);

CREATE TABLE crypto_agent.t4_observation_session_events (
    observation_session_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    campaign_id                 uuid NOT NULL REFERENCES
        crypto_agent.t4_observation_campaigns(campaign_id),
    sequence_no                 integer NOT NULL CHECK (sequence_no > 0),
    event_at                    timestamptz NOT NULL,
    event_type                  text NOT NULL CHECK (
        event_type ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    outcome                     text NOT NULL CHECK (outcome IN ('pass', 'fail')),
    scenario_code               text CHECK (
        scenario_code IS NULL OR scenario_code ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    detail_code                 text CHECK (
        detail_code IS NULL OR detail_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    ),
    read_only                   boolean NOT NULL CHECK (read_only),
    execution_enabled           boolean NOT NULL CHECK (NOT execution_enabled),
    previous_event_hash         crypto_agent.sha256_hex,
    content_hash                crypto_agent.sha256_hex NOT NULL UNIQUE,
    created_at                  timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (campaign_id, sequence_no),
    CHECK (
        (sequence_no = 1 AND previous_event_hash IS NULL)
        OR (sequence_no > 1 AND previous_event_hash IS NOT NULL)
    ),
    -- Scenario PASS is intentionally unavailable until a later migration can
    -- validate an objective, scenario-specific evidence reference in PostgreSQL.
    CHECK (scenario_code IS NULL OR outcome = 'fail')
);

CREATE TABLE crypto_agent.t4_observation_quality_reports (
    observation_quality_report_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    campaign_id                 uuid NOT NULL UNIQUE REFERENCES
        crypto_agent.t4_observation_campaigns(campaign_id),
    generated_at                timestamptz NOT NULL,
    observed_until              timestamptz NOT NULL,
    overall_status              text NOT NULL CHECK (
        overall_status IN ('PASS', 'FAIL', 'NOT_OBSERVED')
    ),
    v1_gate_passed              boolean NOT NULL,
    observation_policy_id       text NOT NULL,
    observation_policy_hash     crypto_agent.sha256_hex NOT NULL,
    frozen_baseline_hash        crypto_agent.sha256_hex NOT NULL,
    report                      jsonb NOT NULL,
    report_hash                 crypto_agent.sha256_hex NOT NULL UNIQUE,
    content_hash                crypto_agent.sha256_hex NOT NULL UNIQUE,
    created_at                  timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (jsonb_typeof(report) = 'object'),
    CHECK (
        report ?& ARRAY[
            'schema_version', 'campaign_id', 'generated_at', 'observed_until',
            'elapsed_seconds', 'overall_status', 'v1_gate_passed',
            'policy_id', 'policy_hash_sha256', 'frozen_baseline_hash_sha256',
            'criteria'
        ]
        AND report - ARRAY[
            'schema_version', 'campaign_id', 'generated_at', 'observed_until',
            'elapsed_seconds', 'overall_status', 'v1_gate_passed',
            'policy_id', 'policy_hash_sha256', 'frozen_baseline_hash_sha256',
            'criteria'
        ] = '{}'::jsonb
    ),
    CHECK (report->'schema_version' = '1'::jsonb),
    CHECK (report->>'campaign_id' = campaign_id::text),
    CHECK ((report->>'generated_at')::timestamptz = generated_at),
    CHECK ((report->>'observed_until')::timestamptz = observed_until),
    CHECK (report->>'overall_status' = overall_status),
    CHECK (report->'v1_gate_passed' = to_jsonb(v1_gate_passed)),
    CHECK (report->>'policy_id' = observation_policy_id),
    CHECK (report->>'policy_hash_sha256' = observation_policy_hash),
    CHECK (report->>'frozen_baseline_hash_sha256' = frozen_baseline_hash),
    CHECK (
        jsonb_typeof(report->'criteria') = 'array'
        AND jsonb_array_length(report->'criteria') > 0
        AND jsonb_path_exists(report->'criteria', '$[*] ? (@.mandatory == true)')
    ),
    CHECK ((report->>'elapsed_seconds')::bigint >= 2419200),
    CHECK (
        NOT v1_gate_passed
        OR NOT jsonb_path_exists(
            report->'criteria',
            '$[*] ? (@.mandatory == true && @.status != "PASS")'
        )
    ),
    CHECK (
        (overall_status = 'PASS' AND v1_gate_passed)
        OR (overall_status <> 'PASS' AND NOT v1_gate_passed)
    )
);

CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_observation_campaign_start()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.started_at > clock_timestamp()
       OR NEW.started_at < clock_timestamp() - interval '5 minutes' THEN
        RAISE EXCEPTION 'observation campaign start must use the current database clock'
            USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_observation_final_report()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    criterion_ids text[];
    criteria_are_canonical boolean;
    required_criterion_ids constant text[] := ARRAY[
        'bridge_round_trip_latency',
        'bridge_schema_conformance',
        'configured_scope_coverage',
        'cycle_attempt_rate',
        'cycle_success_rate',
        'exact_batch_analysis_linkage',
        'fail_closed_violations',
        'frozen_baseline',
        'integrity_failures',
        'live_environment_only',
        'maximum_unexplained_gap_seconds',
        'order_route_attempts',
        'prohibited_data_mode_violations',
        'read_only_violations',
        'real_elapsed_time',
        'scenario_bridge_restart',
        'scenario_missing_data',
        'scenario_rate_limit',
        'scenario_reconnect',
        'scenario_replay_blocked',
        'scenario_roll_transition',
        'scenario_stale_data',
        'secret_leaks'
    ];
BEGIN
    SELECT * INTO campaign_row
    FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = NEW.campaign_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'observation campaign is missing' USING ERRCODE = '23503';
    END IF;

    IF EXISTS (
        SELECT 1 FROM crypto_agent.t4_observation_quality_reports
        WHERE campaign_id = NEW.campaign_id
    ) THEN
        RAISE EXCEPTION 'observation campaign is already finalized'
            USING ERRCODE = '55000';
    END IF;

    IF clock_timestamp() < campaign_row.planned_ends_at
            + make_interval(secs => campaign_row.cycle_interval_seconds)
       OR NEW.generated_at IS DISTINCT FROM campaign_row.planned_ends_at
       OR NEW.observed_until IS DISTINCT FROM campaign_row.planned_ends_at
       OR NEW.observation_policy_id IS DISTINCT FROM
            campaign_row.observation_policy_id
       OR NEW.observation_policy_hash IS DISTINCT FROM
            campaign_row.observation_policy_hash
       OR NEW.frozen_baseline_hash IS DISTINCT FROM
            campaign_row.frozen_baseline_hash
       OR (NEW.report->>'elapsed_seconds')::bigint IS DISTINCT FROM
            floor(extract(epoch FROM (
                campaign_row.planned_ends_at - campaign_row.started_at
            )))::bigint THEN
        RAISE EXCEPTION 'observation final report does not match its frozen campaign'
            USING ERRCODE = '22023';
    END IF;

    SELECT
        array_agg(item->>'criterion_id' ORDER BY item->>'criterion_id'),
        bool_and(
            jsonb_typeof(item) = 'object'
            AND item ?& ARRAY[
                'criterion_id', 'status', 'actual', 'threshold', 'mandatory'
            ]
            AND item - ARRAY[
                'criterion_id', 'status', 'actual', 'threshold', 'mandatory'
            ] = '{}'::jsonb
            AND item->'mandatory' = 'true'::jsonb
            AND item->>'status' IN ('PASS', 'FAIL', 'NOT_OBSERVED')
        )
    INTO criterion_ids, criteria_are_canonical
    FROM jsonb_array_elements(NEW.report->'criteria') AS criteria(item);

    IF criterion_ids IS DISTINCT FROM required_criterion_ids
       OR criteria_are_canonical IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION 'observation final report criteria are not canonical'
            USING ERRCODE = '22023';
    END IF;

    -- No scenario event can yet prove PASS from an objective database reference.
    -- Keep the release gate closed until a later migration adds those validators.
    IF NEW.v1_gate_passed THEN
        RAISE EXCEPTION 'verified scenario evidence is not implemented'
            USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_observation_research_input()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    ingestion_row crypto_agent.t4_ingestion_batches%ROWTYPE;
    research_row crypto_agent.research_runs%ROWTYPE;
BEGIN
    SELECT * INTO campaign_row
    FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = NEW.campaign_id
    FOR SHARE;

    IF NOT FOUND
       OR NOT (campaign_row.scope_manifest ? NEW.scope_key)
       OR NEW.expected_at <> campaign_row.started_at
            + make_interval(
                secs => campaign_row.cycle_interval_seconds::double precision
                        * NEW.sequence_no
            )
       OR NEW.expected_at > campaign_row.planned_ends_at THEN
        RAISE EXCEPTION 'observation research input is outside the frozen schedule'
            USING ERRCODE = '22023';
    END IF;

    SELECT * INTO ingestion_row
    FROM crypto_agent.t4_ingestion_batches
    WHERE t4_batch_id = NEW.t4_batch_id;
    IF NOT FOUND
       OR ingestion_row.environment IS DISTINCT FROM 'live_t4'
       OR ingestion_row.bridge_schema_version <> 5
       OR ingestion_row.requested_as_of IS DISTINCT FROM NEW.expected_at
       OR ingestion_row.raw_payload_hash IS DISTINCT FROM NEW.raw_payload_hash
       OR ingestion_row.logical_symbol || ':'
            || (ingestion_row.interval_seconds / 60)::text || 'm'
          IS DISTINCT FROM NEW.scope_key THEN
        RAISE EXCEPTION 'observation research input batch is invalid'
            USING ERRCODE = '22023';
    END IF;

    SELECT * INTO research_row
    FROM crypto_agent.research_runs
    WHERE research_run_id = NEW.research_run_id;
    IF NOT FOUND
       OR research_row.run_kind IS DISTINCT FROM 'live_t4_analysis'
       OR research_row.as_of_at IS DISTINCT FROM NEW.expected_at
       OR research_row.trace_key IS DISTINCT FROM NEW.trace_id::text
       OR research_row.run_key IS DISTINCT FROM 'monitoring:' || NEW.trace_id::text
       OR research_row.dataset_manifest_hash IS DISTINCT FROM NEW.analysis_input_hash
       OR NOT EXISTS (
            SELECT 1
            FROM crypto_agent.research_run_events event
            WHERE event.research_run_id = NEW.research_run_id
              AND event.event_type = 'completed'
       )
       OR NOT EXISTS (
            SELECT 1
            FROM crypto_agent.research_artifacts artifact
            WHERE artifact.research_run_id = NEW.research_run_id
              AND artifact.artifact_key = 'safe-report'
              AND artifact.revision_no = 1
              AND artifact.artifact_json->>'trace_id' = NEW.trace_id::text
              AND artifact.artifact_json->>'data_snapshot_id'
                    = 'sha256:' || NEW.analysis_input_hash
              AND artifact.artifact_json#>>'{metadata,input_fingerprint_sha256}'
                    = NEW.analysis_input_hash
              AND artifact.artifact_json#>>'{metadata,t4_environment}' = 'live_t4'
              AND artifact.artifact_json#>>'{metadata,t4_bridge_schema_version}' = '5'
       ) THEN
        RAISE EXCEPTION 'observation research input run is invalid'
            USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_observation_cycle_chain()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    previous_row crypto_agent.t4_observation_cycles%ROWTYPE;
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    ingestion_row crypto_agent.t4_ingestion_batches%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            't4-observation:' || NEW.campaign_id::text || ':' || NEW.scope_key,
            0
        )
    );

    SELECT * INTO campaign_row FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = NEW.campaign_id FOR SHARE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'observation campaign is missing' USING ERRCODE = '23503';
    END IF;

    IF EXISTS (
        SELECT 1 FROM crypto_agent.t4_observation_quality_reports
        WHERE campaign_id = NEW.campaign_id
    ) THEN
        RAISE EXCEPTION 'observation campaign is already finalized'
            USING ERRCODE = '55000';
    END IF;

    IF NOT (campaign_row.scope_manifest ? NEW.scope_key)
       OR NEW.expected_at <> campaign_row.started_at
            + make_interval(
                secs => campaign_row.cycle_interval_seconds::double precision
                        * NEW.sequence_no
            )
       OR NEW.expected_at > campaign_row.planned_ends_at THEN
        RAISE EXCEPTION 'observation cycle is outside the frozen schedule'
            USING ERRCODE = '22023';
    END IF;

    IF NEW.outcome = 'missed' THEN
        IF clock_timestamp() <= NEW.expected_at
                + make_interval(secs => campaign_row.cycle_interval_seconds) THEN
            RAISE EXCEPTION 'missed observation cycle is not yet elapsed'
                USING ERRCODE = '22023';
        END IF;
    ELSE
        IF NEW.started_at < transaction_timestamp()
           OR NEW.started_at > clock_timestamp()
           OR NEW.started_at < NEW.expected_at
           OR NEW.started_at > NEW.expected_at
                + make_interval(secs => campaign_row.cycle_interval_seconds)
           OR NEW.finished_at > clock_timestamp() + interval '5 minutes' THEN
            RAISE EXCEPTION 'observation cycle cannot be predeclared or backfilled'
                USING ERRCODE = '22023';
        END IF;
    END IF;

    IF NEW.outcome = 'success' THEN
        SELECT * INTO ingestion_row
        FROM crypto_agent.t4_ingestion_batches
        WHERE t4_batch_id = NEW.t4_batch_id;
        IF NOT FOUND
           OR ingestion_row.environment IS DISTINCT FROM 'live_t4'
           OR ingestion_row.bridge_schema_version <> 5
           OR ingestion_row.requested_as_of IS DISTINCT FROM NEW.expected_at
           OR ingestion_row.raw_payload_hash IS DISTINCT FROM NEW.raw_payload_hash THEN
            RAISE EXCEPTION 'observation batch linkage is invalid'
                USING ERRCODE = '22023';
        END IF;

        PERFORM 1
        FROM crypto_agent.t4_observation_research_inputs input
        WHERE input.campaign_id = NEW.campaign_id
          AND input.scope_key = NEW.scope_key
          AND input.sequence_no = NEW.sequence_no
          AND input.expected_at = NEW.expected_at
          AND input.t4_batch_id = NEW.t4_batch_id
          AND input.research_run_id = NEW.research_run_id
          AND input.raw_payload_hash = NEW.raw_payload_hash
          AND input.analysis_input_hash = NEW.analysis_input_hash
          AND input.trace_id = NEW.trace_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'observation exact batch-to-run linkage is invalid'
                USING ERRCODE = '22023';
        END IF;
    END IF;

    SELECT * INTO previous_row
    FROM crypto_agent.t4_observation_cycles
    WHERE campaign_id = NEW.campaign_id AND scope_key = NEW.scope_key
    ORDER BY sequence_no DESC
    LIMIT 1;

    IF NOT FOUND THEN
        IF NEW.sequence_no <> 1 OR NEW.previous_cycle_hash IS NOT NULL THEN
            RAISE EXCEPTION 'first observation cycle must be sequence 1'
                USING ERRCODE = '22023';
        END IF;
    ELSIF NEW.sequence_no <> previous_row.sequence_no + 1
          OR NEW.previous_cycle_hash <> previous_row.content_hash
          OR NEW.expected_at <= previous_row.expected_at THEN
        RAISE EXCEPTION 'invalid observation cycle sequence or hash chain'
            USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_observation_event_chain()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    previous_row crypto_agent.t4_observation_session_events%ROWTYPE;
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
BEGIN
    SELECT * INTO campaign_row FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = NEW.campaign_id FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'observation campaign is missing' USING ERRCODE = '23503';
    END IF;

    IF EXISTS (
        SELECT 1 FROM crypto_agent.t4_observation_quality_reports
        WHERE campaign_id = NEW.campaign_id
    ) THEN
        RAISE EXCEPTION 'observation campaign is already finalized'
            USING ERRCODE = '55000';
    END IF;

    IF NEW.event_at < campaign_row.started_at
       OR NEW.event_at > campaign_row.planned_ends_at
       OR NEW.event_at > clock_timestamp() + interval '5 minutes' THEN
        RAISE EXCEPTION 'observation event is outside the live campaign window'
            USING ERRCODE = '22023';
    END IF;

    SELECT * INTO previous_row
    FROM crypto_agent.t4_observation_session_events
    WHERE campaign_id = NEW.campaign_id
    ORDER BY sequence_no DESC
    LIMIT 1;

    IF NOT FOUND THEN
        IF NEW.sequence_no <> 1 OR NEW.previous_event_hash IS NOT NULL THEN
            RAISE EXCEPTION 'first observation event must be sequence 1'
                USING ERRCODE = '22023';
        END IF;
    ELSIF NEW.sequence_no <> previous_row.sequence_no + 1
          OR NEW.previous_event_hash <> previous_row.content_hash
          OR NEW.event_at < previous_row.event_at THEN
        RAISE EXCEPTION 'invalid observation event sequence or hash chain'
            USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER t4_observation_campaign_start_guard
BEFORE INSERT ON crypto_agent.t4_observation_campaigns
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_t4_observation_campaign_start();

CREATE TRIGGER t4_observation_cycle_chain_guard
BEFORE INSERT ON crypto_agent.t4_observation_cycles
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_t4_observation_cycle_chain();

CREATE TRIGGER t4_observation_research_input_guard
BEFORE INSERT ON crypto_agent.t4_observation_research_inputs
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_t4_observation_research_input();

CREATE TRIGGER t4_observation_event_chain_guard
BEFORE INSERT ON crypto_agent.t4_observation_session_events
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_t4_observation_event_chain();

CREATE TRIGGER t4_observation_final_report_guard
BEFORE INSERT ON crypto_agent.t4_observation_quality_reports
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_t4_observation_final_report();

CREATE INDEX t4_observation_cycles_campaign_time_idx
ON crypto_agent.t4_observation_cycles (campaign_id, expected_at, scope_key);

CREATE INDEX t4_observation_research_inputs_campaign_time_idx
ON crypto_agent.t4_observation_research_inputs (
    campaign_id, expected_at, scope_key
);

CREATE INDEX t4_observation_session_events_campaign_time_idx
ON crypto_agent.t4_observation_session_events (campaign_id, event_at);

CREATE INDEX t4_observation_session_events_scenario_idx
ON crypto_agent.t4_observation_session_events (campaign_id, scenario_code, outcome)
WHERE scenario_code IS NOT NULL;

DO $$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        't4_observation_campaigns',
        't4_observation_research_inputs',
        't4_observation_cycles',
        't4_observation_session_events',
        't4_observation_quality_reports'
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

COMMENT ON TABLE crypto_agent.t4_observation_campaigns IS
    'Frozen live-T4 read-only campaign baselines; minimum planned duration is 672 hours.';
COMMENT ON TABLE crypto_agent.t4_observation_research_inputs IS
    'Immutable exact T4 ingestion-batch to completed research-run input attestations.';
COMMENT ON TABLE crypto_agent.t4_observation_cycles IS
    'Append-only scheduled-cycle evidence with exact ingestion-batch and research-run links.';
COMMENT ON TABLE crypto_agent.t4_observation_session_events IS
    'Append-only reconnect, rate-limit, restart and safety-scenario evidence.';
COMMENT ON TABLE crypto_agent.t4_observation_quality_reports IS
    'Immutable final acceptance result; PASS is the only state that can open the V1 gate.';
