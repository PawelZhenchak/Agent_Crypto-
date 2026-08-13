-- PostgreSQL-verifiable, signed evidence for the seven mandatory T4 V1 scenarios.
--
-- Cryptographic ECDSA verification is performed by a deliberately separate
-- application verifier role.  PostgreSQL independently verifies the signed
-- canonical byte hash, signer fingerprint binding, event hash chain, immutable
-- evidence references and every scenario/gate semantic.  The operational
-- runtime must not be a database owner or a member of the verifier role.

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

DO $$
BEGIN
    IF (SELECT extnamespace <> 'public'::regnamespace
        FROM pg_extension WHERE extname = 'pgcrypto') THEN
        ALTER EXTENSION pgcrypto SET SCHEMA public;
    END IF;
END;
$$;

-- Every verifier entry point evaluates the original login, not the effective
-- SECURITY DEFINER owner.  A login that can own or redefine any part of the
-- database trust boundary is never eligible to attest evidence.
CREATE OR REPLACE FUNCTION crypto_agent.t4_evidence_verifier_session_is_safe()
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog, crypto_agent
AS $$
    SELECT coalesce((
        SELECT role.rolcanlogin AND role.rolinherit
               AND NOT role.rolsuper AND NOT role.rolcreaterole
               AND NOT role.rolcreatedb AND NOT role.rolreplication
               AND NOT role.rolbypassrls
               AND EXISTS (
                    SELECT 1
                    FROM pg_auth_members membership
                    JOIN pg_roles target ON target.oid = membership.roleid
                    WHERE target.rolname = 'crypto_agent_evidence_verifier'
                      AND membership.member = role.oid
               )
               AND NOT pg_has_role(
                    session_user, 'crypto_agent_evidence_owner', 'MEMBER'
               )
               AND NOT pg_has_role(
                    session_user, 'crypto_agent_evidence_reader', 'MEMBER'
               )
               AND NOT EXISTS (
                    SELECT 1 FROM pg_database database
                    WHERE database.datname = current_database()
                      AND database.datdba = role.oid
               )
               AND NOT EXISTS (
                    SELECT 1 FROM pg_namespace namespace
                    WHERE namespace.nspname = 'crypto_agent'
                      AND namespace.nspowner = role.oid
               )
               AND NOT EXISTS (
                    SELECT 1 FROM pg_class relation
                    JOIN pg_namespace namespace
                      ON namespace.oid = relation.relnamespace
                    WHERE namespace.nspname = 'crypto_agent'
                      AND relation.relowner = role.oid
               )
               AND NOT EXISTS (
                    SELECT 1 FROM pg_proc routine
                    JOIN pg_namespace namespace
                      ON namespace.oid = routine.pronamespace
                    WHERE namespace.nspname = 'crypto_agent'
                      AND routine.proowner = role.oid
               )
        FROM pg_roles role
        WHERE role.rolname = session_user
    ), FALSE)
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'crypto_agent_evidence_owner') THEN
        CREATE ROLE crypto_agent_evidence_owner NOLOGIN NOSUPERUSER NOCREATEDB
            NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'crypto_agent_evidence_verifier') THEN
        CREATE ROLE crypto_agent_evidence_verifier NOLOGIN NOSUPERUSER NOCREATEDB
            NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'crypto_agent_evidence_reader') THEN
        CREATE ROLE crypto_agent_evidence_reader NOLOGIN NOSUPERUSER NOCREATEDB
            NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
END;
$$;

-- Never silently inherit a pre-existing privileged role with the expected
-- name.  Production membership is provisioned only after this migration.
DO $$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'crypto_agent_evidence_owner',
        'crypto_agent_evidence_verifier',
        'crypto_agent_evidence_reader'
    ]
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM pg_roles role
            WHERE role.rolname = role_name
              AND NOT role.rolsuper AND NOT role.rolinherit
              AND NOT role.rolcreaterole AND NOT role.rolcreatedb
              AND NOT role.rolcanlogin AND NOT role.rolreplication
              AND NOT role.rolbypassrls
        ) THEN
            RAISE EXCEPTION 'unsafe pre-existing T4 evidence role: %', role_name
                USING ERRCODE = '42501';
        END IF;
    END LOOP;
    IF EXISTS (
        SELECT 1
        FROM pg_auth_members membership
        JOIN pg_roles target ON target.oid = membership.roleid
        WHERE target.rolname IN (
            'crypto_agent_evidence_owner',
            'crypto_agent_evidence_verifier',
            'crypto_agent_evidence_reader'
        )
    ) THEN
        RAISE EXCEPTION 'T4 evidence roles must have no pre-migration members'
            USING ERRCODE = '42501';
    END IF;
END;
$$;

CREATE TABLE crypto_agent.t4_bridge_evidence_keys (
    evidence_key_fingerprint       crypto_agent.sha256_hex PRIMARY KEY,
    public_key_spki_base64         text NOT NULL UNIQUE,
    signature_algorithm            text NOT NULL CONSTRAINT
        t4_bridge_evidence_keys_signature_algorithm_check CHECK (
        signature_algorithm = 'ecdsa-p256-sha256-der'
    ),
    registered_at                  timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at                     timestamptz,
    CONSTRAINT t4_bridge_evidence_keys_retirement_check
        CHECK (retired_at IS NULL OR retired_at > registered_at),
    CONSTRAINT t4_bridge_evidence_keys_spki_size_check CHECK (
        octet_length(decode(public_key_spki_base64, 'base64')) BETWEEN 80 AND 256
    ),
    CONSTRAINT t4_bridge_evidence_keys_fingerprint_check CHECK (
        evidence_key_fingerprint = encode(
            public.digest(decode(public_key_spki_base64, 'base64'), 'sha256'), 'hex'
        )
    )
);

ALTER TABLE crypto_agent.t4_observation_campaigns
    ADD COLUMN bridge_evidence_key_fingerprint crypto_agent.sha256_hex,
    ADD CONSTRAINT t4_observation_campaigns_bridge_evidence_key_fk
        FOREIGN KEY (bridge_evidence_key_fingerprint)
        REFERENCES crypto_agent.t4_bridge_evidence_keys(evidence_key_fingerprint);

CREATE TABLE crypto_agent.t4_bridge_observation_events (
    bridge_observation_event_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id                       uuid NOT NULL UNIQUE,
    -- Deliberately not an FK: the bridge may sign preflight/start events for a
    -- caller-selected campaign UUID before that frozen campaign row is inserted.
    campaign_id                    uuid,
    sequence_no                    bigint NOT NULL CONSTRAINT
        t4_bridge_observation_events_sequence_positive_check
        CHECK (sequence_no > 0),
    event_at                       timestamptz NOT NULL,
    event_type                     text NOT NULL CONSTRAINT
        t4_bridge_observation_events_event_type_check CHECK (event_type IN (
        'bridge_started', 'session_connecting', 'session_authenticated',
        'session_ready', 'session_disconnected', 'cache_cleared',
        'missing_data_detected', 'stale_data_detected', 'rate_limited',
        'contract_roll_observed', 'control_requested', 'control_applied',
        'bridge_stopping', 'outbound_rejected', 'campaign_checkpoint'
    )),
    reason_code                    text NOT NULL CONSTRAINT
        t4_bridge_observation_events_reason_code_check CHECK (
        reason_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    ),
    boot_id                        uuid NOT NULL,
    session_generation             bigint NOT NULL CONSTRAINT
        t4_bridge_observation_events_session_generation_check
        CHECK (session_generation >= 0),
    reconnect_count                integer NOT NULL CONSTRAINT
        t4_bridge_observation_events_reconnect_count_check
        CHECK (reconnect_count >= 0),
    environment                    text NOT NULL CONSTRAINT
        t4_bridge_observation_events_live_environment_check
        CHECK (environment = 'live_t4'),
    bridge_schema_version          smallint NOT NULL CONSTRAINT
        t4_bridge_observation_events_schema_v5_check
        CHECK (bridge_schema_version = 5),
    read_only                      boolean NOT NULL CONSTRAINT
        t4_bridge_observation_events_read_only_check CHECK (read_only),
    order_routes_exposed           boolean NOT NULL CONSTRAINT
        t4_bridge_observation_events_no_order_routes_check
        CHECK (NOT order_routes_exposed),
    scenario_code                  text CONSTRAINT
        t4_bridge_observation_events_scenario_code_check CHECK (
        scenario_code IS NULL OR scenario_code IN (
        'bridge_restart', 'missing_data', 'rate_limit', 'reconnect',
        'replay_blocked', 'roll_transition', 'stale_data'
    )),
    action_request_id              uuid,
    scope_key                      text,
    control_action                text,
    control_step                   text,
    roll_from_market_id            text,
    roll_to_market_id              text,
    payload                        jsonb NOT NULL,
    previous_event_hash            crypto_agent.sha256_hex,
    payload_hash_sha256            crypto_agent.sha256_hex NOT NULL,
    event_hash_sha256              crypto_agent.sha256_hex NOT NULL UNIQUE,
    canonical_payload_base64       text NOT NULL,
    signature_algorithm            text NOT NULL CONSTRAINT
        t4_bridge_observation_events_signature_algorithm_check CHECK (
        signature_algorithm = 'ecdsa-p256-sha256-der'
    ),
    evidence_key_fingerprint_sha256 crypto_agent.sha256_hex NOT NULL REFERENCES
        crypto_agent.t4_bridge_evidence_keys(evidence_key_fingerprint),
    signature_base64               text NOT NULL,
    signature_verified             boolean NOT NULL CONSTRAINT
        t4_bridge_observation_events_signature_verified_check
        CHECK (signature_verified),
    signature_verified_at          timestamptz NOT NULL,
    signature_verified_by          name NOT NULL,
    received_at                    timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (evidence_key_fingerprint_sha256, sequence_no),
    UNIQUE (event_id, campaign_id, event_hash_sha256),
    CONSTRAINT t4_bridge_observation_events_chain_shape_check CHECK (
        (sequence_no = 1 AND previous_event_hash IS NULL)
        OR (sequence_no > 1 AND previous_event_hash IS NOT NULL)
    ),
    CONSTRAINT t4_bridge_observation_events_attestation_time_check CHECK (
        event_at <= signature_verified_at AND signature_verified_at <= received_at
    ),
    CONSTRAINT t4_bridge_observation_events_received_at_check CHECK (
        received_at <= clock_timestamp() + interval '5 minutes'
    ),
    CONSTRAINT t4_bridge_observation_events_payload_check CHECK (
        jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 16384
    ),
    CONSTRAINT t4_bridge_observation_events_scope_key_check CHECK (
        scope_key IS NULL OR scope_key ~ '^(BTC|ETH)/USD:(240|1440|10080)m$'
    ),
    CONSTRAINT t4_bridge_observation_events_control_action_check CHECK (
        control_action IS NULL OR control_action IN (
        'restart', 'reconnect', 'missing_data', 'stale_data', 'rate_limit',
        'checkpoint'
    )),
    CONSTRAINT t4_bridge_observation_events_control_step_check CHECK (
        control_step IS NULL OR control_step IN (
        'requested', 'applied', 'consumed'
    )),
    CONSTRAINT t4_bridge_observation_events_control_projection_check CHECK (
        (control_action IS NULL) = (control_step IS NULL)
    ),
    CONSTRAINT t4_bridge_observation_events_roll_projection_check CHECK (
        (event_type = 'contract_roll_observed'
         AND roll_from_market_id IS NOT NULL AND roll_to_market_id IS NOT NULL
         AND roll_from_market_id <> roll_to_market_id)
        OR
        (event_type <> 'contract_roll_observed'
         AND roll_from_market_id IS NULL AND roll_to_market_id IS NULL)
    ),
    CONSTRAINT t4_bridge_observation_events_control_event_shape_check CHECK (
        (event_type IN ('control_requested', 'control_applied')
         AND action_request_id IS NOT NULL AND control_step IS NOT NULL)
        OR event_type NOT IN ('control_requested', 'control_applied')
    ),
    CONSTRAINT t4_bridge_observation_events_canonical_size_check CHECK (
        octet_length(decode(canonical_payload_base64, 'base64')) BETWEEN 32 AND 65536
    ),
    CONSTRAINT t4_bridge_observation_events_hash_check CHECK (
        event_hash_sha256 = encode(
            public.digest(decode(canonical_payload_base64, 'base64'), 'sha256'), 'hex'
        )
    ),
    CONSTRAINT t4_bridge_observation_events_signature_size_check CHECK (
        octet_length(decode(signature_base64, 'base64')) BETWEEN 8 AND 256
    )
);

-- Nullable on migration so historical reports stay readable.  The 0018 final
-- report guard requires the complete, signed checkpoint anchor for every new
-- report and freezes the exact signer-journal prefix ending at that event.
ALTER TABLE crypto_agent.t4_observation_quality_reports
    ADD COLUMN bridge_checkpoint_event_id uuid,
    ADD COLUMN bridge_checkpoint_sequence_no bigint,
    ADD COLUMN bridge_checkpoint_event_hash crypto_agent.sha256_hex,
    ADD COLUMN bridge_checkpoint_action_request_id uuid,
    ADD CONSTRAINT t4_observation_quality_reports_checkpoint_event_fk
        FOREIGN KEY (bridge_checkpoint_event_id)
        REFERENCES crypto_agent.t4_bridge_observation_events(event_id),
    ADD CONSTRAINT t4_observation_quality_reports_checkpoint_event_key
        UNIQUE (bridge_checkpoint_event_id),
    ADD CONSTRAINT t4_observation_quality_reports_checkpoint_shape_check CHECK (
        (bridge_checkpoint_event_id IS NULL
         AND bridge_checkpoint_sequence_no IS NULL
         AND bridge_checkpoint_event_hash IS NULL
         AND bridge_checkpoint_action_request_id IS NULL)
        OR
        (bridge_checkpoint_event_id IS NOT NULL
         AND bridge_checkpoint_sequence_no > 0
         AND bridge_checkpoint_event_hash IS NOT NULL
         AND bridge_checkpoint_action_request_id IS NOT NULL)
    );

-- A replay has no signed bridge event.  It is therefore accepted only after a
-- separate verifier login has independently replayed the frozen T4 source
-- snapshot through a read-only connection and attested the exact inputs.  The
-- caller supplies no PASS/outcome flag; PostgreSQL derives eligibility from
-- the frozen campaign, point-in-time batch and immutable attestation fields.
CREATE TABLE crypto_agent.t4_replay_verifier_attestations (
    replay_attestation_id          bigint GENERATED ALWAYS AS IDENTITY,
    attestation_id                 uuid NOT NULL,
    campaign_id                    uuid NOT NULL,
    scope_key                      text NOT NULL,
    replay_as_of                   timestamptz NOT NULL,
    replay_limit                   integer NOT NULL DEFAULT 120,
    replay_fingerprint_sha256      crypto_agent.sha256_hex NOT NULL,
    source_batch_ids               bigint[] NOT NULL,
    source_batch_hashes            crypto_agent.sha256_hex[] NOT NULL,
    provenance_sha256              crypto_agent.sha256_hex NOT NULL,
    read_only                      boolean NOT NULL DEFAULT true,
    execution_enabled              boolean NOT NULL DEFAULT false,
    external_delivery_eligible     boolean NOT NULL DEFAULT false,
    attested_at                    timestamptz NOT NULL DEFAULT clock_timestamp(),
    attested_by                    name NOT NULL,
    content_hash                   crypto_agent.sha256_hex NOT NULL,
    CONSTRAINT t4_replay_verifier_attestations_pk
        PRIMARY KEY (replay_attestation_id),
    CONSTRAINT t4_replay_verifier_attestations_attestation_key
        UNIQUE (attestation_id),
    CONSTRAINT t4_replay_verifier_attestations_campaign_scope_key
        UNIQUE (campaign_id, scope_key),
    CONSTRAINT t4_replay_verifier_attestations_content_hash_key
        UNIQUE (content_hash),
    CONSTRAINT t4_replay_verifier_attestations_campaign_fk
        FOREIGN KEY (campaign_id) REFERENCES
            crypto_agent.t4_observation_campaigns(campaign_id),
    CONSTRAINT t4_replay_verifier_attestations_scope_key_check CHECK (
        scope_key ~ '^(BTC|ETH)/USD:(240|1440|10080)m$'
    ),
    CONSTRAINT t4_replay_verifier_attestations_limit_check CHECK (
        replay_limit = 120
    ),
    CONSTRAINT t4_replay_verifier_attestations_source_cardinality_check CHECK (
        cardinality(source_batch_ids) = 1
        AND cardinality(source_batch_hashes) = 1
        AND source_batch_ids[1] > 0
    ),
    CONSTRAINT t4_replay_verifier_attestations_safety_check CHECK (
        read_only AND NOT execution_enabled AND NOT external_delivery_eligible
    ),
    CONSTRAINT t4_replay_verifier_attestations_time_check CHECK (
        replay_as_of <= attested_at
    )
);

CREATE OR REPLACE FUNCTION crypto_agent.t4_replay_attestations_are_verified(
    p_campaign_id uuid,
    p_attestation_ids bigint[]
)
RETURNS boolean
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    expected_scope_count integer;
    verified_count integer;
BEGIN
    SELECT * INTO campaign_row
    FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = p_campaign_id;
    IF NOT FOUND OR campaign_row.environment <> 'live_t4'
       OR NOT campaign_row.read_only OR campaign_row.execution_enabled
       OR jsonb_typeof(campaign_row.scope_manifest) <> 'array' THEN
        RETURN FALSE;
    END IF;

    expected_scope_count := jsonb_array_length(campaign_row.scope_manifest);
    IF p_attestation_ids IS NULL
       OR cardinality(p_attestation_ids) <> expected_scope_count
       OR (SELECT count(DISTINCT item)
           FROM unnest(p_attestation_ids) AS item) <> expected_scope_count THEN
        RETURN FALSE;
    END IF;

    SELECT count(*) INTO verified_count
    FROM crypto_agent.t4_replay_verifier_attestations attestation
    JOIN pg_roles verifier_login
      ON verifier_login.rolname = attestation.attested_by
    WHERE attestation.replay_attestation_id = ANY(p_attestation_ids)
      AND attestation.campaign_id = p_campaign_id
      AND campaign_row.scope_manifest ? attestation.scope_key
      AND attestation.replay_as_of = campaign_row.planned_ends_at
      AND attestation.replay_limit = 120
      AND attestation.read_only
      AND NOT attestation.execution_enabled
      AND NOT attestation.external_delivery_eligible
      AND attestation.attested_at >= campaign_row.planned_ends_at
      AND cardinality(attestation.source_batch_ids) = 1
      AND cardinality(attestation.source_batch_hashes) = 1
      AND attestation.provenance_sha256 = encode(public.digest(convert_to(
            't4-replay-provenance-v1' || E'\n'
            || attestation.source_batch_ids[1]::text || ':'
            || lower(attestation.source_batch_hashes[1]) || E'\n',
            'UTF8'), 'sha256'), 'hex')
      AND attestation.content_hash = encode(public.digest(convert_to(
            replace(jsonb_build_object(
                'attestation_id', attestation.attestation_id::text,
                'campaign_id', attestation.campaign_id::text,
                'external_delivery_eligible', false,
                'provenance_sha256', attestation.provenance_sha256::text,
                'read_only', true,
                'replay_as_of', attestation.replay_as_of::text,
                'replay_fingerprint_sha256',
                    attestation.replay_fingerprint_sha256::text,
                'replay_limit', 120,
                'scope_key', attestation.scope_key,
                'source_batch_hashes', attestation.source_batch_hashes,
                'source_batch_ids', attestation.source_batch_ids
            )::text, ', ', ','), 'UTF8'), 'sha256'), 'hex')
      AND verifier_login.rolcanlogin AND verifier_login.rolinherit
      AND NOT verifier_login.rolsuper
      AND NOT verifier_login.rolcreaterole
      AND NOT verifier_login.rolcreatedb
      AND NOT verifier_login.rolreplication
      AND NOT verifier_login.rolbypassrls
      AND EXISTS (
            SELECT 1
            FROM pg_auth_members membership
            JOIN pg_roles target ON target.oid = membership.roleid
            WHERE target.rolname = 'crypto_agent_evidence_verifier'
              AND membership.member = verifier_login.oid
      )
      AND NOT pg_has_role(
            verifier_login.rolname,
            'crypto_agent_evidence_owner', 'MEMBER'
      )
      AND NOT pg_has_role(
            verifier_login.rolname,
            'crypto_agent_evidence_reader', 'MEMBER'
      )
      AND NOT EXISTS (
            SELECT 1 FROM pg_database database
            WHERE database.datname = current_database()
              AND database.datdba = verifier_login.oid
      )
      AND NOT EXISTS (
            SELECT 1 FROM pg_namespace namespace
            WHERE namespace.nspname = 'crypto_agent'
              AND namespace.nspowner = verifier_login.oid
      )
      AND NOT EXISTS (
            SELECT 1 FROM pg_class relation
            JOIN pg_namespace namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'crypto_agent'
              AND relation.relowner = verifier_login.oid
      )
      AND NOT EXISTS (
            SELECT 1 FROM pg_proc routine
            JOIN pg_namespace namespace
              ON namespace.oid = routine.pronamespace
            WHERE namespace.nspname = 'crypto_agent'
              AND routine.proowner = verifier_login.oid
      )
      AND attestation.source_batch_ids[1] = (
            SELECT candidate.t4_batch_id
            FROM crypto_agent.t4_ingestion_batches candidate
            WHERE candidate.logical_symbol =
                    split_part(attestation.scope_key, ':', 1)
              AND candidate.interval_seconds =
                    rtrim(split_part(attestation.scope_key, ':', 2), 'm')::integer
                    * 60
              AND candidate.requested_as_of <= attestation.replay_as_of
              AND candidate.available_at <= attestation.replay_as_of
              AND candidate.ingested_at <= attestation.replay_as_of
              AND candidate.record_count >= 120
              AND candidate.status = 'completed'
              AND (
                    SELECT count(*)
                    FROM crypto_agent.t4_canonical_candles eligible
                    WHERE eligible.t4_batch_id = candidate.t4_batch_id
                      AND eligible.close_time <= attestation.replay_as_of
                      AND eligible.available_at <= attestation.replay_as_of
                      AND eligible.provider_ingested_at <= attestation.replay_as_of
              ) >= 120
            ORDER BY candidate.available_at DESC,
                     candidate.ingested_at DESC,
                     candidate.t4_batch_id DESC
            LIMIT 1
      )
      AND EXISTS (
            SELECT 1
            FROM crypto_agent.t4_ingestion_batches selected_batch
            WHERE selected_batch.t4_batch_id = attestation.source_batch_ids[1]
              AND selected_batch.environment = 'live_t4'
              AND selected_batch.bridge_schema_version = 5
      )
      AND attestation.source_batch_hashes[1] = (
            SELECT batch.raw_payload_hash
            FROM crypto_agent.t4_ingestion_batches batch
            WHERE batch.t4_batch_id = attestation.source_batch_ids[1]
      );

    RETURN verified_count = expected_scope_count
       AND NOT EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(campaign_row.scope_manifest) scope
            WHERE NOT EXISTS (
                SELECT 1
                FROM crypto_agent.t4_replay_verifier_attestations attestation
                WHERE attestation.replay_attestation_id = ANY(p_attestation_ids)
                  AND attestation.campaign_id = p_campaign_id
                  AND attestation.scope_key = scope.value
            )
       );
END;
$$;

CREATE TABLE crypto_agent.t4_observation_scenario_trials (
    scenario_trial_id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trial_id                       uuid NOT NULL UNIQUE,
    campaign_id                    uuid NOT NULL REFERENCES
        crypto_agent.t4_observation_campaigns(campaign_id),
    scenario_code                  text NOT NULL CONSTRAINT
        t4_observation_scenario_trials_scenario_code_check CHECK (scenario_code IN (
        'bridge_restart', 'missing_data', 'rate_limit', 'reconnect',
        'replay_blocked', 'roll_transition', 'stale_data'
    )),
    outcome                        text NOT NULL CONSTRAINT
        t4_observation_scenario_trials_outcome_check
        CHECK (outcome IN ('pass', 'fail')),
    started_at                     timestamptz NOT NULL,
    completed_at                   timestamptz NOT NULL,
    action_request_id              uuid,
    bridge_boot_id                 uuid,
    first_event_id                 uuid REFERENCES
        crypto_agent.t4_bridge_observation_events(event_id),
    last_event_id                  uuid REFERENCES
        crypto_agent.t4_bridge_observation_events(event_id),
    evidence_event_ids             uuid[] NOT NULL DEFAULT '{}'::uuid[],
    evidence_event_hashes          crypto_agent.sha256_hex[] NOT NULL
        DEFAULT '{}'::crypto_agent.sha256_hex[],
    pre_t4_batch_id                bigint REFERENCES
        crypto_agent.t4_ingestion_batches(t4_batch_id),
    post_t4_batch_id               bigint REFERENCES
        crypto_agent.t4_ingestion_batches(t4_batch_id),
    observation_cycle_id           bigint REFERENCES
        crypto_agent.t4_observation_cycles(observation_cycle_id),
    research_run_id                bigint REFERENCES crypto_agent.research_runs(research_run_id),
    replay_fingerprint_sha256      crypto_agent.sha256_hex,
    replay_attestation_ids         bigint[] NOT NULL DEFAULT '{}'::bigint[],
    detail_code                    text NOT NULL CONSTRAINT
        t4_observation_scenario_trials_detail_code_check CHECK (
        detail_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    ),
    content_hash                   crypto_agent.sha256_hex NOT NULL UNIQUE,
    created_at                     timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (campaign_id, trial_id, scenario_code),
    CONSTRAINT t4_observation_scenario_trials_time_check
        CHECK (started_at <= completed_at AND completed_at <= created_at),
    CONSTRAINT t4_observation_scenario_trials_event_cardinality_check CHECK (
        cardinality(evidence_event_ids) = cardinality(evidence_event_hashes)
        AND cardinality(evidence_event_ids) <= 64
    ),
    CONSTRAINT t4_observation_scenario_trials_replay_attestation_shape_check CHECK (
        (scenario_code = 'replay_blocked'
         AND research_run_id IS NULL
         AND replay_fingerprint_sha256 IS NULL
         AND cardinality(replay_attestation_ids) > 0)
        OR
        (scenario_code <> 'replay_blocked'
         AND cardinality(replay_attestation_ids) = 0)
    ),
    CONSTRAINT t4_observation_scenario_trials_shape_check CHECK (
        outcome = 'fail'
        OR
        (scenario_code = 'replay_blocked'
         AND first_event_id IS NULL AND last_event_id IS NULL
         AND cardinality(evidence_event_ids) = 0)
        OR
        (scenario_code <> 'replay_blocked'
         AND first_event_id IS NOT NULL AND last_event_id IS NOT NULL
         AND cardinality(evidence_event_ids) > 0
         AND evidence_event_ids[1] = first_event_id
         AND evidence_event_ids[cardinality(evidence_event_ids)] = last_event_id)
    )
);

CREATE TABLE crypto_agent.t4_observation_scenario_trial_requests (
    trial_id                       uuid PRIMARY KEY,
    campaign_id                    uuid NOT NULL REFERENCES
        crypto_agent.t4_observation_campaigns(campaign_id),
    scenario_code                  text NOT NULL CONSTRAINT
        t4_observation_scenario_trial_requests_scenario_code_check CHECK (
        scenario_code IN (
        'bridge_restart', 'missing_data', 'rate_limit', 'reconnect', 'stale_data'
    )),
    scope_key                      text NOT NULL CONSTRAINT
        t4_observation_scenario_trial_requests_scope_key_check CHECK (
        scope_key ~ '^(BTC|ETH)/USD:(240|1440|10080)m$'
    ),
    action_request_id              uuid NOT NULL,
    started_at                     timestamptz NOT NULL,
    content_hash                   crypto_agent.sha256_hex NOT NULL UNIQUE,
    UNIQUE (campaign_id, scenario_code, action_request_id),
    CONSTRAINT t4_observation_scenario_trial_requests_campaign_action_key
        UNIQUE (campaign_id, action_request_id)
);

CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_observation_scenario_trial()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    event_count integer;
    distinct_event_count integer;
    bad_event_count integer;
    first_event crypto_agent.t4_bridge_observation_events%ROWTYPE;
    last_event crypto_agent.t4_bridge_observation_events%ROWTYPE;
    pre_batch crypto_agent.t4_ingestion_batches%ROWTYPE;
    post_batch crypto_agent.t4_ingestion_batches%ROWTYPE;
    cycle_row crypto_agent.t4_observation_cycles%ROWTYPE;
    run_row crypto_agent.research_runs%ROWTYPE;
    request_row crypto_agent.t4_observation_scenario_trial_requests%ROWTYPE;
    scenario_verified boolean := false;
    recovery_anchors_verified boolean := false;
    control_envelope_verified boolean := false;
BEGIN
    IF NOT crypto_agent.t4_evidence_verifier_session_is_safe() THEN
        RAISE EXCEPTION 'scenario trial start requires a separate verifier login'
            USING ERRCODE = '42501';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation-campaign:' || NEW.campaign_id::text, 0
    ));
    SELECT * INTO campaign_row
    FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = NEW.campaign_id;
    IF NOT FOUND
       OR campaign_row.bridge_evidence_key_fingerprint IS NULL
       OR NEW.started_at < campaign_row.started_at
       OR (
            NEW.scenario_code <> 'replay_blocked'
            AND NEW.completed_at > campaign_row.planned_ends_at
       )
       OR (
            NEW.scenario_code = 'replay_blocked'
            AND NEW.completed_at < campaign_row.planned_ends_at
       )
       OR EXISTS (
            SELECT 1 FROM crypto_agent.t4_observation_quality_reports report
            WHERE report.campaign_id = NEW.campaign_id
       ) THEN
        RAISE EXCEPTION 'scenario trial does not match an open signed campaign'
            USING ERRCODE = '22023';
    END IF;

    IF NEW.scenario_code NOT IN ('replay_blocked', 'roll_transition') THEN
        SELECT * INTO request_row
        FROM crypto_agent.t4_observation_scenario_trial_requests request
        WHERE request.trial_id = NEW.trial_id;
        IF NOT FOUND
           OR request_row.campaign_id IS DISTINCT FROM NEW.campaign_id
           OR request_row.scenario_code IS DISTINCT FROM NEW.scenario_code
           OR request_row.action_request_id IS DISTINCT FROM NEW.action_request_id
           OR request_row.started_at IS DISTINCT FROM NEW.started_at THEN
            RAISE EXCEPTION 'scenario trial does not match its database request'
                USING ERRCODE = '22023';
        END IF;
    END IF;

    SELECT count(*), count(DISTINCT item)
    INTO event_count, distinct_event_count
    FROM unnest(NEW.evidence_event_ids) AS item;
    IF event_count <> distinct_event_count THEN
        RAISE EXCEPTION 'scenario trial contains duplicate evidence events'
            USING ERRCODE = '22023';
    END IF;

    SELECT count(*) INTO bad_event_count
    FROM unnest(NEW.evidence_event_ids, NEW.evidence_event_hashes)
        WITH ORDINALITY AS evidence(event_id, event_hash, position)
    LEFT JOIN crypto_agent.t4_bridge_observation_events event
      ON event.event_id = evidence.event_id
     AND event.event_hash_sha256 = evidence.event_hash
     AND event.campaign_id = NEW.campaign_id
     AND event.evidence_key_fingerprint_sha256 =
            campaign_row.bridge_evidence_key_fingerprint
     AND event.signature_verified
     AND event.event_at BETWEEN NEW.started_at AND NEW.completed_at
    WHERE event.event_id IS NULL;
    IF bad_event_count <> 0 THEN
        RAISE EXCEPTION 'scenario trial contains unverifiable signed events'
            USING ERRCODE = '22023';
    END IF;

    IF NEW.first_event_id IS NOT NULL THEN
        SELECT * INTO first_event FROM crypto_agent.t4_bridge_observation_events
        WHERE event_id = NEW.first_event_id;
        SELECT * INTO last_event FROM crypto_agent.t4_bridge_observation_events
        WHERE event_id = NEW.last_event_id;
        IF NOT FOUND
           OR first_event.sequence_no > last_event.sequence_no
           OR last_event.sequence_no - first_event.sequence_no + 1 <>
                cardinality(NEW.evidence_event_ids)
           OR first_event.event_at < NEW.started_at
           OR last_event.event_at > NEW.completed_at THEN
            RAISE EXCEPTION 'scenario event boundary is invalid'
                USING ERRCODE = '22023';
        END IF;
        SELECT count(*) INTO bad_event_count
        FROM unnest(NEW.evidence_event_ids, NEW.evidence_event_hashes)
            WITH ORDINALITY AS evidence(event_id, event_hash, position)
        LEFT JOIN crypto_agent.t4_bridge_observation_events event
          ON event.event_id = evidence.event_id
         AND event.event_hash_sha256 = evidence.event_hash
         AND event.sequence_no = first_event.sequence_no + evidence.position - 1
         AND event.evidence_key_fingerprint_sha256 =
                campaign_row.bridge_evidence_key_fingerprint
        WHERE event.event_id IS NULL;
        IF bad_event_count <> 0 THEN
            RAISE EXCEPTION 'scenario evidence is not in exact signed chain order'
                USING ERRCODE = '22023';
        END IF;
    END IF;

    IF NEW.pre_t4_batch_id IS NOT NULL THEN
        SELECT * INTO pre_batch FROM crypto_agent.t4_ingestion_batches
        WHERE t4_batch_id = NEW.pre_t4_batch_id;
    END IF;
    IF NEW.post_t4_batch_id IS NOT NULL THEN
        SELECT * INTO post_batch FROM crypto_agent.t4_ingestion_batches
        WHERE t4_batch_id = NEW.post_t4_batch_id;
    END IF;
    IF NEW.observation_cycle_id IS NOT NULL THEN
        SELECT * INTO cycle_row FROM crypto_agent.t4_observation_cycles
        WHERE observation_cycle_id = NEW.observation_cycle_id;
    END IF;
    IF NEW.research_run_id IS NOT NULL THEN
        SELECT * INTO run_row FROM crypto_agent.research_runs
        WHERE research_run_id = NEW.research_run_id;
    END IF;

    IF NEW.scenario_code NOT IN ('replay_blocked', 'roll_transition') THEN
        control_envelope_verified := first_event.event_type = 'control_requested'
          AND first_event.campaign_id = NEW.campaign_id
          AND first_event.scenario_code = NEW.scenario_code
          AND first_event.action_request_id = NEW.action_request_id
          AND first_event.scope_key = request_row.scope_key
          AND first_event.control_step = 'requested'
          AND first_event.control_action = (CASE NEW.scenario_code
                WHEN 'bridge_restart' THEN 'restart'
                ELSE NEW.scenario_code
              END)
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
              WHERE event.event_id = ANY(NEW.evidence_event_ids)
                AND event.sequence_no > first_event.sequence_no
                AND event.sequence_no < last_event.sequence_no
                AND event.event_type = 'control_applied'
                AND event.campaign_id = NEW.campaign_id
                AND event.scenario_code = NEW.scenario_code
                AND event.action_request_id = NEW.action_request_id
                AND event.scope_key = request_row.scope_key
                AND event.control_step = 'applied'
                AND event.control_action = (CASE NEW.scenario_code
                    WHEN 'bridge_restart' THEN 'restart'
                    ELSE NEW.scenario_code
                  END)
          );
        recovery_anchors_verified := NEW.pre_t4_batch_id IS NOT NULL
          AND NEW.post_t4_batch_id IS NOT NULL
          AND pre_batch.environment = 'live_t4'
          AND pre_batch.bridge_schema_version = 5
          AND pre_batch.logical_symbol || ':'
                || (pre_batch.interval_seconds / 60)::text || 'm'
                = request_row.scope_key
          AND pre_batch.ingested_at <= NEW.started_at
          AND pre_batch.ingested_at >= NEW.started_at
                - make_interval(secs => campaign_row.cycle_interval_seconds * 2)
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_observation_cycles pre_cycle
              WHERE pre_cycle.campaign_id = NEW.campaign_id
                AND pre_cycle.t4_batch_id = NEW.pre_t4_batch_id
                AND pre_cycle.scope_key = request_row.scope_key
                AND pre_cycle.outcome = 'success'
                AND pre_cycle.finished_at <= NEW.started_at
                AND pre_cycle.finished_at >= NEW.started_at
                    - make_interval(
                        secs => campaign_row.cycle_interval_seconds * 2
                      )
                AND pre_cycle.expected_at = (
                    SELECT max(latest_cycle.expected_at)
                    FROM crypto_agent.t4_observation_cycles latest_cycle
                    WHERE latest_cycle.campaign_id = NEW.campaign_id
                      AND latest_cycle.scope_key = request_row.scope_key
                      AND latest_cycle.expected_at < NEW.started_at
                )
          )
          AND post_batch.environment = 'live_t4'
          AND post_batch.bridge_schema_version = 5
          AND post_batch.logical_symbol || ':'
                || (post_batch.interval_seconds / 60)::text || 'm'
                = request_row.scope_key
          AND post_batch.ingested_at >= last_event.event_at
          AND post_batch.ingested_at <= NEW.completed_at;
    END IF;

    CASE NEW.scenario_code
    WHEN 'bridge_restart' THEN
        scenario_verified := recovery_anchors_verified
          AND control_envelope_verified
          AND NEW.action_request_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
              WHERE event.event_id = ANY(NEW.evidence_event_ids)
                AND event.event_type = 'control_requested'
                AND event.control_action = 'restart'
                AND event.control_step = 'requested'
                AND event.action_request_id = NEW.action_request_id
                AND event.scope_key = request_row.scope_key
          )
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
              WHERE event.event_id = ANY(NEW.evidence_event_ids)
                AND event.event_type = 'control_applied'
                AND event.control_action = 'restart'
                AND event.control_step = 'applied'
                AND event.action_request_id = NEW.action_request_id
                AND event.scope_key = request_row.scope_key
          )
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
              WHERE event.event_id = ANY(NEW.evidence_event_ids)
                AND event.event_type = 'bridge_stopping'
                AND event.control_action = 'restart'
                AND event.control_step = 'consumed'
                AND event.action_request_id = NEW.action_request_id
                AND event.scope_key = request_row.scope_key
                AND event.sequence_no > first_event.sequence_no
                AND event.sequence_no < last_event.sequence_no
          )
          AND first_event.boot_id <> last_event.boot_id
          AND last_event.event_type = 'session_ready'
          AND post_batch.environment = 'live_t4'
          AND post_batch.bridge_schema_version = 5
          AND post_batch.logical_symbol || ':'
                || (post_batch.interval_seconds / 60)::text || 'm'
                = request_row.scope_key
          AND post_batch.ingested_at >= last_event.event_at;
    WHEN 'reconnect' THEN
        scenario_verified := recovery_anchors_verified
          AND control_envelope_verified
          AND NEW.action_request_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
              WHERE event.event_id = ANY(NEW.evidence_event_ids)
                AND event.event_type = 'control_requested'
                AND event.control_action = 'reconnect'
                AND event.control_step = 'requested'
                AND event.action_request_id = NEW.action_request_id
                AND event.scope_key = request_row.scope_key
          )
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
              WHERE event.event_id = ANY(NEW.evidence_event_ids)
                AND event.event_type = 'control_applied'
                AND event.control_action = 'reconnect'
                AND event.control_step = 'applied'
                AND event.action_request_id = NEW.action_request_id
                AND event.scope_key = request_row.scope_key
          )
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
              WHERE event.event_id = ANY(NEW.evidence_event_ids)
                AND event.event_type = 'session_disconnected'
                AND event.sequence_no > first_event.sequence_no
                AND event.sequence_no < last_event.sequence_no
          )
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
              WHERE event.event_id = ANY(NEW.evidence_event_ids)
                AND event.event_type = 'cache_cleared'
                AND event.campaign_id = NEW.campaign_id
                AND event.scenario_code = 'reconnect'
                AND event.action_request_id = NEW.action_request_id
                AND event.scope_key = request_row.scope_key
                AND event.control_action IS NULL
                AND event.sequence_no > first_event.sequence_no
                AND event.sequence_no < last_event.sequence_no
          )
          AND last_event.event_type = 'session_ready'
          AND last_event.control_action = 'reconnect'
          AND last_event.control_step = 'consumed'
          AND last_event.scope_key = request_row.scope_key
          AND first_event.boot_id = last_event.boot_id
          AND last_event.reconnect_count > first_event.reconnect_count
          AND last_event.session_generation > first_event.session_generation
          AND post_batch.environment = 'live_t4'
          AND post_batch.bridge_schema_version = 5
          AND post_batch.logical_symbol || ':'
                || (post_batch.interval_seconds / 60)::text || 'm'
                = request_row.scope_key
          AND post_batch.ingested_at >= last_event.event_at;
    WHEN 'missing_data' THEN
        scenario_verified := recovery_anchors_verified
          AND control_envelope_verified
          AND NEW.action_request_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
              WHERE event.event_id = ANY(NEW.evidence_event_ids)
                AND event.event_type = 'cache_cleared'
                AND event.campaign_id = NEW.campaign_id
                AND event.scenario_code = 'missing_data'
                AND event.action_request_id = NEW.action_request_id
                AND event.scope_key = request_row.scope_key
                AND event.control_action IS NULL
                AND event.sequence_no > first_event.sequence_no
                AND event.sequence_no < last_event.sequence_no
          )
          AND EXISTS (
            SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
            WHERE event.event_id = ANY(NEW.evidence_event_ids)
              AND event.event_type = 'missing_data_detected'
              AND event.reason_code = 'T4_MISSING_DATA'
              AND event.control_action = 'missing_data'
              AND event.control_step = 'consumed'
              AND event.action_request_id = NEW.action_request_id
              AND event.scope_key = request_row.scope_key
        ) AND last_event.event_type = 'missing_data_detected'
          AND last_event.action_request_id = NEW.action_request_id
          AND last_event.control_action = 'missing_data'
          AND last_event.control_step = 'consumed'
          AND last_event.scope_key = request_row.scope_key
          AND NEW.observation_cycle_id IS NOT NULL
          AND cycle_row.campaign_id = NEW.campaign_id
          AND cycle_row.outcome = 'failure'
          AND cycle_row.error_code = 'T4_MISSING_DATA'
          AND cycle_row.scope_key = request_row.scope_key
          AND cycle_row.started_at <= last_event.event_at
          AND cycle_row.finished_at >= last_event.event_at
          AND cycle_row.finished_at <= NEW.completed_at
          AND NEW.post_t4_batch_id IS NOT NULL
          AND post_batch.environment = 'live_t4'
          AND post_batch.bridge_schema_version = 5
          AND post_batch.ingested_at >= last_event.event_at
          AND post_batch.logical_symbol || ':'
                || (post_batch.interval_seconds / 60)::text || 'm'
                = request_row.scope_key
          AND NEW.research_run_id IS NOT NULL
          AND run_row.trace_key = cycle_row.trace_id::text
          AND run_row.run_kind = 'live_t4_analysis'
          AND run_row.requested_at BETWEEN cycle_row.started_at
                                       AND NEW.completed_at
          AND EXISTS (
              SELECT 1 FROM crypto_agent.research_runs run
              JOIN crypto_agent.research_run_events run_event
                ON run_event.research_run_id = run.research_run_id
              WHERE run.research_run_id = NEW.research_run_id
                AND run.trace_key = cycle_row.trace_id::text
                AND run.run_kind = 'live_t4_analysis'
                AND run_event.event_type = 'failed'
                AND run_event.details->>'error_code' = 'T4_MISSING_DATA'
                AND run_event.event_at BETWEEN last_event.event_at
                                           AND NEW.completed_at
          )
          AND NOT EXISTS (
              SELECT 1 FROM crypto_agent.alerts alert
              JOIN crypto_agent.research_runs run
                ON run.research_run_id = alert.research_run_id
              WHERE run.trace_key = cycle_row.trace_id::text
          )
          AND NOT EXISTS (
              SELECT 1 FROM crypto_agent.alert_delivery_outbox outbox
              JOIN crypto_agent.alerts alert USING (alert_id)
              JOIN crypto_agent.research_runs run
                ON run.research_run_id = alert.research_run_id
              WHERE run.trace_key = cycle_row.trace_id::text
          )
          AND NOT EXISTS (
              SELECT 1 FROM crypto_agent.alert_delivery_attempts attempt
              JOIN crypto_agent.alert_delivery_outbox outbox
                USING (alert_delivery_outbox_id)
              JOIN crypto_agent.alerts alert USING (alert_id)
              JOIN crypto_agent.research_runs run
                ON run.research_run_id = alert.research_run_id
              WHERE run.trace_key = cycle_row.trace_id::text
          );
    WHEN 'rate_limit' THEN
        scenario_verified := recovery_anchors_verified
          AND control_envelope_verified
          AND NEW.action_request_id IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
            WHERE event.event_id = ANY(NEW.evidence_event_ids)
              AND event.event_type = 'rate_limited'
              AND event.reason_code = 'T4_RATE_LIMITED'
              AND event.control_action = 'rate_limit'
              AND event.control_step = 'consumed'
              AND event.action_request_id = NEW.action_request_id
              AND event.scope_key = request_row.scope_key
        ) AND last_event.event_type = 'rate_limited'
          AND last_event.action_request_id = NEW.action_request_id
          AND last_event.control_action = 'rate_limit'
          AND last_event.control_step = 'consumed'
          AND last_event.scope_key = request_row.scope_key
          AND NEW.observation_cycle_id IS NOT NULL
          AND cycle_row.campaign_id = NEW.campaign_id
          AND cycle_row.outcome = 'failure'
          AND cycle_row.error_code = 'T4_RATE_LIMITED'
          AND cycle_row.scope_key = request_row.scope_key
          AND cycle_row.started_at <= last_event.event_at
          AND cycle_row.finished_at >= last_event.event_at
          AND cycle_row.finished_at <= NEW.completed_at
          AND NEW.post_t4_batch_id IS NOT NULL
          AND post_batch.environment = 'live_t4'
          AND post_batch.bridge_schema_version = 5
          AND post_batch.ingested_at >= last_event.event_at
          AND post_batch.logical_symbol || ':'
                || (post_batch.interval_seconds / 60)::text || 'm'
                = request_row.scope_key
          AND NEW.research_run_id IS NOT NULL
          AND run_row.trace_key = cycle_row.trace_id::text
          AND run_row.run_kind = 'live_t4_analysis'
          AND run_row.requested_at BETWEEN cycle_row.started_at
                                       AND NEW.completed_at
          AND EXISTS (
              SELECT 1 FROM crypto_agent.research_runs run
              JOIN crypto_agent.research_run_events run_event
                ON run_event.research_run_id = run.research_run_id
              WHERE run.research_run_id = NEW.research_run_id
                AND run.trace_key = cycle_row.trace_id::text
                AND run.run_kind = 'live_t4_analysis'
                AND run_event.event_type = 'failed'
                AND run_event.details->>'error_code' = 'T4_RATE_LIMITED'
                AND run_event.event_at BETWEEN last_event.event_at
                                           AND NEW.completed_at
          )
          AND NOT EXISTS (
              SELECT 1 FROM crypto_agent.alerts alert
              JOIN crypto_agent.research_runs run
                ON run.research_run_id = alert.research_run_id
              WHERE run.trace_key = cycle_row.trace_id::text
          )
          AND NOT EXISTS (
              SELECT 1 FROM crypto_agent.alert_delivery_outbox outbox
              JOIN crypto_agent.alerts alert USING (alert_id)
              JOIN crypto_agent.research_runs run
                ON run.research_run_id = alert.research_run_id
              WHERE run.trace_key = cycle_row.trace_id::text
          )
          AND NOT EXISTS (
              SELECT 1 FROM crypto_agent.alert_delivery_attempts attempt
              JOIN crypto_agent.alert_delivery_outbox outbox
                USING (alert_delivery_outbox_id)
              JOIN crypto_agent.alerts alert USING (alert_id)
              JOIN crypto_agent.research_runs run
                ON run.research_run_id = alert.research_run_id
              WHERE run.trace_key = cycle_row.trace_id::text
          );
    WHEN 'stale_data' THEN
        scenario_verified := recovery_anchors_verified
          AND control_envelope_verified
          AND NEW.action_request_id IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
            WHERE event.event_id = ANY(NEW.evidence_event_ids)
              AND event.event_type = 'stale_data_detected'
              AND event.reason_code = 'T4_STALE_DATA'
              AND event.control_action = 'stale_data'
              AND event.control_step = 'consumed'
              AND event.action_request_id = NEW.action_request_id
              AND event.scope_key = request_row.scope_key
        ) AND last_event.event_type = 'stale_data_detected'
          AND last_event.action_request_id = NEW.action_request_id
          AND last_event.control_action = 'stale_data'
          AND last_event.control_step = 'consumed'
          AND last_event.scope_key = request_row.scope_key
          AND NEW.observation_cycle_id IS NOT NULL
          AND cycle_row.campaign_id = NEW.campaign_id
          AND cycle_row.outcome = 'failure'
          AND cycle_row.error_code = 'T4_STALE_DATA'
          AND cycle_row.scope_key = request_row.scope_key
          AND cycle_row.started_at <= last_event.event_at
          AND cycle_row.finished_at >= last_event.event_at
          AND cycle_row.finished_at <= NEW.completed_at
          AND NEW.post_t4_batch_id IS NOT NULL
          AND post_batch.environment = 'live_t4'
          AND post_batch.bridge_schema_version = 5
          AND post_batch.ingested_at >= last_event.event_at
          AND post_batch.logical_symbol || ':'
                || (post_batch.interval_seconds / 60)::text || 'm'
                = request_row.scope_key
          AND NEW.research_run_id IS NOT NULL
          AND run_row.trace_key = cycle_row.trace_id::text
          AND run_row.run_kind = 'live_t4_analysis'
          AND run_row.requested_at BETWEEN cycle_row.started_at
                                       AND NEW.completed_at
          AND EXISTS (
              SELECT 1 FROM crypto_agent.research_runs run
              JOIN crypto_agent.research_run_events run_event
                ON run_event.research_run_id = run.research_run_id
              WHERE run.research_run_id = NEW.research_run_id
                AND run.trace_key = cycle_row.trace_id::text
                AND run.run_kind = 'live_t4_analysis'
                AND run_event.event_type = 'failed'
                AND run_event.details->>'error_code' = 'T4_STALE_DATA'
                AND run_event.event_at BETWEEN last_event.event_at
                                           AND NEW.completed_at
          )
          AND NOT EXISTS (
              SELECT 1 FROM crypto_agent.alerts alert
              JOIN crypto_agent.research_runs run
                ON run.research_run_id = alert.research_run_id
              WHERE run.trace_key = cycle_row.trace_id::text
          )
          AND NOT EXISTS (
              SELECT 1 FROM crypto_agent.alert_delivery_outbox outbox
              JOIN crypto_agent.alerts alert USING (alert_id)
              JOIN crypto_agent.research_runs run
                ON run.research_run_id = alert.research_run_id
              WHERE run.trace_key = cycle_row.trace_id::text
          )
          AND NOT EXISTS (
              SELECT 1 FROM crypto_agent.alert_delivery_attempts attempt
              JOIN crypto_agent.alert_delivery_outbox outbox
                USING (alert_delivery_outbox_id)
              JOIN crypto_agent.alerts alert USING (alert_id)
              JOIN crypto_agent.research_runs run
                ON run.research_run_id = alert.research_run_id
              WHERE run.trace_key = cycle_row.trace_id::text
          );
    WHEN 'roll_transition' THEN
        scenario_verified := EXISTS (
            SELECT 1 FROM crypto_agent.t4_bridge_observation_events event
            JOIN crypto_agent.t4_observation_cycles roll_cycle
              ON roll_cycle.campaign_id = NEW.campaign_id
             AND roll_cycle.t4_batch_id = NEW.post_t4_batch_id
             AND roll_cycle.outcome = 'success'
            WHERE event.event_id = ANY(NEW.evidence_event_ids)
              AND event.event_type = 'contract_roll_observed'
              AND event.campaign_id = NEW.campaign_id
              AND event.scope_key = post_batch.logical_symbol || ':'
                    || (post_batch.interval_seconds / 60)::text || 'm'
              AND event.roll_from_market_id = post_batch.rolled_from_market_id
              AND event.roll_to_market_id = post_batch.active_market_id
              AND roll_cycle.started_at <= event.event_at
              AND event.event_at <= post_batch.ingested_at
              AND post_batch.ingested_at <= roll_cycle.finished_at
              AND roll_cycle.finished_at <= NEW.completed_at
        ) AND post_batch.environment = 'live_t4'
          AND post_batch.bridge_schema_version = 5
          AND post_batch.contract_selection = 'rolled'
          AND post_batch.rolled_from_market_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_observation_cycles cycle
              WHERE cycle.campaign_id = NEW.campaign_id
                AND cycle.t4_batch_id = NEW.post_t4_batch_id
                AND cycle.outcome = 'success'
          )
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_contract_transition_evidence transition
              WHERE transition.t4_batch_id = NEW.post_t4_batch_id
                AND transition.from_market_id = post_batch.rolled_from_market_id
                AND transition.to_market_id = post_batch.active_market_id
          )
          AND EXISTS (
              SELECT 1 FROM crypto_agent.t4_canonical_candles candle
              WHERE candle.t4_batch_id = NEW.post_t4_batch_id
                AND candle.market_id = post_batch.active_market_id
          );
    WHEN 'replay_blocked' THEN
        scenario_verified := NEW.research_run_id IS NULL
          AND NEW.replay_fingerprint_sha256 IS NULL
          AND crypto_agent.t4_replay_attestations_are_verified(
                NEW.campaign_id, NEW.replay_attestation_ids
          );
    END CASE;

    IF NEW.outcome = 'pass' AND scenario_verified IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION 'scenario PASS is not proven by objective database evidence'
            USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER t4_observation_scenario_trial_integrity_guard
BEFORE INSERT ON crypto_agent.t4_observation_scenario_trials
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_t4_observation_scenario_trial();

GRANT USAGE, CREATE ON SCHEMA crypto_agent TO crypto_agent_evidence_owner;
GRANT SELECT, INSERT ON crypto_agent.t4_bridge_evidence_keys,
    crypto_agent.t4_bridge_observation_events
    TO crypto_agent_evidence_owner;
GRANT SELECT ON crypto_agent.t4_observation_campaigns
    TO crypto_agent_evidence_owner;
GRANT USAGE, SELECT ON SEQUENCE
    crypto_agent.t4_bridge_observation_events_bridge_observation_event_id_seq
    TO crypto_agent_evidence_owner;
REVOKE ALL ON crypto_agent.t4_bridge_evidence_keys,
    crypto_agent.t4_bridge_observation_events FROM PUBLIC;

CREATE OR REPLACE FUNCTION crypto_agent.register_t4_bridge_evidence_key(
    p_public_key_spki_base64 text
)
RETURNS crypto_agent.sha256_hex
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    fingerprint crypto_agent.sha256_hex;
BEGIN
    IF NOT crypto_agent.t4_evidence_verifier_session_is_safe() THEN
        RAISE EXCEPTION 'T4 evidence key registration requires the verifier role'
            USING ERRCODE = '42501';
    END IF;
    fingerprint := encode(
        public.digest(decode(p_public_key_spki_base64, 'base64'), 'sha256'), 'hex'
    );
    INSERT INTO crypto_agent.t4_bridge_evidence_keys (
        evidence_key_fingerprint, public_key_spki_base64, signature_algorithm
    ) VALUES (
        fingerprint, p_public_key_spki_base64, 'ecdsa-p256-sha256-der'
    )
    ON CONFLICT (evidence_key_fingerprint) DO NOTHING;
    RETURN fingerprint;
END;
$$;

ALTER FUNCTION crypto_agent.register_t4_bridge_evidence_key(text)
    OWNER TO crypto_agent_evidence_owner;
REVOKE ALL ON FUNCTION crypto_agent.register_t4_bridge_evidence_key(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION crypto_agent.register_t4_bridge_evidence_key(text)
    TO crypto_agent_evidence_verifier;

CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_bridge_observation_event()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    previous_row crypto_agent.t4_bridge_observation_events%ROWTYPE;
    signed_payload jsonb;
BEGIN
    IF current_user <> 'crypto_agent_evidence_owner' THEN
        RAISE EXCEPTION 'signed T4 evidence must enter through the verifier boundary'
            USING ERRCODE = '42501';
    END IF;

    BEGIN
        signed_payload := convert_from(
            decode(NEW.canonical_payload_base64, 'base64'), 'UTF8'
        )::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'signed T4 canonical payload is invalid'
            USING ERRCODE = '22023';
    END;

    IF jsonb_typeof(signed_payload) <> 'object'
       OR NOT signed_payload ?& ARRAY[
            'schema_version', 'event_id', 'sequence_no', 'event_at',
            'event_type', 'reason_code', 'campaign_id', 'boot_id',
            'session_generation', 'reconnect_count', 'environment',
            'bridge_schema_version', 'read_only', 'order_routes_exposed',
            'scope_key', 'scenario_code', 'action_request_id',
            'previous_event_hash', 'payload', 'payload_hash_sha256'
       ]
       OR signed_payload - ARRAY[
            'schema_version', 'event_id', 'sequence_no', 'event_at',
            'event_type', 'reason_code', 'campaign_id', 'boot_id',
            'session_generation', 'reconnect_count', 'environment',
            'bridge_schema_version', 'read_only', 'order_routes_exposed',
            'scope_key', 'scenario_code', 'action_request_id',
            'previous_event_hash', 'payload', 'payload_hash_sha256'
       ] <> '{}'::jsonb
       OR signed_payload->'schema_version' <> '1'::jsonb
       OR signed_payload->>'event_id' <> NEW.event_id::text
       OR (signed_payload->>'sequence_no')::bigint <> NEW.sequence_no
       OR (signed_payload->>'event_at')::timestamptz IS DISTINCT FROM NEW.event_at
       OR signed_payload->>'event_type' <> NEW.event_type
       OR signed_payload->>'reason_code' <> NEW.reason_code
       OR signed_payload->>'boot_id' <> NEW.boot_id::text
       OR (signed_payload->>'session_generation')::bigint <> NEW.session_generation
       OR (signed_payload->>'reconnect_count')::integer <> NEW.reconnect_count
       OR signed_payload->>'environment' <> NEW.environment
       OR (signed_payload->>'bridge_schema_version')::smallint
            <> NEW.bridge_schema_version
       OR signed_payload->'read_only' <> to_jsonb(NEW.read_only)
       OR signed_payload->'order_routes_exposed' <> to_jsonb(NEW.order_routes_exposed)
       OR signed_payload->>'payload_hash_sha256' <> NEW.payload_hash_sha256
       OR signed_payload->>'campaign_id' IS DISTINCT FROM NEW.campaign_id::text
       OR signed_payload->>'scenario_code' IS DISTINCT FROM NEW.scenario_code
       OR signed_payload->>'action_request_id'
            IS DISTINCT FROM NEW.action_request_id::text
       OR signed_payload->>'previous_event_hash'
            IS DISTINCT FROM NEW.previous_event_hash::text THEN
        RAISE EXCEPTION 'signed T4 fields do not match canonical payload'
            USING ERRCODE = '22023';
    END IF;

    NEW.payload := signed_payload->'payload';
    NEW.scope_key := signed_payload->>'scope_key';
    NEW.control_action := NEW.payload->>'action';
    NEW.control_step := NEW.payload->>'control_step';
    NEW.roll_from_market_id := NEW.payload->>'from_market_id';
    NEW.roll_to_market_id := NEW.payload->>'to_market_id';

    IF NEW.reason_code IS DISTINCT FROM (CASE NEW.event_type
            WHEN 'bridge_started' THEN 'BRIDGE_STARTED'
            WHEN 'session_connecting' THEN 'T4_SESSION_CONNECTING'
            WHEN 'session_authenticated' THEN 'T4_SESSION_AUTHENTICATED'
            WHEN 'session_ready' THEN 'READY'
            WHEN 'session_disconnected' THEN 'T4_SESSION_DISCONNECTED'
            WHEN 'cache_cleared' THEN 'T4_CACHE_CLEARED'
            WHEN 'missing_data_detected' THEN 'T4_MISSING_DATA'
            WHEN 'stale_data_detected' THEN 'T4_STALE_DATA'
            WHEN 'rate_limited' THEN 'T4_RATE_LIMITED'
            WHEN 'contract_roll_observed' THEN 'T4_CONTRACT_ROLL_OBSERVED'
            WHEN 'control_requested' THEN 'OBSERVATION_CONTROL_REQUESTED'
            WHEN 'control_applied' THEN 'OBSERVATION_CONTROL_APPLIED'
            WHEN 'bridge_stopping' THEN 'BRIDGE_STOPPING'
            WHEN 'outbound_rejected' THEN 'T4_OUTBOUND_REJECTED'
            WHEN 'campaign_checkpoint' THEN 'OBSERVATION_CAMPAIGN_CHECKPOINT'
       END)
       OR jsonb_typeof(NEW.payload) <> 'object'
       OR EXISTS (
            SELECT 1 FROM jsonb_object_keys(NEW.payload) payload_key
            WHERE payload_key NOT IN (
                'action', 'control_step', 'from_market_id', 'to_market_id'
            )
       )
       OR NEW.payload_hash_sha256 IS DISTINCT FROM encode(
            public.digest(convert_to(
                '{' || array_to_string(array_remove(ARRAY[
                    CASE WHEN NEW.payload ? 'action' THEN
                        '"action":' || to_jsonb(NEW.payload->>'action')::text END,
                    CASE WHEN NEW.payload ? 'control_step' THEN
                        '"control_step":' ||
                            to_jsonb(NEW.payload->>'control_step')::text END,
                    CASE WHEN NEW.payload ? 'from_market_id' THEN
                        '"from_market_id":' ||
                            to_jsonb(NEW.payload->>'from_market_id')::text END,
                    CASE WHEN NEW.payload ? 'to_market_id' THEN
                        '"to_market_id":' ||
                            to_jsonb(NEW.payload->>'to_market_id')::text END
                ], NULL), ',') || '}', 'UTF8'), 'sha256'), 'hex'
       )
       OR (NEW.event_type IN (
            'missing_data_detected', 'stale_data_detected',
            'contract_roll_observed'
        ) AND NEW.scope_key IS NULL)
       OR (NEW.event_type = 'rate_limited'
           AND NEW.scenario_code IS NOT NULL AND NEW.scope_key IS NULL)
       OR (NEW.event_type = 'missing_data_detected'
           AND NEW.scenario_code IS NOT NULL
           AND NEW.scenario_code <> 'missing_data')
       OR (NEW.event_type = 'stale_data_detected'
           AND NEW.scenario_code IS NOT NULL
           AND NEW.scenario_code <> 'stale_data')
       OR (NEW.event_type = 'rate_limited'
           AND NEW.scenario_code IS NOT NULL
           AND NEW.scenario_code <> 'rate_limit')
       OR (NEW.event_type = 'contract_roll_observed'
           AND NEW.scenario_code IS DISTINCT FROM 'roll_transition')
       OR (NEW.control_action IS NULL) <> (NEW.control_step IS NULL)
       OR (NEW.control_action IS NOT NULL AND NEW.control_action NOT IN (
            'restart', 'reconnect', 'missing_data', 'stale_data', 'rate_limit',
            'checkpoint'
       ))
       OR (NEW.control_step IS NOT NULL AND NEW.control_step NOT IN (
            'requested', 'applied', 'consumed'
       ))
       OR (NEW.event_type = 'control_requested'
           AND NEW.control_step IS DISTINCT FROM 'requested')
       OR (NEW.event_type = 'control_applied'
           AND NEW.control_step NOT IN ('applied', 'consumed'))
       OR (NEW.event_type IN ('control_requested', 'control_applied')
           AND (NEW.campaign_id IS NULL OR NEW.scenario_code IS NULL
                OR NEW.action_request_id IS NULL))
       OR (NEW.scenario_code IS NULL
           AND NEW.event_type <> 'campaign_checkpoint'
           AND (NEW.action_request_id IS NOT NULL
                OR NEW.control_action IS NOT NULL))
       OR (NEW.scenario_code IN (
                'bridge_restart', 'missing_data', 'rate_limit', 'reconnect',
                'stale_data'
           ) AND (
                NEW.campaign_id IS NULL
                OR NEW.scope_key IS NULL
                OR NEW.action_request_id IS NULL
                OR NOT (
                    (NEW.event_type = 'cache_cleared'
                     AND NEW.scenario_code IN ('reconnect', 'missing_data')
                     AND NEW.control_action IS NULL)
                    OR
                    (NEW.control_action IS NOT NULL
                     AND NEW.scenario_code = (CASE NEW.control_action
                        WHEN 'restart' THEN 'bridge_restart'
                        WHEN 'reconnect' THEN 'reconnect'
                        WHEN 'missing_data' THEN 'missing_data'
                        WHEN 'stale_data' THEN 'stale_data'
                        WHEN 'rate_limit' THEN 'rate_limit'
                     END))
                )
           ))
       OR (NEW.event_type = 'contract_roll_observed'
           AND (NEW.action_request_id IS NOT NULL
                OR NEW.control_action IS NOT NULL))
       OR (NEW.event_type = 'campaign_checkpoint'
           AND (NEW.campaign_id IS NULL
                OR NEW.scope_key IS NOT NULL
                OR NEW.scenario_code IS NOT NULL
                OR NEW.action_request_id IS NULL
                OR NEW.control_action IS DISTINCT FROM 'checkpoint'
                OR NEW.control_step IS DISTINCT FROM 'consumed'
                OR NEW.payload IS DISTINCT FROM
                    '{"action":"checkpoint","control_step":"consumed"}'::jsonb))
       OR (NEW.event_type <> 'campaign_checkpoint'
           AND NEW.control_action = 'checkpoint') THEN
        RAISE EXCEPTION 'signed T4 event payload projection is invalid'
            USING ERRCODE = '22023';
    END IF;

    IF NEW.signature_verified_by IS DISTINCT FROM session_user
       OR NEW.signature_verified_at > clock_timestamp()
       OR NEW.received_at IS DISTINCT FROM NEW.signature_verified_at THEN
        RAISE EXCEPTION 'signed T4 verification attestation is invalid'
            USING ERRCODE = '22023';
    END IF;

    IF NEW.campaign_id IS NOT NULL THEN
        SELECT * INTO campaign_row
        FROM crypto_agent.t4_observation_campaigns
        WHERE campaign_id = NEW.campaign_id;
        IF FOUND AND EXISTS (
            SELECT 1 FROM crypto_agent.t4_observation_quality_reports report
            WHERE report.campaign_id = NEW.campaign_id
        ) THEN
            RAISE EXCEPTION 'signed T4 campaign ledger is already finalized'
                USING ERRCODE = '55000';
        END IF;
        IF FOUND AND (
              campaign_row.environment IS DISTINCT FROM 'live_t4'
           OR campaign_row.bridge_evidence_key_fingerprint IS NULL
           OR campaign_row.bridge_evidence_key_fingerprint IS DISTINCT FROM
                NEW.evidence_key_fingerprint_sha256
        ) THEN
            RAISE EXCEPTION 'signed T4 event does not match its frozen campaign'
                USING ERRCODE = '22023';
        END IF;
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            't4-bridge-evidence:' || NEW.evidence_key_fingerprint_sha256, 0
        )
    );
    SELECT * INTO previous_row
    FROM crypto_agent.t4_bridge_observation_events
    WHERE evidence_key_fingerprint_sha256 = NEW.evidence_key_fingerprint_sha256
    ORDER BY sequence_no DESC
    LIMIT 1;
    IF NOT FOUND THEN
        IF NEW.sequence_no <> 1 OR NEW.previous_event_hash IS NOT NULL
           OR NEW.event_type <> 'bridge_started' THEN
            RAISE EXCEPTION 'first signed bridge event is invalid'
                USING ERRCODE = '22023';
        END IF;
    ELSIF NEW.sequence_no <> previous_row.sequence_no + 1
          OR NEW.previous_event_hash IS DISTINCT FROM previous_row.event_hash_sha256
          OR NEW.event_at < previous_row.event_at
          OR (
               NEW.boot_id = previous_row.boot_id
               AND (NEW.session_generation < previous_row.session_generation
                    OR NEW.reconnect_count < previous_row.reconnect_count)
          )
          OR (
               NEW.boot_id <> previous_row.boot_id
               AND (NEW.event_type <> 'bridge_started'
                    OR NEW.session_generation <> 0
                    OR NEW.reconnect_count <> 0)
          )
          OR NEW.evidence_key_fingerprint_sha256 IS DISTINCT FROM
                previous_row.evidence_key_fingerprint_sha256 THEN
        RAISE EXCEPTION 'signed bridge event hash chain is invalid'
            USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER t4_bridge_observation_event_integrity_guard
BEFORE INSERT ON crypto_agent.t4_bridge_observation_events
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_t4_bridge_observation_event();

CREATE OR REPLACE FUNCTION crypto_agent.record_verified_t4_bridge_observation_event(
    p_event_id uuid,
    p_campaign_id uuid,
    p_sequence_no bigint,
    p_event_at timestamptz,
    p_event_type text,
    p_reason_code text,
    p_boot_id uuid,
    p_session_generation bigint,
    p_reconnect_count integer,
    p_scenario_code text,
    p_action_request_id uuid,
    p_previous_event_hash crypto_agent.sha256_hex,
    p_payload_hash_sha256 crypto_agent.sha256_hex,
    p_event_hash_sha256 crypto_agent.sha256_hex,
    p_canonical_payload_base64 text,
    p_evidence_key_fingerprint_sha256 crypto_agent.sha256_hex,
    p_signature_base64 text
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    result bigint;
    existing crypto_agent.t4_bridge_observation_events%ROWTYPE;
    verified_at timestamptz := clock_timestamp();
BEGIN
    IF NOT crypto_agent.t4_evidence_verifier_session_is_safe() THEN
        RAISE EXCEPTION 'signed T4 event recording requires the verifier role'
            USING ERRCODE = '42501';
    END IF;
    IF p_campaign_id IS NOT NULL THEN
        PERFORM pg_advisory_xact_lock(hashtextextended(
            't4-observation-campaign:' || p_campaign_id::text, 0
        ));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-bridge-evidence:' || p_evidence_key_fingerprint_sha256, 0
    ));
    SELECT * INTO existing
    FROM crypto_agent.t4_bridge_observation_events event
    WHERE event.event_id = p_event_id
       OR event.event_hash_sha256 = p_event_hash_sha256
       OR (event.evidence_key_fingerprint_sha256 =
                p_evidence_key_fingerprint_sha256
           AND event.sequence_no = p_sequence_no)
    LIMIT 1;
    IF FOUND THEN
        IF existing.event_id IS DISTINCT FROM p_event_id
           OR existing.campaign_id IS DISTINCT FROM p_campaign_id
           OR existing.sequence_no IS DISTINCT FROM p_sequence_no
           OR existing.event_at IS DISTINCT FROM p_event_at
           OR existing.event_type IS DISTINCT FROM p_event_type
           OR existing.reason_code IS DISTINCT FROM p_reason_code
           OR existing.boot_id IS DISTINCT FROM p_boot_id
           OR existing.session_generation IS DISTINCT FROM p_session_generation
           OR existing.reconnect_count IS DISTINCT FROM p_reconnect_count
           OR existing.scenario_code IS DISTINCT FROM p_scenario_code
           OR existing.action_request_id IS DISTINCT FROM p_action_request_id
           OR existing.previous_event_hash IS DISTINCT FROM p_previous_event_hash
           OR existing.payload_hash_sha256 IS DISTINCT FROM p_payload_hash_sha256
           OR existing.event_hash_sha256 IS DISTINCT FROM p_event_hash_sha256
           OR existing.canonical_payload_base64 IS DISTINCT FROM
                p_canonical_payload_base64
           OR existing.evidence_key_fingerprint_sha256 IS DISTINCT FROM
                p_evidence_key_fingerprint_sha256
           OR existing.signature_base64 IS DISTINCT FROM p_signature_base64 THEN
            RAISE EXCEPTION 'signed T4 event idempotency conflict'
                USING ERRCODE = '23505';
        END IF;
        RETURN existing.bridge_observation_event_id;
    END IF;
    INSERT INTO crypto_agent.t4_bridge_observation_events (
        event_id, campaign_id, sequence_no, event_at, event_type, reason_code,
        boot_id, session_generation, reconnect_count, environment,
        bridge_schema_version, read_only, order_routes_exposed, scenario_code,
        action_request_id, previous_event_hash, payload_hash_sha256,
        event_hash_sha256, canonical_payload_base64, signature_algorithm, payload,
        evidence_key_fingerprint_sha256, signature_base64,
        signature_verified, signature_verified_at, signature_verified_by, received_at
    ) VALUES (
        p_event_id, p_campaign_id, p_sequence_no, p_event_at, p_event_type,
        p_reason_code, p_boot_id, p_session_generation, p_reconnect_count,
        'live_t4', 5, TRUE, FALSE, p_scenario_code, p_action_request_id,
        p_previous_event_hash, p_payload_hash_sha256, p_event_hash_sha256,
        p_canonical_payload_base64, 'ecdsa-p256-sha256-der', '{}'::jsonb,
        p_evidence_key_fingerprint_sha256, p_signature_base64, TRUE,
        verified_at, session_user, verified_at
    ) RETURNING bridge_observation_event_id INTO result;
    RETURN result;
END;
$$;

ALTER FUNCTION crypto_agent.record_verified_t4_bridge_observation_event(
    uuid, uuid, bigint, timestamptz, text, text, uuid, bigint, integer,
    text, uuid, crypto_agent.sha256_hex, crypto_agent.sha256_hex,
    crypto_agent.sha256_hex, text, crypto_agent.sha256_hex, text
) OWNER TO crypto_agent_evidence_owner;
REVOKE ALL ON FUNCTION crypto_agent.record_verified_t4_bridge_observation_event(
    uuid, uuid, bigint, timestamptz, text, text, uuid, bigint, integer,
    text, uuid, crypto_agent.sha256_hex, crypto_agent.sha256_hex,
    crypto_agent.sha256_hex, text, crypto_agent.sha256_hex, text
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION crypto_agent.record_verified_t4_bridge_observation_event(
    uuid, uuid, bigint, timestamptz, text, text, uuid, bigint, integer,
    text, uuid, crypto_agent.sha256_hex, crypto_agent.sha256_hex,
    crypto_agent.sha256_hex, text, crypto_agent.sha256_hex, text
) TO crypto_agent_evidence_verifier;

CREATE OR REPLACE FUNCTION crypto_agent.record_verified_t4_replay_attestation(
    p_attestation_id uuid,
    p_campaign_id uuid,
    p_scope_key text,
    p_replay_as_of timestamptz,
    p_replay_fingerprint_sha256 crypto_agent.sha256_hex,
    p_source_batch_ids bigint[],
    p_source_batch_hashes crypto_agent.sha256_hex[],
    p_provenance_sha256 crypto_agent.sha256_hex
)
RETURNS TABLE (
    replay_attestation_id bigint,
    content_hash crypto_agent.sha256_hex
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    existing crypto_agent.t4_replay_verifier_attestations%ROWTYPE;
    expected_batch_id bigint;
    expected_batch_hash crypto_agent.sha256_hex;
    expected_provenance crypto_agent.sha256_hex;
    derived_content_hash crypto_agent.sha256_hex;
    attested timestamptz := clock_timestamp();
BEGIN
    IF NOT crypto_agent.t4_evidence_verifier_session_is_safe() THEN
        RAISE EXCEPTION 'T4 replay attestation requires the isolated verifier login'
            USING ERRCODE = '42501';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation-campaign:' || p_campaign_id::text, 0
    ));
    SELECT * INTO campaign_row
    FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = p_campaign_id;
    IF NOT FOUND
       OR campaign_row.environment <> 'live_t4'
       OR NOT campaign_row.read_only OR campaign_row.execution_enabled
       OR NOT (campaign_row.scope_manifest ? p_scope_key)
       OR p_replay_as_of IS DISTINCT FROM campaign_row.planned_ends_at
       OR attested < campaign_row.planned_ends_at
       OR EXISTS (
            SELECT 1 FROM crypto_agent.t4_observation_quality_reports report
            WHERE report.campaign_id = p_campaign_id
       ) THEN
        RAISE EXCEPTION 'replay attestation does not match an open frozen campaign'
            USING ERRCODE = '22023';
    END IF;
    IF cardinality(p_source_batch_ids) <> 1
       OR cardinality(p_source_batch_hashes) <> 1
       OR p_source_batch_ids[1] <= 0 THEN
        RAISE EXCEPTION 'replay attestation source provenance is invalid'
            USING ERRCODE = '22023';
    END IF;

    SELECT candidate.t4_batch_id, candidate.raw_payload_hash
    INTO expected_batch_id, expected_batch_hash
    FROM crypto_agent.t4_ingestion_batches candidate
    WHERE candidate.logical_symbol = split_part(p_scope_key, ':', 1)
      AND candidate.interval_seconds =
            rtrim(split_part(p_scope_key, ':', 2), 'm')::integer * 60
      AND candidate.requested_as_of <= p_replay_as_of
      AND candidate.available_at <= p_replay_as_of
      AND candidate.ingested_at <= p_replay_as_of
      AND candidate.record_count >= 120
      AND candidate.status = 'completed'
      AND (
            SELECT count(*)
            FROM crypto_agent.t4_canonical_candles eligible
            WHERE eligible.t4_batch_id = candidate.t4_batch_id
              AND eligible.close_time <= p_replay_as_of
              AND eligible.available_at <= p_replay_as_of
              AND eligible.provider_ingested_at <= p_replay_as_of
      ) >= 120
    ORDER BY candidate.available_at DESC,
             candidate.ingested_at DESC,
             candidate.t4_batch_id DESC
    LIMIT 1;
    expected_provenance := encode(public.digest(convert_to(
        't4-replay-provenance-v1' || E'\n'
        || p_source_batch_ids[1]::text || ':'
        || lower(p_source_batch_hashes[1]) || E'\n',
        'UTF8'), 'sha256'), 'hex');
    IF expected_batch_id IS NULL
       OR p_source_batch_ids[1] IS DISTINCT FROM expected_batch_id
       OR p_source_batch_hashes[1] IS DISTINCT FROM expected_batch_hash
       OR p_provenance_sha256 IS DISTINCT FROM expected_provenance THEN
        RAISE EXCEPTION 'replay attestation does not match the frozen source batch'
            USING ERRCODE = '22023';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM crypto_agent.t4_ingestion_batches selected_batch
        WHERE selected_batch.t4_batch_id = expected_batch_id
          AND selected_batch.environment = 'live_t4'
          AND selected_batch.bridge_schema_version = 5
    ) THEN
        RAISE EXCEPTION 'replay attestation requires live T4 schema-v5 inputs'
            USING ERRCODE = '22023';
    END IF;

    derived_content_hash := encode(public.digest(convert_to(
        replace(jsonb_build_object(
            'attestation_id', p_attestation_id::text,
            'campaign_id', p_campaign_id::text,
            'external_delivery_eligible', false,
            'provenance_sha256', p_provenance_sha256::text,
            'read_only', true,
            'replay_as_of', p_replay_as_of::text,
            'replay_fingerprint_sha256', p_replay_fingerprint_sha256::text,
            'replay_limit', 120,
            'scope_key', p_scope_key,
            'source_batch_hashes', p_source_batch_hashes,
            'source_batch_ids', p_source_batch_ids
        )::text, ', ', ','), 'UTF8'), 'sha256'), 'hex');

    SELECT * INTO existing
    FROM crypto_agent.t4_replay_verifier_attestations attestation
    WHERE attestation.attestation_id = p_attestation_id
       OR (attestation.campaign_id = p_campaign_id
           AND attestation.scope_key = p_scope_key)
    LIMIT 1;
    IF FOUND THEN
        IF existing.attestation_id IS DISTINCT FROM p_attestation_id
           OR existing.campaign_id IS DISTINCT FROM p_campaign_id
           OR existing.scope_key IS DISTINCT FROM p_scope_key
           OR existing.replay_as_of IS DISTINCT FROM p_replay_as_of
           OR existing.replay_limit IS DISTINCT FROM 120
           OR existing.replay_fingerprint_sha256 IS DISTINCT FROM
                p_replay_fingerprint_sha256
           OR existing.source_batch_ids IS DISTINCT FROM p_source_batch_ids
           OR existing.source_batch_hashes IS DISTINCT FROM p_source_batch_hashes
           OR existing.provenance_sha256 IS DISTINCT FROM p_provenance_sha256
           OR existing.content_hash IS DISTINCT FROM derived_content_hash THEN
            RAISE EXCEPTION 'replay attestation idempotency conflict'
                USING ERRCODE = '23505';
        END IF;
        RETURN QUERY SELECT existing.replay_attestation_id,
                            existing.content_hash;
        RETURN;
    END IF;

    RETURN QUERY
    INSERT INTO crypto_agent.t4_replay_verifier_attestations (
        attestation_id, campaign_id, scope_key, replay_as_of,
        replay_limit, replay_fingerprint_sha256, source_batch_ids,
        source_batch_hashes, provenance_sha256, read_only,
        execution_enabled, external_delivery_eligible, attested_at,
        attested_by, content_hash
    ) VALUES (
        p_attestation_id, p_campaign_id, p_scope_key, p_replay_as_of,
        120, p_replay_fingerprint_sha256, p_source_batch_ids,
        p_source_batch_hashes, p_provenance_sha256, TRUE,
        FALSE, FALSE, attested, session_user, derived_content_hash
    )
    RETURNING t4_replay_verifier_attestations.replay_attestation_id,
              t4_replay_verifier_attestations.content_hash;
END;
$$;

ALTER FUNCTION crypto_agent.record_verified_t4_replay_attestation(
    uuid, uuid, text, timestamptz, crypto_agent.sha256_hex,
    bigint[], crypto_agent.sha256_hex[], crypto_agent.sha256_hex
) OWNER TO crypto_agent_evidence_owner;
REVOKE ALL ON FUNCTION crypto_agent.record_verified_t4_replay_attestation(
    uuid, uuid, text, timestamptz, crypto_agent.sha256_hex,
    bigint[], crypto_agent.sha256_hex[], crypto_agent.sha256_hex
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION crypto_agent.record_verified_t4_replay_attestation(
    uuid, uuid, text, timestamptz, crypto_agent.sha256_hex,
    bigint[], crypto_agent.sha256_hex[], crypto_agent.sha256_hex
) TO crypto_agent_evidence_verifier;

CREATE OR REPLACE FUNCTION crypto_agent.begin_t4_observation_scenario_trial(
    p_campaign_id uuid,
    p_scenario_code text,
    p_action_request_id uuid,
    p_scope_key text
)
RETURNS TABLE (trial_id uuid, started_at timestamptz)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    existing_request crypto_agent.t4_observation_scenario_trial_requests%ROWTYPE;
    result_id uuid;
    result_started_at timestamptz;
    result_hash crypto_agent.sha256_hex;
BEGIN
    IF NOT crypto_agent.t4_evidence_verifier_session_is_safe() THEN
        RAISE EXCEPTION 'scenario trial start requires a separate verifier login'
            USING ERRCODE = '42501';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation-campaign:' || p_campaign_id::text, 0
    ));
    SELECT * INTO existing_request
    FROM crypto_agent.t4_observation_scenario_trial_requests request
    WHERE request.campaign_id = p_campaign_id
      AND request.action_request_id = p_action_request_id;
    IF FOUND THEN
        IF existing_request.scenario_code IS DISTINCT FROM p_scenario_code
           OR existing_request.scope_key IS DISTINCT FROM p_scope_key THEN
            RAISE EXCEPTION 'scenario action identifier collision'
                USING ERRCODE = '23505';
        END IF;
        RETURN QUERY SELECT existing_request.trial_id,
                            existing_request.started_at;
        RETURN;
    END IF;
    SELECT * INTO campaign_row
    FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = p_campaign_id;
    IF NOT FOUND
       OR p_scenario_code NOT IN (
            'bridge_restart', 'missing_data', 'rate_limit', 'reconnect', 'stale_data'
       )
       OR p_action_request_id IS NULL
       OR NOT (campaign_row.scope_manifest ? p_scope_key)
       OR clock_timestamp() NOT BETWEEN campaign_row.started_at
                                    AND campaign_row.planned_ends_at
       OR campaign_row.bridge_evidence_key_fingerprint IS NULL
       OR NOT EXISTS (
            SELECT 1
            FROM crypto_agent.t4_observation_cycles pre_cycle
            JOIN crypto_agent.t4_ingestion_batches pre_batch
              ON pre_batch.t4_batch_id = pre_cycle.t4_batch_id
            WHERE pre_cycle.campaign_id = p_campaign_id
              AND pre_cycle.scope_key = p_scope_key
              AND pre_cycle.outcome = 'success'
              AND pre_cycle.finished_at <= clock_timestamp()
              AND pre_cycle.finished_at >= clock_timestamp()
                    - make_interval(
                        secs => campaign_row.cycle_interval_seconds * 2
                      )
              AND pre_cycle.expected_at = (
                    SELECT max(latest_cycle.expected_at)
                    FROM crypto_agent.t4_observation_cycles latest_cycle
                    WHERE latest_cycle.campaign_id = p_campaign_id
                      AND latest_cycle.scope_key = p_scope_key
                      AND latest_cycle.expected_at < clock_timestamp()
              )
              AND pre_batch.environment = 'live_t4'
              AND pre_batch.bridge_schema_version = 5
              AND pre_batch.logical_symbol || ':'
                    || (pre_batch.interval_seconds / 60)::text || 'm'
                    = p_scope_key
              AND pre_batch.ingested_at <= clock_timestamp()
              AND pre_batch.ingested_at >= clock_timestamp()
                    - make_interval(
                        secs => campaign_row.cycle_interval_seconds * 2
                      )
       )
       OR EXISTS (
            SELECT 1 FROM crypto_agent.t4_observation_quality_reports report
            WHERE report.campaign_id = p_campaign_id
       ) THEN
        RAISE EXCEPTION 'controlled scenario trial request is invalid'
            USING ERRCODE = '22023';
    END IF;
    result_started_at := clock_timestamp();
    result_id := pg_catalog.gen_random_uuid();
    result_hash := encode(public.digest(convert_to(
        '{"action_request_id":' || to_jsonb(p_action_request_id::text)::text
        || ',"campaign_id":' || to_jsonb(p_campaign_id::text)::text
        || ',"scenario_code":' || to_jsonb(p_scenario_code)::text
        || ',"scope_key":' || to_jsonb(p_scope_key)::text
        || ',"started_at":' || to_jsonb(result_started_at::text)::text
        || ',"trial_id":' || to_jsonb(result_id::text)::text || '}',
        'UTF8'), 'sha256'), 'hex');
    INSERT INTO crypto_agent.t4_observation_scenario_trial_requests (
        trial_id, campaign_id, scenario_code, scope_key, action_request_id,
        started_at, content_hash
    ) VALUES (
        result_id, p_campaign_id, p_scenario_code, p_scope_key,
        p_action_request_id, result_started_at, result_hash
    );
    RETURN QUERY SELECT result_id, result_started_at;
END;
$$;

CREATE OR REPLACE FUNCTION crypto_agent.complete_t4_observation_scenario_trial(
    p_trial_id uuid,
    p_event_hashes crypto_agent.sha256_hex[],
    p_control_receipt_event_hash crypto_agent.sha256_hex
)
RETURNS TABLE (
    trial_id uuid,
    campaign_id uuid,
    scenario_code text,
    outcome text,
    completed_at timestamptz,
    content_hash crypto_agent.sha256_hex
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    request_row crypto_agent.t4_observation_scenario_trial_requests%ROWTYPE;
    result_row crypto_agent.t4_observation_scenario_trials%ROWTYPE;
    event_ids uuid[] := '{}'::uuid[];
    event_hashes crypto_agent.sha256_hex[] := '{}'::crypto_agent.sha256_hex[];
    event_boots uuid[] := '{}'::uuid[];
    first_event crypto_agent.t4_bridge_observation_events%ROWTYPE;
    last_event crypto_agent.t4_bridge_observation_events%ROWTYPE;
    receipt_event crypto_agent.t4_bridge_observation_events%ROWTYPE;
    pre_batch_id bigint;
    post_batch_id bigint;
    failure_cycle_id bigint;
    failure_run_id bigint;
    completed timestamptz := clock_timestamp();
    trial_content_hash crypto_agent.sha256_hex;
    derived_outcome text := 'fail';
BEGIN
    IF NOT crypto_agent.t4_evidence_verifier_session_is_safe() THEN
        RAISE EXCEPTION 'scenario trial completion requires a separate verifier login'
            USING ERRCODE = '42501';
    END IF;
    SELECT * INTO request_row
    FROM crypto_agent.t4_observation_scenario_trial_requests request
    WHERE request.trial_id = p_trial_id;
    IF NOT FOUND OR completed < request_row.started_at THEN
        RAISE EXCEPTION 'controlled scenario trial request is missing'
            USING ERRCODE = '22023';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation-campaign:' || request_row.campaign_id::text, 0
    ));
    SELECT * INTO result_row
    FROM crypto_agent.t4_observation_scenario_trials trial
    WHERE trial.trial_id = p_trial_id;
    IF FOUND THEN
        RETURN QUERY SELECT result_row.trial_id, result_row.campaign_id,
            result_row.scenario_code, result_row.outcome,
            result_row.completed_at, result_row.content_hash;
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM crypto_agent.t4_observation_quality_reports report
        WHERE report.campaign_id = request_row.campaign_id
    ) THEN
        RAISE EXCEPTION 'observation campaign is already finalized'
            USING ERRCODE = '55000';
    END IF;
    SELECT array_agg(event.event_id ORDER BY event.sequence_no),
           array_agg(event.event_hash_sha256 ORDER BY event.sequence_no),
           array_agg(DISTINCT event.boot_id ORDER BY event.boot_id)
    INTO event_ids, event_hashes, event_boots
    FROM crypto_agent.t4_bridge_observation_events event
    WHERE event.event_hash_sha256 = ANY(p_event_hashes)
      AND event.event_at BETWEEN request_row.started_at AND completed;
    IF cardinality(event_hashes) = cardinality(p_event_hashes)
       AND cardinality(event_hashes) > 0 THEN
        SELECT * INTO first_event
        FROM crypto_agent.t4_bridge_observation_events event
        WHERE event.event_id = event_ids[1];
        SELECT * INTO last_event
        FROM crypto_agent.t4_bridge_observation_events event
        WHERE event.event_id = event_ids[cardinality(event_ids)];
    END IF;
    SELECT * INTO receipt_event
    FROM crypto_agent.t4_bridge_observation_events event
    WHERE event.event_hash_sha256 = p_control_receipt_event_hash
      AND event.event_id = ANY(event_ids)
      AND event.event_type = 'control_applied'
      AND event.control_step = 'applied'
      AND event.campaign_id = request_row.campaign_id
      AND event.scenario_code = request_row.scenario_code
      AND event.action_request_id = request_row.action_request_id;
    SELECT batch.t4_batch_id INTO pre_batch_id
    FROM crypto_agent.t4_ingestion_batches batch
    JOIN crypto_agent.t4_observation_cycles pre_cycle
      ON pre_cycle.t4_batch_id = batch.t4_batch_id
     AND pre_cycle.campaign_id = request_row.campaign_id
     AND pre_cycle.scope_key = request_row.scope_key
     AND pre_cycle.outcome = 'success'
    JOIN crypto_agent.t4_observation_campaigns campaign
      ON campaign.campaign_id = request_row.campaign_id
    WHERE batch.environment = 'live_t4'
      AND batch.bridge_schema_version = 5
      AND batch.logical_symbol || ':'
            || (batch.interval_seconds / 60)::text || 'm' = request_row.scope_key
      AND batch.ingested_at <= request_row.started_at
      AND batch.ingested_at >= request_row.started_at
            - make_interval(secs => campaign.cycle_interval_seconds * 2)
      AND pre_cycle.finished_at <= request_row.started_at
      AND pre_cycle.finished_at >= request_row.started_at
            - make_interval(secs => campaign.cycle_interval_seconds * 2)
      AND pre_cycle.expected_at = (
            SELECT max(latest_cycle.expected_at)
            FROM crypto_agent.t4_observation_cycles latest_cycle
            WHERE latest_cycle.campaign_id = request_row.campaign_id
              AND latest_cycle.scope_key = request_row.scope_key
              AND latest_cycle.expected_at < request_row.started_at
      )
    ORDER BY batch.ingested_at DESC LIMIT 1;
    SELECT batch.t4_batch_id INTO post_batch_id
    FROM crypto_agent.t4_ingestion_batches batch
    WHERE batch.environment = 'live_t4'
      AND batch.bridge_schema_version = 5
      AND batch.logical_symbol || ':'
            || (batch.interval_seconds / 60)::text || 'm' = request_row.scope_key
      AND batch.ingested_at >= last_event.event_at
      AND batch.ingested_at <= completed
    ORDER BY batch.ingested_at ASC LIMIT 1;
    IF request_row.scenario_code IN ('missing_data', 'rate_limit', 'stale_data') THEN
        SELECT cycle.observation_cycle_id INTO failure_cycle_id
        FROM crypto_agent.t4_observation_cycles cycle
        WHERE cycle.campaign_id = request_row.campaign_id
          AND cycle.scope_key = request_row.scope_key
          AND cycle.outcome = 'failure'
          AND cycle.error_code = CASE request_row.scenario_code
                WHEN 'missing_data' THEN 'T4_MISSING_DATA'
                WHEN 'rate_limit' THEN 'T4_RATE_LIMITED'
                WHEN 'stale_data' THEN 'T4_STALE_DATA'
              END
          AND cycle.started_at BETWEEN request_row.started_at AND completed
        ORDER BY cycle.started_at DESC LIMIT 1;
        SELECT run.research_run_id INTO failure_run_id
        FROM crypto_agent.research_runs run
        JOIN crypto_agent.t4_observation_cycles cycle
          ON cycle.trace_id::text = run.trace_key
        JOIN crypto_agent.research_run_events event
          ON event.research_run_id = run.research_run_id
        WHERE cycle.observation_cycle_id = failure_cycle_id
          AND run.run_kind = 'live_t4_analysis'
          AND event.event_type = 'failed'
          AND event.details->>'error_code' = CASE request_row.scenario_code
                WHEN 'missing_data' THEN 'T4_MISSING_DATA'
                WHEN 'rate_limit' THEN 'T4_RATE_LIMITED'
                WHEN 'stale_data' THEN 'T4_STALE_DATA'
              END
        LIMIT 1;
    END IF;
    IF receipt_event.event_id IS NOT NULL
       AND pre_batch_id IS NOT NULL AND post_batch_id IS NOT NULL
       AND (request_row.scenario_code NOT IN (
                'missing_data', 'rate_limit', 'stale_data'
           ) OR (failure_cycle_id IS NOT NULL AND failure_run_id IS NOT NULL)) THEN
        derived_outcome := 'pass';
    END IF;
    IF derived_outcome <> 'pass' THEN
        RAISE EXCEPTION 'controlled scenario evidence is not complete yet'
            USING ERRCODE = '55000';
    END IF;
    trial_content_hash := encode(public.digest(convert_to(
        '{"campaign_id":' || to_jsonb(request_row.campaign_id::text)::text
        || ',"completed_at":' || to_jsonb(completed::text)::text
        || ',"evidence_event_hashes":'
        || coalesce(to_jsonb(event_hashes)::text, '[]')
        || ',"outcome":' || to_jsonb(derived_outcome)::text
        || ',"scenario_code":' || to_jsonb(request_row.scenario_code)::text
        || ',"trial_id":' || to_jsonb(request_row.trial_id::text)::text || '}',
        'UTF8'), 'sha256'), 'hex');
    INSERT INTO crypto_agent.t4_observation_scenario_trials (
        trial_id, campaign_id, scenario_code, outcome, started_at, completed_at,
        action_request_id, bridge_boot_id, first_event_id, last_event_id,
        evidence_event_ids, evidence_event_hashes, pre_t4_batch_id,
        post_t4_batch_id, observation_cycle_id, research_run_id, detail_code,
        content_hash
    ) VALUES (
        request_row.trial_id, request_row.campaign_id, request_row.scenario_code,
        derived_outcome, request_row.started_at, completed,
        request_row.action_request_id,
        CASE WHEN cardinality(event_boots) = 1 THEN event_boots[1] ELSE NULL END,
        first_event.event_id, last_event.event_id,
        coalesce(event_ids, '{}'::uuid[]),
        coalesce(event_hashes, '{}'::crypto_agent.sha256_hex[]),
        pre_batch_id, post_batch_id, failure_cycle_id, failure_run_id,
        CASE WHEN derived_outcome = 'pass' THEN 'DB_EVIDENCE_VERIFIED'
             ELSE 'DB_EVIDENCE_INCOMPLETE' END,
        trial_content_hash
    ) RETURNING * INTO result_row;
    RETURN QUERY SELECT result_row.trial_id, result_row.campaign_id,
        result_row.scenario_code, result_row.outcome,
        result_row.completed_at, result_row.content_hash;
END;
$$;

CREATE OR REPLACE FUNCTION crypto_agent.verify_passive_t4_observation_scenario(
    p_campaign_id uuid,
    p_scenario_code text
)
RETURNS TABLE (
    trial_id uuid,
    campaign_id uuid,
    scenario_code text,
    outcome text,
    completed_at timestamptz,
    content_hash crypto_agent.sha256_hex
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    event_row crypto_agent.t4_bridge_observation_events%ROWTYPE;
    replay_attestation_ids bigint[] := '{}'::bigint[];
    batch_id bigint;
    completed timestamptz := clock_timestamp();
    result_id uuid := pg_catalog.gen_random_uuid();
    derived_outcome text := 'fail';
    result_hash crypto_agent.sha256_hex;
    result_row crypto_agent.t4_observation_scenario_trials%ROWTYPE;
BEGIN
    IF NOT crypto_agent.t4_evidence_verifier_session_is_safe() THEN
        RAISE EXCEPTION 'passive scenario verification requires a separate verifier login'
            USING ERRCODE = '42501';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation-campaign:' || p_campaign_id::text, 0
    ));
    SELECT * INTO result_row
    FROM crypto_agent.t4_observation_scenario_trials trial
    WHERE trial.campaign_id = p_campaign_id
      AND trial.scenario_code = p_scenario_code
      AND trial.outcome = 'pass'
    ORDER BY trial.completed_at DESC LIMIT 1;
    IF FOUND THEN
        IF p_scenario_code = 'replay_blocked'
           AND NOT crypto_agent.t4_replay_attestations_are_verified(
                p_campaign_id, result_row.replay_attestation_ids
           ) THEN
            RAISE EXCEPTION 'stored replay trial is no longer verified'
                USING ERRCODE = '55000';
        END IF;
        RETURN QUERY SELECT result_row.trial_id, result_row.campaign_id,
            result_row.scenario_code, result_row.outcome,
            result_row.completed_at, result_row.content_hash;
        RETURN;
    END IF;
    SELECT * INTO campaign_row FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = p_campaign_id;
    IF NOT FOUND OR p_scenario_code NOT IN ('replay_blocked', 'roll_transition')
       OR (p_scenario_code = 'roll_transition'
           AND completed NOT BETWEEN campaign_row.started_at
                                 AND campaign_row.planned_ends_at)
       OR (p_scenario_code = 'replay_blocked'
           AND completed < campaign_row.planned_ends_at) THEN
        RAISE EXCEPTION 'passive scenario request is invalid'
            USING ERRCODE = '22023';
    END IF;
    IF p_scenario_code = 'roll_transition' THEN
        SELECT * INTO event_row
        FROM crypto_agent.t4_bridge_observation_events event
        WHERE event.campaign_id = p_campaign_id
          AND event.scenario_code = 'roll_transition'
          AND event.event_type = 'contract_roll_observed'
          AND event.event_at BETWEEN campaign_row.started_at AND completed
        ORDER BY event.event_at DESC LIMIT 1;
        SELECT batch.t4_batch_id INTO batch_id
        FROM crypto_agent.t4_ingestion_batches batch
        JOIN crypto_agent.t4_observation_cycles cycle
          ON cycle.t4_batch_id = batch.t4_batch_id
         AND cycle.campaign_id = p_campaign_id
         AND cycle.outcome = 'success'
        JOIN crypto_agent.t4_contract_transition_evidence transition
          ON transition.t4_batch_id = batch.t4_batch_id
        WHERE batch.environment = 'live_t4'
          AND batch.bridge_schema_version = 5
          AND transition.from_market_id = event_row.roll_from_market_id
          AND transition.to_market_id = event_row.roll_to_market_id
          AND batch.logical_symbol || ':'
                || (batch.interval_seconds / 60)::text || 'm' = event_row.scope_key
          AND cycle.scope_key = event_row.scope_key
          AND cycle.started_at <= event_row.event_at
          AND event_row.event_at <= batch.ingested_at
          AND batch.ingested_at <= cycle.finished_at
          AND cycle.finished_at <= completed
        ORDER BY batch.ingested_at DESC LIMIT 1;
        IF event_row.event_id IS NOT NULL AND batch_id IS NOT NULL THEN
            derived_outcome := 'pass';
        END IF;
    ELSE
        SELECT coalesce(
            array_agg(attestation.replay_attestation_id
                      ORDER BY attestation.scope_key),
            '{}'::bigint[]
        ) INTO replay_attestation_ids
        FROM crypto_agent.t4_replay_verifier_attestations attestation
        WHERE attestation.campaign_id = p_campaign_id;
        IF crypto_agent.t4_replay_attestations_are_verified(
            p_campaign_id, replay_attestation_ids
        ) THEN
            derived_outcome := 'pass';
        END IF;
    END IF;
    IF derived_outcome <> 'pass' THEN
        RAISE EXCEPTION 'passive scenario evidence is not complete yet'
            USING ERRCODE = '55000';
    END IF;
    result_hash := encode(public.digest(convert_to(
        '{"campaign_id":' || to_jsonb(p_campaign_id::text)::text
        || ',"completed_at":' || to_jsonb(completed::text)::text
        || ',"replay_attestation_ids":'
        || replace(to_jsonb(replay_attestation_ids)::text, ', ', ',')
        || ',"outcome":' || to_jsonb(derived_outcome)::text
        || ',"scenario_code":' || to_jsonb(p_scenario_code)::text
        || ',"trial_id":' || to_jsonb(result_id::text)::text || '}',
        'UTF8'), 'sha256'), 'hex');
    INSERT INTO crypto_agent.t4_observation_scenario_trials (
        trial_id, campaign_id, scenario_code, outcome, started_at, completed_at,
        first_event_id, last_event_id, evidence_event_ids, evidence_event_hashes,
        post_t4_batch_id, research_run_id, replay_fingerprint_sha256,
        replay_attestation_ids,
        detail_code, content_hash
    ) VALUES (
        result_id, p_campaign_id, p_scenario_code, derived_outcome,
        campaign_row.started_at, completed,
        event_row.event_id, event_row.event_id,
        CASE WHEN event_row.event_id IS NULL THEN '{}'::uuid[]
             ELSE ARRAY[event_row.event_id] END,
        CASE WHEN event_row.event_hash_sha256 IS NULL
             THEN '{}'::crypto_agent.sha256_hex[]
             ELSE ARRAY[event_row.event_hash_sha256]::crypto_agent.sha256_hex[] END,
        batch_id, NULL, NULL, replay_attestation_ids,
        CASE WHEN derived_outcome = 'pass' THEN 'DB_EVIDENCE_VERIFIED'
             ELSE 'DB_EVIDENCE_INCOMPLETE' END,
        result_hash
    ) RETURNING * INTO result_row;
    RETURN QUERY SELECT result_row.trial_id, result_row.campaign_id,
        result_row.scenario_code, result_row.outcome,
        result_row.completed_at, result_row.content_hash;
END;
$$;

ALTER TABLE crypto_agent.t4_bridge_evidence_keys
    OWNER TO crypto_agent_evidence_owner;
ALTER TABLE crypto_agent.t4_bridge_observation_events
    OWNER TO crypto_agent_evidence_owner;
ALTER TABLE crypto_agent.t4_replay_verifier_attestations
    OWNER TO crypto_agent_evidence_owner;
ALTER TABLE crypto_agent.t4_observation_scenario_trial_requests
    OWNER TO crypto_agent_evidence_owner;
ALTER TABLE crypto_agent.t4_observation_scenario_trials
    OWNER TO crypto_agent_evidence_owner;

GRANT USAGE ON SCHEMA crypto_agent TO crypto_agent_evidence_verifier;
GRANT USAGE ON SCHEMA crypto_agent TO crypto_agent_evidence_reader;
GRANT SELECT ON crypto_agent.t4_bridge_evidence_keys,
    crypto_agent.t4_bridge_observation_events,
    crypto_agent.t4_replay_verifier_attestations,
    crypto_agent.t4_observation_scenario_trial_requests,
    crypto_agent.t4_observation_scenario_trials
    TO crypto_agent_evidence_reader;
-- The isolated replay verifier reads the same frozen point-in-time inputs as
-- T4ReplayProvider.  It receives no DML on these or any protected ledger.
GRANT SELECT ON crypto_agent.t4_observation_campaigns,
    crypto_agent.t4_observation_quality_reports,
    crypto_agent.t4_ingestion_batches,
    crypto_agent.t4_canonical_candles,
    crypto_agent.t4_futures_snapshots,
    crypto_agent.t4_orderbook_levels,
    crypto_agent.t4_contract_transition_evidence
    TO crypto_agent_evidence_reader;
GRANT SELECT ON crypto_agent.t4_observation_campaigns,
    crypto_agent.t4_observation_quality_reports,
    crypto_agent.t4_ingestion_batches,
    crypto_agent.t4_observation_cycles,
    crypto_agent.t4_observation_session_events,
    crypto_agent.research_runs,
    crypto_agent.research_run_events,
    crypto_agent.research_run_inputs,
    crypto_agent.research_artifacts,
    crypto_agent.alerts,
    crypto_agent.alert_delivery_outbox,
    crypto_agent.alert_delivery_attempts,
    crypto_agent.t4_contract_transition_evidence,
    crypto_agent.t4_canonical_candles
    TO crypto_agent_evidence_owner;

REVOKE ALL ON crypto_agent.t4_bridge_evidence_keys,
    crypto_agent.t4_bridge_observation_events,
    crypto_agent.t4_replay_verifier_attestations,
    crypto_agent.t4_observation_scenario_trial_requests,
    crypto_agent.t4_observation_scenario_trials
    FROM PUBLIC, crypto_agent_evidence_verifier;

GRANT SELECT, INSERT ON crypto_agent.t4_replay_verifier_attestations
    TO crypto_agent_evidence_owner;
GRANT USAGE, SELECT ON SEQUENCE
    crypto_agent.t4_replay_verifier_attestations_replay_attestation_id_seq
    TO crypto_agent_evidence_owner;
GRANT SELECT ON crypto_agent.t4_futures_snapshots,
    crypto_agent.t4_orderbook_levels
    TO crypto_agent_evidence_owner;

ALTER FUNCTION crypto_agent.t4_evidence_verifier_session_is_safe()
    OWNER TO crypto_agent_evidence_owner;
ALTER FUNCTION crypto_agent.t4_replay_attestations_are_verified(uuid, bigint[])
    OWNER TO crypto_agent_evidence_owner;
REVOKE ALL ON FUNCTION crypto_agent.t4_evidence_verifier_session_is_safe()
    FROM PUBLIC;
REVOKE ALL ON FUNCTION crypto_agent.t4_replay_attestations_are_verified(
    uuid, bigint[]
) FROM PUBLIC;

ALTER FUNCTION crypto_agent.begin_t4_observation_scenario_trial(
    uuid, text, uuid, text
) OWNER TO crypto_agent_evidence_owner;
ALTER FUNCTION crypto_agent.complete_t4_observation_scenario_trial(
    uuid, crypto_agent.sha256_hex[], crypto_agent.sha256_hex
) OWNER TO crypto_agent_evidence_owner;
ALTER FUNCTION crypto_agent.verify_passive_t4_observation_scenario(uuid, text)
    OWNER TO crypto_agent_evidence_owner;

REVOKE ALL ON FUNCTION crypto_agent.begin_t4_observation_scenario_trial(
    uuid, text, uuid, text
) FROM PUBLIC;
REVOKE ALL ON FUNCTION crypto_agent.complete_t4_observation_scenario_trial(
    uuid, crypto_agent.sha256_hex[], crypto_agent.sha256_hex
) FROM PUBLIC;
REVOKE ALL ON FUNCTION crypto_agent.verify_passive_t4_observation_scenario(
    uuid, text
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION crypto_agent.begin_t4_observation_scenario_trial(
    uuid, text, uuid, text
) TO crypto_agent_evidence_verifier;
GRANT EXECUTE ON FUNCTION crypto_agent.complete_t4_observation_scenario_trial(
    uuid, crypto_agent.sha256_hex[], crypto_agent.sha256_hex
) TO crypto_agent_evidence_verifier;
GRANT EXECUTE ON FUNCTION crypto_agent.verify_passive_t4_observation_scenario(
    uuid, text
) TO crypto_agent_evidence_verifier;

-- Serialize scheduled ledger writes with scenario completion and finalization.
-- This avoids relying on UPDATE privileges for immutable campaign rows.
CREATE OR REPLACE FUNCTION crypto_agent.serialize_t4_observation_campaign_write()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, crypto_agent
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation-campaign:' || NEW.campaign_id::text, 0
    ));
    RETURN NEW;
END;
$$;

CREATE TRIGGER t4_observation_campaign_serialization_input_guard
BEFORE INSERT ON crypto_agent.t4_observation_research_inputs
FOR EACH ROW EXECUTE FUNCTION
    crypto_agent.serialize_t4_observation_campaign_write();

CREATE TRIGGER t4_observation_campaign_serialization_cycle_guard
BEFORE INSERT ON crypto_agent.t4_observation_cycles
FOR EACH ROW EXECUTE FUNCTION
    crypto_agent.serialize_t4_observation_campaign_write();

CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_observation_research_input()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    ingestion_row crypto_agent.t4_ingestion_batches%ROWTYPE;
    research_row crypto_agent.research_runs%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation-campaign:' || NEW.campaign_id::text, 0
    ));
    SELECT * INTO campaign_row
    FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = NEW.campaign_id;
    IF NOT FOUND
       OR EXISTS (
            SELECT 1
            FROM crypto_agent.t4_observation_quality_reports report
            WHERE report.campaign_id = NEW.campaign_id
       )
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
            SELECT 1 FROM crypto_agent.research_run_events event
            WHERE event.research_run_id = NEW.research_run_id
              AND event.event_type = 'completed'
       )
       OR NOT EXISTS (
            SELECT 1 FROM crypto_agent.research_artifacts artifact
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

-- Keep the complete 0017 cycle semantics, but replace its row lock with the
-- campaign advisory lock used by the final-report and scenario paths.
CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_observation_cycle_chain()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    previous_row crypto_agent.t4_observation_cycles%ROWTYPE;
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    ingestion_row crypto_agent.t4_ingestion_batches%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation-campaign:' || NEW.campaign_id::text, 0
    ));
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation:' || NEW.campaign_id::text || ':' || NEW.scope_key, 0
    ));
    SELECT * INTO campaign_row FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = NEW.campaign_id;
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
    ORDER BY sequence_no DESC LIMIT 1;
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
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    previous_row crypto_agent.t4_observation_session_events%ROWTYPE;
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation-campaign:' || NEW.campaign_id::text, 0
    ));
    SELECT * INTO campaign_row FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = NEW.campaign_id;
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

-- The column stays nullable so pre-0018 campaign rows remain readable, but every
-- campaign created by the current runtime must freeze an independently
-- registered bridge key.  Historical campaigns are consequently gate-ineligible.
CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_observation_campaign_start()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    started_iso text;
    ends_iso text;
    expected_baseline crypto_agent.sha256_hex;
    expected_content crypto_agent.sha256_hex;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation-campaign:' || NEW.campaign_id::text, 0
    ));
    IF NEW.started_at > clock_timestamp()
       OR NEW.started_at < clock_timestamp() - interval '5 minutes' THEN
        RAISE EXCEPTION 'observation campaign start must use the current database clock'
            USING ERRCODE = '22023';
    END IF;
    IF NEW.bridge_evidence_key_fingerprint IS NULL
       OR NOT EXISTS (
            SELECT 1
            FROM crypto_agent.t4_bridge_evidence_keys evidence_key
            WHERE evidence_key.evidence_key_fingerprint =
                    NEW.bridge_evidence_key_fingerprint
              AND evidence_key.retired_at IS NULL
       ) THEN
        RAISE EXCEPTION 'observation campaign requires a registered bridge evidence key'
            USING ERRCODE = '22023';
    END IF;
    started_iso := to_char(NEW.started_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS')
        || CASE WHEN extract(microseconds FROM NEW.started_at)::integer % 1000000 = 0
             THEN ''
             ELSE to_char(NEW.started_at AT TIME ZONE 'UTC', '.US')
           END || 'Z';
    ends_iso := to_char(NEW.planned_ends_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS')
        || CASE WHEN extract(microseconds FROM NEW.planned_ends_at)::integer % 1000000 = 0
             THEN ''
             ELSE to_char(NEW.planned_ends_at AT TIME ZONE 'UTC', '.US')
           END || 'Z';
    expected_baseline := encode(public.digest(convert_to(
        '{"bridge_evidence_key_fingerprint":'
        || to_jsonb(NEW.bridge_evidence_key_fingerprint::text)::text
        || ',"campaign_id":' || to_jsonb(NEW.campaign_id::text)::text
        || ',"code_commit_hash":' || to_jsonb(NEW.code_commit_hash::text)::text
        || ',"cycle_interval_seconds":'
        || NEW.cycle_interval_seconds::text
        || ',"environment":' || to_jsonb(NEW.environment)::text
        || ',"execution_enabled":false'
        || ',"observation_policy_hash_sha256":'
        || to_jsonb(NEW.observation_policy_hash::text)::text
        || ',"observation_policy_id":'
        || to_jsonb(NEW.observation_policy_id)::text
        || ',"planned_ends_at":' || to_jsonb(ends_iso)::text
        || ',"read_only":true'
        || ',"runtime_config_hash":'
        || to_jsonb(NEW.runtime_config_hash::text)::text
        || ',"scope_manifest_hash_sha256":'
        || to_jsonb(NEW.scope_manifest_hash::text)::text
        || ',"started_at":' || to_jsonb(started_iso)::text
        || ',"t4_protocol_commit_hash":'
        || to_jsonb(NEW.t4_protocol_commit_hash::text)::text || '}',
        'UTF8'), 'sha256'), 'hex');
    expected_content := encode(public.digest(convert_to(
        '{"campaign_id":' || to_jsonb(NEW.campaign_id::text)::text
        || ',"frozen_baseline_hash_sha256":'
        || to_jsonb(expected_baseline::text)::text || '}',
        'UTF8'), 'sha256'), 'hex');
    IF NEW.scope_manifest_hash IS DISTINCT FROM encode(public.digest(convert_to(
            replace(NEW.scope_manifest::text, ', ', ','), 'UTF8'
       ), 'sha256'), 'hex')
       OR NEW.frozen_baseline_hash IS DISTINCT FROM expected_baseline
       OR NEW.content_hash IS DISTINCT FROM expected_content THEN
        RAISE EXCEPTION 'observation campaign frozen hashes are invalid'
            USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$;

-- Independent database predicate for a positive V1 decision.  It intentionally
-- does not consume caller-computed report statuses or outcome summaries.
CREATE OR REPLACE FUNCTION crypto_agent.canonical_jsonb_text(p_value jsonb)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
STRICT
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    result text;
BEGIN
    CASE jsonb_typeof(p_value)
        WHEN 'object' THEN
            SELECT '{' || coalesce(string_agg(
                to_jsonb(entry.key)::text || ':' ||
                    crypto_agent.canonical_jsonb_text(entry.value),
                ',' ORDER BY entry.key
            ), '') || '}' INTO result
            FROM jsonb_each(p_value) entry;
        WHEN 'array' THEN
            SELECT '[' || coalesce(string_agg(
                crypto_agent.canonical_jsonb_text(entry.value),
                ',' ORDER BY entry.position
            ), '') || ']' INTO result
            FROM jsonb_array_elements(p_value)
                WITH ORDINALITY entry(value, position);
        ELSE
            result := p_value::text;
    END CASE;
    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION crypto_agent.t4_observation_gate_is_verified(
    p_campaign_id uuid,
    p_checkpoint_event_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    checkpoint_row crypto_agent.t4_bridge_observation_events%ROWTYPE;
    expected_cycles bigint;
    attempted_cycles bigint;
    successful_cycles bigint;
    linked_successful_cycles bigint;
    schema_valid_successful_cycles bigint;
    covered_scope_count bigint;
    maximum_unexplained_gap_seconds numeric;
    rtt_p95_milliseconds numeric;
    rtt_p99_milliseconds numeric;
    safety_violations bigint;
    passing_scenarios bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation-campaign:' || p_campaign_id::text, 0
    ));
    SELECT * INTO campaign_row
    FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = p_campaign_id;
    IF NOT FOUND
       OR campaign_row.environment <> 'live_t4'
       OR NOT campaign_row.read_only
       OR campaign_row.execution_enabled
       OR campaign_row.bridge_evidence_key_fingerprint IS NULL
       OR campaign_row.observation_policy_id <>
            't4-observation-v1-2026-08-12'
       OR campaign_row.observation_policy_hash <>
            'e4c7baebd97a06207b4018bc5081fb391227c93ccf80cd1d9fb011d7dc4595cf'
       OR campaign_row.scope_manifest <>
            '["BTC/USD:240m","ETH/USD:240m"]'::jsonb
       OR campaign_row.planned_ends_at - campaign_row.started_at <
            interval '672 hours'
       OR clock_timestamp() < campaign_row.planned_ends_at
            + make_interval(secs => campaign_row.cycle_interval_seconds)
       OR NOT EXISTS (
            SELECT 1 FROM crypto_agent.t4_bridge_evidence_keys evidence_key
            WHERE evidence_key.evidence_key_fingerprint =
                    campaign_row.bridge_evidence_key_fingerprint
              AND evidence_key.retired_at IS NULL
       )
       THEN
        RETURN FALSE;
    END IF;

    -- The campaign lock serializes normal observation writers.  The signer-key
    -- lock then serializes NULL-campaign bridge events too, in the same
    -- campaign->key order used by the verifier recorder.  The signed checkpoint
    -- freezes an immutable global-chain prefix; later monotonic journal events
    -- are deliberately outside this campaign snapshot.
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-bridge-evidence:'
            || campaign_row.bridge_evidence_key_fingerprint, 0
    ));
    SELECT * INTO checkpoint_row
    FROM crypto_agent.t4_bridge_observation_events event
    WHERE event.event_id = p_checkpoint_event_id;
    IF NOT FOUND
       OR checkpoint_row.evidence_key_fingerprint_sha256 IS DISTINCT FROM
            campaign_row.bridge_evidence_key_fingerprint
       OR checkpoint_row.campaign_id IS DISTINCT FROM p_campaign_id
       OR checkpoint_row.event_type IS DISTINCT FROM 'campaign_checkpoint'
       OR checkpoint_row.reason_code IS DISTINCT FROM
            'OBSERVATION_CAMPAIGN_CHECKPOINT'
       OR checkpoint_row.scope_key IS NOT NULL
       OR checkpoint_row.scenario_code IS NOT NULL
       OR checkpoint_row.action_request_id IS NULL
       OR checkpoint_row.control_action IS DISTINCT FROM 'checkpoint'
       OR checkpoint_row.control_step IS DISTINCT FROM 'consumed'
       OR checkpoint_row.payload IS DISTINCT FROM
            '{"action":"checkpoint","control_step":"consumed"}'::jsonb
       OR checkpoint_row.event_at < campaign_row.planned_ends_at
            + make_interval(secs => campaign_row.cycle_interval_seconds)
       OR checkpoint_row.event_at > clock_timestamp()
       THEN
        RETURN FALSE;
    END IF;

    WITH scopes AS (
        SELECT jsonb_array_elements_text(campaign_row.scope_manifest) AS scope_key
    ), expected_slots AS (
        SELECT scope.scope_key,
               generate_series(
                   campaign_row.started_at
                       + make_interval(secs => campaign_row.cycle_interval_seconds),
                   campaign_row.planned_ends_at,
                   make_interval(secs => campaign_row.cycle_interval_seconds)
               ) AS expected_at
        FROM scopes scope
    ), slot_evidence AS (
        SELECT slot.scope_key, slot.expected_at,
               cycle.observation_cycle_id, cycle.outcome,
               cycle.gap_explanation_code, cycle.t4_batch_id,
               cycle.research_run_id, cycle.bridge_schema_version,
               cycle.bridge_rtt_milliseconds,
               (cycle.observation_cycle_id IS NULL
                OR (cycle.outcome = 'missed'
                    AND cycle.gap_explanation_code IS NULL)) AS unexplained
        FROM expected_slots slot
        LEFT JOIN crypto_agent.t4_observation_cycles cycle
          ON cycle.campaign_id = p_campaign_id
         AND cycle.scope_key = slot.scope_key
         AND cycle.expected_at = slot.expected_at
    ), marked AS (
        SELECT *,
               sum(CASE WHEN unexplained THEN 0 ELSE 1 END)
                 OVER (PARTITION BY scope_key ORDER BY expected_at) AS gap_group
        FROM slot_evidence
    ), unexplained_runs AS (
        SELECT scope_key, gap_group,
               count(*) * campaign_row.cycle_interval_seconds AS gap_seconds
        FROM marked WHERE unexplained
        GROUP BY scope_key, gap_group
    )
    SELECT count(*),
           count(*) FILTER (WHERE outcome IN ('success', 'failure')),
           count(*) FILTER (WHERE outcome = 'success'),
           count(*) FILTER (
               WHERE outcome = 'success'
                 AND t4_batch_id IS NOT NULL AND research_run_id IS NOT NULL
           ),
           count(*) FILTER (
               WHERE outcome = 'success' AND bridge_schema_version = 5
           ),
           count(DISTINCT scope_key) FILTER (WHERE outcome = 'success'),
           coalesce((SELECT max(gap_seconds) FROM unexplained_runs), 0),
           percentile_cont(0.95) WITHIN GROUP (
               ORDER BY bridge_rtt_milliseconds
           ) FILTER (WHERE outcome = 'success'),
           percentile_cont(0.99) WITHIN GROUP (
               ORDER BY bridge_rtt_milliseconds
           ) FILTER (WHERE outcome = 'success')
    INTO expected_cycles, attempted_cycles, successful_cycles,
         linked_successful_cycles, schema_valid_successful_cycles,
         covered_scope_count, maximum_unexplained_gap_seconds,
         rtt_p95_milliseconds, rtt_p99_milliseconds
    FROM slot_evidence;

    SELECT count(*) INTO safety_violations
    FROM crypto_agent.t4_observation_session_events event
    WHERE event.campaign_id = p_campaign_id
      AND event.event_at BETWEEN campaign_row.started_at
                             AND campaign_row.planned_ends_at
                                 + make_interval(
                                     secs => campaign_row.cycle_interval_seconds
                                   )
      AND event.event_type IN (
          'read_only_violation', 'order_route_attempt', 'secret_leak_detected',
          'integrity_failure', 'fail_closed_violation', 'fixture_data_attempt',
          'replay_data_attempt', 'backfill_attempt'
      );
    safety_violations := safety_violations + (
        SELECT count(*)
        FROM crypto_agent.t4_bridge_observation_events event
        WHERE event.evidence_key_fingerprint_sha256 =
                campaign_row.bridge_evidence_key_fingerprint
          AND event.event_at BETWEEN campaign_row.started_at
                                 AND checkpoint_row.event_at
          AND event.sequence_no <= checkpoint_row.sequence_no
          AND (event.campaign_id IS DISTINCT FROM p_campaign_id
               OR NOT event.read_only OR event.order_routes_exposed
               OR event.environment <> 'live_t4'
               OR event.bridge_schema_version <> 5
               OR NOT event.signature_verified
               OR event.evidence_key_fingerprint_sha256 <>
                    campaign_row.bridge_evidence_key_fingerprint
               OR event.event_type = 'outbound_rejected')
    );

    WITH mandatory(scenario_code) AS (VALUES
        ('bridge_restart'), ('missing_data'), ('rate_limit'), ('reconnect'),
        ('replay_blocked'), ('roll_transition'), ('stale_data')
    )
    SELECT count(*) INTO passing_scenarios
    FROM mandatory
    WHERE EXISTS (
        SELECT 1
        FROM crypto_agent.t4_observation_scenario_trials trial
        WHERE trial.campaign_id = p_campaign_id
          AND trial.scenario_code = mandatory.scenario_code
        GROUP BY trial.scenario_code
        HAVING bool_and(trial.outcome = 'pass')
    );

    RETURN expected_cycles > 0
       AND attempted_cycles * 100 >= expected_cycles * 99
       AND attempted_cycles > 0
       AND successful_cycles * 100 >= attempted_cycles * 99
       AND successful_cycles > 0
       AND linked_successful_cycles = successful_cycles
       AND schema_valid_successful_cycles = successful_cycles
       AND covered_scope_count = 2
       AND maximum_unexplained_gap_seconds <=
            campaign_row.cycle_interval_seconds * 3
       AND rtt_p95_milliseconds IS NOT NULL
       AND rtt_p95_milliseconds <= 5000
       AND rtt_p99_milliseconds IS NOT NULL
       AND rtt_p99_milliseconds <= 10000
       AND safety_violations = 0
       AND passing_scenarios = 7
       -- Replay has no bridge anchor.  Revalidate the isolated verifier's
       -- immutable per-scope attestations against the current frozen source
       -- batch selection instead of trusting a stored trial outcome.
       AND NOT EXISTS (
            SELECT 1
            FROM crypto_agent.t4_observation_scenario_trials trial
            WHERE trial.campaign_id = p_campaign_id
              AND trial.scenario_code = 'replay_blocked'
              AND trial.outcome = 'pass'
              AND (
                    NOT crypto_agent.t4_replay_attestations_are_verified(
                        p_campaign_id, trial.replay_attestation_ids
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM crypto_agent.t4_replay_verifier_attestations
                            attestation
                        WHERE attestation.replay_attestation_id = ANY(
                                trial.replay_attestation_ids
                              )
                          AND attestation.attested_at > checkpoint_row.event_at
                    )
              )
       )
       -- Re-evaluate negative safety predicates at report time.  A PASS trial
       -- cannot hide an alert appended after the trial was completed.
       AND NOT EXISTS (
            SELECT 1
            FROM crypto_agent.t4_observation_scenario_trials trial
            JOIN crypto_agent.alerts alert
              ON alert.research_run_id = trial.research_run_id
            WHERE trial.campaign_id = p_campaign_id
              AND trial.outcome = 'pass'
              AND trial.scenario_code IN (
                    'missing_data', 'rate_limit', 'stale_data'
              )
       )
       AND NOT EXISTS (
            SELECT 1
            FROM crypto_agent.t4_observation_scenario_trials trial
            JOIN crypto_agent.alerts alert
              ON alert.research_run_id = trial.research_run_id
            JOIN crypto_agent.alert_delivery_outbox outbox
              ON outbox.alert_id = alert.alert_id
            WHERE trial.campaign_id = p_campaign_id
              AND trial.outcome = 'pass'
              AND trial.scenario_code IN (
                    'missing_data', 'rate_limit', 'stale_data'
              )
       )
       AND NOT EXISTS (
            SELECT 1
            FROM crypto_agent.t4_observation_scenario_trials trial
            JOIN crypto_agent.alerts alert
              ON alert.research_run_id = trial.research_run_id
            JOIN crypto_agent.alert_delivery_outbox outbox
              ON outbox.alert_id = alert.alert_id
            JOIN crypto_agent.alert_delivery_attempts attempt
              ON attempt.alert_delivery_outbox_id =
                    outbox.alert_delivery_outbox_id
            WHERE trial.campaign_id = p_campaign_id
              AND trial.outcome = 'pass'
              AND trial.scenario_code IN (
                    'missing_data', 'rate_limit', 'stale_data'
              )
       )
       AND NOT EXISTS (
            SELECT 1
            FROM crypto_agent.t4_observation_scenario_trial_requests request
            LEFT JOIN crypto_agent.t4_observation_scenario_trials trial
              ON trial.trial_id = request.trial_id
            WHERE request.campaign_id = p_campaign_id
              AND trial.trial_id IS NULL
       );
END;
$$;

-- Public post-finalization predicate.  Before the immutable final report has
-- frozen a signed checkpoint this deliberately returns FALSE.
CREATE OR REPLACE FUNCTION crypto_agent.t4_observation_gate_is_verified(
    p_campaign_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    checkpoint_event_id uuid;
BEGIN
    SELECT report.bridge_checkpoint_event_id INTO checkpoint_event_id
    FROM crypto_agent.t4_observation_quality_reports report
    WHERE report.campaign_id = p_campaign_id;
    IF checkpoint_event_id IS NULL THEN
        RETURN FALSE;
    END IF;
    RETURN crypto_agent.t4_observation_gate_is_verified(
        p_campaign_id, checkpoint_event_id
    );
END;
$$;

CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_observation_final_report()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    campaign_row crypto_agent.t4_observation_campaigns%ROWTYPE;
    criterion_ids text[];
    criteria_are_canonical boolean;
    expected_report_hash crypto_agent.sha256_hex;
    expected_content_hash crypto_agent.sha256_hex;
    required_criterion_ids constant text[] := ARRAY[
        'bridge_round_trip_latency', 'bridge_schema_conformance',
        'configured_scope_coverage', 'cycle_attempt_rate',
        'cycle_success_rate', 'exact_batch_analysis_linkage',
        'fail_closed_violations', 'frozen_baseline', 'integrity_failures',
        'live_environment_only', 'maximum_unexplained_gap_seconds',
        'order_route_attempts', 'prohibited_data_mode_violations',
        'read_only_violations', 'real_elapsed_time',
        'scenario_bridge_restart', 'scenario_missing_data',
        'scenario_rate_limit', 'scenario_reconnect',
        'scenario_replay_blocked', 'scenario_roll_transition',
        'scenario_stale_data', 'secret_leaks'
    ];
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-observation-campaign:' || NEW.campaign_id::text, 0
    ));
    SELECT * INTO campaign_row
    FROM crypto_agent.t4_observation_campaigns
    WHERE campaign_id = NEW.campaign_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'observation campaign is missing' USING ERRCODE = '23503';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        't4-bridge-evidence:'
            || campaign_row.bridge_evidence_key_fingerprint, 0
    ));
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
    SELECT array_agg(item->>'criterion_id' ORDER BY item->>'criterion_id'),
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
    expected_report_hash := encode(public.digest(convert_to(
        crypto_agent.canonical_jsonb_text(NEW.report), 'UTF8'
    ), 'sha256'), 'hex');
    expected_content_hash := encode(public.digest(convert_to(
        '{"campaign_id":' || to_jsonb(NEW.campaign_id::text)::text
        || ',"report_hash_sha256":'
        || to_jsonb(expected_report_hash::text)::text || '}',
        'UTF8'), 'sha256'), 'hex');
    IF NEW.report_hash IS DISTINCT FROM expected_report_hash
       OR NEW.content_hash IS DISTINCT FROM expected_content_hash THEN
        RAISE EXCEPTION 'observation final report hashes are invalid'
            USING ERRCODE = '22023';
    END IF;
    IF NEW.bridge_checkpoint_event_id IS NULL
       OR NEW.bridge_checkpoint_sequence_no IS NULL
       OR NEW.bridge_checkpoint_event_hash IS NULL
       OR NEW.bridge_checkpoint_action_request_id IS NULL
       OR NOT EXISTS (
            SELECT 1
            FROM crypto_agent.t4_bridge_observation_events checkpoint
            WHERE checkpoint.event_id = NEW.bridge_checkpoint_event_id
              AND checkpoint.sequence_no = NEW.bridge_checkpoint_sequence_no
              AND checkpoint.event_hash_sha256 = NEW.bridge_checkpoint_event_hash
              AND checkpoint.event_type = 'campaign_checkpoint'
              AND checkpoint.reason_code = 'OBSERVATION_CAMPAIGN_CHECKPOINT'
              AND checkpoint.campaign_id = NEW.campaign_id
              AND checkpoint.scope_key IS NULL
              AND checkpoint.scenario_code IS NULL
              AND checkpoint.action_request_id =
                    NEW.bridge_checkpoint_action_request_id
              AND checkpoint.control_action = 'checkpoint'
              AND checkpoint.control_step = 'consumed'
              AND checkpoint.payload =
                    '{"action":"checkpoint","control_step":"consumed"}'::jsonb
              AND checkpoint.event_at >= campaign_row.planned_ends_at
                    + make_interval(
                        secs => campaign_row.cycle_interval_seconds
                      )
              AND checkpoint.event_at <= clock_timestamp()
              AND checkpoint.evidence_key_fingerprint_sha256 =
                    campaign_row.bridge_evidence_key_fingerprint
              AND checkpoint.sequence_no = (
                    SELECT max(head.sequence_no)
                    FROM crypto_agent.t4_bridge_observation_events head
                    WHERE head.evidence_key_fingerprint_sha256 =
                            campaign_row.bridge_evidence_key_fingerprint
              )
       ) THEN
        RAISE EXCEPTION 'final report requires an exact signed bridge checkpoint'
            USING ERRCODE = '22023';
    END IF;
    IF NEW.v1_gate_passed IS DISTINCT FROM
            crypto_agent.t4_observation_gate_is_verified(
                NEW.campaign_id, NEW.bridge_checkpoint_event_id
            ) THEN
        RAISE EXCEPTION 'V1 result does not match PostgreSQL evidence'
            USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$;

-- Alerts are append-only and may be produced asynchronously.  Once a run has
-- been used as scenario evidence, serialize any later alert with that
-- campaign's final report.  This makes the negative "no alert/no delivery"
-- predicate stable after an immutable PASS report.
CREATE OR REPLACE FUNCTION crypto_agent.enforce_t4_scenario_alert_finalization()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, crypto_agent
AS $$
DECLARE
    linked_campaign_id uuid;
BEGIN
    FOR linked_campaign_id IN
        SELECT DISTINCT trial.campaign_id
        FROM crypto_agent.t4_observation_scenario_trials trial
        WHERE trial.research_run_id = NEW.research_run_id
          AND trial.scenario_code IN (
                'missing_data', 'rate_limit', 'stale_data', 'replay_blocked'
          )
        ORDER BY trial.campaign_id
    LOOP
        PERFORM pg_advisory_xact_lock(hashtextextended(
            't4-observation-campaign:' || linked_campaign_id::text, 0
        ));
        IF EXISTS (
            SELECT 1
            FROM crypto_agent.t4_observation_quality_reports report
            WHERE report.campaign_id = linked_campaign_id
        ) THEN
            RAISE EXCEPTION
                'cannot append an alert to finalized T4 scenario evidence'
                USING ERRCODE = '55000';
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$;

CREATE TRIGGER t4_scenario_alert_finalization_guard
BEFORE INSERT ON crypto_agent.alerts
FOR EACH ROW EXECUTE FUNCTION
    crypto_agent.enforce_t4_scenario_alert_finalization();

CREATE INDEX t4_bridge_observation_events_campaign_time_idx
ON crypto_agent.t4_bridge_observation_events (campaign_id, event_at, sequence_no);

CREATE INDEX t4_bridge_observation_events_scenario_idx
ON crypto_agent.t4_bridge_observation_events (
    campaign_id, scenario_code, action_request_id, sequence_no
)
WHERE scenario_code IS NOT NULL;

CREATE INDEX t4_observation_scenario_trials_campaign_idx
ON crypto_agent.t4_observation_scenario_trials (
    campaign_id, scenario_code, completed_at
);

CREATE TRIGGER t4_bridge_evidence_keys_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.t4_bridge_evidence_keys
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_bridge_evidence_keys_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.t4_bridge_evidence_keys
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_bridge_observation_events_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.t4_bridge_observation_events
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_bridge_observation_events_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.t4_bridge_observation_events
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_replay_verifier_attestations_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.t4_replay_verifier_attestations
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_replay_verifier_attestations_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.t4_replay_verifier_attestations
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_observation_scenario_trials_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.t4_observation_scenario_trials
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_observation_scenario_trials_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.t4_observation_scenario_trials
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_observation_scenario_trial_requests_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.t4_observation_scenario_trial_requests
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_observation_scenario_trial_requests_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.t4_observation_scenario_trial_requests
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

ALTER FUNCTION crypto_agent.enforce_t4_observation_campaign_start()
    OWNER TO crypto_agent_evidence_owner;
ALTER FUNCTION crypto_agent.canonical_jsonb_text(jsonb)
    OWNER TO crypto_agent_evidence_owner;
ALTER FUNCTION crypto_agent.t4_observation_gate_is_verified(uuid, uuid)
    OWNER TO crypto_agent_evidence_owner;
ALTER FUNCTION crypto_agent.t4_observation_gate_is_verified(uuid)
    OWNER TO crypto_agent_evidence_owner;
ALTER FUNCTION crypto_agent.enforce_t4_observation_final_report()
    OWNER TO crypto_agent_evidence_owner;
ALTER FUNCTION crypto_agent.enforce_t4_scenario_alert_finalization()
    OWNER TO crypto_agent_evidence_owner;
ALTER FUNCTION crypto_agent.forbid_append_only_change()
    OWNER TO crypto_agent_evidence_owner;
ALTER FUNCTION crypto_agent.forbid_append_only_change()
    SET search_path = pg_catalog, crypto_agent;

REVOKE ALL ON FUNCTION crypto_agent.t4_observation_gate_is_verified(uuid, uuid)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION crypto_agent.canonical_jsonb_text(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION crypto_agent.t4_observation_gate_is_verified(uuid)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION crypto_agent.t4_observation_gate_is_verified(uuid)
    TO PUBLIC;

REVOKE CREATE ON SCHEMA crypto_agent FROM crypto_agent_evidence_owner;

COMMENT ON TABLE crypto_agent.t4_bridge_observation_events IS
'Append-only verifier-attested canonical T4 bridge event journal; global hash chain per SPKI fingerprint.';

COMMENT ON TABLE crypto_agent.t4_observation_scenario_trials IS
'Append-only scenario verdicts whose PASS is derived from objective PostgreSQL evidence.';

COMMENT ON TABLE crypto_agent.t4_replay_verifier_attestations IS
'Append-only verifier attestations for independent read-only replay of exact frozen T4 inputs; callers cannot supply PASS.';
