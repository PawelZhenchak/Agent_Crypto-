-- Immutable transactional outbox for read-only research alerts.
-- The only V1 delivery route is a canonical JSON document written to stdout.

-- Schema v4 adds an explicit live-environment attestation. Historical v2/v3
-- batches remain replayable but can never enter the live alert outbox.
ALTER TABLE crypto_agent.t4_ingestion_batches
    DROP CONSTRAINT t4_ingestion_batches_bridge_schema_version_check;

ALTER TABLE crypto_agent.t4_ingestion_batches
    ADD CONSTRAINT t4_ingestion_batches_bridge_schema_version_check
    CHECK (bridge_schema_version IN (2, 3, 4));

CREATE TABLE crypto_agent.alert_delivery_outbox (
    alert_delivery_outbox_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alert_id                 bigint NOT NULL,
    channel                  text NOT NULL,
    destination              text NOT NULL,
    idempotency_key          crypto_agent.sha256_hex NOT NULL UNIQUE,
    monitoring_policy_id     text NOT NULL,
    monitoring_policy_hash   crypto_agent.sha256_hex NOT NULL,
    retention_days           smallint NOT NULL,
    payload                  jsonb NOT NULL,
    payload_hash             crypto_agent.sha256_hex NOT NULL,
    available_at             timestamptz NOT NULL,
    expires_at               timestamptz NOT NULL,
    max_attempts             smallint NOT NULL DEFAULT 3
        CHECK (max_attempts BETWEEN 1 AND 5),
    created_at               timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash             crypto_agent.sha256_hex NOT NULL UNIQUE,
    CONSTRAINT alert_delivery_outbox_alert_fk
        FOREIGN KEY (alert_id) REFERENCES crypto_agent.alerts(alert_id),
    CONSTRAINT alert_delivery_outbox_channel_check
        CHECK (channel = 'stdout_json'),
    CONSTRAINT alert_delivery_outbox_destination_check
        CHECK (destination = 'process_stdout'),
    CONSTRAINT alert_delivery_outbox_policy_id_check
        CHECK (btrim(monitoring_policy_id) <> '' AND length(monitoring_policy_id) <= 128),
    CONSTRAINT alert_delivery_outbox_retention_check
        CHECK (retention_days BETWEEN 35 AND 365),
    UNIQUE (alert_id, channel, destination),
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK (
        payload ?& ARRAY[
            'schema_version', 'alert_type', 'decision', 'decision_id', 'trace_id',
            'asset_id', 'instrument_id', 'horizon', 'as_of', 'expires_at',
            'reason_codes', 'data_snapshot_id', 'policy_id', 'policy_hash_sha256',
            'futures_policy_id', 'futures_policy_hash_sha256', 'environment',
            'external_delivery', 'read_only', 'execution_enabled',
            'not_financial_advice', 'monitoring_policy_id',
            'monitoring_policy_hash_sha256', 'retention_days'
        ]
        AND payload - ARRAY[
            'schema_version', 'alert_type', 'decision', 'decision_id', 'trace_id',
            'asset_id', 'instrument_id', 'horizon', 'as_of', 'expires_at',
            'reason_codes', 'data_snapshot_id', 'policy_id', 'policy_hash_sha256',
            'futures_policy_id', 'futures_policy_hash_sha256', 'environment',
            'external_delivery', 'read_only', 'execution_enabled',
            'not_financial_advice', 'monitoring_policy_id',
            'monitoring_policy_hash_sha256', 'retention_days'
        ] = '{}'::jsonb
    ),
    CHECK (payload->'schema_version' = '1'::jsonb),
    CHECK (payload->>'alert_type' = 'research_alert_v1'),
    CHECK (payload ? 'decision' AND payload->>'decision' = 'ALERT'),
    CHECK (payload->>'environment' = 'live_t4'),
    CHECK (payload->'external_delivery' = 'false'::jsonb),
    CHECK (
        payload ? 'read_only'
        AND jsonb_typeof(payload->'read_only') = 'boolean'
        AND payload->'read_only' = 'true'::jsonb
    ),
    CHECK (payload->'execution_enabled' = 'false'::jsonb),
    CHECK (payload->'not_financial_advice' = 'true'::jsonb),
    CHECK (payload->>'monitoring_policy_id' = monitoring_policy_id),
    CHECK (payload->>'monitoring_policy_hash_sha256' = monitoring_policy_hash),
    CHECK (payload->'retention_days' = to_jsonb(retention_days::integer)),
    CHECK (octet_length(payload::text) <= 16384),
    CHECK (available_at < expires_at)
);

CREATE TABLE crypto_agent.alert_delivery_attempts (
    alert_delivery_attempt_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alert_delivery_outbox_id  bigint NOT NULL,
    attempt_no                smallint NOT NULL CHECK (attempt_no > 0),
    outcome                   text NOT NULL CHECK (
        outcome IN ('delivered', 'retryable_failure', 'permanent_failure', 'expired')
    ),
    started_at                timestamptz NOT NULL,
    finished_at               timestamptz NOT NULL,
    next_attempt_at           timestamptz,
    error_code                text,
    request_payload_hash      crypto_agent.sha256_hex NOT NULL,
    previous_attempt_hash     crypto_agent.sha256_hex,
    content_hash              crypto_agent.sha256_hex NOT NULL UNIQUE,
    created_at                timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT alert_delivery_attempts_outbox_fk
        FOREIGN KEY (alert_delivery_outbox_id)
        REFERENCES crypto_agent.alert_delivery_outbox(alert_delivery_outbox_id),
    UNIQUE (alert_delivery_outbox_id, attempt_no),
    CHECK (started_at <= finished_at),
    CHECK (
        error_code IS NULL
        OR error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    ),
    CHECK (
        (outcome = 'delivered'
         AND next_attempt_at IS NULL
         AND error_code IS NULL)
        OR
        (outcome = 'retryable_failure'
         AND next_attempt_at IS NOT NULL
         AND next_attempt_at > finished_at
         AND error_code IS NOT NULL)
        OR
        (outcome = 'permanent_failure'
         AND next_attempt_at IS NULL
         AND error_code IS NOT NULL)
        OR
        (outcome = 'expired'
         AND next_attempt_at IS NULL)
    ),
    CHECK (
        (attempt_no = 1 AND previous_attempt_hash IS NULL)
        OR
        (attempt_no > 1 AND previous_attempt_hash IS NOT NULL)
    )
);

CREATE OR REPLACE FUNCTION crypto_agent.enforce_alert_delivery_attempt()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    outbox_row crypto_agent.alert_delivery_outbox%ROWTYPE;
    existing_hash crypto_agent.sha256_hex;
    previous_row crypto_agent.alert_delivery_attempts%ROWTYPE;
BEGIN
    SELECT * INTO outbox_row
    FROM crypto_agent.alert_delivery_outbox
    WHERE alert_delivery_outbox_id = NEW.alert_delivery_outbox_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'alert delivery outbox row is missing'
            USING ERRCODE = '23503';
    END IF;

    IF NEW.request_payload_hash <> outbox_row.payload_hash THEN
        RAISE EXCEPTION 'alert delivery attempt payload hash does not match outbox'
            USING ERRCODE = '22023';
    END IF;

    SELECT content_hash INTO existing_hash
    FROM crypto_agent.alert_delivery_attempts
    WHERE alert_delivery_outbox_id = NEW.alert_delivery_outbox_id
      AND attempt_no = NEW.attempt_no;

    IF FOUND THEN
        IF existing_hash <> NEW.content_hash THEN
            RAISE EXCEPTION 'alert delivery attempt idempotency conflict'
                USING ERRCODE = '22023';
        END IF;
        RETURN NULL;
    END IF;

    SELECT * INTO previous_row
    FROM crypto_agent.alert_delivery_attempts
    WHERE alert_delivery_outbox_id = NEW.alert_delivery_outbox_id
    ORDER BY attempt_no DESC
    LIMIT 1;

    IF NOT FOUND THEN
        IF NEW.attempt_no <> 1 OR NEW.previous_attempt_hash IS NOT NULL THEN
            RAISE EXCEPTION 'first alert delivery attempt must be sequence 1'
                USING ERRCODE = '22023';
        END IF;
    ELSE
        IF previous_row.outcome IN ('delivered', 'permanent_failure', 'expired') THEN
            RAISE EXCEPTION 'alert delivery already reached a terminal outcome'
                USING ERRCODE = '22023';
        END IF;
        IF NEW.attempt_no <> previous_row.attempt_no + 1
           OR NEW.previous_attempt_hash <> previous_row.content_hash THEN
            RAISE EXCEPTION 'invalid alert delivery attempt sequence or hash chain'
                USING ERRCODE = '22023';
        END IF;
        IF NEW.started_at < previous_row.finished_at THEN
            RAISE EXCEPTION 'alert delivery attempt time precedes the previous attempt'
                USING ERRCODE = '22023';
        END IF;
        IF NEW.outcome <> 'expired'
           AND NEW.started_at < previous_row.next_attempt_at THEN
            RAISE EXCEPTION 'alert delivery retry started before its retry boundary'
                USING ERRCODE = '22023';
        END IF;
    END IF;

    IF NEW.attempt_no > outbox_row.max_attempts THEN
        RAISE EXCEPTION 'alert delivery attempt exceeds the configured maximum'
            USING ERRCODE = '22023';
    END IF;

    IF NEW.outcome = 'retryable_failure'
       AND NEW.attempt_no = outbox_row.max_attempts THEN
        RAISE EXCEPTION 'last alert delivery attempt must have a terminal outcome'
            USING ERRCODE = '22023';
    END IF;

    IF NEW.outcome = 'expired' THEN
        IF NEW.started_at < outbox_row.expires_at THEN
            RAISE EXCEPTION 'alert cannot expire before its expiry boundary'
                USING ERRCODE = '22023';
        END IF;
    ELSIF NEW.started_at < outbox_row.available_at
          OR NEW.started_at >= outbox_row.expires_at
          OR (
              NEW.outcome = 'delivered'
              AND NEW.finished_at > outbox_row.expires_at
          ) THEN
        RAISE EXCEPTION 'alert delivery attempt is outside its delivery window'
            USING ERRCODE = '22023';
    END IF;

    IF NEW.next_attempt_at IS NOT NULL
       AND NEW.next_attempt_at >= outbox_row.expires_at THEN
        RAISE EXCEPTION 'alert retry boundary must precede alert expiry'
            USING ERRCODE = '22023';
    END IF;

    RETURN NEW;
END;
$$;

CREATE INDEX alert_delivery_outbox_due_idx
ON crypto_agent.alert_delivery_outbox (
    available_at, expires_at, alert_delivery_outbox_id
);

CREATE INDEX alert_delivery_attempts_outbox_idx
ON crypto_agent.alert_delivery_attempts (
    alert_delivery_outbox_id, attempt_no DESC
);

CREATE INDEX alert_delivery_attempts_terminal_idx
ON crypto_agent.alert_delivery_attempts (
    outcome, finished_at DESC
)
WHERE outcome IN ('delivered', 'permanent_failure', 'expired');

CREATE TRIGGER alert_delivery_attempt_integrity_guard
BEFORE INSERT ON crypto_agent.alert_delivery_attempts
FOR EACH ROW EXECUTE FUNCTION crypto_agent.enforce_alert_delivery_attempt();

CREATE TRIGGER alert_delivery_outbox_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.alert_delivery_outbox
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER alert_delivery_outbox_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.alert_delivery_outbox
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER alert_delivery_attempts_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.alert_delivery_attempts
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER alert_delivery_attempts_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.alert_delivery_attempts
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

COMMENT ON TABLE crypto_agent.alert_delivery_outbox IS
    'Transactional, immutable stdout-only outbox for read-only ALERT decisions.';

COMMENT ON TABLE crypto_agent.alert_delivery_attempts IS
    'Append-only delivery outcomes with a per-outbox hash chain and strict retry bounds.';
