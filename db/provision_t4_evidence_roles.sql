\set ON_ERROR_STOP on

-- Run with the migration-administrator DSN after migration 0018.
-- This file never embeds a password.  Configure LOGIN authentication through
-- the platform secret store, a managed identity, or an interactive `\password`
-- command after provisioning.
\if :{?runtime_role}
\else
\set runtime_role crypto_agent_runtime
\endif
\if :{?verifier_role}
\else
\set verifier_role crypto_agent_evidence_runtime
\endif
\if :{?replay_reader_role}
\else
\set replay_reader_role crypto_agent_replay_verifier_reader
\endif

SELECT format(
    'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT '
    'NOREPLICATION NOBYPASSRLS', :'runtime_role'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'runtime_role'
) \gexec

SELECT format(
    'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT '
    'NOREPLICATION NOBYPASSRLS', :'replay_reader_role'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'replay_reader_role'
) \gexec

SELECT format(
    'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT '
    'NOREPLICATION NOBYPASSRLS', :'verifier_role'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'verifier_role'
) \gexec

BEGIN;

SELECT pg_catalog.set_config(
    'crypto_agent.provision_runtime_role', :'runtime_role', true
);
SELECT pg_catalog.set_config(
    'crypto_agent.provision_verifier_role', :'verifier_role', true
);
SELECT pg_catalog.set_config(
    'crypto_agent.provision_replay_reader_role', :'replay_reader_role', true
);

ALTER ROLE :"runtime_role" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    INHERIT NOREPLICATION NOBYPASSRLS;
ALTER ROLE :"verifier_role" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    INHERIT NOREPLICATION NOBYPASSRLS;
ALTER ROLE :"replay_reader_role" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    INHERIT NOREPLICATION NOBYPASSRLS;

REVOKE crypto_agent_evidence_owner
    FROM :"runtime_role", :"verifier_role", :"replay_reader_role";
REVOKE crypto_agent_evidence_verifier FROM :"runtime_role";
REVOKE crypto_agent_evidence_verifier FROM :"replay_reader_role";
REVOKE crypto_agent_evidence_reader FROM :"verifier_role";
GRANT crypto_agent_evidence_reader TO :"runtime_role";
GRANT crypto_agent_evidence_reader TO :"replay_reader_role";
GRANT crypto_agent_evidence_verifier TO :"verifier_role";

GRANT SELECT, INSERT ON
    crypto_agent.t4_ingestion_batches,
    crypto_agent.t4_canonical_candles,
    crypto_agent.t4_futures_snapshots,
    crypto_agent.t4_orderbook_levels,
    crypto_agent.t4_contract_transition_evidence,
    crypto_agent.research_runs,
    crypto_agent.research_run_events,
    crypto_agent.research_run_inputs,
    crypto_agent.research_artifacts,
    crypto_agent.data_quality_incidents,
    crypto_agent.data_quality_incident_events,
    crypto_agent.alerts,
    crypto_agent.alert_events,
    crypto_agent.alert_delivery_outbox,
    crypto_agent.alert_delivery_attempts,
    crypto_agent.t4_observation_campaigns,
    crypto_agent.t4_observation_research_inputs,
    crypto_agent.t4_observation_cycles,
    crypto_agent.t4_observation_session_events,
    crypto_agent.t4_observation_quality_reports
    TO :"runtime_role";
GRANT SELECT ON
    crypto_agent.schema_migrations,
    crypto_agent.data_sources,
    crypto_agent.exchanges,
    crypto_agent.markets,
    crypto_agent.risk_policies,
    crypto_agent.market_data_source_bindings,
    crypto_agent.canonical_candle_series,
    crypto_agent.t4_runtime_config,
    crypto_agent.t4_ingestion_batches,
    crypto_agent.research_runs,
    crypto_agent.research_run_events,
    crypto_agent.research_run_inputs,
    crypto_agent.research_artifacts,
    crypto_agent.alerts,
    crypto_agent.alert_delivery_outbox,
    crypto_agent.alert_delivery_attempts
    TO :"runtime_role";
-- PostgreSQL row-locking SELECTs require UPDATE on at least one column of the
-- locked table.  Grant only the immutable outbox identity column; append-only
-- triggers still reject every attempted UPDATE.
GRANT UPDATE (alert_delivery_outbox_id) ON crypto_agent.alert_delivery_outbox
    TO :"runtime_role";
GRANT USAGE, SELECT ON SEQUENCE
    crypto_agent.t4_ingestion_batches_t4_batch_id_seq,
    crypto_agent.t4_canonical_candles_t4_candle_id_seq,
    crypto_agent.t4_futures_snapshots_t4_snapshot_id_seq,
    crypto_agent.t4_contract_transition_evidence_t4_transition_id_seq,
    crypto_agent.research_runs_research_run_id_seq,
    crypto_agent.research_run_events_research_run_event_id_seq,
    crypto_agent.research_run_inputs_research_run_input_id_seq,
    crypto_agent.research_artifacts_research_artifact_id_seq,
    crypto_agent.data_quality_incidents_data_quality_incident_id_seq,
    crypto_agent.data_quality_incident_events_data_quality_incident_event_id_seq,
    crypto_agent.alerts_alert_id_seq,
    crypto_agent.alert_events_alert_event_id_seq,
    crypto_agent.alert_delivery_outbox_alert_delivery_outbox_id_seq,
    crypto_agent.alert_delivery_attempts_alert_delivery_attempt_id_seq,
    crypto_agent.t4_observation_research_input_observation_research_input_id_seq,
    crypto_agent.t4_observation_cycles_observation_cycle_id_seq,
    crypto_agent.t4_observation_session_events_observation_session_event_id_seq,
    crypto_agent.t4_observation_quality_report_observation_quality_report_id_seq
    TO :"runtime_role";

DO $provision$
DECLARE
    runtime_name constant text := current_setting(
        'crypto_agent.provision_runtime_role'
    );
    verifier_name constant text := current_setting(
        'crypto_agent.provision_verifier_role'
    );
    replay_reader_name constant text := current_setting(
        'crypto_agent.provision_replay_reader_role'
    );
BEGIN
    IF cardinality(ARRAY[
        runtime_name, verifier_name, replay_reader_name
    ]) <> cardinality(ARRAY(
        SELECT DISTINCT item FROM unnest(ARRAY[
            runtime_name, verifier_name, replay_reader_name
        ]) item
    )) THEN
        RAISE EXCEPTION
            'runtime, evidence verifier and replay reader must be distinct logins'
            USING ERRCODE = '42501';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_database database
        JOIN pg_catalog.pg_roles role ON role.oid = database.datdba
        WHERE database.datname = current_database()
          AND role.rolname IN (
              runtime_name, verifier_name, replay_reader_name
          )
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_namespace namespace
        JOIN pg_catalog.pg_roles role ON role.oid = namespace.nspowner
        WHERE namespace.nspname = 'crypto_agent'
          AND role.rolname IN (
              runtime_name, verifier_name, replay_reader_name
          )
    ) THEN
        RAISE EXCEPTION
            'runtime/verifier/replay login must not own the database or schema'
            USING ERRCODE = '42501';
    END IF;
END;
$provision$;

COMMIT;
