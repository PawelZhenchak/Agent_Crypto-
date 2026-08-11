-- Plus500 Futures / T4 becomes the only operational market-data source.
-- Migrations 0011 and 0012 remain immutable for checksum and historical replay.

CREATE TABLE crypto_agent.t4_runtime_config (
    runtime_config_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id         bigint NOT NULL REFERENCES crypto_agent.data_sources(source_id),
    provider_key      text NOT NULL UNIQUE
        CHECK (provider_key = 'plus500_t4_futures_v1'),
    venue_key         text NOT NULL
        CHECK (venue_key = 'plus500_t4'),
    bridge_protocol   text NOT NULL
        CHECK (bridge_protocol = 'loopback_http_json_v1'),
    read_only         boolean NOT NULL CHECK (read_only IS TRUE),
    order_routes_enabled boolean NOT NULL CHECK (order_routes_enabled IS FALSE),
    observed_at       timestamptz NOT NULL,
    available_at      timestamptz NOT NULL,
    ingested_at       timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_hash      crypto_agent.sha256_hex NOT NULL UNIQUE,
    CHECK (observed_at <= available_at AND available_at <= ingested_at)
);

CREATE TRIGGER t4_runtime_config_append_only_row_guard
BEFORE UPDATE OR DELETE ON crypto_agent.t4_runtime_config
FOR EACH ROW EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

CREATE TRIGGER t4_runtime_config_append_only_truncate_guard
BEFORE TRUNCATE ON crypto_agent.t4_runtime_config
FOR EACH STATEMENT EXECUTE FUNCTION crypto_agent.forbid_append_only_change();

COMMENT ON TABLE crypto_agent.t4_runtime_config IS
    'Immutable T4-only runtime boundary. Credentials and order routes are not exposed.';
