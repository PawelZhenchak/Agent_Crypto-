-- Plus500 Futures / T4-only operational registry.
-- Apply after schema.sql and migrations 0011, 0012 and 0013.

SET LOCAL search_path TO crypto_agent, public;
SET LOCAL TIME ZONE 'UTC';

INSERT INTO data_sources (
    source_key, display_name, source_kind, trust_tier, homepage_url,
    registry_version, config_hash, observed_at, available_at, ingested_at
) VALUES
    ('plus500_t4_futures_v1', 'Plus500 Futures T4', 'futures_market_data', 2,
     'https://futures-technologies.plus500.com/api/', 'v1', repeat('1', 64),
     '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
     '2026-08-11 00:00:00+00'),
    ('crypto_agent_registry_v1', 'Crypto Agent Registry', 'internal_registry', 1,
     NULL, 'v1', repeat('2', 64),
     '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
     '2026-08-11 00:00:00+00')
ON CONFLICT (source_key) DO NOTHING;

INSERT INTO exchanges (
    exchange_key, venue_kind, source_id, source_record_key, source_version,
    revision_no, observed_at, available_at, ingested_at, content_hash
) VALUES (
    'plus500_t4', 'futures_platform',
    (SELECT source_id FROM data_sources WHERE source_key = 'plus500_t4_futures_v1'),
    'venue:plus500-t4', 'v1', 1,
    '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
    '2026-08-11 00:00:00+00', repeat('3', 64)
)
ON CONFLICT (exchange_key) DO NOTHING;

INSERT INTO assets (
    asset_key, asset_type, source_id, source_record_key, source_version,
    revision_no, observed_at, available_at, ingested_at, content_hash
) VALUES
    ('btc', 'crypto_asset',
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'asset:btc', 'v1', 1, '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
     '2026-08-11 00:00:00+00', repeat('4', 64)),
    ('eth', 'crypto_asset',
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'asset:eth', 'v1', 1, '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
     '2026-08-11 00:00:00+00', repeat('5', 64)),
    ('usd', 'fiat_currency',
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'asset:usd', 'v1', 1, '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
     '2026-08-11 00:00:00+00', repeat('6', 64))
ON CONFLICT (asset_key) DO NOTHING;

INSERT INTO asset_versions (
    asset_id, display_name, canonical_symbol, lifecycle_status, attributes,
    source_id, source_record_key, source_version, revision_no,
    observed_at, available_at, ingested_at, content_hash
) VALUES
    ((SELECT asset_id FROM assets WHERE asset_key = 'btc'), 'Bitcoin', 'BTC',
     'active', '{}'::jsonb,
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'asset-version:btc:t4-v1', 'v1', 1,
     '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
     '2026-08-11 00:00:00+00', repeat('7', 64)),
    ((SELECT asset_id FROM assets WHERE asset_key = 'eth'), 'Ethereum', 'ETH',
     'active', '{}'::jsonb,
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'asset-version:eth:t4-v1', 'v1', 1,
     '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
     '2026-08-11 00:00:00+00', repeat('8', 64)),
    ((SELECT asset_id FROM assets WHERE asset_key = 'usd'), 'US Dollar', 'USD',
     'active', '{}'::jsonb,
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'asset-version:usd:t4-v1', 'v1', 1,
     '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
     '2026-08-11 00:00:00+00', repeat('9', 64))
ON CONFLICT (source_id, source_record_key, revision_no) DO NOTHING;

INSERT INTO markets (
    market_key, exchange_id, base_asset_id, quote_asset_id, instrument_type,
    source_id, source_record_key, source_version, revision_no,
    observed_at, available_at, ingested_at, content_hash
)
SELECT 'plus500_t4:' || lower(symbol) || '-usd-front',
       (SELECT exchange_id FROM exchanges WHERE exchange_key = 'plus500_t4'),
       (SELECT asset_id FROM assets WHERE asset_key = lower(symbol)),
       (SELECT asset_id FROM assets WHERE asset_key = 'usd'),
       'future',
       (SELECT source_id FROM data_sources WHERE source_key = 'plus500_t4_futures_v1'),
       'market:plus500-t4:' || lower(symbol) || '-usd-front', 'v1', 1,
       '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
       '2026-08-11 00:00:00+00', content_hash
FROM (VALUES ('BTC', repeat('a', 64)), ('ETH', repeat('b', 64))) AS item(symbol, content_hash)
ON CONFLICT (market_key) DO NOTHING;

INSERT INTO market_symbols (
    market_id, symbol_namespace, symbol, source_id, source_record_key,
    source_version, revision_no, observed_at, available_at, ingested_at, content_hash
)
SELECT (SELECT market_id FROM markets
        WHERE market_key = 'plus500_t4:' || lower(asset_symbol) || '-usd-front'),
       'plus500_t4_logical', venue_symbol,
       (SELECT source_id FROM data_sources WHERE source_key = 'plus500_t4_futures_v1'),
       'market-symbol:plus500-t4:' || venue_symbol, 'v1', 1,
       '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
       '2026-08-11 00:00:00+00', content_hash
FROM (VALUES
    ('BTC', 'BTC-FUTURES-FRONT', repeat('c', 64)),
    ('ETH', 'ETH-FUTURES-FRONT', repeat('d', 64))
) AS item(asset_symbol, venue_symbol, content_hash)
ON CONFLICT (source_id, source_record_key, revision_no) DO NOTHING;

INSERT INTO risk_policies (
    policy_key, policy_version, effective_from, policy_document, source_id,
    source_record_key, source_version, revision_no, observed_at, available_at,
    ingested_at, content_hash
) VALUES (
    'v1-read-only-plus500-t4', '2026-08-11', '2026-08-11 00:00:00+00',
    $policy${
      "policy_id":"v1-read-only-plus500-t4-2026-08-11",
      "policy_schema_version":3,
      "mode":"V1_READ_ONLY",
      "execution_enabled":false,
      "allowed_decisions":["NO_SIGNAL","ALERT"],
      "allowed_assets":["BTC/USD","ETH/USD"],
      "allowed_intervals_minutes":[240,1440,10080],
      "min_samples":60,
      "min_data_quality":0.85,
      "max_staleness_multiplier":1.25,
      "max_reference_price_age_seconds":300,
      "max_gap_multiplier":1.5,
      "max_annualized_volatility":1.2,
      "max_absolute_period_return":0.2,
      "report_ttl_seconds":3600,
      "max_clock_skew_seconds":30,
      "required_source_count":1,
      "required_history_candles":120,
      "max_market_price":1000000000.0,
      "max_base_volume":1000000000000.0,
      "leverage_allowed":false,
      "martingale_allowed":false,
      "exchange_credentials_allowed":false,
      "human_approval_required_for_execution":true
    }$policy$::jsonb,
    (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
    'risk-policy:v1-read-only-plus500-t4:2026-08-11', 'v1', 1,
    '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
    '2026-08-11 00:00:00+00',
    '589a39880a7bff780e510864525817dd890764e85b43ffdd6419cabc80a62d7d'
)
ON CONFLICT (policy_key, policy_version) DO NOTHING;

INSERT INTO market_data_source_bindings (
    source_id, market_id, canonical_symbol, venue_symbol,
    observed_at, available_at, ingested_at, content_hash
)
SELECT
    (SELECT source_id FROM data_sources WHERE source_key = 'plus500_t4_futures_v1'),
    (SELECT market_id FROM markets
     WHERE market_key = 'plus500_t4:' || lower(asset_symbol) || '-usd-front'),
    asset_symbol || '/USD', venue_symbol,
    '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
    '2026-08-11 00:00:00+00', content_hash
FROM (VALUES
    ('BTC', 'BTC-FUTURES-FRONT', repeat('e', 64)),
    ('ETH', 'ETH-FUTURES-FRONT', repeat('f', 64))
) AS item(asset_symbol, venue_symbol, content_hash)
ON CONFLICT (source_id, market_id) DO NOTHING;

INSERT INTO t4_runtime_config (
    source_id, provider_key, venue_key, bridge_protocol, read_only,
    order_routes_enabled, observed_at, available_at, ingested_at, content_hash
) VALUES (
    (SELECT source_id FROM data_sources WHERE source_key = 'plus500_t4_futures_v1'),
    'plus500_t4_futures_v1', 'plus500_t4', 'loopback_http_json_v1', TRUE, FALSE,
    '2026-08-11 00:00:00+00', '2026-08-11 00:00:00+00',
    '2026-08-11 00:00:00+00',
    'f01d1e313c0b3f23250ac89a91241134e7a77e58a8e5727e1338c7ddde63c249'
)
ON CONFLICT (provider_key) DO NOTHING;
