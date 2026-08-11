-- Crypto Agent V1 operational registry seeds.
-- Apply only after schema.sql and migrations 0011/0012.
-- The statements are idempotent for the exact immutable registry keys. Any
-- conflicting pre-existing data remains visible to the fail-closed health check.

SET LOCAL search_path TO crypto_agent, public;
SET LOCAL TIME ZONE 'UTC';

INSERT INTO data_sources (
    source_key, display_name, source_kind, trust_tier, homepage_url,
    registry_version, config_hash, observed_at, available_at, ingested_at
) VALUES
    ('kraken_spot_rest_v1', 'Kraken Spot REST', 'market_data', 2,
     'https://docs.kraken.com/api/', 'v1', repeat('1', 64),
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'),
    ('coinbase_exchange_spot_rest_v1', 'Coinbase Exchange Spot REST', 'market_data', 2,
     'https://docs.cdp.coinbase.com/exchange/', 'v1', repeat('2', 64),
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'),
    ('crypto_agent_registry_v1', 'Crypto Agent Registry', 'internal_registry', 1,
     NULL, 'v1', repeat('3', 64),
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00'),
    ('cross_exchange_spot_consensus_v1', 'Cross-exchange Spot Consensus',
     'derived_market_data', 1, NULL, 'v1', repeat('4', 64),
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00')
ON CONFLICT (source_key) DO NOTHING;

INSERT INTO exchanges (
    exchange_key, venue_kind, source_id, source_record_key, source_version,
    revision_no, observed_at, available_at, ingested_at, content_hash
) VALUES
    ('kraken', 'centralized_exchange',
     (SELECT source_id FROM data_sources WHERE source_key = 'kraken_spot_rest_v1'),
     'exchange:kraken', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('5', 64)),
    ('coinbase', 'centralized_exchange',
     (SELECT source_id FROM data_sources
      WHERE source_key = 'coinbase_exchange_spot_rest_v1'),
     'exchange:coinbase', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('6', 64)),
    ('crypto_agent_consensus', 'derived_venue',
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'exchange:crypto_agent_consensus', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('7', 64))
ON CONFLICT (exchange_key) DO NOTHING;

INSERT INTO assets (
    asset_key, asset_type, source_id, source_record_key, source_version,
    revision_no, observed_at, available_at, ingested_at, content_hash
) VALUES
    ('btc', 'crypto_asset',
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'asset:btc', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('8', 64)),
    ('eth', 'crypto_asset',
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'asset:eth', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('9', 64)),
    ('usd', 'fiat_currency',
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'asset:usd', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('a', 64))
ON CONFLICT (asset_key) DO NOTHING;

INSERT INTO asset_versions (
    asset_id, display_name, canonical_symbol, lifecycle_status, attributes,
    source_id, source_record_key, source_version, revision_no,
    observed_at, available_at, ingested_at, content_hash
) VALUES
    ((SELECT asset_id FROM assets WHERE asset_key = 'btc'), 'Bitcoin', 'BTC',
     'active', '{}'::jsonb,
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'asset-version:btc:v1', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('b', 64)),
    ((SELECT asset_id FROM assets WHERE asset_key = 'eth'), 'Ethereum', 'ETH',
     'active', '{}'::jsonb,
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'asset-version:eth:v1', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('c', 64)),
    ((SELECT asset_id FROM assets WHERE asset_key = 'usd'), 'US Dollar', 'USD',
     'active', '{}'::jsonb,
     (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
     'asset-version:usd:v1', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('d', 64))
ON CONFLICT (source_id, source_record_key, revision_no) DO NOTHING;

INSERT INTO markets (
    market_key, exchange_id, base_asset_id, quote_asset_id, instrument_type,
    source_id, source_record_key, source_version, revision_no,
    observed_at, available_at, ingested_at, content_hash
)
SELECT venue || ':' || lower(symbol) || '-usd', exchange_id, asset_id,
       (SELECT asset_id FROM assets WHERE asset_key = 'usd'), 'spot', source_id,
       'market:' || venue || ':' || lower(symbol) || '-usd', 'v1', 1,
       '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
       '2026-01-01 00:00:00+00', content_hash
FROM (
    VALUES
        ('kraken', 'BTC',
         (SELECT exchange_id FROM exchanges WHERE exchange_key = 'kraken'),
         (SELECT asset_id FROM assets WHERE asset_key = 'btc'),
         (SELECT source_id FROM data_sources WHERE source_key = 'kraken_spot_rest_v1'),
         repeat('e', 64)),
        ('kraken', 'ETH',
         (SELECT exchange_id FROM exchanges WHERE exchange_key = 'kraken'),
         (SELECT asset_id FROM assets WHERE asset_key = 'eth'),
         (SELECT source_id FROM data_sources WHERE source_key = 'kraken_spot_rest_v1'),
         repeat('f', 64)),
        ('coinbase', 'BTC',
         (SELECT exchange_id FROM exchanges WHERE exchange_key = 'coinbase'),
         (SELECT asset_id FROM assets WHERE asset_key = 'btc'),
         (SELECT source_id FROM data_sources
          WHERE source_key = 'coinbase_exchange_spot_rest_v1'),
         repeat('0', 64)),
        ('coinbase', 'ETH',
         (SELECT exchange_id FROM exchanges WHERE exchange_key = 'coinbase'),
         (SELECT asset_id FROM assets WHERE asset_key = 'eth'),
         (SELECT source_id FROM data_sources
          WHERE source_key = 'coinbase_exchange_spot_rest_v1'),
         repeat('1', 64)),
        ('crypto_agent_consensus', 'BTC',
         (SELECT exchange_id FROM exchanges WHERE exchange_key = 'crypto_agent_consensus'),
         (SELECT asset_id FROM assets WHERE asset_key = 'btc'),
         (SELECT source_id FROM data_sources
          WHERE source_key = 'cross_exchange_spot_consensus_v1'),
         repeat('2', 64)),
        ('crypto_agent_consensus', 'ETH',
         (SELECT exchange_id FROM exchanges WHERE exchange_key = 'crypto_agent_consensus'),
         (SELECT asset_id FROM assets WHERE asset_key = 'eth'),
         (SELECT source_id FROM data_sources
          WHERE source_key = 'cross_exchange_spot_consensus_v1'),
         repeat('3', 64))
) AS registry(venue, symbol, exchange_id, asset_id, source_id, content_hash)
ON CONFLICT (market_key) DO NOTHING;

INSERT INTO market_symbols (
    market_id, symbol_namespace, symbol, source_id, source_record_key,
    source_version, revision_no, observed_at, available_at, ingested_at, content_hash
) VALUES
    ((SELECT market_id FROM markets WHERE market_key = 'kraken:btc-usd'),
     'kraken', 'XBTUSD',
     (SELECT source_id FROM data_sources WHERE source_key = 'kraken_spot_rest_v1'),
     'market-symbol:kraken:XBTUSD', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('4', 64)),
    ((SELECT market_id FROM markets WHERE market_key = 'kraken:eth-usd'),
     'kraken', 'ETHUSD',
     (SELECT source_id FROM data_sources WHERE source_key = 'kraken_spot_rest_v1'),
     'market-symbol:kraken:ETHUSD', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('5', 64)),
    ((SELECT market_id FROM markets WHERE market_key = 'coinbase:btc-usd'),
     'coinbase', 'BTC-USD',
     (SELECT source_id FROM data_sources
      WHERE source_key = 'coinbase_exchange_spot_rest_v1'),
     'market-symbol:coinbase:BTC-USD', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('6', 64)),
    ((SELECT market_id FROM markets WHERE market_key = 'coinbase:eth-usd'),
     'coinbase', 'ETH-USD',
     (SELECT source_id FROM data_sources
      WHERE source_key = 'coinbase_exchange_spot_rest_v1'),
     'market-symbol:coinbase:ETH-USD', 'v1', 1,
     '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('7', 64))
ON CONFLICT (source_id, source_record_key, revision_no) DO NOTHING;

INSERT INTO risk_policies (
    policy_key, policy_version, effective_from, policy_document, source_id,
    source_record_key, source_version, revision_no, observed_at, available_at,
    ingested_at, content_hash
) VALUES (
    'v1-read-only', '2026-08-10-r2', '2026-01-01 00:00:00+00',
    $policy${
      "policy_id": "v1-read-only-2026-08-10-r2",
      "policy_schema_version": 2,
      "mode": "V1_READ_ONLY",
      "execution_enabled": false,
      "allowed_decisions": ["NO_SIGNAL", "ALERT"],
      "allowed_assets": ["BTC/USD", "ETH/USD"],
      "allowed_intervals_minutes": [240, 1440, 10080],
      "min_samples": 60,
      "min_data_quality": 0.85,
      "max_staleness_multiplier": 1.25,
      "max_reference_price_age_seconds": 300,
      "max_gap_multiplier": 1.5,
      "max_annualized_volatility": 1.2,
      "max_absolute_period_return": 0.2,
      "report_ttl_seconds": 3600,
      "max_clock_skew_seconds": 30,
      "min_consensus_sources": 2,
      "min_consensus_overlap": 120,
      "max_reference_price_deviation_from_median_bps": 50.0,
      "max_cross_source_divergence_bps": 100.0,
      "max_cross_source_ohlc_divergence_bps": 500.0,
      "max_cross_source_volume_zscore_delta": 3.0,
      "max_divergent_candle_fraction": 0.0,
      "max_market_price": 1000000000.0,
      "max_base_volume": 1000000000000.0,
      "leverage_allowed": false,
      "martingale_allowed": false,
      "exchange_credentials_allowed": false,
      "human_approval_required_for_execution": true
    }$policy$::jsonb,
    (SELECT source_id FROM data_sources WHERE source_key = 'crypto_agent_registry_v1'),
    'risk-policy:v1-read-only:2026-08-10-r2', 'v1', 1,
    '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
    '2026-01-01 00:00:00+00',
    'c44f0366fae8cb8605855999c9afb8ff9c55a0777c4deb423330632b78dc7ec8'
)
ON CONFLICT (policy_key, policy_version) DO NOTHING;

INSERT INTO market_data_source_bindings (
    source_id, market_id, canonical_symbol, venue_symbol,
    observed_at, available_at, ingested_at, content_hash
) VALUES
    ((SELECT source_id FROM data_sources WHERE source_key = 'kraken_spot_rest_v1'),
     (SELECT market_id FROM markets WHERE market_key = 'kraken:btc-usd'),
     'BTC/USD', 'XBTUSD', '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('8', 64)),
    ((SELECT source_id FROM data_sources WHERE source_key = 'kraken_spot_rest_v1'),
     (SELECT market_id FROM markets WHERE market_key = 'kraken:eth-usd'),
     'ETH/USD', 'ETHUSD', '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('9', 64)),
    ((SELECT source_id FROM data_sources
      WHERE source_key = 'coinbase_exchange_spot_rest_v1'),
     (SELECT market_id FROM markets WHERE market_key = 'coinbase:btc-usd'),
     'BTC/USD', 'BTC-USD', '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('a', 64)),
    ((SELECT source_id FROM data_sources
      WHERE source_key = 'coinbase_exchange_spot_rest_v1'),
     (SELECT market_id FROM markets WHERE market_key = 'coinbase:eth-usd'),
     'ETH/USD', 'ETH-USD', '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
     '2026-01-01 00:00:00+00', repeat('b', 64))
ON CONFLICT (source_id, market_id) DO NOTHING;

INSERT INTO canonical_candle_series (
    series_key, canonical_market_id, canonical_source_id, risk_policy_id,
    canonical_symbol, interval_seconds, algorithm_version, policy_hash,
    observed_at, available_at, ingested_at, content_hash
)
SELECT 'canonical:' || lower(replace(symbol, '/', '-')) || ':' || interval_seconds,
       CASE symbol
           WHEN 'BTC/USD' THEN
               (SELECT market_id FROM markets
                WHERE market_key = 'crypto_agent_consensus:btc-usd')
           ELSE
               (SELECT market_id FROM markets
                WHERE market_key = 'crypto_agent_consensus:eth-usd')
       END,
       (SELECT source_id FROM data_sources
        WHERE source_key = 'cross_exchange_spot_consensus_v1'),
       (SELECT risk_policy_id FROM risk_policies
        WHERE policy_key = 'v1-read-only' AND policy_version = '2026-08-10-r2'),
       symbol, interval_seconds, 'cross_exchange_spot_consensus_v1',
       'c44f0366fae8cb8605855999c9afb8ff9c55a0777c4deb423330632b78dc7ec8',
       '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00',
       '2026-01-01 00:00:00+00',
       CASE symbol || ':' || interval_seconds
           WHEN 'BTC/USD:14400' THEN repeat('c', 64)
           WHEN 'BTC/USD:86400' THEN repeat('d', 64)
           WHEN 'BTC/USD:604800' THEN repeat('e', 64)
           WHEN 'ETH/USD:14400' THEN repeat('f', 64)
           WHEN 'ETH/USD:86400' THEN repeat('0', 64)
           ELSE repeat('1', 64)
       END
FROM (VALUES
    ('BTC/USD', 14400), ('BTC/USD', 86400), ('BTC/USD', 604800),
    ('ETH/USD', 14400), ('ETH/USD', 86400), ('ETH/USD', 604800)
) AS required_series(symbol, interval_seconds)
ON CONFLICT (series_key) DO NOTHING;
