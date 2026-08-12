# Model danych T4

Aktywny rejestr zawiera:

- source: `plus500_t4_futures_v1`;
- venue: `plus500_t4`;
- binding `BTC/USD` → `BTC-FUTURES-FRONT`;
- binding `ETH/USD` → `ETH-FUTURES-FRONT`;
- runtime protocol: `loopback_http_json_v1`;
- envelope schema: `2` z `contract_roll_at`, `contract_selection` i
  `rolled_from_contract_id`;
- `read_only=true` i `order_routes_enabled=false`.

Nazwy `*-FUTURES-FRONT` są logicznymi aliasami. Worker musi utrwalać rzeczywisty
identyfikator kontraktu T4 i regułę roll w każdym batchu. Bez tej proweniencji batch
nie może zostać uznany za operacyjny.

Stare tabele canonical/reference z migracji `0011/0012` pozostają tylko dla
reprodukowalności poprzedniego prototypu. Nowy ingest T4 ma osobny kontrakt w
migracji `0014`:

- `t4_ingestion_batches` zachowuje dokładny payload bridge’a w base64, SHA-256,
  rzeczywisty kontrakt, roll, cutoff i cenę referencyjną;
- `t4_canonical_candles` zachowuje znormalizowane świece oraz hash każdego rekordu;
- oba zbiory są append-only;
- `raw_payload_hash` jest unikalny, więc ponowny odbiór tego samego batcha nie
  tworzy duplikatów;
- replay wybiera wyłącznie rekordy `available_at <= as_of`, rozstrzyga rewizje
  deterministycznie i zwraca fingerprint SHA-256 całego wyniku.
