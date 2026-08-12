# Model danych T4 i monitoringu

Aktywny rejestr zawiera:

- source: `plus500_t4_futures_v1`;
- venue: `plus500_t4`;
- binding `BTC/USD` → `BTC-FUTURES-FRONT`;
- binding `ETH/USD` → `ETH-FUTURES-FRONT`;
- runtime protocol: `loopback_http_json_v1`;
- operacyjny envelope schema: `5` z `exchange_id`, produktowym `contract_id`,
  nieprzezroczystym `market_id`, `rolled_from_market_id`, per-candle `market_id`,
  niezależną tożsamością indeksu, `futures_evidence` i jawnym środowiskiem
  `t4_simulator` albo `live_t4`;
- `read_only=true` i `order_routes_enabled=false`.

Nazwy `*-FUTURES-FRONT` są logicznymi aliasami. `ContractID` identyfikuje produkt,
a `MarketID` konkretną handlowalną serię/expiry. Worker zachowuje wartość
`MarketID` dokładnie tak, jak zwróciło ją T4, także osobno dla każdej świecy. Nie
wolno jej parsować ani konstruować. Bez tej proweniencji batch nie może zostać
uznany za operacyjny.

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

Migracja `0015` rozszerza append-only model schema v3:

- `t4_futures_snapshots` wiąże dokładnie jeden batch z kontraktem, statusem sesji,
  czasami snapshotu oraz typed basis reference;
- basis reference musi mieć typ `index`, source ID `plus500_t4_index_v1` i symbol
  zgodny z logicznym symbolem batcha;
- `t4_orderbook_levels` zapisuje uporządkowane poziomy bid/ask i hash treści
  każdego poziomu;
- `t4_contract_transition_evidence` zapisuje zsynchronizowane ceny typu `mid`
  starego i nowego kontraktu po kontrolowanym rollu;
- snapshot i transition mają własne `content_hash`, a surowy batch zachowuje
  niezależny `raw_payload_hash`;
- triggery blokują `UPDATE`, `DELETE` i `TRUNCATE` wszystkich nowych tabel.

Replay schema v3 odtwarza pełny `FuturesEvidence` i włącza go do fingerprintu
wejścia analizy. Komenda `analyze-replay` używa tego samego risk gate co analiza
bieżąca, ale wynik replayu nigdy nie kwalifikuje się do external delivery. Schema
v2 pozostaje obsługiwana tylko dla historycznego odczytu: nie ma wierszy evidence
z migracji `0015`, więc metryki mają status `UNAVAILABLE`, a decyzja musi być
`NO_SIGNAL`. W wersji 0.8.0 tylko bieżący payload schema v5 z pełną tożsamością,
niezależnym indeksem i `environment=live_t4` może wejść do kwalifikacji live
alertu. `t4_simulator` jest zawsze wykluczony.

Raport futures rozróżnia `AVAILABLE`, `UNAVAILABLE`, `INVALID` oraz
`NOT_APPLICABLE`; brakująca wartość nigdy nie jest kodowana jako zero. Model
metryk obejmuje spread w bps, depth obu stron, imbalance, basis w bps,
annualized basis, z-score i względny wolumen, czas do rollu i expiry, expiry risk
oraz wpływ rollu.

## Monitoring 0.7.0

Monitoring wykorzystuje istniejące tabele badawcze zamiast duplikować model:

- `research_runs`, `research_run_events` i `research_artifacts` przechowują trace,
  stan operacji i bezpieczny, zredukowany artifact raportu;
- `data_quality_incidents` i `data_quality_incident_events` przechowują
  deduplikowane incydenty, bez surowych wyjątków i danych dostępowych;
- `alerts` i `alert_events` przechowują wyłącznie kwalifikujące się decyzje live
  `ALERT` oraz ich cykl życia.

Migracja `0016` dodaje dwa append-only zbiory:

- `alert_delivery_outbox` wiąże alert z jedyną trasą
  `stdout_json` → `process_stdout`, kanonicznym payloadem, SHA-256, idempotency key,
  oknem dostępności/wygaśnięcia i maksymalną liczbą prób;
- `alert_delivery_attempts` przechowuje wynik każdej próby, numer sekwencji,
  payload hash, poprzedni hash, content hash, czas i bezpieczny kod błędu.

Triggery wymuszają kolejność prób, zgodność hasha payloadu, retry nie wcześniejszy
niż wyznaczona granica, limit prób, terminalność oraz okno expiry. `UPDATE`,
`DELETE` i `TRUNCATE` obu tabel są zabronione.

Research run/artifact, alert, alert event i outbox powstają w jednej transakcji
PostgreSQL. SQLite nie uczestniczy w tej transakcji i nie należy interpretować
tego jako atomowości między różnymi silnikami baz danych.

Domyślna wersjonowana polityka ustawia horyzont retencji na 90 dni. Model audytowy
pozostaje append-only; wartość retencji nie oznacza automatycznego kasowania
rekordów przez worker delivery.

Outbox jest trwały, ale granica zapisu do stdout ma semantykę at-least-once.
Konsument musi deduplikować po `idempotency_key`; model nie obiecuje exactly-once
między bazą a procesem odbiorcy.

## T4 schema v5 i kampania odbiorowa 0.8.0

Migracja `0017` rozszerza batch, świece i futures evidence o pełną tożsamość T4:

- batch przechowuje środowisko, `exchange_id`, produktowy `contract_id`, aktywny
  i poprzedni `market_id` oraz tożsamość niezależnego rynku basis;
- każda świeca przechowuje `market_id` zwrócony przez Chart REST;
- snapshot wiąże futures oraz indeks z dokładnym batchem i ich tożsamościami;
- transition evidence wiąże `from_market_id` i `to_market_id`, nie domyślone nazwy
  kontraktów;
- append-only triggery nadal blokują `UPDATE`, `DELETE` i `TRUNCATE`.

Ta sama migracja dodaje cztery zbiory odbiorowe:

- `t4_observation_campaigns` — niezmienny baseline: czas startu/końca, scope,
  interwał cyklu oraz hashe polityki, kodu, protokołu i konfiguracji;
- `t4_observation_cycles` — append-only wynik każdego planowego cyklu z hash
  chain oraz powiązaniem udanego odczytu z batchem i runem analizy;
- `t4_observation_session_events` — append-only zdarzenia i kontrolowane
  scenariusze z osobnym hash chain;
- `t4_observation_quality_reports` — niezmienny raport końcowy i decyzja bramki.

Triggery wykorzystują czas bazy i blokują deklarowanie cykli z wyprzedzeniem oraz
backfill. Raport rozróżnia `PASS`, `FAIL` i `NOT_OBSERVED`, a jego finalizacja
jest dozwolona dopiero po `planned_ends_at + cycle_interval_seconds`. Schema
`0.8.0` celowo akceptuje dla scenariusza wyłącznie `fail` i odrzuca
`v1_gate_passed=true`, ponieważ nie ma jeszcze obiektywnych referencji dowodów
scenariuszy. Późniejsza migracja musi dodać te referencje i ich walidację, zanim
wynik dodatni stanie się osiągalny.

Provisioning i rzeczywiste testy Simulator/live nie zostały wykonane, kampania nie
została rozpoczęta; zakres odbioru 0.8.0 to BTC/ETH 4h, a
`v1_gate_passed=false`.
