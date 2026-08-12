# Instrukcja uruchomienia Plus500 Futures / T4

## 1. Instalacja i test offline

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
cp .env.example .env
crypto-agent analyze --provider synthetic --symbol BTC/USD --interval 240
```

Na Windows aktywacja środowiska to `.venv\Scripts\activate`. Synthetic nie
łączy się z T4 i nigdy nie kwalifikuje się do delivery ani odbioru V1.

## 2. PostgreSQL 16

Uzupełnij prywatnie `POSTGRES_PASSWORD` i `CRYPTO_AGENT_POSTGRES_DSN`. Nie
commituj `.env`.

```bash
docker compose up -d postgres
crypto-agent db migrate
crypto-agent db seed
crypto-agent db health
```

Health wymaga migracji do `0017`. Ostatnia migracja rozszerza magazyn o schema v5
oraz append-only kampanie, cykle, zdarzenia sesji i końcowe raporty odbiorowe.

## 3. Provisioning T4

Worker `0.8.0` używa oficjalnych `Plus500US.T4Proto` `1.0.73` oraz
`Plus500US.T4ChartDecoder` `1.0.97` i protokołu przypiętego do publicznego commita
`1a68b674482194f1cf3b9d7f129ce5fbed8bcb51`. Implementacja klienta nie zapewnia
jednak dostępu. Przed testem potrzebne są:

- aktywny `T4_API_KEY` dla Simulator albo live;
- uprawnienia Market By Price/depth dla używanych giełd;
- rzeczywiste `ExchangeID`, produktowe `ContractID` i nieprzezroczyste `MarketID`
  bieżących oraz następnych serii BTC i ETH;
- osobne identyfikatory rynku indeksowego używanego do basis;
- przedłużenie Simulator albo live na pełną kampanię — standardowe dwa tygodnie
  Simulator nie pokrywają wymaganych 28 dni.

Klucza API i prywatnego katalogu nie wysyłaj do procesu Python ani repozytorium.

## 4. Prywatny katalog schema v2

Skopiuj `configs/t4-contract-catalog.example.json` poza repozytorium. Dla każdej
serii wpisz wartości zwrócone przez T4:

- `exchange_id`, `contract_id`, `market_id`;
- `expires_at` i opcjonalny `roll_at`;
- `basis_exchange_id`, `basis_contract_id`, `basis_market_id`.

`ContractID` oznacza produkt, a `MarketID` konkretną handlowalną serię. `MarketID`
jest nieprzezroczysty: zachowaj go dokładnie, bez parsowania i normalizacji.
Tożsamość basis musi wskazywać niezależny indeks, nie ten sam rynek futures.

## 5. Worker .NET

Ustaw losowy token o długości 32–256 znaków oraz środowisko:

```bash
export T4_BRIDGE_TOKEN='<losowy-token>'
export T4_CONTRACT_CATALOG_PATH='/private/t4-contracts.json'
export T4_API_ENVIRONMENT='simulator' # albo live
export T4_API_KEY='<prywatny-klucz>'
dotnet run --project t4-bridge/CryptoAgent.T4Bridge.csproj
```

W procesie Python ustaw ten sam token jako
`CRYPTO_AGENT_T4_BRIDGE_TOKEN`. Środowisko wybiera stałe oficjalne endpointy:

- `simulator`: `wss-sim.t4login.com` + `api-sim.t4login.com`, envelope
  `t4_simulator`;
- `live`: `wss.t4login.com` + `api.t4login.com`, envelope `live_t4`;
- `pending`: bez klucza, celowo `NOT_READY`/`503`.

Worker pobiera depth przez WebSocket/Protobuf, a świece przez Chart REST. Health
przechodzi w `READY` dopiero po zalogowaniu, weryfikacji uprawnień i prewarmie
cache. Brak danych kończy się `503` i `NO_SIGNAL`, nigdy fallbackiem.

## 6. Test Simulator i live

Po `READY` wykonaj wyłącznie odczytowy test:

```bash
crypto-agent analyze --provider t4 --symbol BTC/USD --interval 240
crypto-agent ingest --symbol BTC/USD --interval 240
crypto-agent replay --symbol BTC/USD --interval 240 \
  --as-of 2026-08-12T00:00:00+00:00
crypto-agent analyze-replay --symbol BTC/USD --interval 240 \
  --as-of 2026-08-12T00:00:00+00:00
```

Envelope operacyjny ma schema v5. Każda świeca zachowuje własny `MarketID`,
futures evidence zawiera pełny order book, a basis pochodzi z niezależnego indeksu
`plus500_t4_index_v1`. Po rollu wymagane są zsynchronizowane snapshoty starego i
nowego `MarketID`.

Simulator ma `environment=t4_simulator`: jego alert nigdy nie trafia do external
delivery. Dopiero osobny test live z `environment=live_t4` sprawdza kwalifikację
alertową. Fixture’y i replay również nie są testem live.

Bridge/parser zachowują interwały 4h/1d/1w, ale bieżący zakres odbiorowy V1 jest
ograniczony do BTC i ETH na 4h (`240` minut).

## 7. Monitoring i delivery

```bash
crypto-agent monitor --symbol BTC/USD --interval 240 --watch --poll-seconds 300
crypto-agent deliver-alerts --watch --poll-seconds 5
crypto-agent monitoring-status
```

Jedyna dozwolona trasa to `stdout_json` → `process_stdout`. Nie konfiguruj
webhooka, Slacka, Telegrama, e-maila ani SMS. Trwały outbox ma semantykę
at-least-once; odbiorca deduplikuje po `idempotency_key`.

Read-only API i dashboard uruchamiaj wyłącznie na loopback:

```bash
uvicorn crypto_agent.api:app --host 127.0.0.1 --port 8000
```

Analiza API jest tylko operacją `POST /v1/analyze` z nagłówkiem
`X-Crypto-Agent-Request: analyze-v1`. CORS, OpenAPI, Swagger i ReDoc są wyłączone.

## 8. Kampania 28-dniowa

Nie rozpoczynaj kampanii na fixture ani Simulatorze bez zapewnionego okna co
najmniej 672 godzin. Po zaliczonym teście live zamroź zakres, kod, publiczny stan
protokołu, politykę i runtime config. CLI wymaga lowercase SHA-256 atestacji
artefaktów baseline:

```bash
crypto-agent observe-start \
  --code-commit-hash <sha256> \
  --t4-protocol-commit-hash <sha256> \
  --runtime-config-hash <sha256>
```

Domyślne scope’y to `BTC/USD:240m` oraz `ETH/USD:240m`. Zachowaj zwrócone
`campaign_id`. Przed zapisem `observe-start` wykonuje live preflight schema v5
każdego scope’u. Start pochodzi z zegara PostgreSQL i nie może być backfillowany.

```bash
crypto-agent observe-run --campaign-id <uuid> --scope BTC/USD:240m --limit 120
crypto-agent observe-run --campaign-id <uuid> --scope ETH/USD:240m --limit 120
crypto-agent observe-status --campaign-id <uuid>
crypto-agent observe-report --campaign-id <uuid>
```

Supervisor wywołuje `observe-run` dla obu zamrożonych scope’ów. Komenda korzysta
z harmonogramu i czasu PostgreSQL, pobiera live batch schema v5 tylko raz, a ten
sam obiekt przekazuje do ingestu i zamkniętego providera analizy. Sukces wiąże
batch, run, trace i raw hash. Retry w tym samym slocie nie pobiera danych, a
wygasłe sloty mogą zostać zapisane tylko jako `missed`. Szczegóły zawiera
[runbook obserwacji](T4_OBSERVATION_RUNBOOK.md).

`observe-report` odmawia zapisu przed
`planned_ends_at + cycle_interval_seconds`; dodatkowy interwał jest okresem grace
na ostatni planowy slot. W schema `0.8.0` raport może być wyłącznie `FAIL` lub
`NOT_OBSERVED`: baza celowo blokuje scenariusz `PASS` i `v1_gate_passed=true` do
późniejszej migracji z obiektywnymi referencjami dowodów oraz ich walidacją.

Provisioning, rzeczywiste testy Simulator/live i kampania nie zostały wykonane,
więc obecny stan to `v1_gate_passed=false`.
