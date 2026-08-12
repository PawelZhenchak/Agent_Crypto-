# Instrukcja uruchomienia Plus500 Futures / T4

## 1. Instalacja

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
cp .env.example .env
```

Na Windows aktywacja środowiska to `.venv\Scripts\activate`.

## 2. Bezpieczny test

```bash
crypto-agent analyze --provider synthetic --symbol BTC/USD --interval 1440
```

Ten test nie łączy się z żadną platformą i nigdy nie kwalifikuje się do delivery.

## 3. PostgreSQL

Uzupełnij lokalnie `POSTGRES_PASSWORD` i `CRYPTO_AGENT_POSTGRES_DSN`. Nie commituj
pliku `.env`.

```bash
docker compose up -d postgres
crypto-agent db migrate
crypto-agent db seed
crypto-agent db health
```

Health wymaga PostgreSQL 16, migracji `0011`–`0016`, dwóch bindingów T4 oraz
jednego runtime config z wyłączonymi trasami zleceń. Migracja `0016` tworzy
immutable delivery outbox i append-only historię prób.

## 4. T4 Simulator

Załóż konto Simulator zgodnie z oficjalną stroną Plus500 Futures. Login i hasło
zapisz wyłącznie w prywatnej konfiguracji workera .NET. Nie wpisuj ich do `.env`
agenta Python i nie wysyłaj ich do repozytorium.

Samo konto Simulator nie wystarcza do API. Zgodnie z oficjalną dokumentacją
trzeba poprosić CTS o zarejestrowanie aplikacji. Po otrzymaniu aktualnego pakietu
i przykładów podłączamy adapter klienta do interfejsu `IT4MarketDataReader`.
Bieżący `T4ApplicationRegistrationPendingReader` nie jest oficjalnym klientem i
zwraca `503`; do tego czasu każde wywołanie T4 kończy się `NO_SIGNAL`, a monitoring
rejestruje lokalny incydent zamiast alertu.

Wygeneruj losowy token minimum 32 znaki i ustaw tę samą wartość prywatnie jako:

- `T4_BRIDGE_TOKEN` w procesie .NET;
- `CRYPTO_AGENT_T4_BRIDGE_TOKEN` w procesie Python.

Skopiuj `configs/t4-contract-catalog.example.json` poza repozytorium, zastąp
wartości `SIM:*` rzeczywistymi identyfikatorami serii otrzymanymi z T4 i ustaw
`T4_CONTRACT_CATALOG_PATH` na ścieżkę tej prywatnej kopii. Katalog musi zawierać
co najmniej bieżącą i następną serię dla każdego aktywnego symbolu, aby roll nie
zatrzymał odczytu.

Następnie uruchom granicę bezpieczeństwa:

```bash
dotnet run --project t4-bridge/CryptoAgent.T4Bridge.csproj
```

Po uruchomieniu mostu na `127.0.0.1:8784`:

```bash
crypto-agent analyze --provider t4 --symbol BTC/USD --interval 1440
```

Most live musi zwrócić schema v4 i potwierdzić `environment=live_t4`,
`read_only=true`, `order_routes_exposed=false`, właściwe source/venue ID,
rzeczywisty i niewygasły contract ID, `roll_at`, proweniencję ewentualnego
przełączenia oraz 120 świec.
`futures_evidence` musi zawierać pełny order book, status sesji i referencję basis
typu `index` ze źródła dokładnie `plus500_t4_index_v1`; po rollu wymagane są też
zsynchronizowane ceny obu kontraktów. Każda niezgodność kończy się `NO_SIGNAL`.

Payloady fixture wykorzystywane przez testy jednostkowe i odbiór PostgreSQL są
danymi sztucznymi. Nie są testem live i nie potwierdzają sesji T4 Simulator.

## 5. Ingest i replay

Po uruchomieniu prawdziwej sesji T4 zapisz pojedynczy batch:

```bash
crypto-agent ingest --symbol BTC/USD --interval 1440
```

Scheduler można uruchomić przez `--watch --poll-seconds 300`. Każdy batch trafia
do PostgreSQL atomowo; identyczny SHA-256 jest rozpoznawany jako duplikat.

Historyczny replay działa bez połączenia z T4:

```bash
crypto-agent replay --symbol BTC/USD --interval 1440 \
  --as-of 2026-08-11T00:00:00+00:00
```

Replay nie zwraca niepełnego okna. Jeśli przed cutoffem nie ma wymaganej liczby
świec, kończy się bezpiecznym `T4_REPLAY_INCOMPLETE`.

Aby odtworzyć pełny raport analityczny dla tego samego cutoffu, uruchom:

```bash
crypto-agent analyze-replay --symbol BTC/USD --interval 1440 \
  --as-of 2026-08-11T00:00:00+00:00
```

Schema v3 odtwarza snapshot order booka, typed basis reference i ewentualny dowód
rollu wraz z hashami w deterministycznym fingerprintcie. Historyczne batche v2 i
v3 pozostają odczytywalne przez `replay`; `analyze-replay` zwraca dla v2
`NO_SIGNAL`, ponieważ nie ma pełnego futures evidence. Żaden replay, także v3,
nie może utworzyć przesyłki alertowej.

## 6. Monitoring i delivery

Pojedynczy odporny cykl analizy T4 wraz z zapisem trace uruchamia:

```bash
crypto-agent monitor --symbol BTC/USD --interval 1440
```

Tryb ciągły:

```bash
crypto-agent monitor --symbol BTC/USD --interval 1440 \
  --watch --poll-seconds 300
```

Osobny worker pobiera kwalifikujące się alerty z trwałego outboxa. Kanoniczny JSON
alertu zapisuje do stdout, a status operacyjny do stderr:

```bash
crypto-agent deliver-alerts
crypto-agent deliver-alerts --watch --poll-seconds 5
```

Jedyną dozwoloną trasą jest `stdout_json` → `process_stdout`. Nie konfiguruj
webhooka ani Slack, Telegram, e-mail lub SMS — 0.7.0 nie ma takich kanałów.
`NO_SIGNAL`, veto, dane stare, synthetic, fixture, replay, provider error i alert
po expiry nie są dostarczane.

Outbox jest trwały, ale stdout ma semantykę at-least-once. Proces odbierający JSON
musi deduplikować po `idempotency_key`; nie zakładaj exactly-once po restarcie lub
zerwaniu procesu.

Lokalną projekcję w JSON odczytasz przez:

```bash
crypto-agent monitoring-status
```

Read-only API i statyczny dashboard można uruchomić wyłącznie na loopback:

```bash
uvicorn crypto_agent.api:app --host 127.0.0.1 --port 8000
```

Dostępne projekcje:

- `GET /health`;
- `POST /v1/analyze` z nagłówkiem `X-Crypto-Agent-Request: analyze-v1`;
- `GET /v1/monitoring/summary`;
- `GET /v1/monitoring/alerts?limit=50`;
- `GET /v1/monitoring/incidents?limit=50`;
- `GET /v1/monitoring/traces/{trace_id}`;
- `GET /dashboard`.

Przykład analizy API, która zapisuje trace:

```bash
curl -X POST \
  -H 'X-Crypto-Agent-Request: analyze-v1' \
  'http://127.0.0.1:8000/v1/analyze?provider=synthetic&symbol=BTC%2FUSD&interval_minutes=1440'
```

`GET /v1/analyze` nie istnieje. Wymagany niestandardowy nagłówek oraz brak CORS
chronią lokalną operację przed prostym żądaniem cross-origin z przeglądarki.

API odrzuca klientów spoza loopback. CORS, OpenAPI, Swagger i ReDoc są wyłączone.
Dashboard nie zawiera JavaScriptu, formularzy, linków ani zdalnych zasobów.

Domyślna polityka `configs/monitoring_policy.v1.json` ma horyzont retencji 90 dni,
trzy próby i ograniczony backoff. Alternatywny plik można wskazać przez
`CRYPTO_AGENT_MONITORING_POLICY_PATH`, ale walidator nadal dopuszcza wyłącznie
lokalny kanał stdout. Polityka retencji nie uruchamia kasowania append-only audytu.

Zapis monitoringu jest atomowy wyłącznie w PostgreSQL: run/artifact + alert +
alert event + outbox. Nie obejmuje osobnego historycznego magazynu SQLite.

## 7. Aktualna bramka V1

`v1_gate_passed=false`. Punkt 7 jest ukończony offline. Oficjalny klient i test
live pozostają niegotowe; następny jest punkt 8: testy live, minimum cztery
tygodnie obserwacji read-only oraz raport jakości.
