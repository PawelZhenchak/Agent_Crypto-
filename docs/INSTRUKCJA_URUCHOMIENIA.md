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

Ten test nie łączy się z żadną platformą.

## 3. PostgreSQL

Uzupełnij lokalnie `POSTGRES_PASSWORD` i `CRYPTO_AGENT_POSTGRES_DSN`. Nie commituj
pliku `.env`.

```bash
docker compose up -d postgres
crypto-agent db migrate
crypto-agent db seed
crypto-agent db health
```

Health wymaga PostgreSQL 16, migracji `0011`–`0015`, dwóch bindingów T4 oraz
jednego runtime config z wyłączonymi trasami zleceń.

## 4. T4 Simulator

Załóż konto Simulator zgodnie z oficjalną stroną Plus500 Futures. Login i hasło
zapisz wyłącznie w prywatnej konfiguracji workera .NET. Nie wpisuj ich do `.env`
agenta Python i nie wysyłaj ich do repozytorium.

Samo konto Simulator nie wystarcza do API. Zgodnie z oficjalną dokumentacją
trzeba poprosić CTS o zarejestrowanie aplikacji. Po otrzymaniu aktualnego pakietu
i przykładów podłączamy adapter klienta do interfejsu `IT4MarketDataReader`.
Bieżący `T4ApplicationRegistrationPendingReader` nie jest oficjalnym klientem i
zwraca `503`; do tego czasu każde wywołanie T4 kończy się `NO_SIGNAL`.

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

Most musi zwrócić schema v3 i potwierdzić `read_only=true`,
`order_routes_exposed=false`, właściwe source/venue ID, rzeczywisty i niewygasły
contract ID, `roll_at`, proweniencję ewentualnego przełączenia oraz 120 świec.
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
rollu wraz z hashami w deterministycznym fingerprintcie. Historyczne batche v2
pozostają odczytywalne przez `replay`, lecz `analyze-replay` zwraca dla nich
`NO_SIGNAL`, ponieważ nie mają pełnego futures evidence.

## 6. Aktualna bramka V1

`v1_gate_passed=false`. Oficjalny klient i test live pozostają niegotowe, a
następnym etapem offline jest punkt 7: monitoring, dashboard, trace, incident log
i dostarczanie alertów.
