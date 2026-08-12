# Plus500 Futures T4 Research Agent

Wersja `0.7.0` analizuje wyłącznie dane futures z **Plus500 Futures / T4**.
System działa tylko w trybie odczytu i może zwrócić `ALERT` albo `NO_SIGNAL`.
Nie loguje się do innych platform i nie składa, nie zmienia ani nie anuluje zleceń.

## Źródło danych

Proces analityczny łączy się wyłącznie z lokalnym mostem T4 pod
`http://127.0.0.1:8784`. Most .NET jest granicą dla docelowej sesji oficjalnego
T4 API i wystawia wyłącznie dane rynkowe. Dane logowania T4 nigdy nie trafiają do
procesu Python. Połączenie loopback wymaga dodatkowo wspólnego, losowego tokenu.

Oficjalny klient T4 nie jest jeszcze podłączony. Bieżący
`T4ApplicationRegistrationPendingReader` zwraca `503`, więc próba analizy live
kończy się bezpiecznym `NO_SIGNAL` i rejestracją lokalnego incydentu. Dane fixture
używane w testach nie są danymi live i nie potwierdzają połączenia z T4 Simulator.

Obsługiwany zakres V1:

- logiczny front-month Bitcoin futures: `BTC-FUTURES-FRONT`;
- logiczny front-month Ether futures: `ETH-FUTURES-FRONT`;
- interwały 4h, 1d i 1w;
- minimum 120 świec oraz pełny snapshot order booka;
- metryki wolumenu, spreadu, depth, imbalance, basis, annualized basis, expiry i
  wpływu rollu;
- jedna zatwierdzona proweniencja futures: `plus500_t4_futures_v1`;
- jedyna zatwierdzona referencja basis: typ `index` ze źródła
  `plus500_t4_index_v1`.

## Szybki test offline

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
crypto-agent analyze --provider synthetic --symbol BTC/USD --interval 1440
```

Tryb `synthetic` służy wyłącznie testom i zawsze pozostaje diagnostyczny. Jego
wyniki, podobnie jak replay i fixture, nigdy nie trafiają do delivery outboxa.

## Uruchomienie z T4

1. Skopiuj `.env.example` do `.env`.
2. Ustaw ten sam, losowy `T4_BRIDGE_TOKEN` w workerze i
   `CRYPTO_AGENT_T4_BRIDGE_TOKEN` w procesie Python.
3. Ustaw `T4_CONTRACT_CATALOG_PATH` na prywatną kopię katalogu opartą na
   `configs/t4-contract-catalog.example.json` i wpisz rzeczywiste serie T4.
4. Uruchom odizolowany worker T4 .NET na loopback `127.0.0.1:8784`.
5. Ustaw `CRYPTO_AGENT_DATA_PROVIDER=t4`.
6. Uruchom:

```bash
crypto-agent analyze --provider t4 --symbol BTC/USD --interval 1440
```

Host bridge i kontrakt live schema v4 są zbudowane. Payload kwalifikowany jako
live musi dodatkowo zawierać `environment=live_t4`. Do czasu zarejestrowania
aplikacji T4 i podłączenia oficjalnego klienta polecenie bezpiecznie zwróci
`NO_SIGNAL` z kodem `T4_BRIDGE_UNAVAILABLE`.

## PostgreSQL 16

```bash
docker compose up -d postgres
crypto-agent db plan
crypto-agent db migrate
crypto-agent db seed
crypto-agent db health
```

Prawidłowy wynik końcowy to `READY`. Migracje `0011` i `0012` są zachowane bez
zmian jako historia wcześniejszego prototypu. Migracja `0013` ustanawia T4 jako
jedyne operacyjne źródło, `0014` dodaje append-only ingest i point-in-time replay,
`0015` utrwala futures evidence, a `0016` dodaje trwały, append-only outbox alertów
i historię prób dostarczenia. Nowy seed nie zawiera konfiguracji innych platform.

Po podłączeniu oficjalnego klienta T4 pojedynczy batch można zapisać poleceniem:

```bash
crypto-agent ingest --symbol BTC/USD --interval 1440
```

Tryb cykliczny dodaje `--watch --poll-seconds 300`. Replay nie łączy się z T4 i
odtwarza wyłącznie dane dostępne w zadanym czasie:

```bash
crypto-agent replay --symbol BTC/USD --interval 1440 \
  --as-of 2026-08-11T00:00:00+00:00
```

Deterministyczną analizę tego samego replayu uruchamia:

```bash
crypto-agent analyze-replay --symbol BTC/USD --interval 1440 \
  --as-of 2026-08-11T00:00:00+00:00
```

Historyczne batche schema v2 i v3 pozostają odczytywalne przez replay. Schema v2
nie zawiera pełnego futures evidence i dlatego analiza zawsze kończy się
`NO_SIGNAL`; schema v3 zachowuje pełny evidence, ale jako dane replay nigdy nie
kwalifikuje się do external delivery. Operacyjny alert live wymaga schema v4,
`environment=live_t4` i jawnej atestacji kwalifikacji do delivery.

## Monitoring i lokalne dostarczanie alertów

Punkt 7 jest ukończony w zakresie offline. Cykliczny monitoring T4, lokalny
dashboard, trace, deduplikowane incydenty oraz trwały delivery outbox uruchamiają:

```bash
crypto-agent monitor --symbol BTC/USD --interval 1440 --watch --poll-seconds 300
crypto-agent deliver-alerts --watch --poll-seconds 5
crypto-agent monitoring-status
```

Jedynym zatwierdzonym kanałem w `0.7.0` jest kanoniczny JSON zapisany do stdout
(`stdout_json` → `process_stdout`). Komunikaty stanu workera delivery trafiają do
stderr. Nie ma webhooków ani integracji Slack, Telegram, e-mail lub SMS.
Dashboard i read-only API są dostępne wyłącznie na loopback; interfejs OpenAPI,
Swagger i ReDoc są wyłączone. Szczegóły operacyjne opisuje
[runbook monitoringu](docs/MONITORING_RUNBOOK.md).

Outbox jest trwały, a zapis do stdout ma semantykę at-least-once. Konsument musi
deduplikować rekordy po polu `idempotency_key`; system nie deklaruje exactly-once
na granicy procesu.

Analiza przez API zapisuje trace i dlatego jest dostępna wyłącznie jako
`POST /v1/analyze` z wymaganym nagłówkiem
`X-Crypto-Agent-Request: analyze-v1`. Wariant `GET /v1/analyze` nie istnieje.

Zapis run/artifact + alert + alert event + outbox jest atomowy w jednej transakcji
PostgreSQL. Nie oznacza to transakcji rozproszonej z historycznym magazynem SQLite.

`v1_gate_passed=false`. Następny etap to punkt 8: test live po otrzymaniu dostępu
T4, minimum cztery tygodnie obserwacji read-only i raport jakości.

Szczegóły: [instrukcja uruchomienia](docs/INSTRUKCJA_URUCHOMIENIA.md),
[architektura](docs/ARCHITECTURE.md), [stan projektu](docs/CURRENT_STATUS.md).

Oficjalne informacje o API: [Plus500 Futures T4 API](https://futures-technologies.plus500.com/api/).
