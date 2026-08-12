# Plus500 Futures T4 Research Agent

Wersja `0.5.0` analizuje wyłącznie dane futures z **Plus500 Futures / T4**.
System działa tylko w trybie odczytu i może zwrócić `ALERT` lub `NO_SIGNAL`.
Nie loguje się do innych platform i nie składa, nie zmienia ani nie anuluje zleceń.

## Źródło danych

Proces analityczny łączy się wyłącznie z lokalnym mostem T4 pod
`http://127.0.0.1:8784`. Most .NET utrzymuje sesję oficjalnego T4 API i wystawia
tylko dane rynkowe. Dane logowania T4 nigdy nie trafiają do procesu Python.
Połączenie loopback wymaga dodatkowo wspólnego, losowego tokenu bridge.

Obsługiwany zakres V1:

- logiczny front-month Bitcoin futures: `BTC-FUTURES-FRONT`;
- logiczny front-month Ether futures: `ETH-FUTURES-FRONT`;
- interwały 4h, 1d i 1w;
- minimum 120 świec oraz świeża cena referencyjna;
- jedna zatwierdzona proweniencja: `plus500_t4_futures_v1`.

## Szybki test offline

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
crypto-agent analyze --provider synthetic --symbol BTC/USD --interval 1440
```

Tryb `synthetic` służy wyłącznie testom i zawsze pozostaje diagnostyczny.

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

Host bridge jest już zbudowany. Do czasu zarejestrowania aplikacji T4 i
podłączenia oficjalnego klienta polecenie bezpiecznie zwróci `NO_SIGNAL` z kodem
`T4_BRIDGE_UNAVAILABLE`.

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
jedyne operacyjne źródło, a `0014` dodaje append-only ingest i point-in-time
replay. Nowy seed nie zawiera konfiguracji innych platform.

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

Szczegóły: [instrukcja uruchomienia](docs/INSTRUKCJA_URUCHOMIENIA.md),
[architektura](docs/ARCHITECTURE.md), [stan projektu](docs/CURRENT_STATUS.md).

Oficjalne informacje o API: [Plus500 Futures T4 API](https://futures-technologies.plus500.com/api/).
