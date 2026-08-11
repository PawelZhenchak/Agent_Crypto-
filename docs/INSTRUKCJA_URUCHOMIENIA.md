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

Health wymaga PostgreSQL 16, migracji `0011`–`0013`, dwóch bindingów T4 oraz
jednego runtime config z wyłączonymi trasami zleceń.

## 4. T4 Simulator

Załóż konto Simulator zgodnie z oficjalną stroną Plus500 Futures. Login i hasło
zapisz wyłącznie w prywatnej konfiguracji workera .NET. Nie wpisuj ich do `.env`
agenta Python i nie wysyłaj ich do repozytorium.

Po uruchomieniu mostu na `127.0.0.1:8784`:

```bash
crypto-agent analyze --provider t4 --symbol BTC/USD --interval 1440
```

Most musi potwierdzić `read_only=true`, `order_routes_exposed=false`, właściwe
source/venue ID, 120 świec i świeżą cenę. Każda niezgodność kończy się `NO_SIGNAL`.
