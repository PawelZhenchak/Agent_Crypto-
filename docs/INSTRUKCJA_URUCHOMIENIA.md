# Instrukcja uruchomienia checkpointu 0.1.4

Ta instrukcja dotyczy systemu **0.1.4-v1.1**. Jest to read-only agent badawczy:
nie składa zleceń, nie używa kluczy giełdowych i nie wykonuje transferów. Wynik
`ALERT` oznacza wyłącznie alert badawczy, a nie rekomendację kupna lub sprzedaży.

## 1. Wymagania

- Python 3.11 lub nowszy;
- Git;
- opcjonalnie Docker z Compose do lokalnego PostgreSQL 16;
- dostęp do internetu tylko dla providera `consensus`;
- klucz OpenAI nie jest wymagany, dopóki narrator pozostaje wyłączony.

## 2. Instalacja

```bash
git clone https://github.com/PawelZhenchak/Agent_Crypto-.git
cd Agent_Crypto-
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev,postgres]"
cp .env.example .env
```

W PowerShell aktywacja środowiska wygląda tak:

```powershell
.venv\Scripts\Activate.ps1
```

Nie wpisuj kluczy giełdowych do `.env`. Agent ich nie potrzebuje, a wykrycie
popularnych zmiennych z sekretami giełdowymi celowo blokuje uruchomienie.

## 3. Najpierw uruchom testy

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
python -m compileall -q src tests
```

Oczekiwany wynik tego checkpointu to **213 zaliczonych testów** i kod wyjścia `0`.
Testy są offline: nie łączą się z giełdami ani z prawdziwą bazą PostgreSQL.

## 4. Bezpieczny test offline

```bash
crypto-agent analyze --symbol BTC/USD --interval 1440 --provider synthetic
```

Polecenie powinno wypisać raport JSON. Provider `synthetic` służy tylko do
diagnostyki i nie może przejść bramki konsensusu, dlatego prawidłowym wynikiem jest
`NO_SIGNAL` z powodem obejmującym `CONSENSUS_REQUIRED`.

## 5. Analiza z dwóch publicznych giełd

```bash
crypto-agent analyze --symbol BTC/USD --interval 1440 --provider consensus
```

Dozwolone symbole to `BTC/USD` i `ETH/USD`, a interwały w minutach to:

- `240` — 4 godziny;
- `1440` — 1 dzień;
- `10080` — 1 tydzień.

Provider `consensus` pobiera publiczne dane spot z Kraken i Coinbase. System wymaga
dokładnie 120 wspólnych, zamkniętych i ciągłych świec z obu źródeł oraz osobnego
snapshotu ceny `2 x 1m`. Snapshot może mieć maksymalnie 300 sekund, a każde źródło
może odchylać się najwyżej o 50 punktów bazowych od mediany.

Oczekiwane zachowanie:

- kompletne, świeże i zgodne dane mogą dać `ALERT` albo `NO_SIGNAL` zależnie od
  deterministycznych reguł analizy;
- błąd jednego feedu, redirect, rate-limit, luka, staleness lub konflikt zawsze
  daje `NO_SIGNAL`;
- system nigdy nie przechodzi awaryjnie na jedno źródło;
- żaden wynik nie powoduje transakcji.

## 6. Lokalne API

```bash
uvicorn crypto_agent.api:app --reload
```

Następnie otwórz:

- `http://127.0.0.1:8000/docs` — dokumentacja i testowanie API;
- `http://127.0.0.1:8000/health` — health aplikacji;
- `http://127.0.0.1:8000/v1/analyze?symbol=BTC%2FUSD&interval_minutes=1440&provider=consensus`
  — przykładowa analiza.

Cała analiza ma domyślny limit 30 sekund. Przekroczenie limitu kończy request bez
późniejszego dopisywania raportu.

## 7. PostgreSQL 16

W `.env` ustaw własne, unikalne hasło i odpowiadający mu DSN, na przykład:

```dotenv
POSTGRES_PASSWORD=ZMIEN_TO_HASLO
CRYPTO_AGENT_POSTGRES_DSN=postgresql://crypto_agent:ZMIEN_TO_HASLO@127.0.0.1:5432/crypto_agent
```

Nie commituj uzupełnionego `.env`. Przed poleceniami bazy wyeksportuj jego wartości
do bieżącej powłoki, a następnie uruchom PostgreSQL i migracje:

```bash
set -a
source .env
set +a
docker compose up -d postgres
crypto-agent db plan
crypto-agent db migrate
crypto-agent db seed
crypto-agent db health
```

Po samym uruchomieniu schematu i migracji health powinien zwrócić `SEEDS_MISSING`.
Polecenie `crypto-agent db seed` dodaje dokładne bindingi Kraken/Coinbase oraz sześć
canonical series. Po nim `crypto-agent db health` powinien zwrócić `READY`.
Ponowne uruchomienie seeda jest bezpieczne i idempotentne; dane konfliktujące nie są
nadpisywane, a health odrzuca brakujący lub nadmiarowy zakres.

## 8. Jak działa przepływ decyzji

1. Adaptery pobierają wyłącznie publiczne dane z przypiętych hostów HTTPS.
2. Warstwa jakości sprawdza format, czas, ciągłość i kompletność obu feedów.
3. Konsensus Kraken–Coinbase tworzy dowód z dokładnie `2 x 120` świec.
4. Osobny snapshot `2 x 1m` potwierdza świeżość ceny referencyjnej.
5. Deterministyczny RiskGate ponownie sprawdza dane, politykę i fingerprinty.
6. Dopiero po przejściu wszystkich bramek może powstać badawczy `ALERT`.
7. Każdy brak lub konflikt zatrzymuje przepływ wynikiem `NO_SIGNAL`.
8. Opcjonalny narrator może jedynie opisać gotowy wynik; nie może go zmienić.

## 9. Obecne ograniczenia

Checkpoint nie jest ukończoną V1. Akceptacja PostgreSQL 16 działa w GitHub Actions,
ale nie wykonano live contract tests Kraken/Coinbase. Brakuje operacyjnego ingestu,
schedulera, order booka, specjalistów analitycznych, dashboardu oraz 4–8
tygodni obserwacji. Dlatego `metadata.v1_gate_passed` musi pozostać `false`.
