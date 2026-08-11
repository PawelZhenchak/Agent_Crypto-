# Crypto Research Agent V1.1

Checkpoint **0.1.4-v1.1** rozwija read-only fundament agenta badawczego o dwa
publiczne feedy spot, deterministyczny konsensus Kraken–Coinbase i point-in-time
persistence dla PostgreSQL. System nadal służy wyłącznie do developmentu i testów.
`metadata.v1_gate_passed` zawsze pozostaje `false`.

> `ALERT` jest alertem badawczym, nigdy poleceniem kupna lub sprzedaży. Każdy brak,
> konflikt albo wadliwy dowód konsensusu kończy się `NO_SIGNAL`.

## Co działa w checkpointcie 0.1.4

- allowlista `BTC/USD` i `ETH/USD`, interwały 4h, 1d i 1w;
- publiczne adaptery Kraken i Coinbase Exchange bez kluczy i prywatnych endpointów;
- przypięte hosty HTTPS, odrzucanie przekierowań oraz walidacja finalnego URL;
- Coinbase: paginacja do 300 rekordów, 4h z czterech pełnych 1h, 1w z siedmiu
  pełnych 1d, bez forward-fill;
- Kraken: odrzucenie bieżącej, niezamkniętej świecy i natywny interwał 1w
  (`10080` minut);
- Coinbase: tygodnie składane z pełnych świec 1d i kotwiczone do epoki Unix
  (czwartek 00:00 UTC), aby odpowiadały natywnym oknom Kraken w kontrakcie fixtures;
- zapieczętowana konfiguracja providera konsensusu: wyłącznie source keys
  `kraken_spot_rest_v1` + `coinbase_exchange_spot_rest_v1`, bez możliwości podmiany
  pary po skonstruowaniu;
- attestation implementacji feedów przed i po fetchu: exact typy, kod/defaulty/closure
  metod, krytyczne mapy, origin, timeout oraz lokalny łańcuch transportu; wykryta
  mutacja kończy się `NO_SIGNAL`;
- jeden wersjonowany algorytm `cross_exchange_spot_consensus_v1` używany zarówno
  przez ścieżkę runtime, jak i trwałe przeliczenie point-in-time;
- dokładnie 120 ciągłych, wspólnych i czasowo wyrównanych świec z obu venue,
  zakończonych najnowszym kwalifikującym się zamkniętym oknem;
- veto dla luki, staleness, złej rewizji, rozjazdu OHLC/close lub niespójnej
  anomalii wolumenu; pojedynczy feed nie może potwierdzić anomalii;
- failed consensus zachowuje dostępne raw candles, źródła, zmierzone wartości,
  progi i hash dowodu zamiast pustego błędu;
- standalone `synthetic`, `kraken` i `coinbase` są diagnostyczne — RiskGate dodaje
  `CONSENSUS_REQUIRED`, więc nie mogą wyemitować `ALERT`;
- osobny snapshot ceny referencyjnej z dokładnie dwóch wyrównanych, zamkniętych świec
  1m Kraken/Coinbase; historia 4h/1d/1w nie jest używana jako zegar ceny;
- RiskGate niezależnie przelicza limit wieku **300 s** oraz maksymalne odchylenie
  każdego źródła **50 pb od mediany** (dla dwóch źródeł dokładnie 100 pb pairwise);
- niezależna walidacja RiskGate: finite numbers, UTC, expiry, SHA-256, allowlista
  decyzji, pełny fingerprint `RiskPolicy` i fail-closed dla wadliwych DTO;
- append-only raporty i snapshoty wejścia w lokalnym SQLite;
- migracje PostgreSQL `0011` i `0012`: repozytorium raw/canonical oraz append-only
  manifest/provenance ceny referencyjnej z przypiętym `(candle_id, receipt_id)`;
- DB-derived wybór najnowszych kwalifikujących się raw revisions; identyfikatory
  przekazane przez wywołującego służą wyłącznie jako assertion równości i nie mogą
  wybierać starszej rewizji ani skracać okna;
- dokładny kontekst persistence `2 × 120`, te same pinned źródła, ciągłość i
  fingerprint polityki co w runtime;
- loader canonical ponownie uruchamia algorytm na pełnych 240 raw rows i odrzuca
  niespójny normalized volume, diagnostykę, policy validity lub dowolne ogniwo
  łańcucha hashy manifestu;
- unitless normalized volume, evidence digest i diagnostyka są zapisywane w
  manifeście; `candles.base_volume` dla canonical pozostaje `NULL`, aby nie udawać
  wolumenu bazowego żadnego venue;
- odczyt canonical ponownie wylicza konsensus z pełnych 240 rekordów provenance,
  sprawdza okres ważności polityki w chwili cutoffu i cały łańcuch hashy manifestu;
- planowanie, wykonanie i health-check migracji przez CLI;
- readiness wymaga dokładnie PostgreSQL 16, kompletnego katalogu tabel, funkcji i
  triggerów, dokładnych kolumn ośmiu tabel migracyjnych, checksummowanych migracji
  oraz dokładnego zestawu semantycznych seedów; pusty manifest migracji ani
  nadmiarowy seed nie mogą ominąć kontroli;
- wersjonowany, idempotentny pakiet seedów tworzy dokładnie cztery bindingi
  Kraken/Coinbase oraz sześć serii canonical dla BTC/ETH i interwałów 4h/1d/1w;
- GitHub Actions uruchamia czysty PostgreSQL 16, wykonuje `schema.sql`, migracje
  `0011/0012`, seedy, testy constraintów/triggerów/rollbacku, restartuje kontener
  i ponownie wymaga statusu `READY`;
- opcjonalny narrator OpenAI otrzymuje wynik dopiero po deterministycznym RiskGate
  i nie może zmienić decyzji; NFKC/casefold, kontrola znaków niewidocznych, Markdownu
  i fraz PL/EN blokują znane bezpośrednie oraz pośrednie sugestie działania.

Kod V1.1 nie zawiera składania ani anulowania zleceń, kluczy giełdowych, transferów,
wypłat, margin, futures ani dźwigni. Obecność popularnych zmiennych z sekretami
giełdowymi blokuje start.

## Czego V1.1 jeszcze nie zalicza

- nie wykonano live contract testów wobec bieżących API Kraken i Coinbase;
- brak operacyjnego external ingestu z atomowym raw HTTP payload, finalnym batch
  manifestem, automatyczną kwarantanną, replayem i schedulerem;
- orchestrator nie czyta jeszcze canonical PostgreSQL jako produkcyjnego wejścia;
- brak trades/order booka, spreadu, depth i price impact;
- brak specjalistów derivatives, on-chain, makro, tokenomics, stablecoin i Sceptyka;
- brak pełnego trace/evals dashboardu i 4–8 tygodni forward observation.
- attestation jest defense-in-depth, nie sandboxem: arbitralny kod lub plugin już
  uruchomiony w tym samym interpreterze może zmienić także sam RiskGate; rzeczywista
  granica wymaga osobnego, minimalnego workera ingestu w przypiętym obrazie;
- deterministyczny filtr tekstu blokuje znane klasy rekomendacji, lecz nie dowodzi
  pokrycia wszystkich możliwych parafraz; narrator pozostaje domyślnie wyłączony do
  czasu ukrytych evali z wymaganym 100% recall.

Te punkty blokują formalny Gate V1 i przejście do V2.

## Szybki start

Wymagany jest Python 3.11+.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,postgres]"
cp .env.example .env
```

Bezpieczny test lokalny:

```bash
crypto-agent analyze --symbol BTC/USD --interval 1440 --provider synthetic
```

Tryb konsensusu publicznych adapterów:

```bash
crypto-agent analyze --symbol BTC/USD --interval 1440 --provider consensus
```

Provider `consensus` wykonuje publiczne żądania sieciowe do obu venue. Awaria,
rate-limit, redirect, brak pełnego okna 120 albo konflikt danych daje `NO_SIGNAL` —
bez fallbacku do jednego źródła. Zachowanie adapterów pokrywają fixtures i mocki;
checkpoint nie potwierdza jeszcze bieżących kontraktów live API.

## Testy

Suite nie wymaga sieci ani usług zewnętrznych:

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
python -m compileall -q src tests
```

Lokalny wynik checkpointu: **215/215 testów standard-library** oraz
pełny `compileall` dla `src` i `tests`. Ten wynik nie zastępuje testów live ani
zielonego przebiegu akceptacji PostgreSQL 16 w CI.

`pytest`, Ruff i mypy są dostępne po instalacji dodatku `dev`, ale bazowa suite używa
standardowej biblioteki, aby regresje bezpieczeństwa dało się uruchomić offline.

## PostgreSQL

Lokalny serwer z Compose nasłuchuje tylko na `127.0.0.1`:

```bash
docker compose up -d postgres
crypto-agent db plan
crypto-agent db migrate
crypto-agent db seed
crypto-agent db health
```

Po samym bootstrapie i migracjach readiness celowo zwraca `SEEDS_MISSING`.
`crypto-agent db seed` atomowo dodaje dokładny registry scope; dopiero wtedy health
może zwrócić `READY`. Ponowne wykonanie seeda jest idempotentne, a konfliktujące lub
nadmiarowe dane pozostają fail-closed.

`db/schema.sql` jest bootstrapem czystej bazy PostgreSQL 16, a `db/migrations/`
zawiera checksummowane migracje aplikacyjne. Ustaw `POSTGRES_PASSWORD` i
`CRYPTO_AGENT_POSTGRES_DSN` w `.env`; CLI nigdy nie wypisuje DSN. Istniejącego wolumenu
nie należy usuwać w celu „naprawienia” migracji — drift checksumy ma zatrzymać proces.

`PointInTimeCandleRepository` sam wyznacza latest eligible revisions według cutoffu,
wiązań źródło–venue–market i zamkniętego okna. Następnie sprawdza dokładny, ciągły
kontekst `Kraken 120 + Coinbase 120`, uruchamia
`cross_exchange_spot_consensus_v1` i zapisuje uporządkowane provenance oraz manifest
dowodu. Caller może potwierdzić oczekiwane IDs, ale nie wybiera nimi wejść.

`PointInTimeCandleRepository.append_reference_price` wyprowadza z bazy dokładnie
dwie najnowsze kwalifikujące się świece 1m, przypina ich receipts, ponownie liczy
medianę i progi oraz zapisuje manifest `cross_exchange_reference_price_v1`.
Migracja `0012` powtarza krytyczne kontrole w deferred triggerze na rzeczywistych
wierszach źródłowych. Historyczna polityka schema-r1 pozostaje dostępna wyłącznie do
replayu; nowe zapisy wymagają schema-r2.

Canonical OHLC jest wynikiem wspólnego algorytmu. Znormalizowany wolumen jest wartością
bezwymiarową i pozostaje w manifeście wraz z diagnostyką; nie jest zapisywany jako
`candles.base_volume`.

## API

```bash
uvicorn crypto_agent.api:app --reload
```

- `GET /health` — readiness aplikacji, PostgreSQL i migracji;
- `GET /v1/analyze?symbol=BTC%2FUSD&interval_minutes=1440&provider=consensus`;
- `/docs` — lokalna dokumentacja OpenAPI.

Budowa orchestratora, inicjalizacja storage, synchroniczne adaptery i analiza są
uruchamiane poza event loop pod jednym kooperatywnym deadline
`CRYPTO_AGENT_ANALYSIS_TIMEOUT_SECONDS` (domyślnie 30 s). Timeout SQLite jest
ograniczony pozostałym budżetem, a wygasły request nie może później dopisać raportu.

## Opcjonalny narrator OpenAI

```bash
python -m pip install -e ".[llm]"
```

Następnie ustaw `OPENAI_API_KEY`, `CRYPTO_AGENT_ENABLE_LLM=true` i użyj `--narrate`.
Subskrypcja ChatGPT i API OpenAI są rozliczane oddzielnie. Narrator ma ścisły schemat,
postwalidację decyzji i fail-closed filtr rekomendacji finansowych PL/EN; pozostaje
funkcją laboratoryjną i domyślnie wyłączoną do czasu formalnych evals.

## Nienaruszalne zasady

1. Brak konsensusu dokładnie dwóch zatwierdzonych venue oznacza `NO_SIGNAL`.
2. LLM nie oblicza ani nie zmienia decyzji RiskGate.
3. Dane po `as_of` nie mogą wejść do analizy point-in-time.
4. Brak danych, luka, staleness, konflikt lub niefinitywna wartość zatrzymują analizę.
5. V1–V3 nie wykonują prawdziwych transakcji.
6. Gate V1 nie może zostać uznany na podstawie samej zielonej suite jednostkowej.

## Źródła kontraktów

- [Coinbase Exchange — Get product candles](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles)
- [Coinbase Exchange — rate limits](https://docs.cdp.coinbase.com/exchange/rest-api/rate-limits)
- [Kraken REST — OHLC data](https://docs.kraken.com/api-reference/market-data/get-ohlc-data)
- [Kraken — historical market data limits](https://support.kraken.com/hc/articles/360000919966)
- [OpenAI Agents SDK](https://openai.github.io/openai-agents-python/)
- [OpenAI evaluation best practices](https://platform.openai.com/docs/guides/evaluation-best-practices)

Stan i kolejne bramki opisują `docs/CURRENT_STATUS.md`, `docs/ROADMAP.md`,
`docs/ARCHITECTURE.md`, `docs/DATA_MODEL.md`, `docs/SAFETY_POLICY.md`,
`docs/EVALUATION_PLAN.md`, `docs/POSTGRES16_ACCEPTANCE.md` i
`docs/V1_1_RELEASE_NOTES.md`. Pełna instrukcja instalacji,
uruchomienia oraz oczekiwanych wyników znajduje się w
[`docs/INSTRUKCJA_URUCHOMIENIA.md`](docs/INSTRUKCJA_URUCHOMIENIA.md).
