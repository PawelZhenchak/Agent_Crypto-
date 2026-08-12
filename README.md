# Plus500 Futures T4 Research Agent

Wersja `0.8.0` przygotowuje punkt 8: oficjalny, wyłącznie odczytowy klient
**Plus500 Futures / T4**, schema bridge `v5` oraz audytowalną kampanię odbiorową
V1. System analizuje tylko dane futures i może zwrócić `ALERT` albo `NO_SIGNAL`.
Nie składa, nie zmienia i nie anuluje zleceń.

To wydanie nie oznacza jeszcze odbioru V1. Provisioning oraz testy na prawdziwym
Simulator/live nie zostały wykonane, a rzeczywista 28-dniowa obserwacja nie
została rozpoczęta. W `0.8.0` `v1_gate_passed=false` jest dodatkowo wymuszane
przez bazę do czasu późniejszej migracji, która doda weryfikowalne, obiektywne
referencje dowodów dla obowiązkowych scenariuszy.

## Oficjalne połączenie T4

Odizolowany worker .NET 8 korzysta z oficjalnych pakietów
`Plus500US.T4Proto` `1.0.73` i `Plus500US.T4ChartDecoder` `1.0.97`; protokół jest
przypięty do publicznego stanu T4
`1a68b674482194f1cf3b9d7f129ce5fbed8bcb51`. Dane bieżące i snapshoty depth są
odbierane przez T4 WebSocket/Protobuf, a binarna odpowiedź Chart REST jest
dekodowana oficjalnym decoderem do zamkniętych świec. Worker nasłuchuje wyłącznie
na `127.0.0.1`, wymaga osobnego tokenu i nie wystawia tras zleceń. Klucz API
pozostaje tylko w procesie .NET.

Środowisko jest jawne i wybiera stałe oficjalne endpointy:

- `T4_API_ENVIRONMENT=simulator` — `wss-sim.t4login.com` i
  `api-sim.t4login.com`, envelope `t4_simulator`;
- `T4_API_ENVIRONMENT=live` — `wss.t4login.com` i `api.t4login.com`, envelope
  `live_t4`;
- `T4_API_ENVIRONMENT=pending` — bez klucza, stan fail-closed `NOT_READY`.

Simulator nie jest live i nigdy nie kwalifikuje alertu do zewnętrznego delivery.
Standardowy publiczny dostęp Simulator trwa dwa tygodnie, więc sam nie wystarcza
do obowiązkowej obserwacji co najmniej `672` godzin; potrzebne jest przedłużenie
lub odpowiednio udostępnione środowisko live.

## Kontrakt danych v5

Schema `v5` rozdziela:

- `ExchangeID` — giełdę;
- `ContractID` — produkt;
- `MarketID` — konkretną, handlowalną serię/expiry;
- niezależne identyfikatory indeksu używanego do basis.

`MarketID` jest nieprzezroczystym identyfikatorem T4: nie wolno go konstruować,
parsować ani wyprowadzać z symbolu. Każda świeca zachowuje własny `MarketID`, co
pozwala udowodnić zmianę serii w historii Chart REST. Basis wymaga niezależnego
rynku indeksowego typu `index` ze źródła `plus500_t4_index_v1`; cena futures nie
może go zastąpić.

Parser i bridge nadal rozpoznają interwały 4h, 1d i 1w, ale zamrożony zakres
odbioru live V1 w `0.8.0` obejmuje obecnie tylko 4h (`240` minut) dla BTC i ETH.

## Szybki test offline

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
crypto-agent analyze --provider synthetic --symbol BTC/USD --interval 240
```

Synthetic, fixture, replay i Simulator są diagnostyczne. Nie potwierdzają danych
live i nigdy nie otwierają bramki V1.

## Uruchomienie T4

1. Skopiuj `.env.example` do prywatnego `.env`.
2. Ustaw losowy `T4_BRIDGE_TOKEN` w workerze oraz tę samą wartość jako
   `CRYPTO_AGENT_T4_BRIDGE_TOKEN` w Pythonie.
3. Przygotuj prywatny katalog schema `v2` na podstawie
   `configs/t4-contract-catalog.example.json`. Wpisz rzeczywiste `ExchangeID`,
   produktowe `ContractID`, nieprzezroczyste `MarketID` oraz niezależny rynek
   indeksu dla każdej serii.
4. Ustaw `T4_CONTRACT_CATALOG_PATH`, `T4_API_ENVIRONMENT` i prywatny `T4_API_KEY`.
5. Uruchom worker:

```bash
dotnet run --project t4-bridge/CryptoAgent.T4Bridge.csproj
```

6. Dopiero po stanie `READY` uruchom test odczytu:

```bash
crypto-agent analyze --provider t4 --symbol BTC/USD --interval 240
```

Provisioning klucza API, rzeczywiste identyfikatory rynków, uprawnienia depth oraz
dostęp do niezależnego indeksu są zależnościami zewnętrznymi. Ich brak kończy się
`503`/`NO_SIGNAL`, bez danych zastępczych.

## PostgreSQL, monitoring i alerty

```bash
docker compose up -d postgres
crypto-agent db migrate
crypto-agent db seed
crypto-agent db health

crypto-agent ingest --symbol BTC/USD --interval 240
crypto-agent monitor --symbol BTC/USD --interval 240 --watch --poll-seconds 300
crypto-agent deliver-alerts --watch --poll-seconds 5
crypto-agent monitoring-status
```

Migracja `0017` utrwala schema v5 oraz append-only kampanie, cykle, zdarzenia
sesji i końcowe raporty odbiorowe. Jedyny kanał alertów to kanoniczny JSON
`stdout_json` → `process_stdout`. Trwały outbox ma semantykę at-least-once;
konsument musi deduplikować po `idempotency_key`. System nie deklaruje
exactly-once.

## Kampania obserwacyjna V1

Po potwierdzonym teście live i zamrożeniu kodu, polityki oraz konfiguracji:

```bash
crypto-agent observe-start \
  --code-commit-hash <sha256> \
  --t4-protocol-commit-hash <sha256> \
  --runtime-config-hash <sha256>

crypto-agent observe-run --campaign-id <uuid> --scope BTC/USD:240m --limit 120
crypto-agent observe-run --campaign-id <uuid> --scope ETH/USD:240m --limit 120
crypto-agent observe-status --campaign-id <uuid>
crypto-agent observe-report --campaign-id <uuid>
```

`observe-start` wykonuje live preflight schema v5 dla każdego scope’u, zanim
utworzy kampanię. `observe-run` wybiera z PostgreSQL dokładnie jeden należny slot,
pobiera batch T4 tylko raz, zapisuje go i podaje ten sam zamknięty obiekt do
analizy. Retry przed następnym slotem nie pobiera danych; wygasłe sloty są
zapisywane uczciwie jako `missed`, nigdy backfillowane sukcesem.

Komendy obserwacyjne nie handlują i nie pozwalają podać historycznego czasu startu
— używany jest zegar PostgreSQL. `observe-report` finalizuje kampanię dopiero po
`planned_ends_at + cycle_interval_seconds`, czyli po dodatkowym okresie grace na
zapis ostatniego slotu. W `0.8.0` wynik może być `FAIL` albo `NOT_OBSERVED`;
schema celowo odrzuca `PASS` scenariusza i `v1_gate_passed=true`, dopóki późniejsza
migracja nie doda obiektywnych referencji dowodów oraz ich walidacji w bazie.

Pełna procedura: [runbook obserwacji](docs/T4_OBSERVATION_RUNBOOK.md).
Pozostałe materiały: [instrukcja uruchomienia](docs/INSTRUKCJA_URUCHOMIENIA.md),
[architektura](docs/ARCHITECTURE.md), [stan projektu](docs/CURRENT_STATUS.md).

Oficjalne informacje: [Plus500 Futures T4 API](https://futures-technologies.plus500.com/api/),
[T4 API tools](https://github.com/CTS-Futures/t4-api-tools/tree/1a68b674482194f1cf3b9d7f129ce5fbed8bcb51),
[`Plus500US.T4Proto` 1.0.73](https://www.nuget.org/packages/Plus500US.T4Proto/1.0.73),
[`Plus500US.T4ChartDecoder` 1.0.97](https://www.nuget.org/packages/Plus500US.T4ChartDecoder/1.0.97).
