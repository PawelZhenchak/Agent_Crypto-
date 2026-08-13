# Plus500 Futures T4 Research Agent

Wersja `0.9.0` dodaje etap 1 dowodów siedmiu obowiązkowych scenariuszy kampanii
V1. Bridge zapisuje własny, podpisany dziennik, a migracja `0018` wiąże go z
realnymi batchami, cyklami i runami. Wynik scenariusza oraz końcową bramkę
wylicza PostgreSQL z obiektywnych rekordów; CLI nie przyjmuje deklaracji `PASS`.

To nadal nie jest odbiór V1. Nie ma jeszcze provisionowanego dostępu T4,
rzeczywistych identyfikatorów rynków ani wykonanej kampanii. Dlatego obecny stan
to `v1_gate_passed=false`. Wartość może zmienić się na `true` dopiero po realnych
28 dniach `live_t4`, spełnieniu wszystkich progów jakości i zaliczeniu wszystkich
siedmiu dowodów, w tym rzeczywistego rollu kontraktu.

System analizuje tylko dane futures i może zwrócić `ALERT` albo `NO_SIGNAL`.
Nie składa, nie zmienia i nie anuluje zleceń.

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
odbioru live V1 w `0.9.0` obejmuje tylko 4h (`240` minut) dla BTC i ETH.

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

1. Utwórz trzy prywatne pliki środowiska na podstawie
   `t4-bridge/.env.example`, `configs/evidence-verifier.env.example` i
   `configs/runtime.env.example`. Każdy uruchamiaj jako inny systemowy UID;
   `.env.example` służy tylko do lokalnych narzędzi/offline i Compose.
2. Ustaw losowy `T4_BRIDGE_TOKEN` w workerze oraz tę samą wartość jako
   `CRYPTO_AGENT_T4_BRIDGE_TOKEN` w osobnych plikach runtime i verifiera.
3. Przygotuj prywatny katalog schema `v2` na podstawie
   `configs/t4-contract-catalog.example.json`. Wpisz rzeczywiste `ExchangeID`,
   produktowe `ContractID`, nieprzezroczyste `MarketID` oraz niezależny rynek
   indeksu dla każdej serii.
4. Dla każdej kampanii wybierz z góry nowy UUID, nowy klucz dowodowy ECDSA P-256
   oraz nowy, pusty plik dziennika na trwałym wolumenie. Ustaw ten sam UUID jako
   `T4_OBSERVATION_CAMPAIGN_ID` i później przekaż go do `observe-start`.
5. Tylko w środowisku bridge-only ustaw `T4_CONTRACT_CATALOG_PATH`,
   `T4_API_ENVIRONMENT`, prywatny `T4_API_KEY`,
   `T4_EVIDENCE_SIGNING_KEY_PATH` i `T4_OBSERVATION_EVENT_JOURNAL_PATH`.
6. Uruchom worker:

```bash
dotnet run --project t4-bridge/CryptoAgent.T4Bridge.csproj
```

7. Dopiero po stanie `READY` uruchom test odczytu:

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

Migracje `0017`–`0018` utrwalają schema v5, append-only kampanie, cykle,
podpisane zdarzenia bridge'a, wyniki scenariuszy i raporty odbiorowe. Podpis
ECDSA pochodzi z naszego bridge'a, nie z T4 ani Plus500.

Bridge, runtime i weryfikator dowodów muszą działać jako trzy osobne procesy i
trzy różne systemowe UID, z osobnymi plikami środowiska. Bridge jako jedyny
czyta klucz prywatny `0400`, journal `0600` i T4 API key. Token sterowania znają
tylko bridge i verifier, a token odczytowego API bridge'a — przy obecnym jednym
sekrecie — bridge, verifier i runtime.
PostgreSQL używa trzech osobnych loginów bez superusera: runtime,
verifier-write oraz evidence-reader read-only. Runtime zna
`CRYPTO_AGENT_POSTGRES_DSN`, token danych bridge'a oraz loopback URL/token
weryfikatora. Nigdy nie może otrzymać `CRYPTO_AGENT_POSTGRES_EVIDENCE_DSN`,
`CRYPTO_AGENT_POSTGRES_EVIDENCE_READ_DSN`, pinu SPKI ani tokenu
sterowania. Osobny proces uruchamiany poleceniem poniżej jako jedyny dostaje DSN
roli `crypto_agent_evidence_verifier`, sam pobiera journal z bridge'a, sprawdza
ECDSA i dopiero wtedy używa wąskich funkcji DB:

```bash
# osobna usługa/UID, środowisko z configs/evidence-verifier.env.example
crypto-agent evidence-verifier --host 127.0.0.1 --port 8791

# osobny runtime/UID, środowisko z configs/runtime.env.example
unset CRYPTO_AGENT_POSTGRES_EVIDENCE_DSN
unset CRYPTO_AGENT_POSTGRES_EVIDENCE_READ_DSN
crypto-agent observe-status --campaign-id <uuid>
```

Token API weryfikatora musi być inny od tokenów bridge'a i sterowania. DSN
administratora lub właściciela bazy nie może być użyty przez żaden z procesów
podczas kampanii.

Granica zakłada poprawnie działający, nieprzejęty normalny runtime i host.
Pełne przejęcie procesu runtime/hosta jest poza modelem zagrożeń. Podpisane przez
bridge receipt każdego batcha pozostaje możliwym przyszłym utwardzeniem, nie
obecną gwarancją.

Jedyny kanał alertów to kanoniczny JSON
`stdout_json` → `process_stdout`. Trwały outbox ma semantykę at-least-once;
konsument musi deduplikować po `idempotency_key`. System nie deklaruje
exactly-once.

## Kampania obserwacyjna V1

Po potwierdzonym teście live i zamrożeniu kodu, polityki oraz konfiguracji:

```bash
crypto-agent observe-start \
  --campaign-id <uuid-wybrany-przed-startem-bridge> \
  --code-commit-hash <sha256> \
  --t4-protocol-commit-hash <sha256> \
  --runtime-config-hash <sha256>

crypto-agent observe-run --campaign-id <uuid> --scope BTC/USD:240m --limit 120
crypto-agent observe-run --campaign-id <uuid> --scope ETH/USD:240m --limit 120
crypto-agent observe-scenarios --campaign-id <uuid> \
  --scenario reconnect --scope BTC/USD:240m
crypto-agent observe-status --campaign-id <uuid>
crypto-agent observe-report --campaign-id <uuid>
```

`observe-start` wykonuje live preflight schema v5 dla każdego scope’u, zanim
utworzy kampanię. `observe-run` wybiera z PostgreSQL dokładnie jeden należny slot,
pobiera batch T4 tylko raz, zapisuje go i podaje ten sam zamknięty obiekt do
analizy. Retry przed następnym slotem nie pobiera danych; wygasłe sloty są
zapisywane uczciwie jako `missed`, nigdy backfillowane sukcesem.

Komendy obserwacyjne nie handlują i nie pozwalają podać historycznego czasu
startu — używany jest zegar PostgreSQL. `observe-scenarios` zapisuje tylko
referencje dowodowe; to baza wyprowadza `pass` albo `fail`.

Kontrolowany `rate_limit` sprawdza granicę naszego handlera, nie dowodzi
rzeczywistej odpowiedzi `429` od T4. Nie wolno spamować dostawcy. `bridge_restart`
w etapie 1 wymaga zewnętrznego supervisora, który ponownie uruchomi proces;
automatyzacja 24/7 należy do etapu 2. `roll_transition` jest pasywny i może zostać
zaliczony wyłącznie po prawdziwym rollu widocznym w live batchu schema v5.

`observe-report` finalizuje kampanię dopiero po
`planned_ends_at + cycle_interval_seconds`, czyli po okresie grace na zapis
ostatniego slotu. PostgreSQL niezależnie wylicza `v1_gate_passed`; sam upływ 28
dni ani sam podpis bridge'a nie wystarczają.

Pełna procedura: [runbook obserwacji](docs/T4_OBSERVATION_RUNBOOK.md).
Pozostałe materiały: [instrukcja uruchomienia](docs/INSTRUKCJA_URUCHOMIENIA.md),
[architektura](docs/ARCHITECTURE.md), [stan projektu](docs/CURRENT_STATUS.md).
Zmiany wydania: [0.9.0](docs/RELEASE_NOTES_0.9.0.md).

Oficjalne informacje: [Plus500 Futures T4 API](https://futures-technologies.plus500.com/api/),
[T4 API tools](https://github.com/CTS-Futures/t4-api-tools/tree/1a68b674482194f1cf3b9d7f129ce5fbed8bcb51),
[`Plus500US.T4Proto` 1.0.73](https://www.nuget.org/packages/Plus500US.T4Proto/1.0.73),
[`Plus500US.T4ChartDecoder` 1.0.97](https://www.nuget.org/packages/Plus500US.T4ChartDecoder/1.0.97).
