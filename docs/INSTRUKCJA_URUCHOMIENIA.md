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
Ten wspólny `.env` służy tylko do testu offline i lokalnego Compose. Nie ładuj
go do żadnej z trzech usług kampanii; ich rozłączne pliki opisuje sekcja 2.

## 2. PostgreSQL 16

Uzupełnij prywatnie `POSTGRES_PASSWORD`. Nie commituj `.env`.

```bash
docker compose up -d postgres
crypto-agent db migrate
crypto-agent db seed
crypto-agent db health
```

Health wymaga migracji do `0018`. Migracje `0017`–`0018` dodają schema v5,
append-only kampanie, cykle, podpisane zdarzenia bridge'a, wyniki scenariuszy i
końcowe raporty odbiorowe.

Po migracji przygotuj trzy osobne loginy PostgreSQL bez superusera oraz trzy
osobne procesy/UID z prywatnymi plikami środowiska: bridge, verifier i runtime.
Nie uruchamiaj żadnej pary z tym samym UID. Bridge jako jedyny czyta klucz
prywatny `0400`, journal `0600` i T4 API key. Token sterowania znają wyłącznie
bridge i verifier; obecny jeden token odczytowego API bridge'a znają bridge,
verifier i runtime. Verifier jako jedyny czyta evidence DSN i pin publiczny;
runtime jako jedyny czyta runtime DSN. Token API verifiera znają tylko verifier
i runtime.

Model zakłada poprawnie działający, nieprzejęty normalny runtime i host. Pełne
przejęcie runtime/hosta jest poza zakresem. Podpisane przez bridge receipt
każdego batcha pozostaje przyszłym utwardzeniem, nie obecną gwarancją.

- runtime dla `CRYPTO_AGENT_POSTGRES_DSN`; nie może być właścicielem bazy ani
  członkiem roli dowodowej;
- verifier dla `CRYPTO_AGENT_POSTGRES_EVIDENCE_DSN`; ma należeć wyłącznie do
  `crypto_agent_evidence_verifier` i korzystać z funkcji `SECURITY DEFINER`.
  Ten DSN występuje tylko w `configs/evidence-verifier.env.example`, nigdy w
  środowisku runtime opisanym przez `configs/runtime.env.example`.
- verifier ma też osobny `CRYPTO_AGENT_POSTGRES_EVIDENCE_READ_DSN` dla LOGIN-u
  należącego wyłącznie do `crypto_agent_evidence_reader`; nie może być równy
  runtime DSN ani verifier DSN.

DSN administratora lub właściciela służy tylko do migracji/provisioningu. Nie
ustawiaj go jako żadnego z powyższych DSN podczas kampanii. Uruchom weryfikator
na loopback przed CLI:

```bash
# proces/UID verifier, tylko jego prywatny EnvironmentFile
unset CRYPTO_AGENT_POSTGRES_DSN
crypto-agent evidence-verifier --host 127.0.0.1 --port 8791

# proces/UID runtime, inny EnvironmentFile
unset CRYPTO_AGENT_POSTGRES_EVIDENCE_DSN
unset CRYPTO_AGENT_POSTGRES_EVIDENCE_READ_DSN
export CRYPTO_AGENT_EVIDENCE_VERIFIER_URL=http://127.0.0.1:8791
```

## 3. Provisioning T4

Worker `0.9.0` używa oficjalnych `Plus500US.T4Proto` `1.0.73` oraz
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
export T4_EVIDENCE_SIGNING_KEY_PATH='/private/t4-evidence-key.pem'
export T4_OBSERVATION_EVENT_JOURNAL_PATH='/private/t4-events-<uuid>.jsonl'
export T4_OBSERVATION_CAMPAIGN_ID='<uuid>'
export T4_OBSERVATION_CONTROL_ENABLED='false'
unset T4_OBSERVATION_CONTROL_TOKEN
dotnet run --project t4-bridge/CryptoAgent.T4Bridge.csproj
```

UUID, klucz ECDSA P-256 PKCS#8 i pusty dziennik JSONL wybierz przed startem
bridge'a. Muszą być nowe dla każdej kampanii. Dziennik przechowuj na trwałym
wolumenie, aby hash chain przetrwał restart.

Podpis ECDSA pochodzi z naszego bridge'a dowodowego. Nie jest podpisem ani
atestacją Plus500/T4.

W procesie runtime ustaw ten sam token danych jako
`CRYPTO_AGENT_T4_BRIDGE_TOKEN`. Fingerprint publicznego SPKI i osobny token
kontrolny ustaw wyłącznie w procesie `evidence-verifier`, ten ostatni dopiero na
czas zaplanowanej próby. Wtedy zrestartuj bridge z
`T4_OBSERVATION_CONTROL_ENABLED=true` i ustaw token kontrolny w bridge'u oraz
verifierze. Token loopback API verifiera jest trzecim, odrębnym sekretem.
Środowisko wybiera stałe oficjalne
endpointy:

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
artefaktów baseline.

Domyślne scope’y to `BTC/USD:240m` oraz `ETH/USD:240m`. W `0.9.0` wybierz
`campaign_id` przed startem bridge'a i przekaż go jawnie. Musi być zgodny z
`T4_OBSERVATION_CAMPAIGN_ID`:

```bash
crypto-agent observe-start \
  --campaign-id <uuid> \
  --code-commit-hash <sha256> \
  --t4-protocol-commit-hash <sha256> \
  --runtime-config-hash <sha256>
```

Przed zapisem `observe-start` rejestruje publiczny klucz bridge'a przez osobny
login weryfikatora, utrwala pierwsze podpisane zdarzenia i wykonuje live preflight
schema v5 każdego scope’u. Start pochodzi z zegara PostgreSQL i nie może być
backfillowany.

```bash
crypto-agent observe-run --campaign-id <uuid> --scope BTC/USD:240m --limit 120
crypto-agent observe-run --campaign-id <uuid> --scope ETH/USD:240m --limit 120
crypto-agent observe-supervise --campaign-id <uuid>
crypto-agent observe-supervisor-status --campaign-id <uuid>
crypto-agent observe-status --campaign-id <uuid>
crypto-agent observe-report --campaign-id <uuid>
```

`observe-supervise` wywołuje `observe-run` dla obu zamrożonych scope’ów. Korzysta
z harmonogramu i czasu PostgreSQL, pobiera live batch schema v5 tylko raz, a ten
sam obiekt przekazuje do ingestu i zamkniętego providera analizy. Sukces wiąże
batch, run, trace i raw hash. Retry w tym samym slocie nie pobiera danych, a
wygasłe sloty mogą zostać zapisane tylko jako `missed`. Szczegóły zawiera
[runbook obserwacji](T4_OBSERVATION_RUNBOOK.md).
Instalację jednostek, restart procesów, status i dzienne snapshoty opisuje
[runbook supervisora](CAMPAIGN_SUPERVISOR_RUNBOOK.md).

Pięć prób kontrolowanych uruchamiaj pojedynczo, w zaplanowanym oknie. Wtedy
zrestartuj bridge z `T4_OBSERVATION_CONTROL_ENABLED=true`, zachowując ten sam
UUID, klucz i dziennik:

```bash
crypto-agent observe-scenarios --campaign-id <uuid> \
  --scenario reconnect --scope BTC/USD:240m
```

Dozwolone scenariusze kontrolowane to `bridge_restart`, `missing_data`,
`rate_limit`, `reconnect` i `stale_data`. `bridge_restart` wymaga zewnętrznego
supervisora. Od `0.10.0` jednostka bridge'a ma `Restart=always`, a nadzorca
sprawdza jej powrót przed następnym cyklem.

Kontrolowany `rate_limit` potwierdza reakcję naszego handlera. Nie jest dowodem
prawdziwego `429` od T4 i nie wolno wywoływać go spamowaniem dostawcy.

`replay_blocked` i `roll_transition` są pasywne. Pierwszy powoduje, że isolated
verifier sam wykonuje in-memory replay 120 świec dla wszystkich zamrożonych
scope’ów przez osobny login tylko-do-odczytu i zapisuje verifier-only atestacje;
runtime nie podaje fingerprintu ani wyniku. Drugi może przejść tylko po
rzeczywistym rollu live ze starym i nowym `MarketID`; injector go nie zaliczy:

```bash
crypto-agent observe-scenarios --campaign-id <uuid> \
  --scenario replay_blocked --scope BTC/USD:240m
crypto-agent observe-scenarios --campaign-id <uuid> \
  --scenario roll_transition --scope BTC/USD:240m
```

`observe-report` odmawia zapisu przed
`planned_ends_at + cycle_interval_seconds`; dodatkowy interwał jest okresem grace
na ostatni planowy slot. PostgreSQL wylicza wynik bramki bezpośrednio z ledgerów.
`true` wymaga pełnych 28 dni, wszystkich progów jakości, zera naruszeń i siedmiu
scenariuszy `pass`, w tym realnego rollu.

Provisioning, rzeczywiste testy Simulator/live i kampania nie zostały wykonane.
Nie ma jeszcze dostępu T4 ani rzeczywistych identyfikatorów rynków, więc obecny
stan to `v1_gate_passed=false`.
