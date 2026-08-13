# Plus500 Futures T4 read-only bridge

Usługa .NET 8 jest izolowaną granicą pomiędzy oficjalnym T4 API a procesem
analitycznym Python. Nasłuchuje wyłącznie na loopback, wymaga osobnego tokenu i
nie wystawia endpointów tworzenia, modyfikowania ani anulowania zleceń.

## Implementacja 0.9.0

Reader używa oficjalnych pakietów NuGet `Plus500US.T4Proto` `1.0.73` oraz
`Plus500US.T4ChartDecoder` `1.0.97` i publicznego protokołu przypiętego do commita
[`1a68b674482194f1cf3b9d7f129ce5fbed8bcb51`](https://github.com/CTS-Futures/t4-api-tools/tree/1a68b674482194f1cf3b9d7f129ce5fbed8bcb51).

- WebSocket/Protobuf dostarcza logowanie, heartbeat i Market By Price depth;
- Chart REST dostarcza binarną odpowiedź dekodowaną oficjalnym
  `Plus500US.T4ChartDecoder` do zamkniętych świec i ich rzeczywistych `MarketID`;
- po zerwaniu sesji cache jest czyszczony, połączenie ponawiane, a subskrypcje
  odtwarzane;
- heartbeat jest wysyłany co 20 sekund, a brak wiadomości przez 60 sekund
  zatrzymuje sesję fail-closed;
- lista dozwolonych wiadomości wychodzących obejmuje wyłącznie `LoginRequest`,
  `AuthenticationTokenRequest`, `Heartbeat` i `MarketDepthSubscribe`; wiadomości
  order-routing są zabronione.

Kod nie jest jeszcze dowodem działającej sesji. Provisioning i rzeczywiste testy
Simulator/live nie zostały wykonane. Potrzebne są: klucz API, uprawnienia market
depth, prawidłowe identyfikatory rynków futures i niezależnego indeksu oraz dostęp
do odpowiedniego środowiska.

## Środowiska

`T4_API_ENVIRONMENT` przyjmuje wyłącznie:

- `pending` — bez `T4_API_KEY`, reader `NOT_READY` i odpowiedź `503`;
- `simulator` — `wss://wss-sim.t4login.com/v1` oraz
  `https://api-sim.t4login.com/`, atestacja `t4_simulator`;
- `live` — `wss://wss.t4login.com/v1` oraz `https://api.t4login.com/`, atestacja
  `live_t4`.

Endpointów nie można nadpisać konfiguracją. Simulator i live są rozdzielone;
payload `t4_simulator` nigdy nie kwalifikuje alertu do external delivery.
Standardowy publiczny Simulator trwa dwa tygodnie i sam nie pokrywa wymaganej
kampanii 28-dniowej.

## Katalog kontraktów schema v2

Oficjalna sesja wymaga prywatnego katalogu na podstawie
`configs/t4-contract-catalog.example.json`. Każdy wpis zawiera:

- logiczny symbol agenta;
- `exchange_id`;
- produktowy `contract_id` T4;
- nieprzezroczysty `market_id` konkretnej serii/expiry;
- `expires_at` i opcjonalny `roll_at`;
- niezależne `basis_exchange_id`, `basis_contract_id` i `basis_market_id` indeksu.

`MarketID` może zawierać znaki i spacje dopuszczone przez T4. Nie wolno go
parsować, normalizować ani budować z symbolu. Chart REST może zwrócić różne
`MarketID` dla świec obejmujących roll; schema bridge `v5` zachowuje identyfikator
każdej świecy. Produktowy `ContractID` pozostaje stabilną tożsamością produktu,
a `MarketID` identyfikuje handlowalną serię.

Basis musi pochodzić z osobnego rynku indeksowego typu `index` i źródła
`plus500_t4_index_v1`. Tożsamość indeksu nie może być równa tożsamości futures.

## Uruchomienie

Ustaw prywatnie:

```powershell
$env:T4_BRIDGE_TOKEN = "<losowy token 32-256 znaków>"
$env:T4_CONTRACT_CATALOG_PATH = "C:\private\t4-contracts.json"
$env:T4_API_ENVIRONMENT = "simulator" # albo live
$env:T4_API_KEY = "<prywatny klucz API>"
$env:T4_EVIDENCE_SIGNING_KEY_PATH = "C:\private\t4-evidence-key.pem"
$env:T4_OBSERVATION_EVENT_JOURNAL_PATH = "C:\private\t4-events.jsonl"
$env:T4_OBSERVATION_CAMPAIGN_ID = "<kanoniczny UUID kampanii>"
$env:T4_OBSERVATION_CONTROL_ENABLED = "false"
$env:T4_OBSERVATION_CONTROL_TOKEN = ""
dotnet run --project t4-bridge/CryptoAgent.T4Bridge.csproj
```

Token kontrolny ustaw dopiero w zaplanowanym oknie próby, razem z
`T4_OBSERVATION_CONTROL_ENABLED=true`. Bridge odrzuca token, gdy kontrola jest
wyłączona.

`/healthz` przechodzi w `READY` dopiero po zalogowaniu, uzyskaniu wymaganych
uprawnień oraz zapełnieniu cache snapshotów futures/index i historii. Przedtem
`/v1/market-data` zwraca `503`; agent odpowiada `NO_SIGNAL` bez danych
zastępczych.

Envelope live schema `v5` wymaga `environment=live_t4`, pełnego i
nieprzeciętego order booka, statusu sesji, point-in-time timestampów, niezależnej
referencji basis oraz — w oknie rollu — zsynchronizowanych snapshotów starego i
nowego `MarketID`. Każda niezgodność kończy się fail-closed.

Bridge i kontrakt danych zachowują obsługę 4h/1d/1w, natomiast zamrożony zakres
odbioru live V1 w `0.9.0` obejmuje tylko 4h (`240` minut) dla BTC i ETH.
Fixture’y kontraktowe nie są testem T4 Simulator ani live.

## Podpisane dowody obserwacyjne

Oficjalna kampania wymaga `T4_EVIDENCE_SIGNING_KEY_PATH` (prywatny PKCS#8 PEM
ECDSA P-256), `T4_OBSERVATION_EVENT_JOURNAL_PATH` na trwałym wolumenie i
kanonicznego małego UUID w `T4_OBSERVATION_CAMPAIGN_ID`.

UUID, klucz i pusty plik dziennika wybierz przed startem bridge'a. Muszą być nowe
dla każdej kampanii. Ten sam UUID jest później obowiązkowym argumentem
`observe-start --campaign-id`. Bridge publikuje fingerprint SHA-256 klucza
publicznego w `/healthz`.

Podpis ECDSA potwierdza pochodzenie zdarzenia z naszego bridge'a dowodowego. Nie
jest podpisem ani atestacją Plus500/T4.

`GET /v1/observation/events?after_sequence=0&limit=100` zwraca stronę z
`public_key_spki_base64` i zdarzeniami. Numeracja oraz `previous_event_hash` są
globalne dla pliku JSONL i nie zerują się po restarcie. Zewnętrzny `boot_id`
strony opisuje bieżący proces; zdarzenia historyczne zachowują własne `boot_id`.
Każde zdarzenie zawiera kanoniczne claims w `canonical_payload_base64`, SHA-256
w `event_hash_sha256` i podpis DER w `signature_base64`; algorytm ma stałą
wartość `ecdsa-p256-sha256-der`.

Kontrolowane próby są domyślnie niedostępne. Wymagają
`T4_OBSERVATION_CONTROL_ENABLED=true`, zgodnego campaign UUID oraz osobnego
`T4_OBSERVATION_CONTROL_TOKEN` w nagłówku
`X-Crypto-Agent-Observation-Control-Token`. Jedyny endpoint zapisu to
`POST /v1/observation/control/{action}`; ścisła lista `action` to `reconnect`,
`missing_data`, `stale_data`, `rate_limit`, `restart`. Body zawiera wyłącznie
`campaign_id`, unikalny `action_request_id` i kanoniczny `scope_key`. Ten sam
scope jest podpisany w zdarzeniach kontrolnych, a fault może zostać zużyty tylko
przez odpowiadający mu odczyt BTC/ETH. Receipt `202` zawiera scope, numer i hash
utrwalonego zdarzenia, `read_only=true` oraz `execution_enabled=false`.
`restart` zatrzymuje host dopiero po zakończeniu odpowiedzi z utrwalonym receipt.
Etap 1 nie uruchamia go ponownie: próba wymaga zewnętrznego supervisora. Stały
nadzór 24/7 należy do etapu 2.

Kontrolowany `rate_limit` sprawdza zachowanie handlera bridge'a i dalszej ścieżki
fail-closed. Nie dowodzi rzeczywistego `429` od T4. Naturalnego limitu upstream
nie wolno wymuszać spamowaniem dostawcy.

Odpowiedź `503` z `/v1/market-data` udostępnia wyłącznie bezpieczny kod w
`X-Crypto-Agent-T4-Reason-Code`; nie zawiera szczegółów sesji T4.

Nie zapisuj w repozytorium klucza API, tokenów, prywatnego klucza ECDSA,
dziennika ani prywatnego katalogu. Do procesu Python przekazuj tylko wymagane
lokalne tokeny, publiczny fingerprint/SPKI oraz osobny DSN weryfikatora bez
uprawnień superusera. DSN administratora jest poza ścieżką dowodową.
