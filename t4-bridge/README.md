# Plus500 Futures T4 read-only bridge

Usługa .NET 8 jest izolowaną granicą pomiędzy oficjalnym T4 API a procesem
analitycznym Python. Nasłuchuje wyłącznie na loopback, wymaga osobnego tokenu i
nie wystawia endpointów tworzenia, modyfikowania ani anulowania zleceń.

## Implementacja 0.8.0

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
  `Heartbeat` i `MarketDepthSubscribe`; wiadomości order-routing są zabronione.

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
dotnet run --project t4-bridge/CryptoAgent.T4Bridge.csproj
```

`/healthz` przechodzi w `READY` dopiero po zalogowaniu, uzyskaniu wymaganych
uprawnień oraz zapełnieniu cache snapshotów futures/index i historii. Przedtem
`/v1/market-data` zwraca `503`; agent odpowiada `NO_SIGNAL` bez danych
zastępczych.

Envelope live schema `v5` wymaga `environment=live_t4`, pełnego i
nieprzeciętego order booka, statusu sesji, point-in-time timestampów, niezależnej
referencji basis oraz — w oknie rollu — zsynchronizowanych snapshotów starego i
nowego `MarketID`. Każda niezgodność kończy się fail-closed.

Bridge i kontrakt danych zachowują obsługę 4h/1d/1w, natomiast zamrożony zakres
odbioru live V1 w `0.8.0` obejmuje obecnie tylko 4h (`240` minut) dla BTC i ETH.
Fixture’y kontraktowe nie są testem T4 Simulator ani live.

Nie zapisuj klucza API, tokenu bridge ani prywatnego katalogu w repozytorium i
nie przekazuj ich do procesu Python poza odpowiadającą wartością lokalnego tokenu
bridge.
