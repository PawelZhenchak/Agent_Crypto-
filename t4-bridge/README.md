# Plus500 Futures T4 read-only bridge

Ta usługa tworzy zabezpieczoną granicę między oficjalnym klientem T4 a procesem
analitycznym Python. Nasłuchuje wyłącznie na loopback, wymaga osobnego tokenu i
nie ma endpointów tworzenia, modyfikowania ani anulowania zleceń.

## Obecny stan

Host, walidacja zapytań, kontrakt JSON i zabezpieczenia są gotowe. Adapter do
oficjalnego klienta T4 pozostaje celowo fail-closed do czasu zarejestrowania
aplikacji u Plus500 Futures Technologies/CTS i otrzymania aktualnego pakietu API.
Bez tego `/healthz` i `/v1/market-data` zwracają `503`, a agent zwraca
`NO_SIGNAL`.

## Uruchomienie granicy bezpieczeństwa

Ustaw prywatnie `T4_BRIDGE_TOKEN` (32-256 znaków) oraz
`T4_CONTRACT_CATALOG_PATH` wskazujący lokalną kopię katalogu kontraktów. Wzór
znajduje się w `configs/t4-contract-catalog.example.json`, ale zawarte tam
identyfikatory `SIM:*:2099*` są wyłącznie przykładami i nie są aktywowanymi
kontraktami T4.

Katalog zawiera logiczny symbol, rzeczywisty `contract_id`, `expires_at` oraz
opcjonalny `roll_at`. Jeżeli `roll_at` nie podano, host używa
`default_roll_days`. Przed granicą roll wybierany jest najbliższy niewygasły
kontrakt; od granicy wymagany jest następny. Brak kolejnej serii, duplikat,
nieprawidłowy identyfikator lub czas inny niż UTC zatrzymuje odczyt fail-closed.

Przykład uruchomienia:

```powershell
$env:T4_CONTRACT_CATALOG_PATH = "C:\private\t4-contracts.json"
dotnet run --project t4-bridge/CryptoAgent.T4Bridge.csproj
```

Pojedyncze zmienne `T4_BTC_CONTRACT_ID` / `T4_BTC_CONTRACT_EXPIRES_AT` i ich
odpowiedniki ETH pozostają zgodne wstecz, ale nie umożliwiają bezpiecznego rollu.
Po wejściu w okno roll host zwróci `503`, dopóki nie otrzyma katalogu z następną
serią.

Nie przesyłaj loginu, hasła, identyfikatora aplikacji ani tokenu bridge do
repozytorium lub procesu Python poza odpowiadającą mu wartością lokalnego tokenu.
