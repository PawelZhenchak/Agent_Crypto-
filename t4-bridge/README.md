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

Ustaw prywatnie `T4_BRIDGE_TOKEN` (32-256 znaków), rzeczywiste identyfikatory
kontraktów i ich daty wygaśnięcia, a następnie:

```powershell
dotnet run --project t4-bridge/CryptoAgent.T4Bridge.csproj
```

Nie przesyłaj loginu, hasła, identyfikatora aplikacji ani tokenu bridge do
repozytorium lub procesu Python poza odpowiadającą mu wartością lokalnego tokenu.
