# Aktualny stan — 0.5.0

## Gotowe

- aktywna konfiguracja przyjmuje tylko `synthetic` lub `t4`;
- jedyne produkcyjne źródło ma identyfikator `plus500_t4_futures_v1`;
- usunięto adaptery, factory choices, seedy i testy poprzednich źródeł;
- dodano ścisły adapter loopback do mostu T4 .NET;
- proces analityczny odrzuca dane logowania T4;
- PostgreSQL ma migrację `0013`, dwa bindingi futures i immutable runtime config;
- polityka schema v3 wymaga jednej atestowanej proweniencji T4;
- host bridge .NET 8 nasłuchuje wyłącznie na loopback;
- każde żądanie bridge wymaga tokenu o długości minimum 32 znaków;
- kontrakt odrzuca nieznany, nieważny lub wygasły rzeczywisty contract ID;
- bridge nie zawiera endpointów zleceń i domyślnie działa fail-closed;
- CI buduje bridge i uruchamia osobne testy kontraktowe .NET.
- lokalny katalog kolejnych serii wybiera front-month według czasu UTC;
- granica `roll_at` wymusza kontrolowane przejście na następną serię;
- schema bridge v2 zachowuje contract ID, expiry, roll i poprzednią serię;
- brak bezpiecznej kolejnej serii kończy się `NOT_READY` / `NO_SIGNAL`.
- migracja `0014` dodaje atomowy, append-only ingest T4;
- każdy batch zachowuje dokładny payload base64 i jego SHA-256;
- rzeczywisty kontrakt, expiry, roll i cena referencyjna są trwale związane z batchem;
- ponowny identyczny payload jest idempotentny i nie duplikuje świec;
- replay respektuje historyczne `as_of`, działa w transakcji read-only i ma
  deterministyczny fingerprint;
- dostępne są pojedyncze i cykliczne uruchomienia ingestu przez CLI.

## Świadomie zachowana historia

Pliki migracji `0011` i `0012` są checksummowane i mogły zostać już zastosowane.
Nie wolno ich przepisywać. Ich dawne nazwy pozostają wyłącznie w historycznym SQL.
Nie są aktywną konfiguracją ani źródłami runtime.

## Jeszcze niegotowe

- rejestracja aplikacji T4 u Plus500 Futures Technologies/CTS;
- adapter oficjalnego klienta T4 po otrzymaniu aktualnego pakietu i przykładów API;
- konto T4 Simulator i test live contract;
- live contract test ingestu po uzyskaniu konta Simulator i oficjalnego klienta;
- reconnect sesji oraz snapshoty głębokości rynku z prawdziwego T4;
- wielotygodniowy odbiór read-only V1.

Do czasu podłączenia workera T4 system ma zwracać `NO_SIGNAL`, a nie dane zastępcze.
