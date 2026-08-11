# Aktualny stan — 0.2.0

## Gotowe

- aktywna konfiguracja przyjmuje tylko `synthetic` lub `t4`;
- jedyne produkcyjne źródło ma identyfikator `plus500_t4_futures_v1`;
- usunięto adaptery, factory choices, seedy i testy poprzednich źródeł;
- dodano ścisły adapter loopback do mostu T4 .NET;
- proces analityczny odrzuca dane logowania T4;
- PostgreSQL ma migrację `0013`, dwa bindingi futures i immutable runtime config;
- polityka schema v3 wymaga jednej atestowanej proweniencji T4;
- lokalne testy jednostkowe: 68/68.

## Świadomie zachowana historia

Pliki migracji `0011` i `0012` są checksummowane i mogły zostać już zastosowane.
Nie wolno ich przepisywać. Ich dawne nazwy pozostają wyłącznie w historycznym SQL.
Nie są aktywną konfiguracją ani źródłami runtime.

## Jeszcze niegotowe

- właściwy worker .NET korzystający z oficjalnych bibliotek T4;
- konto T4 Simulator i test live contract;
- rozwiązywanie rzeczywistego kontraktu front-month oraz kontrolowany roll;
- operacyjny zapis danych T4 i replay w PostgreSQL;
- ponowny test migracji `0013` na prawdziwym PostgreSQL 16 w GitHub Actions;
- wielotygodniowy odbiór read-only V1.

Do czasu podłączenia workera T4 system ma zwracać `NO_SIGNAL`, a nie dane zastępcze.
