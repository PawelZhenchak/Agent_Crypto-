# 0.2.0 — przejście na Plus500 Futures T4

- usunięto aktywne adaptery i konfiguracje poprzednich źródeł;
- dodano `Plus500T4Provider` i ścisły lokalny kontrakt bridge;
- polityka schema v3 wymaga jednej proweniencji T4;
- dodano migrację `0013_plus500_t4_runtime.sql`;
- seed i health-check wymagają wyłącznie T4;
- bump wersji z 0.1.4 do 0.2.0;
- do podłączenia pozostał właściwy worker .NET i T4 Simulator.
