# Plan ewaluacji T4

## Zaliczone offline w 0.6.0

1. Parser schema v3 i fail-closed dla błędnych, niepełnych lub zmienionych payloadów.
2. Bridge tylko na loopback, prawidłowy token, `read_only=true` i brak order routes.
3. Walidacja pełnego order booka, statusu sesji i synchronizacji timestampów UTC.
4. Deterministyczne metryki spread, depth, imbalance, basis, annualized basis,
   wolumen, expiry risk i wpływ rollu.
5. Basis tylko z typed reference `index` o source ID `plus500_t4_index_v1`;
   niezatwierdzona lub stara referencja wymusza `NO_SIGNAL`.
6. Test wygaśnięcia, kontrolowanego rollu i zsynchronizowanego transition evidence.
7. Migracja `0015`, append-only evidence hashes, restart PostgreSQL 16 i ponowne
   `READY`.
8. Deterministyczny replay/analyze-replay; ten sam evidence daje ten sam
   fingerprint i wynik, a historyczne v2 pozostaje odczytywalne, lecz daje
   `NO_SIGNAL`.
9. Scenariusze bull/base/bear, przeciwne dowody i warunki unieważnienia raportu.

Fixture’y używane w tych testach nie są danymi live i nie zaliczają testu T4
Simulator.

## Pozostałe przed Gate V1

1. Rejestracja aplikacji i podłączenie oficjalnego klienta T4; obecny reader
   pending zwraca `503`.
2. Live contract test schema v3 na T4 Simulator.
3. Test sesji, reconnectu, limitów, opóźnień i braków danych live.
4. Weryfikacja spread/depth/basis/roll na rzeczywistych danych T4.
5. Punkt 7: monitoring, trace, dashboard, incident log i dostarczanie alertów.
6. Minimum cztery tygodnie obserwacji read-only oraz raport jakości przed Gate V1.

Do ukończenia powyższych kroków `v1_gate_passed=false`.
