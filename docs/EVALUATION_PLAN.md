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

## Zaliczone offline w 0.7.0

1. Trace ID tworzony przed analizą i bezpieczny zapis runu, eventów oraz artifactu.
2. Fail-closed provider error i timeout z lokalnym, deduplikowanym incident logiem;
   brak surowych wyjątków, credentials, URL i nagłówków w projekcjach.
3. Jednoznaczna kwalifikacja delivery: tylko aktualny live T4 `ALERT` po risk gate,
   z bridge schema v4 i `environment=live_t4`.
4. Negatywne przypadki `NO_SIGNAL`, veto, stale, synthetic, fixture, replay,
   provider error i expiry nigdy nie tworzą dostawy.
5. Migracja `0016`, atomowy PostgreSQL run/artifact + alert + event + outbox,
   idempotency key, payload SHA-256 i append-only historia prób.
6. Retry z ograniczonym backoffem, limit prób, expiry, terminalność i hash chain.
7. Jedyna trasa `stdout_json` → `process_stdout`; brak arbitralnego webhooka oraz
   integracji Slack, Telegram, e-mail i SMS.
8. Semantyka at-least-once trwałego outboxa oraz deduplikacja konsumenta po
   `idempotency_key`; brak deklaracji exactly-once.
9. Analiza API wyłącznie przez `POST /v1/analyze` z nagłówkiem
   `X-Crypto-Agent-Request: analyze-v1`; brak mutującego wariantu GET.
10. Read-only dashboard i projekcje alertów/incydentów/trace z escapingiem danych.
11. API tylko na loopback, bez CORS i dokumentacji API, z restrykcyjnymi nagłówkami
   bezpieczeństwa.

Fixture’y używane w tych testach nie są danymi live i nie zaliczają testu T4
Simulator ani realnego dostarczenia live.

## Pozostałe przed Gate V1 — punkt 8

1. Rejestracja aplikacji i podłączenie oficjalnego klienta T4; obecny reader
   pending zwraca `503`.
2. Live contract test schema v4 z `environment=live_t4` na T4 Simulator.
3. Test sesji, reconnectu, limitów, opóźnień i braków danych live.
4. Weryfikacja spread/depth/basis/roll na rzeczywistych danych T4.
5. Walidacja trace, incydentów i lokalnego stdout delivery w realnym cyklu live.
6. Minimum cztery tygodnie obserwacji read-only oraz raport jakości przed Gate V1.

Do ukończenia powyższych kroków `v1_gate_passed=false`.
