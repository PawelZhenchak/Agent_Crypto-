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
3. Jednoznaczna kwalifikacja delivery: tylko aktualny live T4 `ALERT` po risk
   gate; historyczne kryterium schema v4 zostało zaostrzone do schema v5 w
   `0.8.0`, nadal wyłącznie z `environment=live_t4`.
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

### Przygotowane w 0.8.0

1. Oficjalny reader `Plus500US.T4Proto` 1.0.73 i
   `Plus500US.T4ChartDecoder` 1.0.97, publiczny protocol commit
   `1a68b674482194f1cf3b9d7f129ce5fbed8bcb51`, WebSocket/Protobuf i Chart REST.
2. Rozdzielone, stałe endpointy Simulator/live, reconnect, resubscribe, heartbeat,
   timeout oraz prewarm cache fail-closed.
3. Schema v5 z `ExchangeID`, produktowym `ContractID`, nieprzezroczystym
   `MarketID` per świeca i niezależną tożsamością indeksu basis.
4. Outbound allowlist bez order routes i bezwarunkowe wykluczenie Simulatora z
   external delivery.
5. Migracja `0017`, zamrożony baseline, append-only cykle/zdarzenia, blokada
   backfillu i niezmienny raport końcowy.
6. Polityka 672 godzin z progami pokrycia, sukcesu, luk i RTT, naruszeniami
   read-only oraz obowiązkowymi scenariuszami.
7. Komendy `observe-start`, `observe-run`, `observe-status`, `observe-report`;
   live preflight przed startem, pojedynczy fetch powiązany z ingestem i analizą
   w należnym slocie oraz brak finalizacji raportu przed
   `planned_ends_at + cycle_interval_seconds`.

### Przygotowane w 0.9.0 — etap 1

1. Migracja `0018` z rejestrem kluczy bridge'a, podpisanymi zdarzeniami,
   żądaniami prób i append-only wynikami scenariuszy.
2. Weryfikacja fingerprintu SPKI, ECDSA P-256, kanonicznych claims i globalnego
   hash chain przez restarty bridge'a.
3. Rozłączne role `crypto_agent_evidence_verifier` i read-only
   `crypto_agent_evidence_reader`; caller nie przekazuje eventów ani wyniku
   `PASS`.
4. Pięć prób kontrolowanych: `bridge_restart`, `missing_data`, `rate_limit`,
   `reconnect`, `stale_data`.
5. Dwie próby pasywne: `replay_blocked` samodzielnie odtwarzany i atestowany
   przez isolated verifier oraz `roll_transition`, który nie ma injectora i
   wymaga realnego live batcha schema v5.
6. Niezależny predykat PostgreSQL dla bramki: pełne okno, progi jakości,
   bezpieczeństwo i siedem scenariuszy `pass`.
7. Komenda `observe-scenarios`, która nie przyjmuje wyniku scenariusza.

Podpisane testy offline potwierdzają nasz bridge i walidator, nie podpis T4.
Kontrolowany `rate_limit` testuje handler, nie rzeczywiste `429` dostawcy.

### Nadal wymagane na rzeczywistym T4

1. Provisioning `T4_API_KEY`, prawidłowych identyfikatorów rynków oraz uprawnień
   depth i niezależnego indeksu.
2. Contract test schema v5 najpierw na `t4_simulator`, potem osobno na
   `live_t4`. Simulator nie zalicza live delivery.
3. Test sesji, reconnectu, naturalnych limitów, opóźnień, braków danych i rollu
   na przydzielonym koncie. Nie wolno wywoływać `429` spamowaniem T4.
4. Weryfikacja spread/depth/basis na rzeczywistych danych i kontrolowane wykonanie
   wszystkich obowiązkowych scenariuszy.
5. Operacyjne uruchomienie i nadzór `observe-run` dla obu scope’ów przez całe
   zamrożone okno.
6. Minimum 672 godziny obserwacji czasu rzeczywistego dla zamrożonego scope’u
   BTC/ETH 4h. Publiczny dwutygodniowy Simulator jest niewystarczający.
7. Zapis końcowego raportu i komplet obowiązkowych kryteriów `PASS`.
8. Automatyczny supervisor `0.10.0` dla kontrolowanego restartu, cykli obu
   scope’ów, timeoutów, missed, dziennego stanu i działania 24/7.

Provisioning, rzeczywiste testy Simulator/live i kampania nie zostały wykonane.
Nie ma jeszcze dostępu T4 ani rzeczywistych `ExchangeID`, `ContractID` i
`MarketID`.

W `0.9.0` `PASS` nie jest blokowany stałą. Wynik pozostaje jednak
`v1_gate_passed=false`, ponieważ nie wykonano realnych 672 godzin ani siedmiu
dowodów, w tym prawdziwego rollu.

Przed kampanią trzeba wybrać nowy UUID, nowy klucz bridge'a i nowy pusty
dziennik. Runtime, verifier-write i evidence-reader korzystają z trzech
rozłącznych loginów PostgreSQL bez superusera; DSN administratora jest poza
wszystkimi ścieżkami.
