# 0.6.0 — deterministyczna analiza futures

## Dodano

- kontrakt bridge schema v3 z pełnym `futures_evidence`;
- metryki spread, depth, book imbalance, basis, annualized basis, wolumenu,
  expiry risk i wpływu rollu;
- wersjonowaną politykę futures, w której jedyną zatwierdzoną referencją basis
  jest typ `index` ze źródła `plus500_t4_index_v1`;
- scenariusze bull/base/bear, przeciwne dowody i warunki unieważnienia raportu;
- migrację `0015_t4_futures_evidence.sql` z append-only snapshotami, poziomami
  order booka i dowodami przejścia kontraktu;
- hashe kanonicznej treści evidence oraz włączenie pełnego evidence do
  deterministycznego fingerprintu;
- point-in-time replay pełnego schema v3 i komendę CLI `analyze-replay`;
- testy zgodności analizy bieżącej i replayu, manipulacji evidence oraz
  fail-closed dla historycznego schema v2.

## Granice bezpieczeństwa

- historyczne batche schema v2 pozostają odczytywalne, lecz zawsze prowadzą do
  `NO_SIGNAL`, ponieważ nie zawierają pełnego futures evidence;
- brakujące, stare, niespójne lub niezatwierdzone evidence nie jest zastępowane
  zerem ani danymi wyprowadzonymi ze świecy;
- oficjalny klient T4 nadal nie jest podłączony, a reader pending zwraca `503`;
- fixture’y służą tylko testom offline i nie udają danych live;
- brak tras składania, zmiany i anulowania zleceń;
- `v1_gate_passed=false`; następnym etapem jest punkt 7 — monitoring i alert
  delivery.
