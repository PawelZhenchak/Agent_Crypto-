# Informacje o wydaniu 0.9.0 — etap 1 dowodów scenariuszy

Wersja `0.9.0` dodaje automatyczne, kryptograficznie weryfikowane dowody
obowiązkowych scenariuszy kampanii T4. Nie oznacza to, że odbiór V1 już przeszedł:
bramka może otrzymać `true` wyłącznie po pełnej, rzeczywistej kampanii 28-dniowej,
ze wszystkimi kryteriami jakości i siedmioma scenariuszami zweryfikowanymi przez
PostgreSQL. Bez provisionowanego T4 i rzeczywistych danych wynik pozostaje
`false`.

## Dodane mechanizmy

- bridge utrzymuje trwały, append-only dziennik JSONL z globalnym łańcuchem
  SHA-256, nowym `boot_id` po restarcie i podpisami ECDSA P-256;
- osobny, domyślnie wyłączony token kontrolny dopuszcza wyłącznie
  `reconnect`, `missing_data`, `stale_data`, `rate_limit` i `restart`;
- kontrola nie przyjmuje dowolnego Protobuf ani danych rynkowych i nie dodaje
  tras zleceń;
- Python przypina fingerprint klucza w baseline kampanii, weryfikuje SPKI,
  podpis, projekcję kanonicznych claims oraz cały łańcuch przed zapisem;
- migracja `0018` rozdziela rolę weryfikatora od runtime, wiąże dowody z realnymi
  batchami/cyklami/runami i sama wylicza wynik scenariusza oraz końcową bramkę;
- runtime, verifier-write i evidence-reader wymagają trzech osobnych loginów
  PostgreSQL bez superusera i bez nakładających się ról; DSN administratora lub
  właściciela nie należy do żadnej ścieżki kampanii;
- błędy bridge'a zachowują bezpieczny kod przyczyny, więc `stale`, brak danych,
  rate limit i disconnect są rozróżnialne w ledgerze;
- prawdziwy roll kontraktu pozostaje scenariuszem pasywnym: nie można go
  zaliczyć przez injector. Musi istnieć live batch schema v5 ze starym i nowym
  `MarketID` oraz zgodny rekord przejścia;
- `replay_blocked` uruchamia po stronie isolated verifiera in-memory replay 120
  świec wszystkich zamrożonych scope’ów przez evidence-reader i zapisuje
  verifier-only atestacje fingerprintu, batch ID/hash oraz provenance. Replay
  potwierdza zachowanie fail-closed i brak delivery; nigdy nie jest dowodem live.

## Granice uczciwości

- kontrolowany `rate_limit` dowodzi zachowania naszego handlera, nie wystąpienia
  rzeczywistego limitu po stronie T4; naturalny upstream `429` jest oznaczany
  osobno i nie wolno go wywoływać spamowaniem dostawcy;
- brak prawdziwego rollu w oknie kampanii daje `NOT_OBSERVED`, a nie sztuczny
  `PASS`;
- podpis bridge'a jest podpisem naszego komponentu dowodowego, nie podpisem
  Plus500/T4;
- stary baseline `0.8.0` nie może zostać podniesiony w miejscu. Po wdrożeniu
  `0.9.0` trzeba przed startem bridge'a wybrać nowy UUID, nowy klucz i nowy pusty
  dziennik, a następnie rozpocząć nową kampanię pełnych 28 dni.

Wydanie nie uruchamia jeszcze automatycznego supervisora kampanii ani wdrożenia
24/7. To pozostaje kolejnym etapem po potwierdzeniu zielonego CI dla warstwy
dowodowej.

Nie ma jeszcze dostępu do rzeczywistego T4 ani prawdziwych identyfikatorów
rynków. Kampania nie została rozpoczęta, więc obecny wynik pozostaje
`v1_gate_passed=false`.
