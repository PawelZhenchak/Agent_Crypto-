# Bieżący status

Data checkpointu: **2026-08-10**  
Pakiet: **0.1.4**  
System: **0.1.4-v1.1**  
Stan: **V1.1 development checkpoint — Gate V1 niezaliczony**

## Ukończone w tym checkpointcie

- dwa publiczne adaptery spot: Kraken i Coinbase Exchange;
- przypięte hosty HTTPS, jawne odrzucanie redirectów i brak obsługi sekretów
  giełdowych;
- deterministyczne 4h/1d/1w bez fill; Kraken używa natywnego interwału 1w, a
  tygodnie Coinbase są składane z pełnych świec 1d od epoki Unix (czwartek UTC),
  zgodnie z kontraktem fixtures;
- zapieczętowany provider cross-exchange consensus tylko dla dokładnych source keys
  `kraken_spot_rest_v1` + `coinbase_exchange_spot_rest_v1` i dwóch różnych venue;
- rewalidacja kodu/defaultów/closure metod, krytycznych map, originu, timeoutu i
  łańcucha transportu przed i po fetchu; znane podmiany kończą się fail-closed;
- wspólny dla runtime i persistence, wersjonowany algorytm
  `cross_exchange_spot_consensus_v1`;
- stałe okno dokładnie 120 wyrównanych, ciągłych świec na źródło, zakończone
  najnowszym kwalifikującym się zamkniętym oknem;
- fail-closed dla awarii feedu, duplikatu, luki, złego symbolu, czasu, OHLC,
  niefinitywnych wartości, rozjazdu close/OHLC i niespójnej anomalii wolumenu;
- wymagany pełny fingerprint tej samej `RiskPolicy` w consensus providerze,
  RiskGate i trwałym recompute;
- standalone i synthetic nie mogą dostarczyć dowodu pozwalającego na `ALERT`;
- RiskGate niezależnie waliduje jakość, metryki, UTC, expiry, SHA-256 i decyzję;
- osobny `cross_exchange_reference_price_v1`: dokładnie dwie zamknięte i wyrównane
  świece 1m, 300 s wieku oraz maksymalnie 50 pb każdego źródła od mediany;
- RiskGate ponownie przelicza snapshot ceny, a raw observations są związane z hashem
  wejścia i zapisywanym snapshotem raportu;
- bounded deadline API obejmuje build, inicjalizację storage, fetch, analizę i zapis;
  synchroniczne operacje nie blokują event loop FastAPI, a 504 nie może zakończyć
  się późnym zapisem raportu;
- CLI PostgreSQL: `plan`, `migrate`, `health`;
- migracja `0011` dla venue bindings, receipts, canonical series, computation
  manifests i raw provenance;
- migracja `0012` dla append-only reference-price manifests/provenance, przypiętych
  receipt IDs i deferred DB-derived replayu 2×1;
- repozytorium wyprowadza z bazy latest eligible revision dla każdego wymaganego
  source/window; caller IDs są tylko assertion równości i nie wybierają danych;
- trwałe przeliczenie wymaga dokładnego kontekstu `Kraken 120 + Coinbase 120`,
  ciągłości, zgodnego końca okna i pełnej polityki;
- wynik OHLC jest obliczany ponownie przez ten sam algorytm, a nie przyjmowany z
  niezaufanego draftu;
- odczyt canonical ponownie wykonuje algorytm na 240 raw rows i sprawdza normalized
  volume, diagnostykę, ważność polityki dla cutoffu oraz pełny łańcuch hashy;
- uporządkowane role wejść, okno, wersja algorytmu, fingerprint polityki, evidence
  digest, normalized volume i diagnostyka trafiają do manifestu/provenance;
- bezwymiarowy normalized volume nie jest podszywany pod wolumen bazowy —
  `candles.base_volume` dla canonical pozostaje `NULL`;
- source market IDs pozostają venue-specific, a canonical używa osobnej serii;
- point-in-time cutoff obejmuje availability, provider ingestion, durable receipt i
  canonical lineage;
- ścieżka odczytu odtwarza wynik z pełnego provenance `2 × 120`, kontroluje okres
  ważności polityki przy cutoffie oraz odrzuca zmianę danych, diagnostyki lub hashy;
- append-only raporty i snapshoty SQLite pozostają dostępne do lokalnego replayu;
- health wymusza PostgreSQL 16, pełny relkind/domain/function/trigger footprint,
  dokładne kolumny ośmiu tabel migracyjnych, packaged manifest migracji i dokładny
  zestaw semantycznych seedów;
- narrator normalizuje Unicode/Markdown i blokuje znane bezpośrednie oraz pośrednie
  rekomendacje PL/EN także wtedy, gdy model błędnie oznaczy tekst jako bezpieczny.

Lokalny dowód regresyjny: **215/215 testów standard-library** i pełny
`compileall` dla `src` oraz `tests`. Nie zastępuje to live contract testów ani
akceptacji na prawdziwym PostgreSQL.

## Wynik audytu czterech defektów

Wszystkie cztery znane reprodukcje mają poprawki i regresje: podmiana feedu nie może
już zatwierdzić konsensusu, health nie akceptuje PG12 ani płytkiego schematu, cena
referencyjna ma oddzielny kontrakt 2×1m/300 s/50 pb, a znane parafrazy narratora są
odrzucane fail-closed.

Pozostają dwie granice architektoniczne. Attestation w jednym interpreterze jest
defense-in-depth, a nie sandboxem dla arbitralnego pluginu — rzeczywista granica
wymaga osobnego immutable ingest workera. Filtr językowy nie jest formalnym dowodem
pokrycia wszystkich parafraz, więc narrator pozostaje domyślnie wyłączony do czasu
ukrytych evali z 100% recall. Te ograniczenia nadal blokują formalny Gate V1.

## Częściowo ukończone

### PostgreSQL

Repo zawiera operacyjne, idempotentne seedy registry oraz workflow akceptacyjny dla
czystego PostgreSQL 16. Workflow wykonuje `schema.sql + 0011 + 0012`, dwukrotnie
uruchamia seeding, wymaga `READY`, sprawdza constrainty, append-only triggery i
rollback, restartuje kontener oraz ponownie potwierdza trwałość danych i `READY`.

### Trwały ingest

Gotowa jest fail-closed warstwa append-only persistence z DB-derived selection,
point-in-time lineage i parytetem algorytmu konsensusu. Nie jest jeszcze kompletnym
operacyjnym pipeline'em external ingest. Brakuje atomowego zapisu raw HTTP payload/body,
finalnego batch manifestu transportowego, automatycznej kwarantanny i replayu oraz
idempotentnego harmonogramu. Orchestrator nie używa jeszcze
canonical PostgreSQL jako live wejścia analizy.

### Obserwowalność i pokrycie rynku

Raport zachowuje snapshot i hash udanej analizy. Nieudany consensus zachowuje dostępne
raw candles, source IDs, zmierzone rozjazdy, progi i kod błędu. Pełny manifest
transportowy każdego nieudanego HTTP fetchu, trace narzędzi, dashboard i alert delivery
pozostają otwarte. Brakuje też order booka, spreadu, depth, price impact oraz
zaakceptowanych live API contracts.

## Najbliższa kolejność prac

1. Wydzielić minimalny ingest worker do przypiętego, immutable obrazu z osobną rolą
   bazy i allowlistą egress.
2. Dodać atomowy external-ingest batch: request URI, raw response bytes/hash,
   extractor/source-registry versions, wynik `completed|failed|quarantined` i replay.
3. Dodać idempotentny `ingest-once` przeznaczony do uruchamiania przez scheduler.
4. Powiązać failed-consensus evidence z trwałym batch/quarantine registry.
5. Uruchomić live contract tests dla publicznych API Kraken i Coinbase.
6. Przeprowadzić ukryte evale narratora PL/EN z 100% recall dla rekomendacji.
7. Zbudować dwusource trades/order book, spread, depth i price impact.
8. Dodać specjalistów V1, Sceptyka, evidence service, trace evals i dashboard.
9. Przeprowadzić 4–8 tygodni forward observation i formalny Gate V1.

Do wykonania wszystkich punktów `metadata.v1_gate_passed=false`, środowisko
`production` jest blokowane, a V2 nie może się rozpocząć.
