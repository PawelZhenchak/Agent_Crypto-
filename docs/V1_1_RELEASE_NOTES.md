# V1.1 — notatki checkpointu 0.1.3

Data: **2026-08-10**  
System: **0.1.3-v1.1**  
Gate V1: **niezaliczony** (`metadata.v1_gate_passed=false`)

## Najważniejsza zmiana

Checkpoint zamyka cztery znane defekty graniczne: podmianę zatwierdzonej implementacji
feedu, płytki health-check PostgreSQL, niespójny kontrakt świeżości/progu ceny
referencyjnej oraz znane pośrednie rekomendacje narratora. Runtime i point-in-time
persistence zachowują wspólny `cross_exchange_spot_consensus_v1`, a cena referencyjna
ma oddzielny `cross_exchange_reference_price_v1` oparty na dokładnie dwóch zamkniętych
i wyrównanych świecach 1m.

## Utwardzenie runtime

- konfiguracja providera konsensusu jest zapieczętowana przed fetchem;
- exact snapshot kodu/defaultów/closure metod, krytycznych map, originu i transportu
  jest sprawdzany przed i po fetchu;
- source keys, venue, composition fingerprint i fingerprint `RiskPolicy` są
  przypięte i ponownie sprawdzane;
- adaptery nie podążają za redirectami i akceptują tylko oczekiwane hosty HTTPS;
- luki, różne końce okna, konflikt OHLC/close, brak feedu i wadliwa anomalia wolumenu
  kończą się `NO_SIGNAL`;
- standalone feed i synthetic pozostają wyłącznie diagnostyczne;
- RiskGate niezależnie przelicza wiek do 300 s oraz maksymalne odchylenie 50 pb od
  mediany; observations ceny są częścią hasha wejścia i trwałego snapshotu;
- malformed DTO i brak ceny kończą się `NO_SIGNAL`, nigdy wyjątkiem omijającym veto;
- narrator normalizuje Unicode, znaki niewidoczne, Markdown i whitespace, po czym
  blokuje znane bezpośrednie oraz pośrednie rekomendacje PL/EN fail-closed.

## Utwardzenie persistence

- latest eligible raw revisions są wyprowadzane z bazy według cutoffu;
- caller-provided IDs nie wybierają danych — służą tylko jako assertion równości;
- akceptowany jest wyłącznie dokładny kontekst Kraken 120 + Coinbase 120, ciągły i
  zakończony właściwą obserwacją;
- pełny fingerprint `RiskPolicy` i ten sam algorytm co runtime są obowiązkowe;
- manifest wiąże uporządkowane wejścia i role, granice okna, wersję algorytmu,
  fingerprint polityki, evidence digest, normalized volume i diagnostykę;
- provenance po zapisie jest sprawdzane względem dokładnie oczekiwanego zbioru;
- loader wykonuje pełny replay 240 raw rows i weryfikuje normalized volume,
  diagnostykę, policy validity dla historycznego cutoffu oraz łańcuch
  inputs/payload/evidence/content hash;
- odczyt trwały ponownie wylicza konsensus z `2 × 120`, weryfikuje okres ważności
  polityki przy zapisanym cutoffie i kompletny łańcuch hashy manifestu;
- canonical `candles.base_volume` pozostaje `NULL`, ponieważ normalized volume jest
  bezwymiarowy i nie jest wolumenem bazowym żadnego venue;
- migracja `0012` dodaje append-only `reference_price_manifests` oraz
  `reference_price_provenance`, przypina dokładny `(candle_id, receipt_id)` i przy
  COMMIT ponownie liczy 2×1, 300 s oraz relację 50 pb ↔ 100 pb pairwise;
- historyczny dokument policy schema-r1 zachowuje oryginalny hash do odczytu/replayu,
  ale nie może zasilać nowego zapisu; bieżące decyzje i manifesty wymagają schema-r2;
- health zwraca `READY` tylko dla PostgreSQL 16 z dokładnym footprintem tabel,
  kolumn migracyjnych i triggerów, checksummowanym packaged manifestem migracji
  oraz dokładnym zestawem semantycznych seedów.

## Dowód checkpointu

- **213/213 testów standard-library**;
- `compileall` dla całego `src` i `tests`;
- fixtures, mocki, testy negatywne, złote przypadki parytetu i fake połączenia DB;
- inspekcja statyczna migracji i reguł read-only.

Nie uruchomiono prawdziwego PostgreSQL. Środowisko nie miało Dockera, `psql`, serwera
ani `psycopg`. Nie wykonano też live contract tests wobec bieżących publicznych API.

## Nadal otwarte przed Gate V1

1. Akceptacja `schema.sql + 0011 + 0012` i seedów na czystym PostgreSQL 16.
2. Osobny immutable ingest worker; attestation w jednym procesie pozostaje tylko
   defense-in-depth wobec przypadkowej/znanej mutacji, nie sandboxem dla pluginów.
3. Operacyjny external ingest: raw HTTP payload, batch manifest, quarantine i replay.
4. Seedy source registry i idempotentny `ingest-once` uruchamiany przez scheduler.
5. Live API contracts Kraken/Coinbase.
6. Ukryte evale narratora z 100% recall; narrator do tego czasu pozostaje default-off.
7. Trades/order book, spread, depth i price impact.
8. Specjaliści V1, Sceptyk, evidence/trace dashboard i alert delivery.
9. 4–8 tygodni forward observation oraz formalny raport bramki.

Checkpoint nie zawiera kluczy giełdowych, składania ani anulowania zleceń, transferów,
wypłat, margin, futures ani dźwigni.
