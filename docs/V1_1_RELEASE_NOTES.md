# V1.1 — notatki checkpointu 0.1.2

Data: **2026-08-10**  
System: **0.1.2-v1.1**  
Gate V1: **niezaliczony** (`metadata.v1_gate_passed=false`)

## Najważniejsza zmiana

Runtime i point-in-time persistence używają teraz jednego czystego, wersjonowanego
algorytmu `cross_exchange_spot_consensus_v1`. Obie ścieżki wymagają source keys
`kraken_spot_rest_v1` + `coinbase_exchange_spot_rest_v1`, dwóch różnych venue oraz
dokładnego, ciągłego i wyrównanego kontekstu `2 × 120` zamkniętych świec. Nie ma
fallbacku do jednego źródła.

## Utwardzenie runtime

- konfiguracja providera konsensusu jest zapieczętowana przed fetchem;
- source keys, venue, composition fingerprint i fingerprint `RiskPolicy` są
  przypięte i ponownie sprawdzane;
- adaptery nie podążają za redirectami i akceptują tylko oczekiwane hosty HTTPS;
- luki, różne końce okna, konflikt OHLC/close, brak feedu i wadliwa anomalia wolumenu
  kończą się `NO_SIGNAL`;
- standalone feed i synthetic pozostają wyłącznie diagnostyczne.

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
  bezwymiarowy i nie jest wolumenem bazowym żadnego venue.

## Dowód checkpointu

- **155/155 testów standard-library**;
- `compileall` dla całego `src` i `tests`;
- fixtures, mocki, testy negatywne, złote przypadki parytetu i fake połączenia DB;
- inspekcja statyczna migracji i reguł read-only.

Nie uruchomiono prawdziwego PostgreSQL. Środowisko nie miało Dockera, `psql`, serwera
ani `psycopg`. Nie wykonano też live contract tests wobec bieżących publicznych API.

## Nadal otwarte przed Gate V1

1. Akceptacja `schema.sql + 0011` na czystym PostgreSQL 16 i testy triggerów.
2. Operacyjny external ingest: raw HTTP payload, batch manifest, quarantine i replay.
3. Seedy source registry i idempotentny `ingest-once` uruchamiany przez scheduler.
4. Live API contracts Kraken/Coinbase.
5. Trades/order book, spread, depth i price impact.
6. Specjaliści V1, Sceptyk, evidence/trace dashboard i alert delivery.
7. 4–8 tygodni forward observation oraz formalny raport bramki.

Checkpoint nie zawiera kluczy giełdowych, składania ani anulowania zleceń, transferów,
wypłat, margin, futures ani dźwigni.
