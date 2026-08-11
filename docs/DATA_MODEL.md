# Model danych Crypto Agent V1–V4

Ten model jest zaprojektowany tak, aby agent potrafił odtworzyć nie tylko **co było prawdą o rynku**, lecz przede wszystkim **co system mógł rzeczywiście wiedzieć w chwili decyzji**. To rozróżnienie jest podstawą uczciwego backtestu, forward testu, audytu i bezpiecznego paper tradingu.

Implementacja bazowa znajduje się w `db/schema.sql`, a checkpoint V1.1 dodaje migracje
`0011_canonical_candle_provenance.sql` i
`0012_reference_price_policy_contract.sql`. Projekt wymaga dokładnie PostgreSQL 16 i
nie zakłada żadnego rozszerzenia. Schemat, migracje i repozytorium zostały sprawdzone
statycznie i na fake cursorach. Nie wykonano jeszcze testu akceptacyjnego na prawdziwym
PostgreSQL, ponieważ środowisko checkpointu nie miało Dockera, `psql`, serwera ani
`psycopg`.

## Najważniejsze reguły

1. Wszystkie znaczące dane są append-only. Nie poprawiamy wiersza — zapisujemy następną rewizję.
2. Każdy rekord wejściowy ma `observed_at`, `available_at` i `ingested_at`.
3. Backtest filtruje jednocześnie `available_at <= :as_of` oraz `ingested_at <= :as_of`.
4. Zwykłe obserwacje rynkowe filtrują również `observed_at <= :as_of`.
5. Każdy rekord ma źródło, wersję źródła, numer rewizji i SHA-256 treści.
6. `research_run_inputs` zamraża dokładny manifest danych użytych przez analizę.
7. LLM nie zapisuje zlecenia z pominięciem deterministycznej oceny ryzyka i wymaganej zgody człowieka.
8. Wszystkie czasy są `TIMESTAMPTZ`; aplikacja, logi i interfejs bazodanowy pracują w UTC.

## Semantyka czasu

| Pole | Znaczenie | Przykład |
|---|---|---|
| `observed_at` | Czas zdarzenia lub obserwacji opisanej przez źródło | czas transakcji, bloku albo publikacji |
| `available_at` | Najwcześniejszy potwierdzony moment, w którym ta konkretna rewizja była dostępna u źródła | czas odpowiedzi API lub oficjalnego wydania danych |
| `ingested_at` | Moment wejścia tej rewizji do granicy danych agenta | czas zapisu surowego rekordu w naszym systemie |
| `as_of_at` | Twardy cutoff wiedzy dla jednego uruchomienia badawczego | chwila podejmowania decyzji w replayu |
| `effective_from` / `effective_to` | Okres obowiązywania metadanych według danego źródła | okres notowania rynku lub obowiązywania polityki |
| `published_at` | Deklarowany czas publikacji dokumentu | czas podany przez wydawcę |
| `release_at` / `vintage_at` | Pierwsza publikacja oraz konkretna rewizja danych makro | CPI za lipiec i jego późniejsza korekta |

Obowiązuje nierówność:

```text
observed_at <= available_at <= ingested_at
```

Wyjątkiem logicznym nie jest przyszłe zdarzenie znane wcześniej. Przykładowo w `token_unlocks` data ogłoszenia znajduje się w polach czasu pochodzenia, a przyszła realizacja w `unlock_at`. Agent może użyć unlocku zaplanowanego na przyszłość wyłącznie wtedy, gdy jego rekord był już dostępny i zaingestowany przed cutoffem.

`available_at` chroni przed wykorzystaniem informacji przed jej publiczną dostępnością. `ingested_at` chroni przed wykorzystaniem danych, których nasz system wtedy jeszcze nie posiadał. Potrzebne są oba warunki.

## Pochodzenie i wersjonowanie

Każda seria danych pochodzi z `data_sources`. Wiersze faktów przechowują:

- `source_id` — identyfikator dostawcy lub wewnętrznego komponentu;
- `source_record_key` — trwały klucz rekordu w obrębie źródła;
- `source_version` — wersja API, schematu, modelu albo kontraktu danych;
- `revision_no` — numer rewizji tego samego rekordu;
- `content_hash` — SHA-256 kanonicznej reprezentacji wejścia;
- `batch_id` — opcjonalny związek z manifestem ingestu.

`source_version` i `revision_no` nie są tym samym. Przykładowo `source_version = fred-api-v2` opisuje kontrakt źródła, a `revision_no = 3` oznacza trzeci vintage konkretnej obserwacji.

Hash jest obliczany w aplikacji. Przed hashowaniem należy stosować jedną wersjonowaną kanonikalizację: stała kolejność kluczy JSON, UTC w formacie ISO-8601, zdefiniowana reprezentacja wartości dziesiętnych i brak niestabilnych pól transportowych.

## Katalog tabel

### Źródła i ingest

| Tabela | Rola |
|---|---|
| `data_sources` | Niezmienny katalog dostawców i komponentów wewnętrznych |
| `data_source_versions` | Wersje kontraktów, schematów i konfiguracji źródła |
| `ingestion_batches` | Manifest zakończonego, częściowego, odrzuconego lub nieudanego ingestu |

`ingestion_batches` jest zapisywany jako rezultat przebiegu. Bieżącego stanu nie aktualizujemy w miejscu.

### Rozszerzenie V1.1 — provenance świec

| Tabela | Rola |
|---|---|
| `schema_migrations` | Ledger wersji i checksum wykonanych migracji |
| `market_data_source_bindings` | Jawne powiązanie źródła i venue-specific market z oczekiwanym symbolem |
| `canonical_candle_series` | Venue-neutral seria pochodna związana z aktywami, interwałem i polityką |
| `source_candle_receipts` | Potwierdzenie przyjęcia raw candle przez wersjonowany extractor |
| `canonical_candle_manifests` | Niezmienny manifest algorytmu, pełnego fingerprintu polityki, okna, evidence digest, normalized volume i diagnostyki |
| `canonical_candle_provenance` | Uporządkowane powiązanie canonical candle z dokładnymi raw revisions, source/venue i rolą `context|observation` |
| `reference_price_manifests` | Niezmienny wynik 2×1m: policy/hash, cutoff, evaluation, median, divergence i evidence hash |
| `reference_price_provenance` | Dokładne dwa powiązania `(source_candle_id, source_candle_receipt_id)` użyte przez referencję |

Constrainty i triggery migracji pilnują bindingów, receipts, zgodności aktywów, cutoffów,
fingerprintu polityki, ról `context|observation` i wieloźródłowego provenance.
Repozytorium dodaje kontrole, których nie wolno pozostawić callerowi: wyprowadza z bazy
latest eligible revision dla każdego source/window według cutoffu, wymaga source keys
`kraken_spot_rest_v1` + `coinbase_exchange_spot_rest_v1` i dwóch różnych venue oraz
buduje dokładnie 120 ciągłych, wspólnie zakończonych okien na źródło. Lista IDs
przekazana przez callera może jedynie potwierdzić równość z wyprowadzonym zbiorem;
nie może wybrać starszej rewizji, skrócić okna ani pominąć luki.

Po walidacji pełnego fingerprintu `RiskPolicy` repozytorium uruchamia tę samą wersję
`cross_exchange_spot_consensus_v1`, której używa provider runtime: te same progi
close/OHLC, z-score, skale wolumenu, bounds i normalizację. Untrusted draft jest
porównywany z wynikiem recompute przed zapisem, a provenance po insercie jest ponownie
sprawdzane względem dokładnie oczekiwanego uporządkowanego zbioru.

Odczyt nie ufa samemu istnieniu manifestu. Loader pobiera pełne 240 raw rows,
ponownie wykonuje consensus, porównuje canonical OHLC i normalized volume,
diagnostykę oraz `inputs_hash`, `computed_payload_hash`, `evidence_hash` i canonical
`content_hash`. Historyczna polityka nie musi być nadal aktywna w czasie zapytania,
ale musiała obowiązywać dokładnie przy `manifest.cutoff_as_of`.

Canonical `candles` przechowuje obliczone OHLC, lecz `base_volume` pozostaje `NULL`.
Wolumen po cross-source normalizacji jest liczbą bezwymiarową, nie wolumenem bazowym
Kraken ani Coinbase. Jego wartość, diagnostyka, evidence digest, algorytm, fingerprint
polityki, granice okna oraz uporządkowane role wszystkich `2 × 120` wejść są częścią
manifestu i jego deterministycznego hasha.

Cena referencyjna ma osobny kontrakt, ponieważ close historycznej świecy 4h/1d/1w
nie jest świeżą ceną decyzyjną. Repozytorium wybiera dokładnie ostatnią kwalifikującą
się, zamkniętą świecę 1m Kraken oraz Coinbase z tego samego okna UTC, a następnie
przelicza `cross_exchange_reference_price_v1`. Wiek od `event_time` do `evaluated_at`
nie może przekroczyć 300 s; każde źródło musi pozostać najwyżej 50 pb od midpoint
mediany. Wartości bps są wersjonowane jako `ROUND_HALF_UP` do 12 miejsc.

Deferred trigger `0012` przy COMMIT ponownie wyprowadza exact eligible revisions,
sprawdza przypięte receipts i liczy wynik bez zaufania do wartości manifestu. Loader
robi ten sam replay przy odczycie. Dokładny dokument polityki schema-r1 zachowuje
historyczny fingerprint, lecz jest replay-only; nowe canonical/reference manifests
wymagają schema-r2.

Ten parytet nie czyni jeszcze PostgreSQL źródłem runtime analizy: factory nie ma jeszcze
external-ingest/replay joba, raw payload capture, kwarantanny, seedów registry ani
schedulera. Warstwa persistence jest gotowym fundamentem, lecz operacyjna integracja i
akceptacja na prawdziwym PostgreSQL pozostają otwarte.

### Tożsamości rynkowe

| Tabela | Rola |
|---|---|
| `exchanges` | Stabilna tożsamość giełdy lub venue |
| `exchange_versions` | Nazwa prawna, jurysdykcja i status znane w danym momencie |
| `assets` | Stabilna tożsamość aktywa, w tym sieć i adres kontraktu |
| `asset_versions` | Symbol kanoniczny, nazwa, decimals, emitent i lifecycle |
| `asset_symbols` | Symbole zależne od dostawcy lub giełdy |
| `markets` | Stabilna tożsamość pary/instrumentu na konkretnej giełdzie |
| `market_versions` | Tick, krok ilości, minima, fee i stan notowań |
| `market_symbols` | Surowe symbole rynku zależne od venue lub API |

Symbol `BTC`, `ETH` albo `USDT` nigdy nie jest kluczem głównym aktywa. `asset_key` powstaje w aplikacji z przestrzeni nazw, sieci i kontraktu, na przykład:

```text
native:bitcoin
eip155:1:native
eip155:1:erc20:0xdac17f958d2ee523a2206206994597c13d831ec7
```

Adres kontraktu jest normalizowany zgodnie z regułami danej sieci, a oryginalna postać może pozostać w `contract_address_display`. To zapobiega pomyleniu tokenów o tej samej nazwie na różnych sieciach.

`market_key` również jest kanoniczny i zawiera co najmniej giełdę, instrument, bazę, kwotowanie, settlement i — jeśli dotyczy — termin, strike oraz stronę opcji.

### Dane rynkowe

| Tabela | Rola |
|---|---|
| `candles` | OHLCV, interwał, finalność i rewizje świec |
| `trades` | Transakcje z kierunkiem agresora, sekwencją i flagą likwidacji |
| `orderbook_snapshots` | Nagłówek pełnego albo częściowego snapshotu order booka |
| `orderbook_levels` | Poziomy bid/ask dziedziczące provenance po snapshocie |
| `derivatives_metrics` | Funding, basis, OI, likwidacje, IV, skew i kolejne metryki |

`derivatives_metrics` jest celowo tabelą długą: jedna metryka na wiersz, z nazwą, jednostką, oknem i opcjonalnymi wymiarami JSON. Pozwala to dodawać nowe wskaźniki bez zmiany tabeli, ale słownik `metric_name` i `unit` musi być wersjonowany w kodzie ingestu.

### Kontekst on-chain, makro i informacyjny

| Tabela | Rola |
|---|---|
| `onchain_metrics` | Metryki aktywa, adresu, protokołu lub całego łańcucha z numerem bloku i finalnością |
| `macro_series` | Katalog serii, częstotliwości, jednostek i geografii |
| `macro_observations` | Obserwacje makro z zachowaniem wszystkich vintages |
| `documents` | Wiadomości, komunikaty, regulacje, whitepapery, audyty i raporty |
| `news_items` | Widok rekordów `documents` o typie `news` |
| `document_entities` | Wersjonowane powiązania dokumentu z aktywem, giełdą lub rynkiem |
| `token_unlocks` | Zapowiedziane unlocki z ilością, podstawą podaży i beneficjentem |

`onchain_metrics` nie nadpisuje bloków po reorganizacji. Nowa wersja ma nową rewizję i odpowiedni `finality_status`. Dla decyzji o wysokim ryzyku należy dopuszczać tylko wymaganą liczbę potwierdzeń.

Wartości makro są wybierane według vintage dostępnego przed cutoffem, a nie według dzisiejszej, skorygowanej serii.

Dokument może mieć tekst w bazie lub wskaźnik `storage_uri` do niezmiennego obiektu. `content_hash` dotyczy oryginalnej treści, a `normalized_text_hash` tekstu po wersjonowanym parserze. Treść z internetu jest niezaufanym wejściem, nie instrukcją dla agenta.

### Jakość danych

| Tabela | Rola |
|---|---|
| `data_quality_incidents` | Wykryty problem, zakres, reguła, źródło i dowody |
| `data_quality_incident_events` | Otwarcie, kwarantanna, potwierdzenie i rozwiązanie bez mutowania incydentu |

Krytyczny incydent dla źródła użytego w analizie powinien ustawić hard veto w risk plane. Rozwiązanie problemu nie usuwa incydentu z historii.

### Research, risk i alerty

| Tabela | Rola |
|---|---|
| `research_runs` | Niezmienna definicja analizy, cutoff, model, kod i prompt hash |
| `research_run_events` | Lifecycle przebiegu bez aktualizacji `research_runs` |
| `research_run_universe` | Dokładny skład aktywów/rynków, także delistowanych później |
| `research_run_inputs` | Manifest dokładnych rewizji wykorzystanych przez przebieg |
| `research_artifacts` | Raporty, cechy, scenariusze i inne wersjonowane wyniki |
| `risk_policies` | Wersjonowana deterministyczna polityka ryzyka |
| `risk_assessments` | Decyzja, scenariusze, kontrdowody, jakość i termin ważności |
| `risk_assessment_flags` | Flagi ostrzegawcze oraz bezwarunkowe hard veto |
| `alerts` | Niezmienna definicja alertu |
| `alert_events` | Dostarczenie, odczytanie, wygaśnięcie lub błąd kanału |

Trigger `research_inputs_cutoff_guard` odrzuca każdy input, którego `observed_at`, `available_at` lub `ingested_at` przekracza `research_runs.as_of_at`. Dzięki temu błąd w kodzie query nie może po cichu wprowadzić przyszłej wiedzy do manifestu.

Ocena ryzyka ma datę `expires_at`. Po wygaśnięciu nie może być podstawą nowej propozycji. `calibrated_score` jest dozwolony wyłącznie dla wyniku modelu skalibrowanego out-of-sample; brak kalibracji oznacza `NULL`, nie intuicyjne prawdopodobieństwo.

### Zgody, paper trading i audyt

| Tabela | Rola |
|---|---|
| `approval_requests` | Niezmienny wniosek o zgodę wraz z pełnym payloadem |
| `approval_decisions` | Jedna decyzja człowieka, osoba, czas i uzasadnienie |
| `paper_accounts` | Wersjonowane środowisko symulacji i saldo początkowe |
| `paper_orders` | Niezmienna intencja zlecenia |
| `paper_order_events` | Event-sourced lifecycle zlecenia |
| `paper_fills` | Częściowe wykonania z fee, poślizgiem, latencją i wersją modelu |
| `paper_position_snapshots` | Migawki pozycji i PnL, nigdy aktualny wiersz nadpisywany w miejscu |
| `audit_log` | Łańcuch hashy wszystkich istotnych działań |

Trigger `paper_order_approval_guard` sprawdza, czy wymagane zatwierdzenie istniało, było pozytywne, nie wygasło i było dostępne przed `submitted_at`. V1–V3 mogą tworzyć wpisy niewymagające zgody wyłącznie w odizolowanym koncie `paper` lub `shadow`; polityka aplikacyjna powinna zabronić takiego wyjątku dla canary.

`audit_log` ma osobne strumienie, rosnący `sequence_no`, `previous_entry_hash` i `entry_hash`. Trigger odrzuca przerwany łańcuch. Inserty do jednego strumienia należy serializować transakcyjnie, ponieważ dwa równoległe procesy mogą próbować użyć tego samego numeru.

## Zapytanie point-in-time

Przykład wyboru ostatniej finalnej rewizji każdej świecy, którą system faktycznie znał o `:as_of`:

```sql
WITH eligible AS (
    SELECT
        c.*,
        row_number() OVER (
            PARTITION BY c.source_id, c.source_record_key
            ORDER BY c.revision_no DESC, c.available_at DESC, c.ingested_at DESC
        ) AS revision_rank
    FROM crypto_agent.candles c
    WHERE c.market_id = :market_id
      AND c.interval_seconds = :interval_seconds
      AND c.open_time >= :from_at
      AND c.close_time <= :as_of
      AND c.observed_at <= :as_of
      AND c.available_at <= :as_of
      AND c.ingested_at <= :as_of
      AND c.is_final
)
SELECT *
FROM eligible
WHERE revision_rank = 1
ORDER BY open_time;
```

Nie wolno używać widoku „latest” opartego na obecnym stanie bazy w replayu historycznym. Najnowsza dzisiejsza rewizja mogła nie istnieć w testowanym momencie.

Przykład kalendarza unlocków znanego w chwili decyzji:

```sql
WITH known_revisions AS (
    SELECT
        u.*,
        row_number() OVER (
            PARTITION BY u.source_id, u.source_record_key
            ORDER BY u.revision_no DESC, u.available_at DESC, u.ingested_at DESC
        ) AS revision_rank
    FROM crypto_agent.token_unlocks u
    WHERE u.asset_id = :asset_id
      AND u.available_at <= :as_of
      AND u.ingested_at <= :as_of
      AND u.unlock_at > :as_of
)
SELECT *
FROM known_revisions
WHERE revision_rank = 1
ORDER BY unlock_at;
```

Tu nie filtrujemy `unlock_at <= :as_of`, ponieważ pytamy o przyszłe zdarzenia już znane w `:as_of`.

## Korekty i deduplikacja

Gdy dostawca poprawia rekord:

1. nie wykonujemy `UPDATE`;
2. zapisujemy ten sam `source_record_key` z wyższym `revision_no`;
3. ustawiamy rzeczywiste czasy nowej dostępności i ingestu;
4. zapisujemy nowy `content_hash`;
5. jeśli korekta wynika z błędu po naszej stronie, otwieramy `data_quality_incident`;
6. wcześniejsze research runs nadal wskazują starą rewizję w swoim manifeście.

Unikalność `(source_id, source_record_key, revision_no)` blokuje sprzeczne duplikaty. Powtórne dostarczenie identycznego payloadu powinno być obsłużone idempotentnie przed insertem albo przez `ON CONFLICT DO NOTHING` bez aktualizacji.

## Append-only

`schema.sql` zakłada triggery blokujące `UPDATE`, `DELETE` i `TRUNCATE` na tabelach bazowych. Stan zmienny jest modelowany zdarzeniami:

- status runu → `research_run_events`;
- stan incydentu → `data_quality_incident_events`;
- stan alertu → `alert_events`;
- stan zlecenia → `paper_order_events`;
- bieżąca pozycja → ostatni dozwolony `paper_position_snapshot`.

W produkcji role aplikacyjne nie powinny mieć uprawnień do wyłączania triggerów. Migracje i retencję wykonuje osobna, audytowana rola. Backupy powinny mieć politykę WORM lub Object Lock dla surowych payloadów i logów audytowych.

## Zakres wersji produktu

| Wersja | Tabele używane aktywnie | Warunek przejścia dalej |
|---|---|---|
| V1 — read-only analyst | źródła, aktywa, rynki, candles, dokumenty, makro, on-chain, DQ, research, risk, alerts | kompletne provenance, poprawne cutoffy, `NO_SIGNAL` i hard veto |
| V2 — backtest/replay | pełny point-in-time store, vintages, unlocks, derivatives, universe i manifests | testy na delistowanych aktywach, realistyczne fee/slippage, brak look-ahead |
| V3 — shadow/paper | paper accounts, orders, events, fills, positions, approvals i audit | forward paper trading, stabilne metryki ryzyka, zero naruszeń polityki |
| V4 — canary | ten sam research/risk/audit contract oraz odseparowany execution service | mały kapitał, spot, zgoda człowieka, limity i niezależny kill switch |

Model nie przechowuje kluczy giełdowych i nie daje LLM bezpośredniego dostępu do execution service. Live execution powinien mieć osobny schemat lub bazę, własne konto techniczne i jednokierunkowo konsumować zatwierdzone propozycje.

## TimescaleDB i skalowanie

Bez rozszerzeń używamy indeksów B-tree do zapytań point-in-time oraz BRIN po `ingested_at` w największych tabelach. Pierwsi kandydaci do partycjonowania lub hypertables to:

- `trades`;
- `orderbook_snapshots` i poziomy;
- `candles`;
- `derivatives_metrics`;
- `onchain_metrics`;
- `audit_log`.

Przed konwersją do hypertable trzeba dobrać kolumnę czasu i dostosować klucze unikalne do wymogu TimescaleDB, według wersji rozszerzenia użytej w środowisku. Nie należy usuwać semantycznej unikalności źródła tylko po to, by wykonać mechaniczną konwersję. Alternatywą jest natywne partycjonowanie PostgreSQL albo warstwa surowa jako hypertable i deduplikowany point-in-time store jako zwykłe tabele.

Retencja nie może usuwać danych potrzebnych do odtworzenia istniejącego `research_run_inputs`. Agregaty nie zastępują surowych danych w audytowanym backteście.

## Kontrole wymagane w aplikacji

Schemat wymusza najważniejsze inwarianty, ale serwis ingestu i risk plane nadal muszą sprawdzać:

- zgodność strefy czasowej i zegarów źródłowych;
- kompletność sekwencji trades/order book oraz resynchronizację po luce;
- mapowanie batcha do tego samego `source_id`;
- normalizację adresów właściwą dla sieci;
- sumę scenariuszy/probabilities i ich kalibrację;
- ważność `risk_assessment.expires_at` w chwili propozycji;
- brak hard veto przed utworzeniem zlecenia;
- maksymalną ekspozycję, drawdown, koncentrację, płynność i limity giełdy;
- serializację łańcucha `audit_log`;
- zgodność hashy z surowym obiektem przechowywanym poza bazą;
- zakaz zasilania narzędzi wykonawczych instrukcjami pochodzącymi z dokumentów lub social mediów.

Najważniejszy test akceptacyjny brzmi: dla dowolnego `research_run_id` system musi umieć odtworzyć identyczny universe, dokładne rewizje wejść, wersję kodu, prompt, model, politykę ryzyka, wynik oraz pełną historię zgód — bez odwoływania się do dzisiejszego stanu danych.
