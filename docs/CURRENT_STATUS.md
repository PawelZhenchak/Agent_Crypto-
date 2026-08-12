# Aktualny stan — 0.7.0

## Gotowe

- aktywna konfiguracja przyjmuje tylko `synthetic` lub `t4`;
- jedyne produkcyjne źródło ma identyfikator `plus500_t4_futures_v1`;
- usunięto adaptery, factory choices, seedy i testy poprzednich źródeł;
- dodano ścisły adapter loopback do mostu T4 .NET;
- proces analityczny odrzuca dane logowania T4;
- host bridge .NET 8 nasłuchuje wyłącznie na loopback, wymaga tokenu i nie ma
  endpointów zleceń;
- lokalny katalog kolejnych serii wybiera front-month według czasu UTC, a granica
  `roll_at` wymusza kontrolowane przejście na następną serię;
- schema bridge live v4 zachowuje contract ID, expiry, roll, poprzednią serię,
  atestowane futures evidence oraz wymaga `environment=live_t4`;
- futures evidence obejmuje status sesji, pełny order book, typed basis reference
  i, po rollu, zsynchronizowane ceny obu kontraktów;
- jedyna zatwierdzona referencja basis ma typ `index` i source ID
  `plus500_t4_index_v1`;
- migracje `0014` i `0015` zapewniają atomowy append-only ingest, pełne futures
  evidence, hashe oraz deterministyczny point-in-time replay;
- silnik oblicza spread, depth, imbalance, basis, annualized basis, wolumen,
  expiry risk i wpływ rollu bez zastępowania braków zerami;
- raport zawiera scenariusze bull/base/bear, przeciwne dowody i jawne warunki
  unieważnienia;
- historyczne batche schema v2 i v3 pozostają odczytywalne przez replay; v2 bez
  futures evidence wymusza `NO_SIGNAL`, a v3 nigdy nie kwalifikuje replayu do
  external delivery;
- dostępne są pojedyncze i cykliczne uruchomienia ingestu oraz komendy `replay`
  i `analyze-replay`;
- punkt 7 jest ukończony offline: `monitor` rejestruje run, trace, bezpieczny
  artifact i deduplikowane incydenty w PostgreSQL;
- migracja `0016` dodaje immutable delivery outbox oraz append-only historię prób
  z hash chain, ograniczonym retry, expiry i idempotency key;
- run/artifact + alert + alert event + outbox są zapisywane w jednej transakcji
  PostgreSQL; atomowość nie obejmuje osobnego magazynu SQLite;
- jedynym kanałem dostarczenia jest `stdout_json` → `process_stdout`;
- trwały outbox i stdout mają semantykę at-least-once, więc konsument deduplikuje
  po `idempotency_key`; exactly-once nie jest deklarowane;
- `deliver-alerts` obsługuje pojedynczy cykl lub odporną pętlę `--watch`, a status
  operacyjny zapisuje do stderr;
- `monitoring-status`, lokalny dashboard i read-only endpointy API pokazują
  podsumowanie, alerty, incydenty i trace;
- API jest loopback-only, bez CORS, OpenAPI, Swagger i ReDoc, z nagłówkami
  `no-store`, CSP, `nosniff`, `DENY` i `no-referrer`;
- analiza API jest dostępna tylko przez `POST /v1/analyze` z wymaganym nagłówkiem
  `X-Crypto-Agent-Request: analyze-v1`;
- synthetic, fixture, replay, `NO_SIGNAL`, veto, stare dane i provider error nigdy
  nie tworzą przesyłki;
- polityka monitoringu jest wersjonowana, ścisła i dopuszcza wyłącznie lokalne
  delivery; domyślny horyzont retencji wynosi 90 dni;
- testy fixture sprawdzają kontrakt offline, ale nie są testami live T4.

## Świadomie zachowana historia

Pliki migracji `0011` i `0012` są checksummowane i mogły zostać już zastosowane.
Nie wolno ich przepisywać. Ich dawne nazwy pozostają wyłącznie w historycznym SQL.
Nie są aktywną konfiguracją ani źródłami runtime.

## Jeszcze niegotowe

- rejestracja aplikacji T4 u Plus500 Futures Technologies/CTS;
- adapter oficjalnego klienta T4 po otrzymaniu aktualnego pakietu i przykładów API;
- konto T4 Simulator i live contract test schema v4 z `environment=live_t4`;
- reconnect sesji, limity, opóźnienia i snapshoty głębokości rynku z prawdziwego T4;
- walidacja monitoringu i alertów na rzeczywistych danych T4;
- punkt 8: minimum cztery tygodnie obserwacji read-only i raport jakości V1.

Webhook, Slack, Telegram, e-mail i SMS nie są „prawie gotowymi” kanałami: celowo
nie należą do bezpiecznego zakresu 0.7.0.

Oficjalny klient T4 nadal nie jest podłączony. Reader
`T4ApplicationRegistrationPendingReader` zwraca `503`, a agent `NO_SIGNAL` zamiast
danych zastępczych. `v1_gate_passed=false`; następnym etapem jest punkt 8.
