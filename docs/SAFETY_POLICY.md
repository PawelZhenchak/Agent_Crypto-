# Polityka bezpieczeństwa T4

- brak automatycznego wykonywania zleceń;
- agent emituje wyłącznie `ALERT` albo `NO_SIGNAL`;
- tylko jedna zatwierdzona proweniencja: Plus500 Futures T4;
- dane logowania są zabronione w procesie analitycznym;
- most działa tylko na loopback, wymaga tokenu i nie wystawia order routes;
- logiczny symbol musi wskazywać skonfigurowany, rzeczywisty i niewygasły kontrakt;
- wymagane są 120 świec, prawidłowe UTC i cena nie starsza niż 300 sekund;
- kwalifikacja bieżącej analizy live i delivery wymaga schema v4,
  `environment=live_t4`, pełnego snapshotu order booka i statusu sesji `OPEN`;
- basis może używać wyłącznie referencji typu `index` o source ID dokładnie
  `plus500_t4_index_v1`; ceny futures ani anonimowej ceny referencyjnej nie wolno
  użyć jako basis;
- spread, depth, imbalance, basis, annualized basis, wolumen, expiry i roll muszą
  wynikać z atestowanych dowodów, a nie z wartości zastępczych;
- brak danych, zła struktura, zły source ID, stary kontrakt, niepełny/stary order
  book albo nieważny basis oznacza `NO_SIGNAL`;
- historyczne schema v2 i v3 są odczytywalne wyłącznie przez replay; v2 bez
  futures evidence zawsze kończy analizę jako `NO_SIGNAL`, a v3 nigdy nie uzyskuje
  kwalifikacji do external delivery;
- evidence schema v3 jest append-only, hashowane i obejmowane fingerprintem replayu;
- tylko aktualny, niewygasły `ALERT` z live T4, bez veto i provider error, może
  trafić do delivery outboxa;
- `NO_SIGNAL`, synthetic, fixture, replay, veto, stale, provider error i expiry
  nigdy nie są dostarczane;
- jedyną trasą jest lokalne `stdout_json` → `process_stdout`; dowolny webhook,
  Slack, Telegram, e-mail i SMS są niedozwolone w 0.7.0;
- outbox i próby są append-only, hashowane, idempotentne i mają ograniczony retry;
  stdout działa at-least-once, więc konsument deduplikuje po `idempotency_key`;
  exactly-once nie jest gwarantowane;
- artifacts, incydenty, dashboard oraz komunikaty błędów nie mogą ujawniać
  surowego payloadu dostawcy, exception text, URL, nagłówków ani credentials;
- API i dashboard działają wyłącznie na loopback, bez CORS, dokumentacji API,
  JavaScriptu i zdalnych zasobów;
- analiza API jest wyłącznie operacją `POST /v1/analyze` z nagłówkiem
  `X-Crypto-Agent-Request: analyze-v1`; mutujący wariant GET jest zabroniony;
- `ALERT` jest alertem badawczym, nie rekomendacją transakcji.

Pojedyncze źródło nie zapewnia niezależnej weryfikacji ceny. Jest to jawny kompromis
decyzji T4-only i musi być uwzględniony w odbiorze jakościowym V1.

Atomowość alertowania obejmuje jedną transakcję PostgreSQL dla run/artifact,
alertu, alert eventu i outboxa. Nie obejmuje osobnego magazynu SQLite.

Oficjalny klient T4 nie jest jeszcze podłączony. Reader pending zwraca `503`, a
fixture’y służą wyłącznie testom offline i nie mogą być przedstawiane jako dane
live. `v1_gate_passed=false`; ukończenie punktu 7 offline nie otwiera wykonywania
zleceń ani nie zamyka odbioru V1.
