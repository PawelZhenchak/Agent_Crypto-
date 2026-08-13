# Roadmap V1 — Plus500 Futures T4

1. **T4-only configuration** — wdrożona w 0.2.0.
2. **Bezpieczna granica workera .NET** — wdrożona w 0.3.0; loopback, token,
   walidacja contract ID, brak order routes i fail-closed.
3. **Oficjalny klient T4** — implementacja dodana w 0.8.0 na oficjalnych pakietach
   `Plus500US.T4Proto` 1.0.73 i `Plus500US.T4ChartDecoder` 1.0.97. Rzeczywisty
   odbiór nadal wymaga provisioningu klucza API, Simulator/live, uprawnień oraz
   prawidłowych identyfikatorów T4.
4. **Cykl życia kontraktów futures offline** — wdrożony w 0.4.0; katalog serii,
   wybór front-month, granica wygaśnięcia, kontrolowany roll i jego proweniencja.
5. **Operacyjny ingest i replay** — część offline wdrożona w 0.5.0: atomowy batch,
   dokładny raw payload hash, idempotencja, append-only candles, scheduler i
   deterministyczny point-in-time replay. Live contract tests, reconnect, sesja i
   głębokość rynku pozostają zablokowane do czasu dostępu T4 z punktu 3.
6. **Analiza futures offline** — wdrożona w 0.6.0: schema v3, spread, depth,
   imbalance, basis i annualized basis z zatwierdzonego indeksu
   `plus500_t4_index_v1`, wolumen, expiry risk, wpływ rollu, scenariusze
   bull/base/bear, migracja `0015`, hashe evidence oraz deterministyczne
   `analyze-replay`. Historyczne v2 są odczytywalne, ale kończą się `NO_SIGNAL`.
   Walidacja na prawdziwych danych nadal zależy od dostępu T4 z punktu 3.
7. **Monitoring i alert delivery offline** — wdrożone w 0.7.0: trace przed
   analizą, bezpieczne artifacts, deduplikowany incident log, lokalny dashboard,
   read-only loopback API, migracja `0016`, transakcyjny outbox i ograniczone
   delivery `stdout_json` → `process_stdout`. Live delivery w 0.8.0 wymaga schema
   v5 i `environment=live_t4`; Simulator i historyczny replay są wykluczone.
   Outbox działa at-least-once, a konsument deduplikuje po `idempotency_key`.
   Analiza API jest
   wyłącznie `POST /v1/analyze` z `X-Crypto-Agent-Request: analyze-v1`. Synthetic,
   fixture, replay, `NO_SIGNAL`, veto i stale nigdy nie są dostarczane. Webhook,
   Slack, Telegram, e-mail i SMS są poza zakresem tej wersji.
8. **Odbiór V1** — warstwa bazowa przygotowana w `0.8.0`: oficjalne
   `Plus500US.T4Proto` `1.0.73` i `Plus500US.T4ChartDecoder` `1.0.97` (publiczny
   protocol commit `1a68b674482194f1cf3b9d7f129ce5fbed8bcb51`), WebSocket/Protobuf + Chart REST,
   bridge schema v5, nieprzezroczyste `MarketID` per świeca, rozdzielenie
   Simulator/live oraz append-only kampania i raport jakości z migracji `0017`.
   Etap 1 w `0.9.0` dodaje migrację `0018`: podpisany przez bridge dziennik,
   osobną rolę weryfikatora, obiektywne referencje do batchy/cykli/runów,
   siedem prób scenariuszy i niezależny predykat bramki PostgreSQL. Caller nie
   może zadeklarować `PASS`.

   Do faktycznego odbioru pozostają provisioning klucza/identyfikatorów/uprawnień,
   test Simulator i live, uruchomienie zatwierdzonego runnera cykli, minimum 672
   godziny obserwacji czasu rzeczywistego oraz końcowy raport ze wszystkimi
   obowiązkowymi kryteriami `PASS`. Wszystkie siedem scenariuszy musi mieć
   obiektywny wynik `pass`; `roll_transition` wymaga prawdziwego rollu live.
   Zakres kampanii `0.9.0` to BTC/ETH 4h;
   publiczny dwutygodniowy Simulator sam nie wystarcza.
9. **Nadzór kampanii 24/7 — etap 2** — zrealizowany w `0.10.0`: supervisor
   frozen schedule BTC/ETH, izolowane cykle z timeoutem, ograniczony retry bez
   duplikatów, wykrywanie `missed`, automatyczna finalizacja, dzienny hash-chain
   stanu i utwardzone restarty systemd. Instalacja na docelowym hoście należy do
   etapu 3.

Provisioning, rzeczywiste testy Simulator/live i kampania nie zostały wykonane.
Nie ma jeszcze dostępu T4 ani rzeczywistych identyfikatorów rynków.
`v1_gate_passed=false`.

W `0.9.0` nie jest to blokada stała. PostgreSQL może wyliczyć `true` dopiero po
pełnym realnym oknie, okresie grace, spełnieniu progów jakości i siedmiu
zweryfikowanych dowodach. Kontrolowany `rate_limit` dowodzi granicy naszego
handlera, nie naturalnego `429` po stronie T4.
