# Informacje o wydaniu 0.8.0

Wydanie przygotowuje warstwę implementacyjną punktu 8. Nie ogłasza zakończenia
odbioru V1: test na provisionowanym T4 i realna kampania 28-dniowa nie zostały
jeszcze wykonane.

## Dodane

- oficjalny reader .NET 8 oparty o `Plus500US.T4Proto` `1.0.73` oraz
  `Plus500US.T4ChartDecoder` `1.0.97`, z publicznym protocol commit
  `1a68b674482194f1cf3b9d7f129ce5fbed8bcb51`;
- WebSocket/Protobuf dla sesji, heartbeat i Market By Price oraz Chart REST dla
  świec;
- stałe endpointy Simulator/live, reconnect, resubscribe, timeout, prewarm cache
  i dynamiczny health fail-closed;
- outbound allowlist bez order-routing;
- schema bridge v5: `ExchangeID`, produktowy `ContractID`, nieprzezroczysty
  `MarketID`, per-candle `MarketID`, niezależny indeks basis i transition evidence
  pomiędzy rzeczywistymi rynkami;
- rozdzielenie `t4_simulator` i `live_t4`; Simulator nie kwalifikuje się do
  external delivery;
- migracja `0017` dla schema v5 oraz append-only ledger kampanii, cykli, zdarzeń
  sesji i końcowego raportu jakości;
- ochrona przed predeklarowaniem/backfillem cykli, hash chain i dokładne
  powiązanie udanego cyklu z batchem oraz runem analizy;
- polityka minimum 672 godzin z progami prób/sukcesu, luk, RTT, bezpieczeństwa i
  obowiązkowymi scenariuszami;
- komendy `observe-start`, `observe-run`, `observe-status` i `observe-report`;
- live preflight każdego frozen scope przed utworzeniem kampanii oraz pojedynczy
  fetch należnego slotu, współdzielony przez ingest i zamknięty provider analizy;
- pakowanie `configs/observation_policy.v1.json` do artefaktu wheel;
- runbook kampanii i jawny model `PASS` / `FAIL` / `NOT_OBSERVED`.

## Zakres odbioru

Zamrożony zakres kampanii `0.8.0` obejmuje obecnie BTC i ETH na interwale 4h
(`240` minut). Bridge i parser zachowują szersze historyczne interwały, ale nie są
one częścią bieżącej akceptacji live V1.

## Świadome ograniczenia i zależności zewnętrzne

- wymagany jest provisioning `T4_API_KEY`, prawidłowych Exchange/Contract/Market
  ID, uprawnień depth i niezależnego rynku indeksowego;
- standardowy publiczny Simulator trwa dwa tygodnie, więc nie wystarcza do
  obowiązkowych 28 dni bez przedłużenia albo dostępu live;
- testy fixture/kontraktowe nie potwierdzają Simulatora ani live;
- `observe-run` wymaga operacyjnego supervisora wywołującego oba scope’y przez
  pełne 28 dni;
- roll transition musi zostać naprawdę zaobserwowany, inaczej kryterium pozostaje
  `NOT_OBSERVED`;
- jedyny kanał alertów to lokalny stdout; outbox zachowuje semantykę at-least-once
  i wymaga deduplikacji po `idempotency_key`;
- system pozostaje całkowicie read-only i nie ma tras zleceń.

## Stan bramki

Provisioning i rzeczywiste testy Simulator/live nie zostały wykonane, a
obserwacja nie została rozpoczęta. `observe-report` odmawia zapisu do czasu
`planned_ends_at + cycle_interval_seconds`; dodatkowy interwał jest okresem grace
na ostatni slot. Schema `0.8.0` celowo odrzuca scenariusz `PASS` i
`v1_gate_passed=true`, dopóki późniejsza migracja nie doda obiektywnych referencji
dowodów oraz ich walidacji.

Aktualny stan: `v1_gate_passed=false`.
