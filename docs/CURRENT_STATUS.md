# Aktualny stan — 0.8.0

## Zaimplementowane

- oficjalny reader .NET 8 oparty o `Plus500US.T4Proto` `1.0.73` oraz
  `Plus500US.T4ChartDecoder` `1.0.97` i publiczny protokół T4 przypięty do commita
  `1a68b674482194f1cf3b9d7f129ce5fbed8bcb51`;
- WebSocket/Protobuf dla sesji, heartbeatów i Market By Price oraz Chart REST dla
  zamkniętych świec;
- stałe, rozdzielone endpointy Simulator/live, reconnect, ponowna subskrypcja,
  prewarm cache i stan health fail-closed;
- outbound allowlist bez wiadomości order-routing;
- bridge schema `v5` z rozdzieleniem `ExchangeID`, produktowego `ContractID` i
  nieprzezroczystego `MarketID`;
- `MarketID` każdej świecy, niezależna tożsamość indeksu basis i dowód rollu
  pomiędzy rzeczywistymi `MarketID`;
- jawne środowisko `t4_simulator` albo `live_t4`; Simulator nigdy nie kwalifikuje
  alertu do external delivery;
- append-only ingest/replay schema v5 oraz zachowana kompatybilność historycznego
  replayu v2-v4 z bezpieczną kwalifikacją;
- migracja `0017` z niezmiennym baseline kampanii, łańcuchem hashy cykli i zdarzeń
  sesji, ochroną przed backfillem oraz niezmiennym raportem końcowym;
- wersjonowana polityka `t4-observation-v1-2026-08-12`: minimum `672` godzin,
  progi prób/sukcesu, limity luk i RTT oraz obowiązkowe scenariusze;
- komendy `observe-start`, `observe-run`, `observe-status` i `observe-report`,
  korzystające z czasu serwera PostgreSQL; start wykonuje live preflight każdego
  scope’u, a run wiąże jeden fetch z dokładnym batchem i analizą;
- bezpieczna ewaluacja `PASS`, `FAIL`, `NOT_OBSERVED`; zapis raportu jest możliwy
  dopiero po `planned_ends_at + cycle_interval_seconds`, a schema `0.8.0` celowo
  blokuje `PASS` scenariusza i `v1_gate_passed=true` do późniejszej migracji z
  obiektywnymi referencjami dowodów;
- monitoring punktu 7, append-only outbox i lokalne delivery stdout nadal mają
  semantykę at-least-once z deduplikacją po `idempotency_key`.

## Nie jest jeszcze potwierdzone

- provisioning działającego `T4_API_KEY` dla wybranego środowiska;
- uprawnienia depth dla giełd futures i niezależnego indeksu;
- rzeczywiste `ExchangeID`, produktowe `ContractID` i `MarketID` bieżących oraz
  następnych serii BTC/ETH;
- contract test na prawdziwej sesji T4 Simulator, a następnie live;
- empiryczne zachowanie Chart REST, reconnectu, limitów, opóźnień, rollu i braków
  danych na przydzielonym koncie;
- operacyjne uruchomienie i nadzór `observe-run` dla każdego scope’u przez cały
  czas kampanii;
- wykonanie obowiązkowych kontrolowanych scenariuszy i zapis dowodów;
- minimum cztery tygodnie nieprzerwanej obserwacji read-only;
- końcowy raport jakości V1.

Zakres zamrożonej kampanii odbiorowej `0.8.0` to obecnie BTC i ETH na interwale
4h (`240` minut). Standardowy publiczny T4 Simulator trwa dwa tygodnie, więc do
pełnych 28 dni potrzebne jest przedłużenie albo właściwy dostęp live.

## Bramka

Rzeczywista kampania 28-dniowa nie została rozpoczęta. Samo istnienie klienta,
fixture’y, testy offline ani sesja Simulator nie zaliczają odbioru live.

`v1_gate_passed=false`.

W `0.8.0` stan nie może zmienić się na `true`: schema PostgreSQL odrzuca zarówno
scenariusz z wynikiem `pass`, jak i raport z `v1_gate_passed=true`. Finalizacja
raportu jest dozwolona dopiero po końcu 672-godzinnego okna i dodatkowym okresie
grace równym jednemu interwałowi cyklu. Późniejsza migracja musi najpierw dodać
obiektywne referencje dowodów scenariuszy i ich walidację w bazie.
