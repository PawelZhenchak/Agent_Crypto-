# Aktualny stan — 0.10.0, etap 2

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
- migracja `0018` z append-only dziennikiem zdarzeń bridge'a, rejestrem kluczy,
  próbami scenariuszy i obiektywnym predykatem bramki w PostgreSQL;
- ECDSA P-256, pin fingerprintu SPKI w baseline oraz weryfikacja podpisu,
  projekcji claims i globalnego hash chain przed zapisaniem zdarzenia;
- podpis pochodzi z naszego bridge'a dowodowego; nie jest podpisem ani
  atestacją kryptograficzną Plus500/T4;
- wersjonowana polityka `t4-observation-v1-2026-08-12`: minimum `672` godzin,
  progi prób/sukcesu, limity luk i RTT oraz obowiązkowe scenariusze;
- komendy `observe-start`, `observe-run`, `observe-scenarios`, `observe-status`
  i `observe-report`,
  korzystające z czasu serwera PostgreSQL; start wykonuje live preflight każdego
  scope’u, a run wiąże jeden fetch z dokładnym batchem i analizą;
- pięć scenariuszy kontrolowanych i dwa pasywne; caller zapisuje referencje, a
  PostgreSQL sam wyprowadza `pass` albo `fail` z powiązanych zdarzeń, batchy,
  cykli i runów;
- niezależny predykat `t4_observation_gate_is_verified(...)`; raport końcowy musi
  mieć dokładnie taki sam wynik bramki jak dane w bazie;
- bezpieczna ewaluacja `PASS`, `FAIL`, `NOT_OBSERVED`; zapis raportu jest możliwy
  dopiero po `planned_ends_at + cycle_interval_seconds`;
- monitoring punktu 7, append-only outbox i lokalne delivery stdout nadal mają
  semantykę at-least-once z deduplikacją po `idempotency_key`.
- automatyczny supervisor uruchamia cykle BTC/ETH z frozen schedule, izoluje je
  w podprocesach, wykrywa timeouty i `missed`, ponawia błędy techniczne bez
  duplikowania slotów i automatycznie finalizuje kampanię po grace;
- atomowy status, append-only event journal i jeden dzienny snapshot UTC są
  odtwarzane po restarcie i chronione hash chainem;
- utwardzone jednostki systemd automatycznie restartują bridge, verifier i sam
  supervisor, zachowując trzy oddzielne UID i granice sekretów.

## Nie jest jeszcze potwierdzone

- provisioning działającego `T4_API_KEY` dla wybranego środowiska — obecnie nie
  ma dostępu do prawdziwego T4;
- uprawnienia depth dla giełd futures i niezależnego indeksu;
- rzeczywiste `ExchangeID`, produktowe `ContractID` i `MarketID` bieżących oraz
  następnych serii BTC/ETH — nie zostały jeszcze pozyskane;
- contract test na prawdziwej sesji T4 Simulator, a następnie live;
- empiryczne zachowanie Chart REST, reconnectu, limitów, opóźnień, rollu i braków
  danych na przydzielonym koncie;
- instalacja przygotowanych jednostek systemd na docelowym hoście 24/7;
- wykonanie obowiązkowych kontrolowanych scenariuszy i zapis dowodów;
- pasywny dowód prawdziwego rollu kontraktu w oknie kampanii;
- minimum cztery tygodnie nieprzerwanej obserwacji read-only;
- końcowy raport jakości V1.

Zakres zamrożonej kampanii odbiorowej `0.9.0` to BTC i ETH na interwale
4h (`240` minut). Standardowy publiczny T4 Simulator trwa dwa tygodnie, więc do
pełnych 28 dni potrzebne jest przedłużenie albo właściwy dostęp live.

Etap 2 dostarcza automatycznego supervisora i jednostki systemd. Kod nie jest
jeszcze zainstalowany na docelowym hoście, ponieważ nadal brakuje dostępu T4 i
prawdziwych identyfikatorów. Samo wdrożenie infrastruktury 24/7 jest etapem 3.

Kontrolowany `rate_limit` potwierdza zachowanie naszego handlera i ścieżkę
fail-closed. Nie potwierdza, że dostawca zwrócił prawdziwe `429`; naturalny limit
upstream pozostaje osobnym zdarzeniem i nie wolno go wywoływać spamowaniem T4.

## Bramka

Rzeczywista kampania 28-dniowa nie została rozpoczęta. Samo istnienie klienta,
fixture’y, testy offline ani sesja Simulator nie zaliczają odbioru live.

`v1_gate_passed=false`.

Nie jest to już blokada „na sztywno”. PostgreSQL może wyliczyć `true`, ale tylko
po zakończeniu realnego okna co najmniej 672 godzin i okresu grace, przy progach
prób/sukcesu/luk/RTT, zerze naruszeń oraz siedmiu scenariuszach `pass`. Każdy
scenariusz musi mieć obiektywny dowód; `roll_transition` wymaga realnego live
batcha schema v5 ze starym i nowym `MarketID`. Brak rollu daje brak zaliczenia,
nie sztuczny sukces.

Kampania wymaga trzech osobnych loginów PostgreSQL bez superusera: runtime,
verifier-write należącego tylko do `crypto_agent_evidence_verifier` oraz
evidence-reader należącego tylko do `crypto_agent_evidence_reader`. Reader służy
isolated verifierowi do in-memory replay zamrożonych scope’ów, a zapis atestacji
przechodzi wyłącznie przez verifier-write. Login runtime nie jest właścicielem
bazy ani członkiem ról dowodowych. DSN administratora jest poza tymi ścieżkami.
