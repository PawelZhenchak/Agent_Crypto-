# Runbook odbioru T4 V1 — 28 dni read-only

## Cel i stan

Kampania ma dostarczyć dowód jakości na rzeczywistym `live_t4`, a nie tylko
sprawdzić, że kod się uruchamia. Minimalne okno wynosi `672` godziny czasu
rzeczywistego. Nie wolno go skracać, symulować ani uzupełniać backfillem.

Provisioning i testy na prawdziwym Simulator/live nie zostały wykonane, a
rzeczywista kampania nie została jeszcze rozpoczęta. W `0.8.0` stan pozostaje
`v1_gate_passed=false` także z definicji schema: scenariusz `PASS` i dodatnia
decyzja bramki są blokowane do późniejszej migracji z obiektywnymi referencjami
dowodów.

## 1. Warunki wejścia

Nie uruchamiaj `observe-start`, dopóki wszystkie warunki nie są spełnione:

- PostgreSQL 16 ma migracje do `0017`, seed i health `READY`;
- bridge działa wyłącznie na loopback i `/healthz` pokazuje `READY`;
- `T4_API_ENVIRONMENT=live`, a envelope ma `environment=live_t4` i schema v5;
- provisionowany klucz ma uprawnienia depth dla futures i niezależnego indeksu;
- prywatny katalog schema v2 zawiera rzeczywiste `ExchangeID`, produktowe
  `ContractID`, nieprzezroczyste `MarketID`, kolejną serię i tożsamość indeksu;
- test Simulator został wykonany osobno, bez traktowania go jako live;
- test live potwierdził WebSocket, Chart REST, per-candle `MarketID`, basis,
  point-in-time cutoff oraz brak order routes;
- `observe-run` jest przetestowany i supervisor może wywoływać go dla obu
  scope’ów przez pełne okno.

Publiczny Simulator trwa dwa tygodnie i sam nie wystarcza do kampanii. Simulator,
fixture, synthetic oraz replay nie mogą być źródłem cyklu live.

## 2. Zamrożenie baseline

Zakres odbioru `0.8.0` to:

- `BTC/USD:240m`;
- `ETH/USD:240m`;
- cykl domyślnie co 300 sekund;
- oficjalny `Plus500US.T4Proto` 1.0.73;
- oficjalny `Plus500US.T4ChartDecoder` 1.0.97;
- publiczny protocol commit
  `1a68b674482194f1cf3b9d7f129ce5fbed8bcb51`.

Przygotuj trzy lowercase SHA-256:

1. identyfikatora zamrożonego commita kodu;
2. dokładnego tekstu przypiętego commita protokołu — dla wartości powyżej SHA-256
   wynosi `eb589f6afe689d1e9037b6f50a0993606f12f09739ff6409d4e77a0fc220c566`;
3. kanonicznego, zredukowanego runtime config bez sekretów, ale z identyfikatorami
   wersji polityk, scope’em, środowiskiem i hashami prywatnych konfiguracji.

Nie wpisuj klucza API, tokenu bridge, DSN ani innych sekretów do baseline lub
logów.

## 3. Start kampanii

```bash
crypto-agent observe-start \
  --cycle-interval-seconds 300 \
  --scope BTC/USD:240m \
  --scope ETH/USD:240m \
  --code-commit-hash <sha256> \
  --t4-protocol-commit-hash eb589f6afe689d1e9037b6f50a0993606f12f09739ff6409d4e77a0fc220c566 \
  --runtime-config-hash <sha256>
```

Przed utworzeniem kampanii `observe-start` wykonuje live preflight schema v5
każdego scope’u z limitem 60 świec. Brak prawdziwego live, zła schema,
Simulator/replay/synthetic albo brak atestacji read-only zatrzymuje start.

Zachowaj zwrócone `campaign_id`, `started_at`, `planned_ends_at`, hash polityki i
`frozen_baseline_hash_sha256`. Czas pochodzi z PostgreSQL; CLI nie przyjmuje
historycznego startu.

Po rozpoczęciu nie zmieniaj kodu, protokołu, polityki, scope’u, interwału cyklu,
środowiska ani konfiguracji runtime. Potrzeba zmiany oznacza nową kampanię pełnych
28 dni. Stary ledger pozostaje w audycie.

## 4. Runner cykli

Supervisor wywołuje dla obu scope’ów:

```bash
crypto-agent observe-run --campaign-id <uuid> --scope BTC/USD:240m --limit 120
crypto-agent observe-run --campaign-id <uuid> --scope ETH/USD:240m --limit 120
```

`observe-run` odczytuje frozen schedule i czas z PostgreSQL. Dla dokładnie jednego
należnego slotu w jednej kontrolowanej ścieżce:

1. pobiera live schema v5 raz, z cutoffem równym `expected_at`;
2. zapisuje niezmienny batch i jego raw payload hash;
3. wykonuje monitorowaną analizę read-only;
4. podaje ten sam batch zamkniętemu providerowi analizy, bez drugiego fetchu;
5. wiąże sukces z dokładnym `t4_batch_id`, `research_run_id`, deterministycznym
   `trace_id`, raw hash, schema v5 i RTT;
6. zapisuje `success`, `failure` albo uczciwy `missed` zgodnie z zegarem bazy.

Retry przed kolejnym slotem nie pobiera nowych danych. Recovery dopisuje wyłącznie
`missed` dla wygasłych slotów; nie udaje historycznego sukcesu.

Nie zapisuj cykli ręcznie przez SQL. Triggery sprawdzają zamrożony harmonogram,
hash chain, realny batch/run, środowisko live oraz chronią przed cyklem z
przyszłości i backfillem. Restart nie usprawiedliwia dopisania fikcyjnego
`success`; miniony slot pozostaje `missed` z bezpiecznym kodem wyjaśnienia.

## 5. Progi jakości

Polityka `configs/observation_policy.v1.json` wymaga:

- co najmniej 672 godzin;
- pokrycia obu scope’ów;
- co najmniej 99% planowych prób;
- co najmniej 99% sukcesów wśród prób;
- pełnego powiązania każdego sukcesu z batchem i runem;
- schema v5 dla każdego sukcesu;
- maksymalnej niewyjaśnionej luki nie większej niż trzy interwały cyklu
  (`900` sekund przy cyklu 300 s);
- RTT p95 nie większego niż 5 s i p99 nie większego niż 10 s;
- zera naruszeń read-only, prób order routing, wycieków sekretów, błędów
  integralności, naruszeń fail-closed i użycia zabronionego trybu danych.

Status braku danych to `NOT_OBSERVED`, nie domyślny sukces.

## 6. Obowiązkowe scenariusze

Docelowo każdy scenariusz musi mieć kontrolowany, audytowalny wynik `pass`; brak
wpisu daje `NOT_OBSERVED`, a jakikolwiek wpis `fail` powoduje wynik negatywny.
Schema `0.8.0` celowo przyjmuje dla scenariusza wyłącznie `fail`: nie istnieje
jeszcze kolumna ani walidator obiektywnej, scenariuszowej referencji dowodu, więc
nie wolno ręcznie dopisywać `pass`. Późniejsza migracja musi najpierw dodać i
zweryfikować takie referencje. Obowiązkowe scenariusze to:

- `bridge_restart` — kontrolowany restart, ponowne logowanie, prewarm i brak
  podania starego cache;
- `missing_data` — brak pełnego okna kończy się `NO_SIGNAL`/failure, bez wartości
  zastępczej;
- `rate_limit` — ograniczenie API prowadzi do bounded retry/fail-closed;
- `reconnect` — zerwanie WebSocket czyści cache i odtwarza subskrypcje;
- `replay_blocked` — replay nie jest uznany za live i nie tworzy delivery;
- `roll_transition` — rzeczywisty roll zachowuje per-candle `MarketID` oraz
  zsynchronizowany dowód starego/nowego rynku;
- `stale_data` — stare dane nie tworzą `ALERT` ani outboxa.

Nie testuj braku order routes przez wysłanie prawdziwego zlecenia. Ta granica ma
pozostać niewystawiona; jakakolwiek próba order routing jest naruszeniem i
obowiązkowym `FAIL`. Zaplanuj kampanię tak, aby obejmowała rzeczywiste okno rollu,
inaczej `roll_transition` pozostanie `NOT_OBSERVED`.

## 7. Codzienna kontrola

```bash
crypto-agent observe-status --campaign-id <uuid>
crypto-agent monitoring-status
```

Archiwizuj kanoniczny JSON statusu i jego hash w systemie operacyjnym bez
sekretów. Sprawdzaj w szczególności:

- `remaining_seconds` i zgodność czasu z PostgreSQL;
- `cycle_attempt_rate`, `cycle_success_rate`, luki i RTT;
- dokładne powiązanie batch↔analysis i zgodność schema;
- wszystkie liczniki naruszeń bezpieczeństwa;
- które scenariusze nadal mają `NOT_OBSERVED`.

Alerty stdout są at-least-once. Odbiorca deduplikuje je po `idempotency_key`;
duplikat delivery nie oznacza dodatkowego cyklu kampanii ani exactly-once.

## 8. Incydenty

- Nie obchodź `503`, `NO_SIGNAL` ani fail-closed fixture’em.
- Nie cofaj zegara i nie dopisuj sukcesów po czasie.
- Zapisz uczciwy `failure` lub `missed` oraz bezpieczny kod incydentu.
- Przy zmianie baseline zamknij operacyjnie starą kampanię i rozpocznij nową.
- Przy wycieku sekretu natychmiast zatrzymaj reader, obróć sekret i uznaj kryterium
  za `FAIL`; nowy pełny baseline jest wymagany.
- Przy próbie order routing zatrzymaj proces i zachowaj dowód. Bramka pozostaje
  zamknięta.

## 9. Raport końcowy

Przed `planned_ends_at + cycle_interval_seconds` polecenie celowo zwraca
`OBSERVATION_WINDOW_INCOMPLETE` i nie zapisuje raportu. Dodatkowy interwał jest
okresem grace na uczciwe zapisanie ostatniego planowego slotu:

```bash
crypto-agent observe-report --campaign-id <uuid>
```

Po pełnym oknie i okresie grace polecenie zapisuje jeden niezmienny raport i jego
SHA-256. Docelowy model wyników to:

- `PASS` — wszystkie obowiązkowe kryteria mają `PASS`,
  `v1_gate_passed=true`;
- `FAIL` — co najmniej jedno kryterium ma `FAIL`, bramka pozostaje zamknięta;
- `NOT_OBSERVED` — brak wymaganego dowodu, bramka pozostaje zamknięta.

W `0.8.0` przypadek `PASS` nie jest osiągalny: baza odrzuca scenariusz `pass` i
każdy raport z `v1_gate_passed=true`. Raport realnej kampanii pozostanie zatem
`FAIL` albo `NOT_OBSERVED`, dopóki późniejsza migracja nie wprowadzi obiektywnych
referencji dowodów scenariuszy i ich walidacji.

Sam upływ 28 dni nie jest sukcesem. Końcowy raport jakości V1 powinien zawierać
kanoniczny raport z bazy, jego hash, frozen baseline hash, zakres, czasy kampanii,
wersje klienta/protokołu, statystyki cykli/RTT/luk, wyniki scenariuszy, incydenty i
jednoznaczną decyzję bramki.
