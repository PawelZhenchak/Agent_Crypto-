# Runbook odbioru T4 V1 — 0.10.0, etap 2

## Cel i stan

Kampania ma dostarczyć dowód jakości na rzeczywistym `live_t4`, a nie tylko
sprawdzić, że kod się uruchamia. Minimalne okno wynosi `672` godziny czasu
rzeczywistego. Nie wolno go skracać, symulować ani uzupełniać backfillem.

Provisioning i testy na prawdziwym Simulator/live nie zostały wykonane. Nie ma
też rzeczywistych identyfikatorów T4 ani rozpoczętej kampanii. Obecny stan to
`v1_gate_passed=false`.

Migracja `0018` usuwa wcześniejszą blokadę „na sztywno”. PostgreSQL może
wyprowadzić dodatni wynik wyłącznie z własnych danych po pełnej kampanii,
spełnieniu progów jakości i zaliczeniu wszystkich siedmiu scenariuszy. Caller
nie podaje `PASS` scenariusza, a wartość bramki w raporcie jest akceptowana tylko
wtedy, gdy dokładnie odpowiada niezależnemu predykatowi bazy.

## 1. Warunki wejścia

Nie uruchamiaj `observe-start`, dopóki wszystkie warunki nie są spełnione:

- PostgreSQL 16 ma migracje do `0018`, seed i health `READY`;
- bridge, weryfikator i runtime działają jako trzy osobne systemowe UID/procesy;
  ten sam UID dla dowolnej pary unieważnia izolację;
- bridge jako jedyny czyta klucz prywatny `0400`, journal `0600` i klucz T4;
  token sterowania znają tylko bridge i verifier, a jeden token odczytowego API
  bridge'a jest współdzielony przez bridge, verifier i runtime;
- verifier jako jedyny czyta evidence DSN, osobny evidence-reader DSN i pin
  publiczny; oba loginy DB muszą być różne i rozłączne; token API verifiera
  znają tylko verifier i runtime;
- runtime jako jedyny czyta runtime DSN;
- `CRYPTO_AGENT_POSTGRES_DSN` wskazuje login runtime, który nie jest właścicielem
  bazy ani członkiem roli dowodowej;
- `CRYPTO_AGENT_POSTGRES_EVIDENCE_DSN` istnieje wyłącznie w środowisku procesu
  weryfikatora i wskazuje login należący do `crypto_agent_evidence_verifier`;
- runtime ma ten DSN, pin SPKI i token sterowania jawnie `unset`; zna token
  danych bridge'a oraz loopback URL i osobny token API verifiera;
- DSN administratora/właściciela służy tylko do migracji i provisioningu; przed
  kampanią jest usunięty z obu zmiennych runtime;
- bridge działa wyłącznie na loopback i `/healthz` pokazuje `READY`;
- `T4_API_ENVIRONMENT=live`, a envelope ma `environment=live_t4` i schema v5;
- provisionowany klucz ma uprawnienia depth dla futures i niezależnego indeksu;
- prywatny katalog schema v2 zawiera rzeczywiste `ExchangeID`, produktowe
  `ContractID`, nieprzezroczyste `MarketID`, kolejną serię i tożsamość indeksu;
- test Simulator został wykonany osobno, bez traktowania go jako live;
- test live potwierdził WebSocket, Chart REST, per-candle `MarketID`, basis,
  point-in-time cutoff oraz brak order routes;
- dla kampanii wybrano z góry nowy UUID, nowy klucz ECDSA P-256 i nowy, pusty
  dziennik JSONL na trwałym wolumenie;
- UUID jest taki sam w bridge'u i `observe-start`;
- `observe-run` jest przetestowany i supervisor może wywoływać go dla obu
  scope’ów przez pełne okno;
- jednostki systemd i `observe-supervise` są zainstalowane oraz potrafią
  ponownie uruchomić bridge po kontrolowanym `restart`.

Publiczny Simulator trwa dwa tygodnie i sam nie wystarcza do kampanii. Simulator,
fixture, synthetic oraz replay nie mogą być źródłem cyklu live.

## 2. Zamrożenie baseline

Zakres odbioru `0.9.0` to:

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

Nie wpisuj klucza API, tokenów, prywatnego klucza, DSN ani innych sekretów do
baseline lub logów. Baseline zawiera fingerprint publicznego SPKI, nie klucz
prywatny.

Przed uruchomieniem bridge'a przygotuj dla tej kampanii:

- nowy `campaign_id` w kanonicznym formacie UUID;
- nowy prywatny klucz ECDSA P-256 PKCS#8 PEM;
- nową ścieżkę pustego dziennika JSONL na trwałym wolumenie;
- osobny, losowy token sterowania scenariuszami.

Nie współdziel klucza ani dziennika między kampaniami. Dziennik jest append-only,
a jego numeracja i hash chain trwają przez restarty bridge'a w obrębie tej samej
kampanii.

## 3. Start kampanii

Najpierw ustaw bridge w środowisku bridge-only UID, używając wcześniej wybranych
wartości:

```bash
export T4_OBSERVATION_CAMPAIGN_ID='<uuid>'
export T4_EVIDENCE_SIGNING_KEY_PATH='/private/t4-evidence-key.pem'
export T4_OBSERVATION_EVENT_JOURNAL_PATH='/private/t4-events-<uuid>.jsonl'
export T4_OBSERVATION_CONTROL_ENABLED='false'
unset T4_OBSERVATION_CONTROL_TOKEN
```

Po starcie bridge'a odczytaj fingerprint klucza z `/healthz` lub podpisanej
strony zdarzeń. Utwórz dwa prywatne pliki na podstawie
`configs/runtime.env.example` i `configs/evidence-verifier.env.example`, nadaj im
tryb `0600` i różnych właścicieli. W środowisku osobnego weryfikatora ustaw:

```bash
export CRYPTO_AGENT_T4_EVIDENCE_KEY_FINGERPRINT='<sha256-spki>'
export CRYPTO_AGENT_POSTGRES_EVIDENCE_DSN='<dsn-loginu-verifier>'
export CRYPTO_AGENT_POSTGRES_EVIDENCE_READ_DSN='<dsn-loginu-reader>'
export CRYPTO_AGENT_T4_BRIDGE_TOKEN='<wspólny-token-danych>'
export CRYPTO_AGENT_T4_OBSERVATION_CONTROL_TOKEN='<token-control-bridge-verifier>'
export CRYPTO_AGENT_EVIDENCE_VERIFIER_TOKEN='<losowy-osobny-token>'
unset CRYPTO_AGENT_POSTGRES_DSN
crypto-agent evidence-verifier --host 127.0.0.1 --port 8791
```

W osobnym środowisku runtime ustaw tylko:

```bash
export CRYPTO_AGENT_POSTGRES_DSN='<dsn-loginu-runtime>'
export CRYPTO_AGENT_EVIDENCE_VERIFIER_URL='http://127.0.0.1:8791'
export CRYPTO_AGENT_EVIDENCE_VERIFIER_TOKEN='<ten-sam-token-verifiera>'
unset CRYPTO_AGENT_POSTGRES_EVIDENCE_DSN
unset CRYPTO_AGENT_POSTGRES_EVIDENCE_READ_DSN
unset CRYPTO_AGENT_T4_EVIDENCE_KEY_FINGERPRINT
unset CRYPTO_AGENT_T4_EVIDENCE_PUBLIC_KEY_SPKI_BASE64
unset CRYPTO_AGENT_T4_OBSERVATION_CONTROL_TOKEN
```

Następnie uruchom start z tym samym UUID:

```bash
crypto-agent observe-start \
  --campaign-id <uuid> \
  --cycle-interval-seconds 300 \
  --scope BTC/USD:240m \
  --scope ETH/USD:240m \
  --code-commit-hash <sha256> \
  --t4-protocol-commit-hash eb589f6afe689d1e9037b6f50a0993606f12f09739ff6409d4e77a0fc220c566 \
  --runtime-config-hash <sha256>
```

`observe-start` prosi osobny proces o rejestrację klucza. Weryfikator sam pobiera
dziennik, sprawdza pin, ECDSA, projekcję kanoniczną i hash chain, a następnie
używa osobnej roli DB. Runtime nie przesyła eventów, `PASS` ani
`signature_verified`.
Potem wykonuje live preflight schema v5 każdego scope’u z limitem 60 świec.
Brak prawdziwego live, zła schema, Simulator/replay/synthetic albo brak atestacji
read-only zatrzymuje start.

Zachowaj `campaign_id`, fingerprint klucza, ścieżkę dziennika, `started_at`,
`planned_ends_at`, hash polityki i `frozen_baseline_hash_sha256`. Czas pochodzi
z PostgreSQL; CLI nie przyjmuje historycznego startu.

Minimalny podział `systemd` to trzy jednostki z trzema różnymi `User=` i
`EnvironmentFile=`: bridge, verifier i runtime. W każdej ustaw
`NoNewPrivileges=true`; tylko bridge czyta prywatny klucz/journal/T4, token
control znają bridge i verifier, tylko verifier czyta evidence DSN i pin
publiczny, a tylko runtime runtime DSN. Nie
uruchamiaj poleceń z jednego załadowanego shella `.env`. Loopback HTTP dotyczy
usług `systemd` na jednym hoście; osobne kontenery Compose wymagają projektu UDS
albo wewnętrznego TLS i odseparowanej sieci, nie `127.0.0.1` między kontenerami.

`replay_blocked` używa osobnego LOGIN-u należącego wyłącznie do
`crypto_agent_evidence_reader`. Verifier odczytuje zamrożone scope’y i cutoff,
sam uruchamia deterministyczny replay 120 świec in-memory, sprawdza flagi
read-only/braku delivery i zapisuje fingerprint, batch ID/hash oraz provenance
wyłącznie przez verifier-only funkcję atestacji. Runtime nie podaje run ID,
artifactu, fingerprintu ani `PASS`.

Model zakłada poprawnie działający, nieprzejęty normalny runtime i host. Pełne
przejęcie runtime/hosta jest poza zakresem. Podpisane przez bridge receipt
każdego batcha jest możliwym przyszłym utwardzeniem, a nie obecną gwarancją.

Po rozpoczęciu nie zmieniaj UUID, klucza, dziennika, kodu, protokołu, polityki,
scope’u, interwału cyklu, środowiska ani konfiguracji analitycznej. Jedynym
planowym przełączeniem operacyjnym jest flaga endpointu kontrolnego na czas
udokumentowanej próby; zachowuje ten sam UUID, klucz, dziennik i token. Inna
zmiana baseline oznacza nową kampanię pełnych 28 dni. Stary ledger pozostaje
w audycie.

## 4. Runner cykli

Automatyczny supervisor wywołuje oba scope’y; ręczne komendy poniżej służą tylko
do diagnostyki:

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

Normalny tryb 28-dniowy:

```bash
crypto-agent observe-supervise --campaign-id <uuid>
crypto-agent observe-supervisor-status --campaign-id <uuid>
```

Supervisor korzysta z czasu PostgreSQL, uruchamia każdy cykl w osobnym
podprocesie, kończy zawieszony proces po timeout i ponawia tylko błędy techniczne.
Utrata odpowiedzi po commicie nie tworzy duplikatu, ponieważ następne wywołanie
jest rozstrzygane przez advisory lock, sekwencję i hash chain w bazie. Pełna
obsługa: [runbook supervisora](CAMPAIGN_SUPERVISOR_RUNBOOK.md).

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

Każdy z siedmiu scenariuszy musi mieć wynik `pass` wyprowadzony przez PostgreSQL.
Brak dowodu daje `NOT_OBSERVED`. Jakikolwiek zapisany `fail` blokuje zaliczenie
danego scenariusza; nie powtarzaj próby, aby ukryć niepowodzenie.

CLI nie wysyła wyniku do bridge'a ani bazy. Przekazuje wyłącznie identyfikatory
i hashe zweryfikowanych zdarzeń. Triggery łączą je z realnym batchem, cyklem lub
runem i dopiero wtedy wyliczają `pass` albo `fail`.

Podpis ECDSA jest podpisem naszego bridge'a dowodowego. Nie jest podpisem T4,
Plus500 ani dowodem, że dostawca wygenerował zdarzenie.

### Scenariusze kontrolowane

Na czas zaplanowanej próby uruchom bridge z
`T4_OBSERVATION_CONTROL_ENABLED=true`. Poza tym oknem ustaw `false`. Zawsze używaj
osobnego tokenu kontrolnego i tego samego UUID/klucza/dziennika kampanii.

Przykład:

```bash
crypto-agent observe-scenarios \
  --campaign-id <uuid> \
  --scenario reconnect \
  --scope BTC/USD:240m
```

Pięć scenariuszy kontrolowanych:

- `bridge_restart` — receipt i `bridge_stopping`, nowy `boot_id`, ponowne
  `session_ready` oraz realny batch po odzyskaniu. Bridge sam się zatrzymuje;
  jednostka systemd z `Restart=always` uruchamia go ponownie, a supervisor
  potwierdza recovery przed dalszym cyklem;
- `missing_data` — kontrolowany brak danych musi utworzyć typowany nieudany cykl
  `T4_MISSING_DATA`, bez alertu, a potem realny batch odzyskania;
- `rate_limit` — kontrolowany fault musi utworzyć nieudany cykl
  `T4_RATE_LIMITED`, bez alertu, a potem realny batch odzyskania. To dowodzi
  zachowania naszego handlera, nie prawdziwego `429` od dostawcy. Nie spamuj T4;
- `reconnect` — cache zostaje wyczyszczony, sesja i subskrypcje odtworzone, a
  odzyskanie potwierdza nowy live batch;
- `stale_data` — typowany cykl `T4_STALE_DATA` kończy się fail-closed, bez alertu,
  po czym pojawia się realny batch odzyskania.

Próby `missing_data`, `rate_limit` i `stale_data` wymagają aktualnie należnego
slotu zamrożonego harmonogramu. CLI odmawia uzbrojenia faultu bez takiego slotu.

### Scenariusze pasywne

- `replay_blocked` — wywołanie pasywne zleca isolated verifierowi samodzielny
  replay wszystkich zamrożonych scope’ów przy dokładnym `planned_ends_at`.
  PostgreSQL wymaga verifier-only atestacji fingerprintu, batch ID/hash,
  provenance i braku eligibility delivery;
- `roll_transition` — nie ma injectora. W oknie kampanii musi wystąpić prawdziwy
  live roll: podpisane zdarzenie bridge'a, batch schema v5 z różnymi starym i
  nowym `MarketID`, transition evidence, udany cykl i kanoniczne świece nowej
  serii.

Weryfikacja pasywna:

```bash
crypto-agent observe-scenarios --campaign-id <uuid> \
  --scenario replay_blocked --scope BTC/USD:240m

crypto-agent observe-scenarios --campaign-id <uuid> \
  --scenario roll_transition --scope BTC/USD:240m
```

Nie testuj braku order routes przez wysłanie prawdziwego zlecenia. Ta granica ma
pozostać niewystawiona; jakakolwiek próba order routing jest naruszeniem i
obowiązkowym `FAIL`. Zaplanuj kampanię tak, aby obejmowała rzeczywiste okno rollu,
inaczej `roll_transition` pozostanie `NOT_OBSERVED`.

## 7. Codzienna kontrola

```bash
crypto-agent observe-status --campaign-id <uuid>
crypto-agent observe-supervisor-status --campaign-id <uuid>
crypto-agent monitoring-status
```

Supervisor automatycznie zapisuje atomowy status, hash-chain zdarzeń i najwyżej
jeden snapshot na dzień UTC. Sprawdzaj w szczególności:

- `remaining_seconds`, `observation_remaining_seconds`,
  `finalization_grace_remaining_seconds` i zgodność czasu z PostgreSQL;
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
`OBSERVATION_WINDOW_INCOMPLETE` albo, po pełnych 672 godzinach,
`OBSERVATION_FINALIZATION_GRACE_INCOMPLETE`, i nie zapisuje raportu. Dodatkowy
interwał jest okresem grace na uczciwe zapisanie ostatniego planowego slotu:

```bash
crypto-agent observe-report --campaign-id <uuid>
```

Po pełnym oknie i okresie grace polecenie zapisuje jeden niezmienny raport i jego
SHA-256. Model wyników to:

- `PASS` — wszystkie obowiązkowe kryteria mają `PASS`,
  `v1_gate_passed=true`;
- `FAIL` — co najmniej jedno kryterium ma `FAIL`, bramka pozostaje zamknięta;
- `NOT_OBSERVED` — brak wymaganego dowodu, bramka pozostaje zamknięta.

W `0.10.0` dodatni wynik nie jest blokowany stałą. Funkcja
`t4_observation_gate_is_verified(...)` liczy go bezpośrednio z ledgerów
PostgreSQL. Trigger odrzuca raport, jeżeli przesłane `v1_gate_passed` różni się od
wyniku tej funkcji.

Bramka może być `true` dopiero, gdy:

- minęło realne 672 godziny oraz okres grace;
- spełniono progi prób, sukcesów, pokrycia, luk i RTT;
- każdy sukces ma dokładny batch/run i schema v5;
- nie ma naruszeń bezpieczeństwa;
- wszystkie siedem scenariuszy ma wyłącznie wyniki `pass`;
- dowód `roll_transition` pochodzi z rzeczywistego rollu live.

W obecnym stanie warunki te nie są spełnione: nie ma provisioningu T4,
rzeczywistych identyfikatorów rynków ani rozpoczętej kampanii. Wynik pozostaje
`v1_gate_passed=false`.

Sam upływ 28 dni nie jest sukcesem. Końcowy raport jakości V1 powinien zawierać
kanoniczny raport z bazy, jego hash, frozen baseline hash, zakres, czasy kampanii,
wersje klienta/protokołu, statystyki cykli/RTT/luk, wyniki scenariuszy, incydenty i
jednoznaczną decyzję bramki.
