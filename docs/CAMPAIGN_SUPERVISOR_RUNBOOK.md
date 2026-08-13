# Runbook automatycznego nadzorcy kampanii 28-dniowej

## Cel i granice

`crypto-agent observe-supervise` utrzymuje zamrożoną kampanię BTC/ETH bez
ręcznego wywoływania cykli. Nadzorca:

- pobiera harmonogram i czas wyłącznie z PostgreSQL;
- uruchamia każdy należny scope w osobnym podprocesie;
- kończy zawieszony podproces po ustawionym limicie;
- ponawia tylko bezpieczne błędy techniczne; zapisany `failure` albo `missed`
  pozostaje niezmienny;
- po utracie odpowiedzi ponownie wywołuje idempotentne `observe-run`, więc baza
  rozstrzyga, czy slot został już zapisany;
- wykrywa nowe `missed`, timeouty i niedostępne usługi;
- zapisuje aktualny `status.json`, hash-chain zdarzeń oraz najwyżej jeden dzienny
  snapshot na datę UTC;
- po końcu 672 godzin i okresie grace automatycznie wywołuje `observe-report`;
- nigdy nie udostępnia ani nie wywołuje operacji składania, zmiany lub anulowania
  zleceń.

Nadzorca nie rozpoczyna kampanii i nie fabrykuje dowodów scenariuszy. Przed jego
startem `observe-start` musi już utworzyć poprawny baseline live T4.

## Model procesów

Wdrożenie korzysta z trzech oddzielnych użytkowników systemowych:

| Usługa | UID | Sekrety | Restart |
|---|---|---|---|
| T4 bridge | `crypto-bridge` | klucz T4, prywatny klucz ECDSA, journal | `Restart=always` |
| Evidence verifier | `crypto-verifier` | verifier/read DSN, pin SPKI, control token | `Restart=always` |
| Campaign supervisor/runtime | `crypto-runtime` | runtime DSN, token danych i API verifiera | `Restart=on-failure` |

Runtime nie może czytać plików bridge'a ani verifiera. Polkit zezwala mu tylko na
`start`/`restart` dwóch dokładnie nazwanych jednostek read-only. Nie wolno
rozszerzać tej reguły do dowolnych usług ani poleceń.

## Pliki konfiguracyjne

Szablony:

- `configs/systemd/crypto-agent-t4-bridge.service`;
- `configs/systemd/crypto-agent-evidence-verifier.service`;
- `configs/systemd/crypto-agent-campaign-supervisor.service`;
- `configs/systemd/50-crypto-agent-supervisor.rules`;
- `configs/campaign-supervisor.env.example`.

Pliki `runtime.env`, `evidence-verifier.env`, `t4-bridge.env` i
`campaign-supervisor.env` muszą mieć tryb `0600` i różnych właścicieli zgodnych z
macierzą powyżej. Prywatny klucz bridge'a ma tryb `0400` i jest przekazywany przez
systemd `LoadCredential`.

## Uruchomienie

Po utworzeniu kampanii wpisz ten sam UUID do root-owned
`/etc/crypto-agent/campaign-supervisor.env`:

```dotenv
CRYPTO_AGENT_OBSERVATION_CAMPAIGN_ID=<uuid>
CRYPTO_AGENT_SUPERVISOR_STATE_DIRECTORY=/var/lib/crypto-agent-supervisor
CRYPTO_AGENT_SUPERVISOR_PROCESS_MANAGER=systemd
```

Jednostki uruchamiają się w kolejności bridge → verifier → supervisor. W
normalnym wdrożeniu wykonuje to administrator/automatyzacja etapu 3:

```bash
systemctl enable --now crypto-agent-t4-bridge.service
systemctl enable --now crypto-agent-evidence-verifier.service
systemctl enable --now crypto-agent-campaign-supervisor.service
```

Do jednorazowego testu bez zarządzania procesami:

```bash
crypto-agent observe-supervise \
  --campaign-id <uuid> \
  --process-manager none \
  --state-directory /absolutna/sciezka/test \
  --once
```

## Status i dzienniki

Bezpieczna projekcja lokalna:

```bash
crypto-agent observe-supervisor-status \
  --campaign-id <uuid> \
  --state-directory /var/lib/crypto-agent-supervisor
```

Katalog kampanii zawiera:

- `status.json` — atomowo zastępowany bieżący stan;
- `events.jsonl` — append-only hash chain restartów, timeoutów, retry, missed i
  prób raportu;
- `daily-snapshots.jsonl` — jeden hash-chain snapshot na dzień UTC;
- `supervisor.lock` — blokada singletona.

Pliki mają tryb `0600`, katalog `0700`. Uszkodzenie zatwierdzonego rekordu
zatrzymuje start (`SUPERVISOR_JOURNAL_INVALID`). Niepełny ostatni rekord po crashu
jest bezpiecznie odrzucany, a wcześniejszy zatwierdzony prefix pozostaje ważny.

`status.json` pokazuje dla BTC i ETH: numer ostatniego slotu, następny termin,
liczbę success/failure/missed, ostatni wynik, pozostały czas, restarty usług,
timeouty oraz stan raportu końcowego.

## Retry i brak duplikatów

Każdy cykl jest osobnym wywołaniem `observe-run`. PostgreSQL trzyma advisory lock
kampanii i scope’u, numer sekwencji oraz hash chain. Dlatego:

- timeout bez commita może zostać ponowiony;
- commit z utraconą odpowiedzią zostanie rozpoznany przy ponowieniu;
- dwa procesy nie zapiszą tego samego slotu;
- wygasły slot staje się wyłącznie `missed`;
- zapisany `failure` nie jest później zamieniany w sukces.

Retry ma ograniczoną liczbę prób i wykładniczy backoff. Nadzorca nigdy nie pętli
bez ograniczeń w jednym slocie.

## Incydenty

- `cycle_timeout_detected` — podproces został zakończony; sprawdź bridge, sieć i
  PostgreSQL. Retry nie tworzy duplikatu.
- `missed_cycles_detected` — kampania odzyskała harmonogram po przerwie, ale
  minionych slotów nie backfilluje.
- `service_restart_requested` z `recovered=false` — jednostka nie wróciła;
  systemd ponawia ją niezależnie, a nadzorca nie uruchamia cykli na martwym
  bridge/verifierze.
- `SUPERVISOR_ALREADY_RUNNING` — drugi nadzorca wskazał ten sam katalog; nie
  usuwaj locka działającego procesu.
- `SUPERVISOR_JOURNAL_INVALID` — zachowaj katalog do analizy; nie twórz nowego
  dziennika w celu ukrycia incydentu.

Po zapisaniu raportu końcowego nadzorca kończy się kodem `0`. Jednostka ma
`Restart=on-failure`, więc nie rozpoczyna go ponownie po prawidłowym zakończeniu.

## Czego nadal potrzeba do realnej kampanii

Kod nadzorcy nie zastępuje dostępu T4. Kampania może wystartować dopiero po
otrzymaniu klucza API, uprawnień depth/index, prawdziwych `ExchangeID`,
`ContractID`, `MarketID`, zaliczeniu Simulator/live i wdrożeniu usług na hoście
24/7. Do tego czasu `v1_gate_passed=false` jest prawidłowym wynikiem.
