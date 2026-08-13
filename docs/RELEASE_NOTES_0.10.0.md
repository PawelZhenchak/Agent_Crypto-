# Crypto Research Agent 0.10.0

## Automatyczny nadzorca kampanii

- dodano `observe-supervise`, który przez całe zamrożone okno uruchamia należne
  cykle BTC i ETH według czasu PostgreSQL;
- każdy cykl działa w osobnym podprocesie z timeoutem, ograniczonym retry i
  wykładniczym backoffem;
- idempotencja pozostaje w ledgerze PostgreSQL: restart, utrata odpowiedzi i
  ponowienie nie tworzą drugiego zapisu slotu;
- nadzorca wykrywa i utrwala timeouty, nowe `missed`, restarty usług oraz próby
  finalizacji;
- stan jest odtwarzalny po restarcie, chroniony singleton lockiem i zapisywany
  jako atomowy `status.json`, append-only event journal oraz jeden dzienny
  snapshot UTC;
- po pełnych 28 dniach i okresie grace nadzorca automatycznie uruchamia
  `observe-report`, po czym kończy się bez restartu;
- dodano `observe-supervisor-status` jako bezpieczną projekcję do późniejszego
  panelu mobilnego.

## Restart procesów 24/7

- dodano utwardzone jednostki systemd dla bridge'a, isolated verifiera i
  supervisora;
- bridge i verifier mają `Restart=always`, supervisor `Restart=on-failure`;
- trzy procesy zachowują oddzielne UID i granice sekretów;
- ścisła reguła polkit pozwala runtime’owi uruchomić ponownie wyłącznie dwie
  zatwierdzone jednostki read-only, bez dowolnej komendy shell.

## Bezpieczeństwo

- żadna nowa ścieżka nie składa, nie zmienia ani nie anuluje zleceń;
- surowy stderr i sekrety podprocesów nie trafiają do statusu ani dzienników;
- uszkodzony zatwierdzony hash chain zatrzymuje supervisor fail-closed;
- niepełny tail po crashu może zostać odrzucony bez utraty wcześniejszego
  zatwierdzonego prefixu;
- niedostępny bridge/verifier blokuje cykle zamiast generować sztuczne dane.

## Odbiór

Testy obejmują oba scope’y, restart procesu, singleton, timeout i retry,
idempotencję, missed, automatyczny raport, restart usług, crash-tail, tamper,
uprawnienia plików oraz parsowanie wyłącznie kanonicznego JSON.

To wydanie dostarcza kod etapu 2. Nie oznacza uruchomienia realnej kampanii:
nadal potrzebne są dostęp T4, prawdziwe identyfikatory, wdrożenie hosta 24/7 i
672 godziny danych live. Do tego czasu `v1_gate_passed=false`.
