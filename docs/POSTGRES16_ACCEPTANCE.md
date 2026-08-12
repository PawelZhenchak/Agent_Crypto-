# Odbiór PostgreSQL 16

Workflow ma uruchomić czystą instancję PostgreSQL 16 i sprawdzić:

- zastosowanie `schema.sql` oraz migracji `0011`–`0017`;
- dwukrotny, idempotentny seed T4;
- dokładnie dwa bindingi T4 i jeden `t4_runtime_config`;
- `read_only=true`, `order_routes_enabled=false`;
- triggery append-only, constrainty i rollback;
- atomowy ingest historycznego fixture schema v3, idempotencję i 120
  znormalizowanych świec; fixture/replay nie kwalifikuje się do external delivery;
- jeden snapshot futures, 10 poziomów order booka i wymagane hashe evidence;
- typed basis reference `index` wyłącznie ze źródła `plus500_t4_index_v1`;
- dwukrotny replay z identycznym fingerprintem SHA-256 i identycznym futures evidence;
- ochronę append-only tabel snapshotu, order booka i przejścia kontraktu;
- utworzenie przez `0016` tabel `alert_delivery_outbox` i
  `alert_delivery_attempts` oraz ich triggerów integralności i append-only;
- jedyną trasę outboxa `stdout_json` → `process_stdout` i payload
  `decision=ALERT`, `read_only=true`;
- idempotency key, zgodność payload hash, kolejność prób, previous hash, limit prób,
  retry boundary, terminalność i expiry;
- brak `UPDATE`, `DELETE` i `TRUNCATE` w outboxie oraz historii prób;
- utrzymanie rekordów outboxa i prób po restarcie kontenera;
- semantykę at-least-once stdout i wymaganie deduplikacji konsumenta po
  `idempotency_key`, bez deklaracji exactly-once;
- kolumny schema v5: środowisko, `ExchangeID`, produktowy `ContractID`, aktywny i
  poprzedni `MarketID`, niezależna tożsamość basis oraz per-candle `MarketID`;
- zgodność tożsamości v5 pomiędzy batchem, świecami, snapshotem i transition
  evidence oraz fail-closed dla niezgodnego indeksu/rynku;
- utworzenie przez `0017` append-only tabel kampanii, cykli, zdarzeń sesji i
  końcowych raportów jakości wraz z ich triggerami;
- zamrożenie baseline i hashy, dwa niezależne hash chain, użycie czasu bazy,
  blokadę cyklu z przyszłości oraz próbę backfillu;
- odmowę końcowego raportu przed
  `planned_ends_at + cycle_interval_seconds`, celową blokadę scenariusza `PASS`
  i `v1_gate_passed=true` do późniejszej migracji z obiektywnymi referencjami
  dowodów;
- restart kontenera i ponowny health `READY`;
- pełną suitę testów Python.

Migracje historyczne zachowują checksumy. Odbiór operacyjny dotyczy wyłącznie
konfiguracji ustanowionej przez `0013`, kontraktu ingestu `0014`, evidence z
`0015`, outboxa z `0016`, warstwy schema v5/obserwacji z `0017` i bieżącego seedu.
Test korzysta wyłącznie z jawnego fixture’a — nie łączy się z T4 i nie jest
testem live.

Testy jednostkowe uzupełniają odbiór o `analyze-replay`, zgodność raportu live i
replay dla tego samego evidence, fail-closed dla historycznego schema v2 oraz
negatywną kwalifikację delivery dla fixture/replay/`NO_SIGNAL`/veto/stale.
Osobny odbiór live wymaga bridge schema v5 i `environment=live_t4`; Simulator i
historyczny replay nigdy nie kwalifikują się do external delivery.

Atomowy zapis monitoringu oznacza jedną transakcję PostgreSQL dla run/artifact,
alertu, alert eventu i outboxa. Odbiór nie deklaruje atomowości pomiędzy PostgreSQL
i historycznym magazynem SQLite. `v1_gate_passed` pozostaje `false`; schema
`0.8.0` nie pozwala zmienić go na `true` bez późniejszej migracji walidującej
obiektywne referencje dowodów scenariuszy. Test migracji `0017` dowodzi mechanizmu,
nie zastępuje provisioningu, testów Simulator/live ani rzeczywistych 672 godzin;
kampania nie została rozpoczęta.
