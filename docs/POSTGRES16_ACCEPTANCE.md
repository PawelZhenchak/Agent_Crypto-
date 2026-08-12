# Odbiór PostgreSQL 16

Workflow ma uruchomić czystą instancję PostgreSQL 16 i sprawdzić:

- zastosowanie `schema.sql` oraz migracji `0011`–`0015`;
- dwukrotny, idempotentny seed T4;
- dokładnie dwa bindingi T4 i jeden `t4_runtime_config`;
- `read_only=true`, `order_routes_enabled=false`;
- triggery append-only, constrainty i rollback;
- atomowy ingest fixture schema v3, idempotencję i 120 znormalizowanych świec;
- jeden snapshot futures, 10 poziomów order booka i wymagane hashe evidence;
- typed basis reference `index` wyłącznie ze źródła `plus500_t4_index_v1`;
- dwukrotny replay z identycznym fingerprintem SHA-256 i identycznym futures evidence;
- ochronę append-only tabel snapshotu, order booka i przejścia kontraktu;
- restart kontenera i ponowny health `READY`;
- pełną suitę testów Python.

Migracje historyczne zachowują checksumy. Odbiór operacyjny dotyczy wyłącznie
konfiguracji ustanowionej przez `0013`, kontraktu ingestu `0014`, evidence z
`0015` i bieżącego seedu. Test korzysta wyłącznie z jawnego fixture’a — nie łączy
się z T4 i nie jest testem live.

Testy jednostkowe uzupełniają odbiór o `analyze-replay`, zgodność raportu live i
replay dla tego samego evidence oraz fail-closed dla historycznego schema v2.
`v1_gate_passed` pozostaje `false` do osobnego odbioru live.
