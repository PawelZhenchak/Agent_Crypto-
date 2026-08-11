# Odbiór PostgreSQL 16

Workflow ma uruchomić czystą instancję PostgreSQL 16 i sprawdzić:

- zastosowanie `schema.sql` oraz migracji `0011`, `0012`, `0013`;
- dwukrotny, idempotentny seed T4;
- dokładnie dwa bindingi T4 i jeden `t4_runtime_config`;
- `read_only=true`, `order_routes_enabled=false`;
- triggery append-only, constrainty i rollback;
- restart kontenera i ponowny health `READY`;
- pełną suitę testów Python.

Migracje historyczne zachowują checksumy. Odbiór operacyjny dotyczy wyłącznie
konfiguracji ustanowionej przez `0013` i bieżący seed.
