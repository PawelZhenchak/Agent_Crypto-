# PostgreSQL 16 — odbiór etapu V1

Checkpoint 0.1.4 dodaje operacyjny odbiór bazy PostgreSQL 16. Workflow
`.github/workflows/postgres16.yml` wykonuje na czystej instancji:

1. bootstrap `db/schema.sql`;
2. checksummowane migracje `0011` i `0012`;
3. dwukrotne uruchomienie idempotentnego `db/seeds/v1_registry.sql`;
4. health-check wymagający dokładnie statusu `READY` i PostgreSQL 16;
5. próbę niedozwolonego `UPDATE` na tabeli append-only (`SQLSTATE 55000`);
6. próbę naruszenia constraintu (`SQLSTATE 23514`);
7. wymuszony rollback i potwierdzenie braku wiersza testowego;
8. restart kontenera PostgreSQL oraz ponowne potwierdzenie danych i `READY`.

Lokalnie ten sam stan można przygotować poleceniami:

```bash
docker compose up -d postgres
crypto-agent db migrate
crypto-agent db seed
crypto-agent db health
```

Oczekiwany końcowy status to `READY`. Samo `db migrate`, bez `db seed`, poprawnie
kończy się `SEEDS_MISSING`. Pakiet seedów nie nadpisuje konfliktujących danych;
health-check odrzuca niepełny, zmieniony lub nadmiarowy zakres registry.

Ten odbiór zamyka wyłącznie etap PostgreSQL 16. Nie zalicza live API contracts,
operacyjnego ingest workera, pełnej analizy V1 ani okresu obserwacji.
