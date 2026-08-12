# 0.5.0 — operacyjny ingest i deterministyczny replay T4

## Dodano

- migrację `0014_t4_operational_ingest.sql`;
- atomowy, idempotentny zapis pełnych batchy T4 i znormalizowanych świec;
- dokładny SHA-256 surowego payloadu oraz jego trwałą kopię base64;
- proweniencję rzeczywistego kontraktu, expiry, rollu i ceny referencyjnej;
- point-in-time replay w transakcji PostgreSQL `READ ONLY`;
- deterministyczny fingerprint wyniku replayu;
- zatrzymywalny scheduler oraz komendy CLI `ingest` i `replay`;
- testy manipulacji payloadem, duplikacji, niepełnej historii i deterministyczności;
- test akceptacyjny PostgreSQL 16 obejmujący ingest, replay i restart.

## Granice bezpieczeństwa

- wyłącznie Plus500 Futures / T4;
- brak tras składania, zmiany i anulowania zleceń;
- brak pełnego okna replayu powoduje `T4_REPLAY_INCOMPLETE`;
- dane z fixture’ów są używane tylko w testach i nie udają połączenia live;
- testy live, reconnect i depth pozostają zablokowane do czasu oficjalnego dostępu T4.
