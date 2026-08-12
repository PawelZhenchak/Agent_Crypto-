# Informacje o wydaniu 0.7.0

Wydanie zamyka punkt 7 w bezpiecznym zakresie offline: monitoring operacyjny i
lokalne dostarczanie alertów badawczych.

## Dodane

- trace ID tworzony przed analizą oraz zapis runów, eventów i zredukowanych
  artifacts bez danych dostępowych i surowych błędów dostawcy;
- deduplikowany incident log dla provider error, timeoutów i degradacji;
- migracja `0016` z immutable `alert_delivery_outbox` oraz append-only
  `alert_delivery_attempts`;
- atomowy zapis PostgreSQL run/artifact + alert + alert event + outbox;
- idempotency key, hashe payloadu i prób, bounded retry, expiry oraz terminalność;
- komendy `monitor`, `deliver-alerts` i `monitoring-status`;
- lokalny statyczny dashboard i read-only endpointy podsumowania, alertów,
  incydentów oraz trace;
- loopback-only API bez CORS, OpenAPI, Swagger i ReDoc, z restrykcyjnymi nagłówkami;
- analizę API wyłącznie jako `POST /v1/analyze` z wymaganym nagłówkiem
  `X-Crypto-Agent-Request: analyze-v1`;
- wersjonowana polityka monitoringu z domyślnym horyzontem retencji 90 dni.

## Świadome ograniczenia

- jedyna trasa delivery to `stdout_json` → `process_stdout`;
- live delivery wymaga bridge schema v4 i `environment=live_t4`; historyczne v2/v3
  pozostają replay-only i nigdy nie kwalifikują się do external delivery;
- trwały outbox ma semantykę at-least-once, dlatego konsument stdout deduplikuje
  po `idempotency_key`; exactly-once nie jest deklarowane;
- brak webhooka oraz integracji Slack, Telegram, e-mail i SMS;
- synthetic, fixture, replay, `NO_SIGNAL`, veto, stale, provider error i expiry
  nigdy nie tworzą dostawy;
- transakcja monitoringu nie obejmuje osobnego magazynu SQLite;
- oficjalny klient T4 nadal nie jest podłączony; reader pending zwraca `503`, więc
  live kończy się `NO_SIGNAL` i lokalnym incydentem;
- testy fixture nie potwierdzają sesji T4 Simulator ani danych live.

`v1_gate_passed=false`. Następny jest punkt 8: oficjalny klient i test live,
minimum cztery tygodnie obserwacji read-only oraz raport jakości.
