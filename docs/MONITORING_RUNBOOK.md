# Runbook monitoringu i lokalnego dostarczania alertów

## Zakres 0.8.0

Monitoring działa read-only i przechowuje w PostgreSQL trace, bezpieczne artifacts,
incydenty, alerty oraz delivery outbox. Jedynym kanałem jest kanoniczny JSON
zapisany do stdout (`stdout_json` → `process_stdout`). Wersja nie wysyła danych do
webhooka, Slacka, Telegrama, e-maila ani SMS.

Oficjalny reader oparty o `Plus500US.T4Proto` 1.0.73 oraz
`Plus500US.T4ChartDecoder` 1.0.97 jest zaimplementowany, ale provisioning i
rzeczywiste testy Simulator/live nie zostały wykonane. Nadal potrzebne są
uprawnienia oraz prawidłowe identyfikatory rynku i niezależnego indeksu. Stan
`pending` albo niegotowy prewarm zwraca `503`: cykl daje `NO_SIGNAL` i lokalny
incydent, a nie przesyłkę.

## Wymagania

1. Skonfiguruj `CRYPTO_AGENT_POSTGRES_DSN` bez commitowania credentials.
2. Zastosuj migracje `0011`–`0017` i seed.
3. Sprawdź `crypto-agent db health`; wynik musi być `READY`.
4. Dla realnego cyklu skonfiguruj prywatny token, katalog schema v2 i lokalny
   bridge T4. Delivery live wymaga schema v5 oraz `environment=live_t4`;
   `t4_simulator` i replay są zawsze wykluczone.

## Procesy

Jednorazowy cykl monitoringu:

```bash
crypto-agent monitor --symbol BTC/USD --interval 240
```

Ciągły monitoring:

```bash
crypto-agent monitor --symbol BTC/USD --interval 240 \
  --watch --poll-seconds 300
```

Worker dostarczający jeden oczekujący alert:

```bash
crypto-agent deliver-alerts
```

Ciągły worker delivery:

```bash
crypto-agent deliver-alerts --watch --poll-seconds 5
```

Alert JSON jest jedyną treścią biznesową stdout. Status cyklu i próby delivery
trafia do stderr, dlatego supervisor powinien zbierać te strumienie oddzielnie.

## Kwalifikacja alertu

Outbox powstaje tylko wtedy, gdy wszystkie warunki są spełnione:

- operacja jest analizą live T4;
- bridge potwierdził schema v5, pełną tożsamość T4 oraz
  `environment=live_t4`;
- decyzja to `ALERT` i risk gate nie zgłosił veto;
- źródło i instrument mają zatwierdzoną atestację T4;
- futures evidence jest kompletne i bieżące;
- brak provider error;
- raport nie wygasł i nadal mieści się w oknie delivery;
- execution pozostaje wyłączone.

`NO_SIGNAL`, synthetic, fixture, Simulator, replay, veto, stale, provider error i
wynik po expiry nigdy nie są dostarczane. Replay może zostać zapisany jako trace
badawczy, ale nie może utworzyć outboxa.

## Retry, idempotencja i trwałość

Domyślna polityka `configs/monitoring_policy.v1.json` ustawia trzy próby, backoff
2–30 sekund, timeout próby 2 sekundy, stronę dashboardu do 50 rekordów i horyzont
retencji 90 dni. Polityka jest walidowana fail-closed i nie może wskazać innego
kanału ani destination.

Każda pozycja outboxa ma idempotency key i hash kanonicznego payloadu. Próby mają
rosnący numer, własny content hash i hash poprzedniej próby. Po `delivered`,
`permanent_failure` lub `expired` nie wolno rozpocząć kolejnej próby. Identyczny
alert nie tworzy drugiej trasy delivery.

Tabele są append-only. Horyzont retencji jest częścią polityki operacyjnej, ale
worker nie kasuje automatycznie audytu.

## Odczyt stanu

```bash
crypto-agent monitoring-status
```

API uruchamiaj wyłącznie na loopback:

```bash
uvicorn crypto_agent.api:app --host 127.0.0.1 --port 8000
```

- `/health` — gotowość runtime i PostgreSQL;
- `POST /v1/analyze` — analiza zapisująca trace; wymaga nagłówka
  `X-Crypto-Agent-Request: analyze-v1`;
- `/v1/monitoring/summary` — liczniki i stan kolejki;
- `/v1/monitoring/alerts?limit=50` — ostatnie alerty;
- `/v1/monitoring/incidents?limit=50` — ostatnie incydenty;
- `/v1/monitoring/traces/{trace_id}` — konkretny przebieg;
- `/dashboard` — statyczna lokalna projekcja HTML.

API odrzuca klienta spoza loopback. Swagger, ReDoc i OpenAPI są wyłączone. Nie
wystawiaj tego serwera publicznie ani przez reverse proxy. `GET /v1/analyze` nie
istnieje; analiza nie jest operacją GET ze skutkiem ubocznym.

## Reakcja na stan awaryjny

- `T4_BRIDGE_UNAVAILABLE` / HTTP `503`: sprawdź provisioning, środowisko, klucz,
  katalog, uprawnienia i prewarm; potwierdź incydent w dashboardzie, nie obchodź
  readera fixture’em.
- `MONITORING_CYCLE_FAILED`: sprawdź health PostgreSQL, komplet migracji i prywatną
  konfigurację bridge’a. Szczegóły nie są wypisywane celowo.
- `ALERT_DELIVERY_FAILED`: zatrzymaj worker, sprawdź PostgreSQL i integralność
  outboxa. Nie kopiuj alertów ręcznie do zewnętrznego kanału jako obejścia.
- `POLL_INTERVAL_INVALID`: ustaw monitor minimum 1 s, delivery minimum 0,5 s;
  maksimum obu wartości to 86400 s.

Po odzyskaniu PostgreSQL uruchom ponownie worker. Outbox i historia prób są trwałe,
więc restart procesu nie usuwa oczekujących pozycji. Ze względu na semantykę
at-least-once konsument stdout musi deduplikować po `idempotency_key`. System nie
deklaruje exactly-once pomiędzy transakcją PostgreSQL a procesem konsumenta.

Końcowe 1,5 sekundy całkowitego limitu API jest zarezerwowane na bounded best-effort
zapis `ANALYSIS_TIMEOUT`. Niedostępność PostgreSQL nie zmienia pierwotnego wyniku
na inny błąd i nie powoduje oczekiwania poza całkowitym limitem. Konfigurowalny
limit API musi mieścić się w przedziale 3–120 sekund.

## Granica transakcji

W jednej transakcji PostgreSQL powstają run/artifact, alert, alert event i outbox.
SQLite jest odrębnym historycznym magazynem raportów i nie uczestniczy w tej
transakcji. Nie należy deklarować atomowości między tymi bazami.

`v1_gate_passed=false`. Monitoring nie zastępuje kampanii punktu 8. Rzeczywisty
test live, zatwierdzony runner cykli i minimum 672 godziny obserwacji BTC/ETH 4h
nie zostały jeszcze wykonane.
