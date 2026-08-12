# Roadmap V1 — Plus500 Futures T4

1. **T4-only configuration** — wdrożona w 0.2.0.
2. **Bezpieczna granica workera .NET** — wdrożona w 0.3.0; loopback, token,
   walidacja contract ID, brak order routes i fail-closed.
3. **Rejestracja aplikacji i oficjalny klient T4** — wymagany dostęp od CTS,
   Simulator i aktualny pakiet/przykłady API.
4. **Cykl życia kontraktów futures offline** — wdrożony w 0.4.0; katalog serii,
   wybór front-month, granica wygaśnięcia, kontrolowany roll i jego proweniencja.
5. **Operacyjny ingest i replay** — część offline wdrożona w 0.5.0: atomowy batch,
   dokładny raw payload hash, idempotencja, append-only candles, scheduler i
   deterministyczny point-in-time replay. Live contract tests, reconnect, sesja i
   głębokość rynku pozostają zablokowane do czasu dostępu T4 z punktu 3.
6. **Analiza futures offline** — wdrożona w 0.6.0: schema v3, spread, depth,
   imbalance, basis i annualized basis z zatwierdzonego indeksu
   `plus500_t4_index_v1`, wolumen, expiry risk, wpływ rollu, scenariusze
   bull/base/bear, migracja `0015`, hashe evidence oraz deterministyczne
   `analyze-replay`. Historyczne v2 są odczytywalne, ale kończą się `NO_SIGNAL`.
   Walidacja na prawdziwych danych nadal zależy od dostępu T4 z punktu 3.
7. **Monitoring i alert delivery** — następny etap; dashboard, trace, incident log.
8. **Odbiór V1** — minimum cztery tygodnie read-only i raport jakości.

`v1_gate_passed=false` do ukończenia dostępu live, punktów 7–8 i pełnego odbioru.
