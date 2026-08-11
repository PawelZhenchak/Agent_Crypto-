# Model danych T4

Aktywny rejestr zawiera:

- source: `plus500_t4_futures_v1`;
- venue: `plus500_t4`;
- binding `BTC/USD` → `BTC-FUTURES-FRONT`;
- binding `ETH/USD` → `ETH-FUTURES-FRONT`;
- runtime protocol: `loopback_http_json_v1`;
- `read_only=true` i `order_routes_enabled=false`.

Nazwy `*-FUTURES-FRONT` są logicznymi aliasami. Worker musi utrwalać rzeczywisty
identyfikator kontraktu T4 i regułę roll w każdym batchu. Bez tej proweniencji batch
nie może zostać uznany za operacyjny.

Stare tabele canonical/reference z migracji `0011/0012` pozostają tylko dla
reprodukowalności poprzedniego prototypu. Nowy ingest T4 otrzyma osobny, wersjonowany
kontrakt w kolejnym etapie.
