# Polityka bezpieczeństwa T4

- brak automatycznego wykonywania zleceń;
- agent emituje wyłącznie `ALERT` albo `NO_SIGNAL`;
- tylko jedna zatwierdzona proweniencja: Plus500 Futures T4;
- dane logowania są zabronione w procesie analitycznym;
- most działa tylko na loopback, wymaga tokenu i nie wystawia order routes;
- logiczny symbol musi wskazywać skonfigurowany, rzeczywisty i niewygasły kontrakt;
- wymagane są 120 świec, prawidłowe UTC i cena nie starsza niż 300 sekund;
- operacyjna analiza wymaga schema v3, pełnego snapshotu order booka i statusu
  sesji `OPEN`;
- basis może używać wyłącznie referencji typu `index` o source ID dokładnie
  `plus500_t4_index_v1`; ceny futures ani anonimowej ceny referencyjnej nie wolno
  użyć jako basis;
- spread, depth, imbalance, basis, annualized basis, wolumen, expiry i roll muszą
  wynikać z atestowanych dowodów, a nie z wartości zastępczych;
- brak danych, zła struktura, zły source ID, stary kontrakt, niepełny/stary order
  book albo nieważny basis oznacza `NO_SIGNAL`;
- historyczne schema v2 jest odczytywalne, ale bez futures evidence zawsze kończy
  analizę jako `NO_SIGNAL`;
- evidence schema v3 jest append-only, hashowane i obejmowane fingerprintem replayu;
- `ALERT` jest alertem badawczym, nie rekomendacją transakcji.

Pojedyncze źródło nie zapewnia niezależnej weryfikacji ceny. Jest to jawny kompromis
decyzji T4-only i musi być uwzględniony w odbiorze jakościowym V1.

Oficjalny klient T4 nie jest jeszcze podłączony. Reader pending zwraca `503`, a
fixture’y służą wyłącznie testom offline i nie mogą być przedstawiane jako dane
live. `v1_gate_passed=false`; ukończenie analityki 0.6.0 nie otwiera wykonywania
zleceń ani nie zamyka odbioru V1.
