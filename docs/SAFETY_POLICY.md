# Polityka bezpieczeństwa T4

- brak automatycznego wykonywania zleceń;
- agent emituje wyłącznie `ALERT` albo `NO_SIGNAL`;
- tylko jedna zatwierdzona proweniencja: Plus500 Futures T4;
- dane logowania są zabronione w procesie analitycznym;
- most działa tylko na loopback, wymaga tokenu i nie wystawia order routes;
- logiczny symbol musi wskazywać skonfigurowany, rzeczywisty i niewygasły kontrakt;
- wymagane są 120 świec, prawidłowe UTC i cena nie starsza niż 300 sekund;
- brak danych, zła struktura, zły source ID lub stary kontrakt oznacza `NO_SIGNAL`;
- `ALERT` jest alertem badawczym, nie rekomendacją transakcji.

Pojedyncze źródło nie zapewnia niezależnej weryfikacji ceny. Jest to jawny kompromis
decyzji T4-only i musi być uwzględniony w odbiorze jakościowym V1.
