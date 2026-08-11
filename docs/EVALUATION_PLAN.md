# Plan ewaluacji T4

1. Testy parsera i fail-closed dla błędnych payloadów.
2. Test, że bridge przyjmuje wyłącznie loopback.
3. Test, że order routes lub brak read-only blokują atestację.
4. Live contract test na T4 Simulator.
5. Test sesji, reconnectu, limitów i braków danych.
6. Test roll kontraktu front-month i zapis rzeczywistego contract ID.
7. PostgreSQL 16: migracje, seed, restart i ponowne `READY`.
8. Minimum cztery tygodnie obserwacji read-only przed Gate V1.
