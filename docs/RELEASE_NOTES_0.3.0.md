# 0.3.0 — zabezpieczona granica T4

- dodano host .NET 8 dla lokalnego bridge T4;
- wymuszono nasłuch wyłącznie na loopback i token 32-256 znaków;
- dodano walidację interwałów, limitu, UTC, contract ID i daty wygaśnięcia;
- bridge nie zawiera tras zleceń i bez oficjalnego klienta zwraca `503`;
- adapter Python wymaga tokenu i odrzuca niespójną atestację payloadu;
- GitHub Actions buduje bridge i uruchamia testy kontraktowe .NET;
- podłączenie oficjalnego klienta pozostaje zablokowane do czasu rejestracji
  aplikacji i otrzymania aktualnego pakietu T4 od CTS.
