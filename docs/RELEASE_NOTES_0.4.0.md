# 0.4.0 — cykl życia kontraktów futures offline

- dodano walidowany lokalny katalog kolejnych serii BTC i ETH futures;
- front-month jest wybierany deterministycznie względem czasu UTC;
- w chwili `roll_at` wymagany jest następny niewygasły kontrakt;
- brak następnej serii, duplikaty i błędne identyfikatory kończą się fail-closed;
- schema bridge v2 przenosi datę rollu, rodzaj wyboru i poprzedni contract ID;
- adapter Python odrzuca niespójną lub sfałszowaną proweniencję rollu;
- zachowano pełny zakaz tras tworzenia, zmiany i anulowania zleceń.

Ta wersja nie łączy się jeszcze z oficjalnym T4 API. Katalog przykładowy nie
zawiera aktywnych danych T4; rzeczywiste identyfikatory zostaną pobrane dopiero po
uzyskaniu dostępu do T4 Simulator i oficjalnego klienta.
