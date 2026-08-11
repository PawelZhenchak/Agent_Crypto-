# Polityka bezpieczeństwa agenta Crypto

**Status:** norma obowiązkowa  
**Zakres:** V1–V4  
**Zasada nadrzędna:** `deny by default` — brak jednoznacznego spełnienia warunku oznacza odmowę działania, a nie zgodę warunkową.

Checkpoint `0.1.4-v1.1` pozostaje read-only i ma `metadata.v1_gate_passed=false`.
Zielona suite lokalna nie jest sama w sobie formalnym dowodem przejścia Gate V1.

## 1. Cel i granice systemu

Agent wspiera analizę rynku, ale nie gwarantuje zysku ani poprawności prognozy. Bezpieczeństwo opiera się na rozdzieleniu trzech płaszczyzn:

1. **Research plane** zbiera dane, przygotowuje tezy, kontrargumenty i scenariusze.
2. **Risk plane** wykonuje deterministyczne kontrole i ma bezwarunkowe prawo veta.
3. **Execution plane** istnieje dopiero w V4, jest osobną usługą i nie udostępnia danych uwierzytelniających modelowi językowemu.

Model językowy nie oblicza samodzielnie PnL, wielkości pozycji, limitów, ekspozycji, poślizgu, CVaR ani statusu przejścia bramki. Wyniki te pochodzą z testowanego kodu deterministycznego.

## 2. Dozwolone działania w kolejnych wersjach

| Wersja | Dozwolone | Bezwzględnie zabronione |
|---|---|---|
| V1 | Odczyt danych, analiza, raporty, alerty i `NO_SIGNAL` | Klucze transakcyjne, podpisywanie wiadomości, zlecenia, anulowanie zleceń, transfery i wypłaty |
| V2 | Wszystko z V1 oraz point-in-time backtest i historyczne replaye | Każde działanie na rachunku lub portfelu; używanie informacji opublikowanych po symulowanym czasie |
| V3 | Wszystko z V2, shadow mode i paper trading w wewnętrznym ledgerze | Wysyłanie zleceń do giełdy, nawet testowych przez endpoint produkcyjny; transfery, wypłaty i podpisy blockchainowe |
| V4 | Mały canary na rynku spot, wyłącznie na allowliście i po zatwierdzeniu każdego zlecenia przez człowieka | Handel autonomiczny, margin, futures, perpetuals, opcje, pożyczki, staking wymagający blokady środków, bridge, transfery i wypłaty |

V1–V3 są **read-only względem wszystkich zewnętrznych rachunków, giełd i portfeli**. Paper trading w V3 może zapisywać wyłącznie symulowany stan we własnej bazie danych.

## 3. Niezmienne zakazy

Poniższe reguły obowiązują we wszystkich wersjach i nie mogą być pominięte przez prompt, administratora sesji ani pojedynczą zgodę użytkownika:

- brak dźwigni finansowej; wymagana ekspozycja brutto nie może przekroczyć posiadanej, rozliczonej gotówki przeznaczonej do V4;
- brak margin, pożyczania aktywów, krótkiej sprzedaży i instrumentów pochodnych;
- brak martingale, reverse martingale oraz algorytmu zwiększającego rozmiar pozycji w celu „odrobienia” straty;
- brak automatycznego uśredniania w dół; nowe zlecenie na aktywo z niezrealizowaną stratą wymaga nowej, niezależnej tezy i oddzielnej zgody, a w pierwszym canary V4 jest całkowicie zabronione;
- brak wysyłania, wypłacania lub mostkowania środków;
- brak udostępniania LLM kluczy prywatnych, seed phrase, sekretów API, kodów 2FA lub uprawnień do podpisu;
- brak zlecenia wywołanego bezpośrednio przez wiadomość, post społecznościowy, dokument, treść strony albo odpowiedź modelu;
- brak handlu aktywem bez jednoznacznego identyfikatora `chain_id + contract_address` albo, dla natywnego aktywa, zatwierdzonego identyfikatora sieci;
- brak obchodzenia odrzuconej bramki przez zmianę promptu, dostawcy modelu, ręczne wywołanie narzędzia lub ponowienie żądania.

Test statyczny i runtime muszą odrzucić strategię, jeżeli jej funkcja rozmiaru pozycji rośnie wraz z liczbą kolejnych strat, wielkością poprzedniej straty albo bieżącym drawdownem. Takie odrzucenie ma kod `MARTINGALE_DETECTED`.

## 4. Kontrakt decyzyjny risk plane

Każda analiza i każde proponowane zlecenie przechodzi przez risk plane. Wynik ma jeden z następujących statusów:

- `PASS` — wszystkie obowiązkowe kontrole są zaliczone;
- `NO_SIGNAL` — nie ma wystarczających, świeżych i spójnych podstaw do wniosku;
- `REJECT` — propozycja narusza politykę albo limit;
- `HALT` — system jest zatrzymany do wyjaśnienia incydentu.

Risk plane zwraca co najmniej:

```json
{
  "decision_id": "uuid",
  "as_of": "RFC3339 UTC",
  "policy_version": "semver+hash",
  "stage": "V1|V2|V3|V4",
  "result": "PASS|NO_SIGNAL|REJECT|HALT",
  "reason_codes": [],
  "input_snapshot_hash": "sha256",
  "expires_at": "RFC3339 UTC"
}
```

`PASS` nie jest poleceniem kupna. W V4 oznacza jedynie, że propozycję można przedstawić człowiekowi do zatwierdzenia.

### 4.1. Dowód konsensusu V1.1

`ALERT` w checkpointcie V1.1 wymaga dowodu wygenerowanego przez dokładny, zatwierdzony
provider `CrossExchangeConsensusProvider` dla niezależnej pary Kraken + Coinbase.
Podklasa, synthetic, dwa aliasy tego samego venue ani ręcznie przygotowany bool nie są
dowodem. Provider jest zapieczętowany na source keys `kraken_spot_rest_v1` +
`coinbase_exchange_spot_rest_v1` i dwa różne venue; próba zmiany kompozycji po
utworzeniu unieważnia dowód przed fetchem. Adaptery akceptują wyłącznie
przypięte hosty HTTPS i odrzucają redirect.

Attestation wiąże dokładne source IDs i venue, uporządkowane wejścia, okno, raw input
sources, algorithm version `cross_exchange_spot_consensus_v1`, evidence digest oraz
pełny fingerprint dokładnie tej samej `RiskPolicy`, której używa RiskGate.

Każde venue musi posiadać dokładnie 120 ciągłych, czasowo wyrównanych obserwacji,
zakończonych najnowszym kwalifikującym się zamkniętym oknem. Przekroczenie progu close
lub OHLC choćby w jednym oknie, luka, różny koniec, brak feedu albo niespójność wolumenu
daje `NO_SIGNAL`. `VOLUME_ANOMALY` wolno zgłosić tylko wtedy, gdy z-score obu źródeł
niezależnie przekracza próg w tym samym kierunku. Sam cross-source normalized volume
nie stanowi dowodu.

Trwała ścieżka ma identyczne wymagania. Repozytorium wyprowadza z bazy latest eligible
raw revisions, a caller może wyłącznie potwierdzić równość oczekiwanych IDs. Zapis
wymaga dokładnego kontekstu `Kraken 120 + Coinbase 120`, pełnego fingerprintu polityki
i tego samego algorytmu co runtime. Normalized volume jest bezwymiarowy: trafia wraz z
diagnostyką do manifestu, podczas gdy canonical `candles.base_volume` pozostaje `NULL`.

## 5. Kolejność bramek ryzyka

Bramki są wykonywane w podanej kolejności. Pierwsze krytyczne niepowodzenie kończy ocenę; wszystkie wykryte przyczyny są jednak logowane.

1. zgodność wersji i trybu działania;
2. integralność schematu, pochodzenie i kompletność danych;
3. świeżość danych;
4. zgodność niezależnych źródeł;
5. ochrona przed prompt injection i niezaufaną treścią;
6. poprawna identyfikacja aktywa i kontraktu;
7. ryzyko stablecoina, kontrahenta i protokołu;
8. płynność, spread, głębokość i oczekiwany poślizg;
9. zakaz dźwigni i martingale;
10. limity ekspozycji i straty;
11. ważna zgoda człowieka — tylko V4;
12. końcowa kontrola kill switcha i uzgodnienie stanu rachunku — tylko V4.

Brak wymaganej bramki w konfiguracji jest błędem krytycznym `POLICY_INCOMPLETE` i powoduje `HALT`.

## 6. Kompletność, staleness i konflikt danych

### 6.1 Wymagane metadane

Każdy rekord krytyczny dla decyzji musi zawierać:

- identyfikator źródła i instrumentu;
- `event_time`, `published_at` (jeżeli dotyczy) oraz `received_at` w UTC;
- numer sekwencji albo identyfikator wersji, jeżeli dostawca go udostępnia;
- jednostkę, walutę kwotowaną i schemat;
- hash surowego rekordu lub snapshotu.

Brak pola krytycznego, luka w wymaganym oknie, błąd schematu, nieznana jednostka lub nieudana walidacja powoduje `NO_SIGNAL`. Imputacja nie może uzupełniać ceny, order booka, salda, pozycji, podaży, unlocku ani flag ryzyka.

### 6.2 Maksymalny wiek danych

Wiek liczy się jako `decision_time - event_time`. Domyślne maksima są następujące; konfiguracja aktywa może być tylko bardziej rygorystyczna:

| Typ danych | V1–V3 | V4 |
|---|---:|---:|
| Cena referencyjna | 5 min | 15 s |
| Order book / kwota wykonawcza | 1 min | 5 s |
| Saldo, otwarte zlecenia, status rachunku | nie dotyczy | 5 s |
| Funding, open interest, basis | 15 min | używane wyłącznie jako kontekst, maks. 5 min |
| Dane on-chain indeksowane | 30 min i nie więcej niż 2 bloki opóźnienia | 15 min i nie więcej niż 2 bloki opóźnienia |
| Wiadomość lub dokument zdarzeniowy | musi mieć znane `published_at` i mieścić się w horyzoncie analizy | nie może samodzielnie uruchomić zlecenia |

Przekroczenie limitu daje `DATA_STALE`. W V1–V3 wynik to `NO_SIGNAL`; w V4 propozycja otrzymuje `REJECT`. Jeżeli staleness dotyczy salda, stanu rachunku, ceny wykonawczej albo więcej niż jednego krytycznego feedu, wynik to `HALT`.

### 6.3 Niezależne źródła i konflikty

- Cena referencyjna wymaga co najmniej dwóch niezależnych źródeł, a w V4 trzech, z czego jedno jest niezależne od venue wykonawczego.
- Dla BTC i ETH maksymalna różnica od mediany wynosi 50 pb; dla pozostałych aktywów 100 pb; dla stablecoinów 25 pb.
- Różnica ceny venue wykonawczego od mediany referencyjnej w V4 nie może przekroczyć 50 pb.
- Sprzeczne informacje o adresie kontraktu, decimals, podaży, statusie wypłat, depegu, exploicie, upgrade proxy lub rezerwach są zawsze konfliktem krytycznym niezależnie od ceny.

Checkpoint V1.1 realizuje ten kontrakt dla BTC/ETH przez dokładnie dwie niezależne,
wyrównane i zamknięte świece 1m Kraken/Coinbase. Granica 300 s jest inkluzywna;
odchylenie o dowolną wartość ponad 50 pb od mediany kończy się `DATA_CONFLICT`.
Przy dokładnie dwóch cenach 50 pb każdego źródła od midpoint mediany odpowiada
100 pb pełnej różnicy pairwise.

Przekroczenie progu daje `DATA_CONFLICT` i `NO_SIGNAL`/`REJECT`. Konflikt dotyczący salda, kontraktu, stablecoina rozliczeniowego albo bezpieczeństwa venue daje `HALT`.

## 7. Prompt injection i niezaufana treść

Każdy tekst spoza zaufanej konfiguracji — strona WWW, social media, e-mail, whitepaper, dokument, pole token metadata i wynik wyszukiwania — jest **danymi**, nigdy instrukcją.

- Treści zewnętrzne nie mogą zmieniać polityki, narzędzi, odbiorcy, limitów ani kolejności bramek.
- Polecenia znalezione w danych są ignorowane i oznaczane `PROMPT_INJECTION_SUSPECTED`.
- Model nie ma bezpośredniego dostępu do execution service ani sekretów.
- Wywołania narzędzi są tworzone z typowanego schematu i sprawdzane allowlistą operacji, hostów, parametrów oraz identyfikatorów aktywów.
- Dane wyjściowe LLM są parsowane do zamkniętego schematu; dodatkowe pola i swobodny tekst w polach sterujących powodują odrzucenie.
- Wiadomość może być dowodem pomocniczym dopiero po potwierdzeniu przez źródło pierwotne albo drugie niezależne źródło.

Jeżeli podejrzana treść jest jedynym dowodem potrzebnym do tezy, wynik musi być `NO_SIGNAL`. Jeżeli doszło do próby wywołania niedozwolonego narzędzia, system przechodzi w `HALT`.

## 8. Płynność i jakość wykonania

W pierwszym canary V4 dopuszczone jest wyłącznie zlecenie limitowane `IOC` albo limitowane z krótkim czasem ważności. Market order jest zabroniony.

Wszystkie poniższe warunki muszą być spełnione jednocześnie:

- instrument i venue znajdują się na podpisanej allowliście;
- medianowy spread z ostatnich 15 minut nie przekracza 30 pb;
- oczekiwany poślizg dla całego zlecenia nie przekracza 20 pb;
- rozmiar zlecenia nie przekracza 2% zweryfikowanej głębokości w paśmie 50 pb po właściwej stronie order booka;
- zweryfikowany średni dzienny wolumen z 30 dni wynosi co najmniej 10 mln USD i pochodzi z danych oczyszczonych z oczywistych anomalii;
- nie występuje halt rynku, opóźnienie feedu, gwałtowna utrata głębokości ani status degraded venue.

Niespełnienie któregokolwiek warunku daje `LOW_LIQUIDITY` albo `EXECUTION_QUALITY_FAIL` i `REJECT`. Parametry per aktywo mogą być bardziej rygorystyczne, ale nigdy łagodniejsze od powyższych limitów globalnych.

## 9. Stablecoiny, kontrahenci i kontrakty

### 9.1 Stablecoiny

Stablecoin nie jest traktowany jak gotówka wolna od ryzyka. Dla każdego stablecoina ocenia się emitenta, mechanizm stabilizacji, rezerwy, banki/repozytoria, możliwość wykupu, sieć, kontrakt i płynność.

- Odchylenie mediany ceny od 1 USD/EUR większe niż 50 pb powoduje `STABLECOIN_DEPEG` i blokuje nowe ekspozycje zależne od tego stablecoina.
- Odchylenie większe niż 100 pb, wstrzymanie wykupu lub potwierdzona niedostępność istotnych rezerw uruchamia `HALT`.
- Brak aktualnego, zweryfikowanego raportu o rezerwach według częstotliwości zapisanej na allowliście blokuje dodanie stablecoina do V4.
- Stablecoin algorytmiczny bez w pełni płynnego, nadmiarowego zabezpieczenia jest zabroniony w V4.

### 9.2 Kontrahenci

Venue V4 musi znajdować się na allowliście z określonym maksymalnym saldem operacyjnym. `HALT` uruchamiają w szczególności:

- wstrzymane lub istotnie opóźnione wypłaty;
- brak uzgodnienia salda albo otwartych zleceń;
- nieoczekiwana zmiana uprawnień API;
- sprzeczne odpowiedzi API, awaria uwierzytelniania lub podejrzenie przejęcia konta;
- potwierdzony incydent bezpieczeństwa, niewypłacalność albo niezdolność venue do prawidłowego rozliczania.

Kapitał operacyjny na jednym venue nie może przekroczyć podpisanego limitu `MAX_VENUE_BALANCE`. Brak limitu oznacza wartość zero i zakaz uruchomienia V4.

### 9.3 Kontrakty i protokoły

V4 dopuszcza tylko aktywa z allowlisty adresów. Każdy wpis zawiera `chain_id`, adres, decimals, hash bytecode, status proxy, administratorów, timelock, funkcje pause/blacklist/mint oraz datę ostatniego przeglądu.

Zmiana bytecode, implementacji proxy, administratora, decimals, polityki transferu albo wykrycie exploita powoduje zamrożenie aktywa i `HALT` dla nowych zleceń. W pierwszym canary V4 zabronione są bridge, wrapped asset wymagający zewnętrznego custodian, tokeny rebasing, fee-on-transfer oraz tokeny z niezweryfikowanym mintem lub blacklistą.

## 10. Limity ekspozycji i strat w V4

Przed uruchomieniem V4 muszą być ustawione, podpisane i dodatnie:

- `CANARY_CAPITAL` — całkowity kapitał przeznaczony do testu;
- `MAX_ORDER_NOTIONAL`;
- `MAX_ASSET_POSITION`;
- `MAX_VENUE_BALANCE`;
- `DAILY_LOSS_LIMIT`;
- `MAX_DRAWDOWN_LIMIT`;
- `MAX_OPEN_ORDERS`.

Brak wartości albo wartość spoza zatwierdzonego zakresu oznacza zero i `HALT`. Risk plane liczy ekspozycję z uwzględnieniem otwartych zleceń i zarezerwowanych środków. Zlecenie, które po pełnym wykonaniu przekroczyłoby choć jeden limit, jest odrzucane przed wysłaniem.

Limity nie mogą być zwiększane podczas aktywnej sesji, po stracie ani w celu przepuszczenia konkretnego zlecenia. Zmiana wymaga nowej wersji polityki i pełnego procesu release.

## 11. Zatwierdzenie przez człowieka

Każde zlecenie V4 wymaga osobnego, świadomego zatwierdzenia. Zgoda jest:

- przypisana do hasha obejmującego `asset_id`, stronę, venue, typ, limit price, maksymalny notional, maksymalną ilość, ważność i `policy_version`;
- jednorazowa i ważna maksymalnie 5 minut;
- nieważna po każdej zmianie parametrów, danych wejściowych, statusu bramki albo wersji polityki;
- rejestrowana z tożsamością zatwierdzającego i czasem UTC;
- udzielana poza rozmową z modelem, w interfejsie pokazującym koszty, ryzyka, tezę, kontrargumenty i warunki unieważnienia.

Brak odpowiedzi nie oznacza zgody. Ogólne polecenia typu „handluj dzisiaj” lub „zatwierdzam wszystko” są nieważne. Człowiek nie może nadpisać `REJECT` ani `HALT`; może jedynie odrzucić propozycję z `PASS`.

## 12. Kill switch

Kill switch jest niezależny od LLM, dostępny ręcznie i uruchamiany automatycznie. Stan `HALT`:

1. blokuje tworzenie i wysyłanie nowych zleceń;
2. próbuje anulować otwarte niewykonane zlecenia za pomocą ograniczonego endpointu;
3. nie wykonuje automatycznej sprzedaży rynkowej ani transferu środków;
4. zapisuje niezmienny raport incydentu i wymaga ręcznego uzgodnienia stanu;
5. wymaga formalnego resetu przez uprawnioną osobę po usunięciu przyczyny i ponownym przejściu testu gotowości.

Automatyczne wyzwalacze obejmują:

- próbę naruszenia któregokolwiek niezmiennego zakazu;
- próbę narzędzia spoza allowlisty lub podejrzenie prompt injection prowadzące do działania;
- przekroczenie `DAILY_LOSS_LIMIT` albo `MAX_DRAWDOWN_LIMIT`;
- rozbieżność salda, pozycji, zleceń lub filli;
- krytyczny konflikt/staleness danych;
- depeg powyżej 100 pb albo wstrzymanie wykupu stablecoina rozliczeniowego;
- incydent bezpieczeństwa venue/kontraktu, zmianę allowlistowanego kontraktu albo anomalię uwierzytelniania;
- brak heartbeat risk plane lub execution plane;
- więcej niż jedno nieoczekiwane odrzucenie/duplikat zlecenia w tej samej sesji.

Kill switch jest ćwiczony w środowisku symulowanym przed każdym releasem oraz cyklicznie w V4.

## 13. Audyt, powtarzalność i zmiany polityki

Każda decyzja zapisuje snapshot danych, wersje modeli i kodu, prompt systemowy, wyniki specjalistów, wynik wszystkich bramek, zgodę człowieka, request/response venue oraz późniejsze fille. Log jest append-only i posiada łańcuch hashy.

Zmiana modelu, promptu systemowego, dostawcy danych, adaptera venue, schematu cech, progów, allowlisty albo kodu risk plane jest zmianą releasową. Wymaga ponownego wykonania właściwych testów z `EVALUATION_PLAN.md`. Nie istnieją wyjątki per transakcja ani „tymczasowe” wyłączenie bramki.
