# Roadmap budowy Crypto Agent: V1–V4

## 1. Cel programu

Crypto Agent powstaje jako system wspomagania decyzji, a nie „bot przewidujący ceny”. Każda wersja musi ograniczać zakres odpowiedzialności, mierzyć jakość na danych niedostępnych podczas tworzenia modelu i domyślnie zwracać `NO_SIGNAL`. Przejście do kolejnej wersji wymaga spełnienia wszystkich kryteriów wyjścia z wersji poprzedniej. Sam dodatni wynik finansowy nie jest wystarczającym kryterium.

Docelowa kolejność rozwoju:

1. **V1 — read-only research i alerty:** zbieranie danych, kontrola jakości, analizy oraz raporty bez możliwości składania zleceń.
2. **V2 — point-in-time backtest i replay wydarzeń:** weryfikacja, czy system działałby na informacjach faktycznie dostępnych w chwili decyzji.
3. **V3 — shadow mode i paper trading:** praca na żywym rynku z realistyczną symulacją wykonania i kosztów.
4. **V4 — canary:** mały kapitał, wyłącznie spot bez dźwigni, a każda transakcja wymaga zgody człowieka.

## 2. Zasady wspólne dla wszystkich wersji

### 2.1. Zakres początkowy

- Aktywa: BTC, ETH oraz najwyżej 10–20 innych płynnych aktywów zaakceptowanych przez politykę ryzyka.
- Horyzonty: 4 godziny, 1 dzień i 1 tydzień.
- Każde aktywo identyfikowane przez kanoniczny identyfikator; dla tokenów musi zawierać sieć i adres kontraktu.
- Każdy wynik zawiera `as_of`, czas ważności, jakość danych, źródła, scenariusze, kontrargumenty i warunki unieważnienia.
- Prawdopodobieństwa wolno publikować wyłącznie po kalibracji out-of-sample. W innym przypadku agent używa opisowych poziomów niepewności.

### 2.2. Niezmienne zakazy

- Brak autonomicznego handlu we wszystkich wersjach objętych tym roadmapem.
- Brak futures, perpetuals, opcji, margin, pożyczek i jakiejkolwiek dźwigni w V4.
- Brak martingale, uśredniania straty bez nowej zatwierdzonej tezy oraz automatycznego zwiększania ryzyka po stracie.
- LLM nie oblicza PnL, wielkości pozycji, limitów ryzyka, prowizji ani metryk portfela; robi to deterministyczny kod.
- Wiadomość, post w social media, dokument z internetu ani wynik LLM nigdy bezpośrednio nie wywołuje zlecenia.
- Dane niskiej jakości, brakujące, stare lub sprzeczne oznaczają `NO_SIGNAL`, nigdy zgadywanie.
- Brak testowania na uniwersum zawierającym wyłącznie aktywa, które przetrwały do dziś.
- Brak użycia dokumentu w backteście, jeżeli jego `published_at` jest późniejsze niż symulowany moment decyzji.
- Brak kluczy giełdowych w research plane i risk plane.
- Brak uprawnień do wypłat na koncie wykonawczym.
- Brak obchodzenia veto risk plane przez operatora, orchestrator, model lub execution plane.

### 2.3. Wspólne mierniki

| Obszar | Mierniki obowiązkowe |
|---|---|
| Jakość prognoz | Brier score, log loss, kalibracja w przedziałach, coverage przedziałów, wynik per reżim i horyzont |
| Ryzyko | maksymalne obsunięcie, volatility, VaR/CVaR, koncentracja, ekspozycja per aktywo/giełda, liczba naruszeń polityki |
| Decyzje | udział `NO_SIGNAL`, precision/recall alertów, stabilność sygnału, czas do wygaśnięcia, liczba odrzuceń przez risk plane |
| Wykonanie | spread, poślizg, prowizje, latency, fill ratio, partial fills, odchylenie ceny symulowanej od osiągalnej |
| Dane | świeżość, kompletność, zgodność między źródłami, liczba luk, rewizji i incydentów, provenance coverage |
| Niezawodność | dostępność, czas od danych do wyniku, odsetek błędnych jobów, recovery time, poprawność audytowego odtworzenia decyzji |

Wymagany wynik bezpieczeństwa dla każdej bramki wersji: **zero naruszeń twardej polityki ryzyka**.

---

## 3. Faza przygotowawcza — fundament wspólny

### Zakres i kolejność prac

1. Spisać politykę ryzyka jako wersjonowany dokument i jako deterministyczne reguły wykonywalne.
2. Ustalić kanoniczne identyfikatory aktywów, giełd, instrumentów, sieci i kontraktów.
3. Zdefiniować kontrakty danych: tick/quote, OHLCV, order book, funding, open interest, on-chain, makro, news, tokenomics i zdarzenia.
4. Ustalić semantykę czasu: `event_time`, `published_at`, `received_at`, `available_at` i `ingested_at`.
5. Utworzyć rejestr źródeł z oceną ich wiarygodności, opóźnienia, warunków licencyjnych i sposobu awaryjnego przełączenia.
6. Zdefiniować schemat wyniku agenta i kody powodów `NO_SIGNAL`.
7. Zbudować repozytorium testów, tracing, audit log oraz proces zarządzania sekretami.
8. Zdefiniować model zagrożeń: prompt injection, manipulacja danymi, kompromitacja dostawcy, błędny symbol, awaria giełdy, stale data i wyciek klucza.

### Deliverables

- Polityka ryzyka v1 oraz jej testy jednostkowe.
- Kontrakty danych i słownik identyfikatorów.
- Rejestr źródeł i macierz zaufania.
- Schemat decyzji z walidacją.
- Model zagrożeń i plan reakcji na incydenty.
- Podstawowy monitoring, dziennik audytowy i procedura reprodukcji wyniku.

### Kryteria wyjścia

- Każdy rekord da się przypisać do źródła i czasu, w którym był dostępny systemowi.
- Walidator odrzuca nieznane aktywa, niezgodne jednostki, stare rekordy oraz duplikaty.
- Risk policy ma testy pozytywne, negatywne i testy wartości granicznych.
- Każdy wynik można odtworzyć z wersji kodu, konfiguracji, danych i promptów.
- Sekrety nie pojawiają się w logach ani kontekście modelu.

---

## 4. V1 — agent analityczny read-only

### Cel

V1 ma dostarczać wiarygodne analizy, alerty i `NO_SIGNAL`, nie posiadając technicznej możliwości składania zleceń. Ta wersja służy do sprawdzenia jakości danych, dyscypliny źródłowej, sposobu rozumowania oraz zachowania przy niepewności.

### Kryteria wejścia

- Ukończony fundament wspólny.
- Zatwierdzona lista aktywów, źródeł i horyzontów.
- Gotowa polityka jakości danych oraz lista sytuacji wymuszających `NO_SIGNAL`.

### Zakres funkcjonalny

1. **Ingestion read-only**
   - dane cenowe i wolumenowe z co najmniej dwóch niezależnych źródeł;
   - spread, głębokość i price impact dla wskazanych wielkości zlecenia;
   - funding, basis, open interest i likwidacje wyłącznie jako dane analityczne;
   - wybrane metryki on-chain z jawną metodologią;
   - kalendarz makro, komunikaty emitentów, governance, unlocki i informacje regulacyjne;
   - normalizacja czasu, symboli, jednostek i jakości.
2. **Kontrola jakości**
   - freshness, completeness, outliery, cross-source divergence i wykrywanie awarii;
   - quarantine danych podejrzanych;
   - ocena `data_quality` oraz kody przyczyn degradacji.
3. **Analiza**
   - klasyfikacja reżimu rynku z prawdopodobieństwami, jeśli są skalibrowane;
   - market structure, płynność, on-chain, makro, tokenomics i ryzyko protokołu;
   - scenariusze bull/base/bear wraz z warunkami unieważnienia;
   - obowiązkowy moduł „Sceptyk”, który szuka dowodów przeciw tezie;
   - deduplikacja wiadomości i weryfikacja twierdzeń w źródłach pierwotnych.
4. **Wynik**
   - `NO_SIGNAL`, `ALERT` albo raport badawczy;
   - nigdy `ORDER`, `BUY` ani `SELL`;
   - wynik ma określony czas ważności i staje się nieważny po zmianie kluczowych danych.
5. **Observability**
   - pełny trace wywołań narzędzi, wersji modeli, promptów i użytych danych;
   - dashboard jakości danych, alertów oraz awarii;
   - ręczna ocena próby raportów według wspólnego rubricu.

### Kolejność realizacji

1. Ingestion rynku spot i kanoniczne identyfikatory.
2. Walidacja, zgodność między źródłami i freshness gates.
3. Deterministyczny feature pipeline.
4. Specjaliści analityczni oraz orchestrator.
5. Sceptyk i niezależny risk review.
6. Walidowany format wyniku, cytowania i expiry.
7. Alerting, dashboard, tracing i zestaw evals.
8. Minimum 4–8 tygodni działania read-only na żywych danych.

### Deliverables

- Działający pipeline danych i feature store z provenance.
- API/CLI generujące raporty i alerty w zatwierdzonym schemacie.
- Raporty dzienne i zdarzeniowe dla całego uniwersum.
- Dashboard jakości danych oraz rejestr incydentów.
- Zestaw evals: poprawność źródeł, odporność na prompt injection, `NO_SIGNAL`, sprzeczne dane i halucynacje.
- Raport z okresu obserwacji V1.

### Kryteria wyjścia do V2

- 100% wyników przechodzi walidację schematu i ma `as_of`, provenance oraz `expires_at`.
- Brak nieudokumentowanych twierdzeń o wysokim wpływie w zaakceptowanej próbie raportów.
- Wszystkie symulowane przypadki stale/missing/conflicting data kończą się właściwym `NO_SIGNAL`.
- Zero technicznych ścieżek z V1 do API składania zleceń.
- Ustalony baseline jakości prognoz i alertów per aktywo, reżim i horyzont.
- Minimum 4 tygodnie stabilnej pracy bez incydentu krytycznego; zalecane 8 tygodni przed zamrożeniem V1.

### Zakazy specyficzne dla V1

- Brak kluczy API z prawem handlu.
- Brak rekomendacji wielkości pozycji i brak symulowanego PnL prezentowanego jako osiągalny wynik.
- Brak automatycznej publikacji alertu, jeśli krytyczne źródło jest niedostępne.

---

## 5. V2 — point-in-time backtest i replay wydarzeń

### Cel

V2 sprawdza, jak agent i polityka ryzyka zachowałyby się, posiadając wyłącznie informacje dostępne w symulowanym momencie. Ma wykryć look-ahead bias, survivorship bias, leakage, błędne założenia wykonania i nadmierne dopasowanie.

### Kryteria wejścia

- Spełnione wszystkie kryteria wyjścia V1.
- Zamrożona wersja schematu decyzji i baseline V1.
- Dostęp do wersjonowanych danych historycznych, w tym delistingów i martwych aktywów.

### Zakres funkcjonalny

1. **Point-in-time data lake**
   - snapshoty uniwersum, symbol mappings, listowania, delistowania i zmiany kontraktów;
   - dane surowe oraz ich rewizje, bez nadpisywania historii;
   - każdy dokument z `published_at`, `available_at` i źródłem;
   - rekonstrukcja prowizji, tick size, min notional, spreadów, płynności i przerw giełdowych.
2. **Silnik backtestu**
   - event-driven, bez dostępu do rekordów z przyszłości;
   - walk-forward oraz purged/embargoed cross-validation tam, gdzie zachodzą nakładające się etykiety;
   - koszty transakcyjne, poślizg, latency i ograniczona pojemność;
   - oddzielne zbiory development, validation i final holdout;
   - testy per reżim, aktywo, horyzont i źródło sygnału.
3. **Replay zdarzeń kryzysowych**
   - Mt. Gox i ryzyko custody/kontrahenta;
   - The DAO i ryzyko smart contract/governance;
   - bańka ICO i rozwodnienie tokenów;
   - marzec 2020 i szok płynności;
   - Terra/Luna, 3AC, Celsius i contagion;
   - FTX i nagła utrata płynności/withdrawals;
   - depeg USDC po kryzysie bankowym;
   - wybrane exploity bridge’ów, protokołów i giełd;
   - nagłe decyzje regulacyjne i błędne wiadomości rynkowe.
4. **Testy kontrfaktyczne i odpornościowe**
   - opóźnienie danych, brak źródła, błędny symbol, skok spreadu i ograniczenie wypłat;
   - zamiana kolejności wiadomości o tym samym zdarzeniu;
   - perturbacje progów, kosztów i parametrów;
   - test „czy jedna transakcja lub jeden okres tworzy cały wynik”.
5. **Ochrona przed pamięcią LLM**
   - LLM nie otrzymuje nazw znanych zdarzeń ani dokumentów opublikowanych później;
   - ocena faktów opiera się na zamkniętym korpusie point-in-time;
   - wyniki ilościowe liczy kod, nie model;
   - kluczowe wnioski potwierdza forward test, nie tylko replay.

### Kolejność realizacji

1. Zbudować wersjonowane dane point-in-time i testy szczelności czasu.
2. Dodać historyczne uniwersum wraz z delistingami.
3. Zaimplementować silnik event-driven i realistyczny model kosztów.
4. Utworzyć baseline’y: buy-and-hold, cash, proste reguły i model bez LLM.
5. Wykonać walk-forward i zamknąć final holdout.
6. Zbudować pakiety replay wydarzeń oraz oczekiwane zachowania risk plane.
7. Przeprowadzić red-team danych i leakage.
8. Zamrozić konfigurację i sporządzić niezależny raport wyników.

### Deliverables

- Wersjonowany point-in-time dataset oraz manifest każdego eksperymentu.
- Silnik backtestu z deterministycznym odtworzeniem wyników.
- Pakiet historycznych replayów i raport reakcji systemu.
- Raport kosztów, pojemności, stabilności parametrów i wyników per reżim.
- Raport bias/leakage oraz lista znanych ograniczeń.
- Rejestr wszystkich eksperymentów, również nieudanych.

### Kryteria wyjścia do V3

- Automatyczne testy dowodzą, że żaden rekord o `available_at > decision_time` nie trafia do decyzji.
- Backtest uwzględnia delistowane i martwe aktywa, historyczne prowizje oraz realistyczne wykonanie.
- Wyniki utrzymują się po podwyższeniu kosztów i opóźnień oraz po niewielkich zmianach parametrów.
- Agent przechodzi replaye bezpieczeństwa: przy krytycznej niepewności redukuje zaufanie lub zwraca `NO_SIGNAL`; risk plane nigdy nie łamie limitu.
- Final holdout został użyty tylko raz po zamrożeniu konfiguracji, a wynik i wszystkie decyzje są reprodukowalne.
- Przewaga, jeśli występuje, nie zależy od pojedynczego aktywa, zdarzenia ani krótkiego okresu.
- Zero naruszeń polityki ryzyka i udokumentowane wszystkie wyjątki operacyjne.

### Zakazy specyficzne dla V2

- Brak optymalizacji na final holdout po poznaniu wyniku.
- Brak uzupełniania brakujących danych informacją znaną dopiero później.
- Brak usuwania niekorzystnych okresów jako „anomalii” bez uprzednio zdefiniowanej reguły.
- Brak porównania wyłącznie do zerowego benchmarku; wymagane są realistyczne baseline’y.

---

## 6. V3 — shadow mode i paper trading

### Cel

V3 mierzy zachowanie całego systemu na nieznanych, napływających danych. Agent generuje propozycje i symuluje wykonanie, lecz nie może wysłać prawdziwego zlecenia. To główny test generalizacji, jakości operacyjnej i realnych kosztów.

### Kryteria wejścia

- Spełnione wszystkie kryteria wyjścia V2.
- Zamrożona wersja modelu, strategii i risk policy na start okresu forward.
- Gotowy sandbox/paper broker całkowicie oddzielony od kont z prawdziwym kapitałem.

### Zakres funkcjonalny

1. **Shadow decisions**
   - agent obserwuje rynek w czasie rzeczywistym i generuje `NO_SIGNAL`, `ALERT` albo `PAPER_PROPOSAL`;
   - każda propozycja zawiera kierunek, limit ważności, scenariusze i warunki unieważnienia;
   - risk plane wylicza dopuszczalną wielkość, ale wynik pozostaje symulacją.
2. **Realistyczny paper broker**
   - bid/ask, order book i ograniczona dostępna wielkość;
   - maker/taker fees i właściwe poziomy prowizji;
   - latency ingestion–decision–order–ack;
   - partial fills, rejections, min notional, tick/lot size i anulowanie;
   - market impact zależny od płynności oraz konserwatywne założenia przy braku danych;
   - brak wypełnienia, jeśli osiągalność ceny nie jest potwierdzona;
   - transfery i wypłaty nie są symulowane jako natychmiastowe ani darmowe.
3. **Shadow portfolio**
   - aktualizacja cash, pozycji, kosztu, unrealized/realized PnL i ekspozycji przez deterministyczny ledger;
   - limity koncentracji, obrotu, strat dziennych, drawdown i ekspozycji per venue;
   - porównanie do baseline’ów i do wyników V2.
4. **Operacje i incydenty**
   - chaos tests: utrata feedu, opóźnienie, rozjazd źródeł, restart, brak potwierdzenia i duży spread;
   - kill switch, który zatrzymuje nowe propozycje i przechodzi do `NO_SIGNAL`;
   - runbook oraz dyżury dla alertów krytycznych.
5. **Ewaluacja człowieka**
   - człowiek ocenia proposal tak, jakby miał go zatwierdzić;
   - zapisywane są decyzja, powód odrzucenia i czas reakcji;
   - interfejs nie może ukrywać niepewności ani sugerować fałszywej pilności.

### Kolejność realizacji

1. Podłączyć żywe feedy i rejestrować je point-in-time.
2. Uruchomić wyłącznie shadow decisions bez portfela.
3. Dodać paper broker i deterministyczny ledger.
4. Skalibrować model filli na obserwowanych quote/order-book data.
5. Dodać limity, kill switch i chaos tests.
6. Uruchomić ocenę człowieka oraz analizę powodów odrzuceń.
7. Prowadzić forward paper trading przez co najmniej 12 tygodni i przez więcej niż jeden reżim; jeśli rynek nie zmieni reżimu, wydłużyć okres.
8. Zamrozić wyniki i przeprowadzić przegląd gotowości V4.

### Deliverables

- Live shadow service z niezależnym paper brokerem.
- Deterministyczny ledger i uzgodnienie wszystkich pozycji/PnL.
- Dashboard decyzji, kosztów, filli, drawdown i odrzuceń.
- Raport różnicy między backtestem, shadow proposal i paper fill.
- Wyniki chaos tests, test kill switcha i runbook operacyjny.
- Raport forward z niezmienianym wstecz dziennikiem decyzji.

### Kryteria wyjścia do V4

- Minimum 12 tygodni forward paper tradingu; dłużej, jeśli nie objął stresu i co najmniej dwóch różnych warunków rynkowych.
- Wszystkie pozycje, prowizje i PnL są uzgodnione przez ledger bez niewyjaśnionych różnic.
- Paper broker nie stosuje filli po cenach nieosiągalnych w chwili decyzji.
- Wyniki po wszystkich kosztach są zgodne z ustalonym progiem projektu i nie pogarszają limitu drawdown/CVaR.
- Stabilna kalibracja i skuteczność per reżim; brak ukrywania słabych segmentów wynikiem zagregowanym.
- Kill switch działa w testach i przy awarii feedu blokuje nowe propozycje w zdefiniowanym czasie.
- Zero naruszeń twardej polityki ryzyka.
- Ręczny komitet gotowości zatwierdził minimalny budżet canary, limity oraz rollback plan.

### Zakazy specyficzne dla V3

- Brak klucza do prawdziwego handlu w środowisku shadow/paper.
- Brak „optymistycznego” fillowania po cenie close świecy lub mid bez potwierdzonej płynności.
- Brak zmiany modelu w trakcie okresu oceny bez rozpoczęcia nowego, osobno oznaczonego eksperymentu.
- Brak awansu na podstawie krótkiego okresu hossy albo samego Sharpe ratio.

---

## 7. V4 — canary spot z zatwierdzaniem każdej transakcji

### Cel

V4 weryfikuje integrację z prawdziwą giełdą i zachowanie pod realnym ryzykiem przy minimalnej, z góry ograniczonej ekspozycji. Nie jest to tryb autonomiczny. Każda propozycja oraz każde zlecenie wymaga świadomego zatwierdzenia przez człowieka.

### Kryteria wejścia

- Spełnione wszystkie kryteria wyjścia V3.
- Pisemnie zatwierdzony budżet straty, limity i lista dozwolonych aktywów/giełd.
- Oddzielne konto/subkonto canary, klucz bez prawa wypłat oraz sprawdzone allowlisty.
- Ukończony security review, tabletop incident exercise i procedura natychmiastowego wycofania klucza.

### Twardy zakres V4

- Wyłącznie rynek **spot**.
- Wyłącznie dozwolone, płynne aktywa z allowlisty.
- **Zero dźwigni**: bez margin, futures, perpetuals, opcji, lending i borrow.
- Minimalny kapitał canary oraz limity: per transakcja, per dzień, per aktywo, per giełda, łączna ekspozycja, obrót i drawdown.
- Każda transakcja ma dwie bramki: deterministyczne `risk_approved=true` oraz jednorazowe zatwierdzenie człowieka.
- Zatwierdzenie jest związane kryptograficznie/logicznie z dokładnym proposalem, instrumentem, stroną, typem zlecenia, limitem ceny, ilością i czasem wygaśnięcia.
- Jakakolwiek zmiana parametrów lub przekroczenie czasu ważności wymaga nowego zatwierdzenia.
- Domyślny stan execution plane to zablokowane składanie zleceń.

### Zakres funkcjonalny

1. **Proposal**
   - research plane generuje `PAPER_PROPOSAL`/`LIVE_PROPOSAL`, nigdy zlecenie;
   - Sceptyk i risk review są obowiązkowe;
   - UI pokazuje tezę, dowody przeciw, jakość danych, możliwą stratę, wszystkie koszty i warunki unieważnienia.
2. **Risk gate**
   - ponownie pobiera niezależny snapshot rynku;
   - wylicza ilość, ekspozycję, limity i dopuszczalny limit ceny;
   - blokuje stale data, rozjazd ceny, szeroki spread, aktywo poza allowlistą, limit straty, drawdown lub awarię giełdy;
   - veto jest ostateczne.
3. **Human approval**
   - wyraźna akcja approve/reject bez domyślnego zaznaczenia;
   - MFA dla zatwierdzenia;
   - jednorazowy token z krótkim TTL;
   - zapis tożsamości, czasu, parametrów i uzasadnienia odrzucenia.
4. **Execution**
   - osobna usługa z minimalnym zestawem uprawnień;
   - idempotency key, client order ID, pre-trade balance check i final price collar;
   - obsługa partial fill, reject, cancel i niejednoznacznego statusu;
   - połączenie awaryjnie kończy się blokadą nowych zleceń, a nie ponowieniem w ciemno.
5. **Reconciliation i monitoring**
   - uzgodnienie zleceń, filli, sald i opłat z giełdą;
   - alarm przy każdej różnicy;
   - realny vs symulowany slippage i fill ratio;
   - automatyczny kill switch po naruszeniu progu.

### Kolejność realizacji

1. Utworzyć subkonto, allowlisty i klucz bez wypłat; przetestować rotację i unieważnienie.
2. Wdrożyć execution service z handlem domyślnie wyłączonym.
3. Zaimplementować podpisany proposal, TTL, idempotency i approval MFA.
4. Wykonać testy sandbox/testnet, a następnie próby o minimalnym nominale.
5. Uruchomić canary dla jednego venue i BTC/ETH, z najniższymi limitami.
6. Przeprowadzać codzienne reconciliation i cotygodniowy przegląd ryzyka.
7. Zwiększać zakres wyłącznie etapami po formalnej bramce; nigdy automatycznie po zysku.
8. Po każdym incydencie zatrzymać nowe zlecenia do zakończenia postmortem.

### Deliverables

- Izolowany execution service i konto canary bez wypłat.
- Panel zatwierdzania z MFA, TTL i pełnym audytem.
- Policy-as-code dla limitów pre-trade i post-trade.
- Reconciliation service oraz dashboard live-vs-paper.
- Kill switch manualny i automatyczny, przetestowany runbook oraz rollback.
- Raport canary obejmujący koszty, błędy, odrzucenia, fill quality i incydenty.

### Kryteria wyjścia V4

V4 nie prowadzi automatycznie do handlu autonomicznego. Po zakończeniu okresu canary zespół podejmuje osobną decyzję, czy pozostać przy V4, rozszerzyć ostrożnie zakres spot, czy zatrzymać system.

Minimalne kryteria uznania V4 za stabilne:

- Każda prawdziwa transakcja ma zgodę risk plane i przypisaną, niewygasłą zgodę człowieka.
- Zero zleceń zdublowanych, poza allowlistą, ponad limit lub wysłanych po wygaśnięciu.
- 100% zleceń, filli, sald i opłat uzgodnione z giełdą.
- Różnica live vs paper w kosztach i fillach mieści się w wcześniej ustalonych granicach.
- Kill switch, rotacja klucza i tryb read-only działają w ćwiczeniu operacyjnym.
- Brak krytycznych luk bezpieczeństwa i zero naruszeń polityki ryzyka.
- Wynik finansowy jest raportowany, lecz nie zastępuje kryteriów bezpieczeństwa, kalibracji i niezawodności.

### Zakazy specyficzne dla V4

- Brak automatycznego zatwierdzania, batch approval i „approve all”.
- Brak rozszerzania limitów przez model lub na podstawie serii zysków.
- Brak market order bez jawnie zdefiniowanego, konserwatywnego price collar i zgody polityki.
- Brak powtórzenia zlecenia o nieznanym statusie bez reconciliation.
- Brak wypłat, transferów między kontami i dodawania nowych adresów przez execution service.
- Brak wspólnego klucza między development, paper i canary.

---

## 8. Bramka decyzyjna między wersjami

Każda bramka V1→V2, V2→V3 i V3→V4 wymaga tego samego pakietu dowodów:

1. Zamrożona wersja kodu, konfiguracji, promptów, modeli i polityki ryzyka.
2. Raport metryk z rozbiciem per reżim, aktywo i horyzont.
3. Lista incydentów, regresji, znanych ograniczeń i nieudanych eksperymentów.
4. Wyniki testów bezpieczeństwa, chaos tests i odtworzenia audytowego.
5. Porównanie z baseline’ami oraz z poprzednią wersją.
6. Pisemna decyzja: `GO`, `REPEAT` albo `STOP`, wraz z uzasadnieniem.

Jeżeli danych jest za mało, wynik jest niestabilny albo wystąpiło naruszenie twardej polityki, właściwą decyzją jest `REPEAT` lub `STOP`, a nie obniżenie kryteriów po fakcie.

## 9. Pierwszy konkretny sprint

Pierwszy sprint powinien dotyczyć wyłącznie fundamentu i V1:

1. Zamrozić uniwersum BTC/ETH, horyzonty 4h/1d/1w oraz schemat wyniku.
2. Zaimplementować kanoniczne identyfikatory i dwa niezależne feedy spot.
3. Dodać freshness, completeness i cross-source divergence gates.
4. Zapisać każdą obserwację z `event_time`, `received_at`, `available_at` i provenance.
5. Zaimplementować `NO_SIGNAL` z kodami powodów.
6. Utworzyć pierwszy raport read-only bez jakiegokolwiek modułu execution.
7. Dodać testy: brak feedu, stale data, rozjazd cen, zły symbol, prompt injection i błędny adres kontraktu.
8. Uruchomić monitoring i rozpocząć niezmienny dziennik obserwacji V1.
