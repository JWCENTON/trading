# RSI Stateful Characterization Harness V1

## Cel i zakres

Harness utrwala aktualne zachowanie state machine RSI przed osobnym etapem integracji z `FinalDecision`. Jest wyłącznie kodem testowym: importuje bieżący `bot/main.py`, wywołuje jego `run_strategy` i odwzorowuje istniejącą w `main_loop` kontrolę `LAST_PROCESSED_OPEN_TIME`. Nie zmienia kodu, konfiguracji ani schematu eventów runtime.

## Scharakteryzowane stany

- brak zamkniętej świecy, nowa świeca i ponowne podanie tej samej świecy;
- brak pozycji: brak rebound signal, poprawny BUY, regime block oraz HALT/disabled;
- otwarta pozycja LONG: hold, stop loss, profit lock, soft exit i time exit;
- czterocyklowa sekwencja: brak akcji, wejście, powtórzona świeca, hold otwartej pozycji.

RSI nie ma osobnej ścieżki thesis/signal invalidation dla otwartej pozycji. Obecne wyjścia są uporządkowane w runtime jako TP/SL, BE protect, profit lock, soft exit, time exit, a następnie hold. Harness tego porządku nie zmienia.

Finding do osobnego review: domyślne `BE_TRIGGER_PCT=0.15` i `BE_OFFSET_PCT=0.03` sprawiają, że stateless LONG `BE_PROTECT` wymaga jednocześnie bieżącego ruchu co najmniej `0.15%` i ceny najwyżej `0.03%` nad entry (analogicznie dla SHORT). Przy dodatnim entry warunki nie mogą zajść równocześnie. Nie jest to naprawiane w patchu characterization.

## Test doubles i observation result

Kontrolowane świece, stały zegar, pamięciowa pozycja oraz strict fake exchange zastępują dane i side effects. Eventy strategii, regime, heartbeat i profit lock są przechwytywane. Próby wejścia/wyjścia aktualizują wyłącznie pamięciową pozycję harnessu. Każdy cykl zwraca ustrukturyzowane: action, reason, signal, stan świecy, pozycję przed/po, próby orderów, eventy oraz mutacje stanu.

`NO_ACTION` jest wynikiem obserwacyjnym testu i nie jest zapisywane. Dostęp do realnej bazy kończy test błędem; exchange nie udostępnia żadnej działającej metody; start background thread jest zabroniony przez fixture.

## Granice

Harness nie uruchamia pełnej pętli ingest/candles/indicators ani rzeczywistego execution/ledger. Nie sprawdza integracji z usługami, DDL, migracji, Compose ani VPS. Nie modeluje ścieżek LIVE fill — charakteryzuje decyzje RSI w bezpiecznym PAPER-like środowisku i rejestruje zamiar execution.

Nie ma DB persistence ani runtime DDL. `decision_sink` pozostaje OFF i nie występuje w API RSI. Istniejący typ `FinalDecision` nie jest importowany ani podłączony; jego integracja i canonical decision write path są kolejnym, osobnym etapem.
