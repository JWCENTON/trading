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

`NO_ACTION` jest wynikiem obserwacyjnym testu i nie jest zapisywane. Dostęp do realnej bazy kończy test błędem; exchange nie udostępnia żadnej działającej metody; start background thread jest zabroniony przez fixture. `operation_log` zamraża kolejność na granicy strategii (event, wywołanie adaptera execution, mutacja stanu, heartbeat), a nie wewnętrzną kolejność adaptera, exchange ani ledger. Osobny test wyniku execution potwierdza propagację `order_accepted` bez wykonywania OKX lub DB.

## Granice

Harness nie uruchamia pełnej pętli ingest/candles/indicators ani rzeczywistego execution/ledger. Nie sprawdza integracji z usługami, DDL, migracji, Compose ani VPS. Modeluje wyniki LIVE jako kontrolowane flagi attempt/ACK/fill, ale nie wywołuje exchange; charakteryzuje decyzje RSI w bezpiecznym środowisku i rejestruje granicę execution.

Nie ma DB persistence ani runtime DDL. `decision_sink` pozostaje OFF i nie występuje w API RSI. `FinalDecision` jest wyłącznie zwracanym wynikiem obserwacyjnym; canonical decision write path pozostaje osobnym etapem.

## FinalDecision integration

RSI zwraca teraz immutable `FinalDecision` po wykonaniu dotychczasowych eventów, gate'ów i execution calls. Obiekt jest terminalnym opisem cyklu, nie steruje side effects. `main_loop` nadal ignoruje wartość zwracaną. Nie istnieje parametr ani wywołanie `decision_sink`, a decyzje nie są logowane ani zapisywane do DB.

## Kanoniczny ExecutionOutcome

RSI entry, regular exit i soft exit normalizują legacy dict result przez ten sam immutable, strategy-neutral i exchange-neutral `ExecutionOutcome`:

- `attempted`: wywołano funkcję składającą zlecenie, nie tylko ledger lub preflight;
- `order_accepted`: giełda zwróciła ACK i przyjęła co najmniej jedno zlecenie;
- `executed`: potwierdzone `executed_qty > 0`, także dla partial fill;
- `fully_executed`: zagregowane wykonanie osiągnęło całe żądane qty;
- `operation_succeeded`: dotychczasowe `live_ok`, zachowane osobno, aby kontrakt opisowy nie zmieniał istniejących mutacji runtime;
- `ledger_ok`: lokalna operacja ledger zakończyła się poprawnie;
- `suppressed`: order path nie został wywołany z powodu policy/runtime gate;
- `order_submitted`: pole `FinalDecision` równe `order_accepted`, nigdy samo `attempted`;
- `trade_executed`: pole `FinalDecision` równe potwierdzonemu `executed`, także dla partial fill i gdy niezależny ledger failure wymusza techniczny typ decyzji.

Adapter zachowuje pełny legacy result w rekurencyjnie zamrożonym `raw`. Brak `order_accepted` ma konserwatywną wartość `false`. Fill wymaga dodatniego `executed_qty`; `fully_executed=true` wymaga fill. Sprzeczne kombinacje (`accepted` bez `attempted`, `executed` bez ACK/ilości lub suppression po próbie) zgłaszają jawny invariant error i nie mogą zostać sklasyfikowane jako sukces.

Mapa producerów pozostaje addytywna i backward-compatible:

| producer | attempted/ACK/fill | ledger/suppression | identyfikatory/status/error |
|---|---|---|---|
| `common.execution.place_live_order` | jawne `attempted`, `order_accepted`, `executed`, `fully_executed` i ilości | preflight zwraca not-attempted | top-level order/client ID, status oraz exception reason |
| `common.execution.place_live_exit_maker_then_market` | agreguje ACK i `executed_qty` maker+fallback; wcześniejszy ACK/fill nie znika po późniejszym failure | bez ledger | oba etapy pozostają w `resp`, a suma/remaining w top-level result |
| `bot.execute_and_record` | propaguje regularny ACK/fill | dodaje `ledger_ok` i policy suppression | client/order response pozostają w result |
| `bot.execute_and_record_soft_exit_maker_then_market` | propaguje zagregowany maker/fallback ACK/fill | dodaje `ledger_ok` i suppression | pełny wynik maker/fallback pozostaje w `resp` |
| PAPER | brak realnego attempt/ACK/fill; `paper_executed` opisuje symulację | zachowuje ledger | brak realnego order id/status |

| RSI path | type / subtype | action | reason code | execution | pozycja przed → po | event runtime |
|---|---|---|---|---|---|---|
| `NO_NEW_CANDLE` | `SYSTEM_NOT_EVALUATED / NO_NEW_MARKET_DATA` | `IDLE` | `NO_NEW_CANDLE` | nie | bez zmiany | identyczny `IDLE` |
| `NO_SIGNAL_REBOUND` | `NO_TRADE / NO_SIGNAL` | `None` | `NO_SIGNAL` | nie | brak → brak | identyczny `SKIP` |
| `BOT_MODE_HALT` | `ENTRY_SUPPRESSED / EXECUTION_DISABLED` | `SUPPRESS` | `BOT_MODE_HALT` | nie | bez zmiany | identyczny `BLOCKED` |
| `BOT_DISABLED` | `ENTRY_SUPPRESSED / LIVE_DISABLED` | `SUPPRESS` | `BOT_DISABLED` | nie | brak → brak | identyczny `BLOCKED` |
| filtry przed sygnałem | `ENTRY_SUPPRESSED / READINESS_BLOCKED` | `SUPPRESS` | `POLICY_BLOCK` | nie | brak → brak | identyczny reason string |
| `ENTRY_BUFFER_BLOCK` | `SIGNAL_REJECTED / READINESS_BLOCKED` | `REJECT` | `POLICY_BLOCK` | nie | brak → brak | identyczny `SKIP` |
| `REGIME_BLOCK` | `ENTRY_BLOCKED / REGIME_BLOCKED` | `BLOCK` | `REGIME_BLOCK` | nie | brak → brak | identyczny `BLOCKED` |
| poprawny BUY w PAPER | `PAPER_SIMULATION / PAPER_ONLY` | `SIMULATE` | `SSOT_EXECUTE_AND_RECORD` | próba PAPER | brak → LONG | identyczne signal/sizing/position events |
| poprawny BUY w LIVE | `TRADE_EXECUTED / EXECUTED` | `EXECUTE` | `SSOT_EXECUTE_AND_RECORD` | dozwolone przez istniejący path | brak → LONG | identyczne eventy |
| LIVE entry not attempted | `ACTION_SUPPRESSED / EXECUTION_NOT_ATTEMPTED` | `SUPPRESS` | `EXECUTION_NOT_ATTEMPTED` | brak exchange attempt | bez zmiany | istniejący blocked event bez zmian |
| LIVE entry rejected przed ACK | `TECHNICAL_FAILURE / ORDER_REJECTED` | `ERROR` | `EXECUTION_FAILED` | próba bez ACK | bez udawanego submit/fill | istniejący blocked event bez zmian |
| LIVE entry ACK bez fill | `TECHNICAL_FAILURE / ORDER_ACCEPTED_NOT_FILLED` | `ERROR` | `EXECUTION_FAILED` | ACK bez potwierdzonego fill | `order_submitted=true`, `trade_executed=false` | istniejący blocked event bez zmian |
| `POSITION_HOLD` | `NO_TRADE / POSITION_MANAGEMENT` | `HOLD` | `POSITION_HOLD` | nie | pozycja → ta sama | identyczny `POSITION_HOLD` |
| stop loss | `PAPER_SIMULATION` lub `TRADE_EXECUTED` / `EXIT_EXECUTED` | `EXIT` | `STOP_LOSS` | istniejący exit | pozycja → brak po sukcesie | bez zmian |
| take profit | jw. | `EXIT` | `TAKE_PROFIT` | istniejący exit | pozycja → brak po sukcesie | bez zmian |
| BE protect | jw. | `EXIT` | `BREAK_EVEN_PROTECT` | istniejący exit | pozycja → brak po sukcesie | bez zmian |
| profit lock | jw. | `EXIT` | `PROFIT_LOCK` | istniejący exit | pozycja → brak po sukcesie | bez zmian |
| RSI soft exit | jw. | `EXIT` | `STRATEGY_EXIT` | istniejący exit | pozycja → brak po sukcesie | bez zmian |
| time exit | jw. | `EXIT` | `TIME_EXIT` | istniejący exit | pozycja → brak po sukcesie | bez zmian |
| LIVE exit not attempted / preflight block | `ACTION_SUPPRESSED / EXECUTION_NOT_ATTEMPTED` | `SUPPRESS` | `EXECUTION_NOT_ATTEMPTED` | brak exchange attempt | bez zmiany | istniejący blocked event bez zmian |
| `EXIT_NO_OPEN_POSITION` | `NO_TRADE / NO_POSITION` | `REJECT` | `NO_OPEN_POSITION` | nie | brak pozycji | istniejący blocked event bez zmian |
| exchange rejection przed ACK | `TECHNICAL_FAILURE / ORDER_REJECTED` | `ERROR` | `EXECUTION_FAILED` | próba bez przyjęcia zlecenia | bez udawanego submit/fill | istniejący blocked event bez zmian |
| ACK accepted bez fill | `TECHNICAL_FAILURE / ORDER_ACCEPTED_NOT_FILLED` | `ERROR` | `EXECUTION_FAILED` | zlecenie przyjęte, fill niepotwierdzony | `order_submitted=true`, `trade_executed=false` | istniejący blocked event bez zmian |
| partial fill | `TECHNICAL_FAILURE / PARTIAL_EXECUTION` | `ERROR` | `EXECUTION_FAILED` | część qty wykonana, operacja niepełna | `order_submitted=true`, `trade_executed=true` | mapper nie wykonuje dodatkowej mutacji |
| ledger failure | `TECHNICAL_FAILURE / LEDGER_FAILURE` | `ERROR` | `EXECUTION_FAILED` | brak skutecznego ledger result | bez udawanego submit/fill | istniejący blocked event bez zmian |
| `PANIC_NO_POSITION` | `NO_TRADE / NO_POSITION` | `REJECT` | `NO_OPEN_POSITION` | brak próby zlecenia | brak pozycji | istniejący blocked event bez zmian |

Wszystkie wiersze są terminalne dla bieżącego cyklu. `reference_price` używa `Decimal`, a dokładny legacy reason pozostaje w `reason_text`; istniejące reason strings eventów nie zostały zmienione.

Publiczne, strategy-neutral fabryki kontraktu `idle`, `position_hold`, `exit_result`, `action_suppressed` i `no_position` wykorzystują wspólny kontrakt. Neutralne `ACTION_SUPPRESSED`, `EXECUTION_NOT_ATTEMPTED` i `NO_POSITION` nie przypisują exitowi semantyki entry. Nie dodano drugiego modelu decyzji ani enumu akcji. `blocked_reason`, pełny `execution_result` oraz surowe flagi `ledger_ok`, `live_attempted`, `order_accepted` i `live_ok` są zachowane w immutable `details`. Brak `order_accepted` w legacy dict-like result jest konserwatywnie interpretowany jako `false`.

Test-only `operation_log` przechowuje jedną oś czasu strategy events, wywołań execution, zmian pozycji i heartbeatów. Testy zamrażają względną kolejność dla PAPER/LIVE entry oraz PAPER, successful, suppressed i failed LIVE exit. Historyczny `classify_legacy_reason()` używa jawnie zamrożonego zestawu wcześniejszych reasonów; nowe structured reasons nie zmieniają jego wyniku dla wcześniej nieznanych wartości.

Następny etap wymaga osobnej decyzji architektonicznej: TREND characterization albo canonical decision write path. Żaden z nich nie jest częścią tej integracji.
