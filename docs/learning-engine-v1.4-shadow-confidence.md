# Learning Engine V1.4 — Shadow Confidence Calibration

## Cel i North Star

V1.4 zamienia wyłącznie stabilne wyniki walidacji V1.3 na audytowalne propozycje delty confidence. Nie ustawia parametrów runtime i nie stosuje propozycji. Wspiera North Star przez ochronę kapitału, deterministyczną politykę, pełne evidence i oddzielenie hipotezy AI od późniejszego dowodu w danych.

## Źródło danych

Źródłem jest `v_learning_proposal_stable_candidates_v1`, połączony z trwałym stanem V1.3 dla pełnego klucza:

```text
environment + symbol + interval + strategy + window_days
```

Kandydat musi mieć `STABLE` oraz wszystkie flagi safety V1.3 ustawione na `true`. Obsługiwane są wyłącznie `INCREASE_CONFIDENCE` i `REDUCE_CONFIDENCE`.

## Polityka delty

Próbka określa maksimum `0.01` dla 30–49 decyzji, `0.02` dla 50–99 i `0.03` od 100. Confidence ogranicza deltę do `0.01` poniżej 0.70, `0.02` od 0.70 do poniżej 0.85 oraz `0.03` od 0.85. Wartość bezwzględna to minimum obu limitów; kierunek wynika z akcji. Constraint ogranicza deltę dodatkowo do `[-0.05, 0.05]`.

Profit Factor, Net PnL, win rate i coverage są wyłącznie evidence. PF nie skaluje delty, więc sentinel `999` nie wpływa na kalibrację.

## Brak SSOT current confidence

Nie istnieje jednoznaczne SSOT konfigurowalnego confidence dla pełnego klucza slotu. V1.4 zapisuje tylko `proposed_delta`. Nie zapisuje `current_value` ani `proposed_value`.

## Idempotencja i superseding

Każdy source refresh run może być przetworzony raz na environment. Proposal key jest deterministyczny i obejmuje silnik, pełny slot, akcję, deltę oraz source proposal key. Identyczna propozycja jest odświeżana bez duplikatu. Zmiana kierunku, delty lub source proposal key oznacza `SUPERSEDED`; historia nie jest usuwana. Partial unique index gwarantuje jedną propozycję `ACTIVE` na pełny slot.

## Zero STABLE

Brak stabilnych wejść kończy run statusem `OK`, licznikami zero i bez placeholderów.

## Automation i izolacja awarii

```text
automation_runner
  -> V1.2 due gate / source refresh scheduler
  -> V1.3 trigger
  -> commit udanego V1.2/V1.3
  -> osobna transakcja V1.4
```

V1.4 nie ma triggera ani osobnego schedulera. Runner wywołuje go dopiero po commicie V1.2/V1.3. Wywołanie ma osobny `try/except` i rollback. W funkcji rekord runu powstaje przed wewnętrznym blokiem wyjątków: błąd cofa propozycje, po czym run zostaje trwale oznaczony `ERROR`; funkcja nie wykonuje `RAISE`.

Logi i `automation_kv` rozdzielają nazwy:

```text
scheduler_version=LEARNING_FEEDBACK_SCHEDULER_V1_2
source_refresh_engine_version=LEARNING_FEEDBACK_ENGINE_V1_2
engine_version=LEARNING_ENGINE_V1_4
engine_mode=SHADOW
apply_enabled=false
```

Status `not_due` oznacza, że scheduler V1.2 nie uruchomił nowego source refresh runu, więc V1.4 nie ma nowego wejścia do przetworzenia.

## Safety invariants

- `SHADOW_ONLY`; brak funkcji apply.
- `apply_enabled` ma constraint `IS FALSE`.
- Brak zapisów do `bot_control`, `strategy_params`, `runtime_params`, `allocation_policy`, `positions`, `orders` i `fills`.
- Brak wpływu na ORC, MME, capital allocation, execution i trading runtime.
- Widok `v_learning_shadow_confidence_safety_audit_v1` powinien zwracać zero wierszy.

## Walidacja

Po przyszłym rollout należy sprawdzić klucze `learning_engine_v14%`, historię runów, active proposals, summary, safety audit i duplikaty ACTIVE. Oczekiwane są: `apply_enabled=false`, zero naruszeń safety i zero duplikatów.

## Rollback logiczny

```sql
UPDATE automation_kv
SET value = '0', updated_at = now()
WHERE key = 'learning_engine_v14_enabled';
```

Wyłączenie zatrzymuje nowe refreshe. Tabele, runy, proposals, historia i evidence pozostają nienaruszone.

## Kolejność wdrożenia

Kanoniczne env files:

```text
LOCAL LIVE:  .env.okx.live
LOCAL PAPER: .env.okx.paper
```

LOCAL PAPER używa publicznych danych produkcyjnego OKX z `OKX_TESTNET=false`,
ale utrzymuje lokalną symulację przez `TRADING_MODE=PAPER`,
`LIVE_ORDERS_ENABLED=0` i `OKX_EXECUTION_ENABLED=0`. Legacy `.env.paper` nie
jest konfiguracją rolloutów OKX PAPER.

LOCAL PAPER wymaga dodatkowo `--profile legacy-paper-ui`.

Migracje należy wykonywać przez `psql -v ON_ERROR_STOP=1`, żeby zatrzymać rollout na pierwszym błędzie SQL.

Po review i testach: LOCAL LIVE, walidacja, LOCAL PAPER, walidacja, commit i push, VPS `pull --ff-only`, VPS LIVE, VPS PAPER, a następnie finalny drift audit wszystkich czterech środowisk.
