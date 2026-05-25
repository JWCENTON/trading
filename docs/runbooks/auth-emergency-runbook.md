# WALTRADE BOT — AUTH / PASSWORD / 2FA EMERGENCY RUNBOOK

## Cel

Runbook dla awaryjnych operacji auth/security:

* reset hasła admina
* revoke wszystkich sesji
* emergency disable 2FA
* recreate admin user
* verify auth state
* verify audit trail

Zakładamy:

```text
LIVE i PAPER są osobnymi środowiskami
LIVE i PAPER mają osobne DB
```

---

# 1. Środowiska

## LIVE DB

```bash
docker compose -p trading-live --env-file .env.live \
  -f docker-compose.yaml \
  -f docker-compose.live.override.yaml \
  exec db psql -U botuser -d trading_live
```

## PAPER DB

```bash
docker compose -p trading-paper --env-file .env.paper \
  -f docker-compose.yaml \
  -f docker-compose.paper.override.yaml \
  --profile legacy-paper-ui \
  exec db psql -U botuser -d trading_paper
```

---

# 2. Verify aktualnego auth state

## Users

```sql
SELECT
  id,
  username,
  role,
  is_active,
  totp_enabled,
  last_login_at,
  password_changed_at
FROM users
ORDER BY id;
```

## Active sessions

```sql
SELECT
  id,
  user_id,
  created_at,
  expires_at,
  revoked_at,
  ip_address,
  user_agent
FROM auth_sessions
WHERE revoked_at IS NULL
ORDER BY created_at DESC;
```

## Ostatnie auth eventy

```sql
SELECT
  created_at,
  username,
  action,
  result,
  reason,
  ip_address
FROM auth_login_events
ORDER BY created_at DESC
LIMIT 50;
```

---

# 3. Emergency password reset

## Generate bcrypt hash

W API container:

```bash
docker compose -p trading-live --env-file .env.live \
  -f docker-compose.yaml \
  -f docker-compose.live.override.yaml \
  exec api python - <<'PY'
from passlib.context import CryptContext
pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")
print(pwd.hash("NEW_PASSWORD_HERE"))
PY
```

Skopiuj wygenerowany hash.

---

## Update password

```sql
UPDATE users
SET
  password_hash = 'PASTE_BCRYPT_HASH_HERE',
  password_changed_at = now()
WHERE username = 'admin';
```

---

# 4. Revoke wszystkich sesji użytkownika

## Revoke sessions dla admina

```sql
UPDATE auth_sessions
SET revoked_at = now()
WHERE user_id = (
  SELECT id FROM users WHERE username = 'admin'
)
AND revoked_at IS NULL;
```

---

# 5. Emergency disable 2FA

Używać tylko awaryjnie.

## Disable TOTP

```sql
UPDATE users
SET
  totp_enabled = false,
  totp_secret = NULL,
  recovery_codes = '[]'::jsonb
WHERE username = 'admin';
```

## Revoke sessions po disable 2FA

```sql
UPDATE auth_sessions
SET revoked_at = now()
WHERE user_id = (
  SELECT id FROM users WHERE username = 'admin'
)
AND revoked_at IS NULL;
```

---

# 6. Re-enable 2FA

Po emergency disable:

1. Zalogować się normalnie
2. Wejść:

   * Security
   * Enable 2FA
3. Zeskanować QR
4. Zapisać recovery codes offline
5. Zweryfikować login:

   * password
   * TOTP

---

# 7. Recreate admin user

Używać tylko gdy admin jest uszkodzony/usunięty.

## Disable stare konto

```sql
UPDATE users
SET is_active = false
WHERE username = 'admin';
```

---

## Create nowy admin

Najpierw wygenerować bcrypt hash.

Potem:

```sql
INSERT INTO users (
  username,
  password_hash,
  role,
  is_active,
  totp_enabled,
  created_at
)
VALUES (
  'admin2',
  'PASTE_BCRYPT_HASH_HERE',
  'admin',
  true,
  false,
  now()
);
```

---

# 8. Verify po recovery

## Login verify

Sprawdzić:

```text
- login działa
- logout działa
- /auth/me działa
- Security panel działa
- 2FA setup działa
- revoke sessions działa
```

---

## Verify audit trail

```sql
SELECT
  created_at,
  username,
  action,
  result,
  reason
FROM auth_login_events
ORDER BY created_at DESC
LIMIT 50;
```

Sprawdzić:

```text
- LOGIN_SUCCESS
- LOGIN_FAILED
- LOGIN_2FA_REQUIRED
- PASSWORD_CHANGED
- 2FA_ENABLED
- 2FA_DISABLED
```

---

# 9. LIVE safety rules

## NEVER

```text
- nie resetować LIVE auth bez potwierdzenia
- nie wyłączać 2FA bez revoke sessions
- nie robić manualnych DELETE users
- nie usuwać audit logs
```

## ALWAYS

```text
- revoke sessions po auth changes
- verify auth_login_events
- verify Security UI
- verify login flow po zmianach
```

---

# 10. PAPER-first rule

Nowe procedury auth/security:

```text
1. najpierw PAPER
2. potem LIVE
```

---

# 11. Minimal smoke test po auth changes

```text
[ ] login OK
[ ] wrong password rejected
[ ] TOTP required
[ ] wrong TOTP rejected
[ ] recovery code works
[ ] revoke sessions works
[ ] logout works
[ ] auth/me works
[ ] Security UI works
[ ] audit events visible
```
