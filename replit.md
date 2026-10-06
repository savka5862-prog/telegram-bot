# Finance Telegram Bot

A Telegram accounting bot with separate ledgers per chat. The workspace uses the existing `bot_database.db` SQLite file; production retains persistent PostgreSQL storage.

## Run & Operate

- The **Project** workflow starts the Python bot service and its public status page.
- The managed **artifacts/api-server: API Server** workflow runs `python3 main.py`.
- Replit manages `DATABASE_URL`; the bot uses `TELEGRAM_BOT_TOKEN` (or `TELEGRAM_TOKEN`) from Secrets.
- Dependencies are managed in `pyproject.toml` and frozen in `uv.lock`.
- The Telegram library is **python-telegram-bot**, not the unrelated `telegram` distribution.
- Workspace polling is enabled with `BOT_POLLING_ENABLED=true`; `BOT_DB_BACKEND=sqlite` selects `bot_database.db`. Switching backends does not transfer or delete records. Do not run workspace and production polling concurrently with the same Telegram bot token.
- `/api/healthz` checks database readiness; no financial data is exposed publicly.

## Publish

- Use **Reserved VM**, not Autoscale: Telegram polling requires an always-running process.
- On the **first publish**, create the production database **with current development data**. This copies the migrated history; schema migration alone does not copy records.
- Subsequent publishes should preserve production data; do not overwrite it with development data unless explicitly requested.
- Publish manages production schema changes. Do not add DDL or import/migration scripts to application startup or the production build.
- The production build installs frozen Python dependencies and builds the static status page.
- Financial records, local databases, and offline backups are excluded from the deployment image.

## Data

- `/start` opens the Telegram keyboard for deals, stock/cash, history, withdrawals, period reports, editing, backup, help, and chat-specific reset.
- Sales increase their payment-channel balance; expenses, advances, and withdrawals decrease it.
- Groups share their originating chat ledger. Legacy records without an originating chat remain unassigned to groups; their original author can access them privately.
- Original SQLite files remain in the workspace. `backups/bot_database.before_postgres.sqlite3` is the preserved migration snapshot.
- `schema/finance.sql` is the PostgreSQL schema source. `scripts/migrate_sqlite_to_postgres.py` is development-only, transactional, idempotent, and verifies every imported field and ID.
- SQLite is retained for isolated offline tests, not as a production fallback.

## Verification

- `python3 -m unittest discover -s tests -q` — offline accounting, Telegram, and storage checks.
- `RUN_POSTGRES_TESTS=1 python3 -m unittest discover -s tests -p test_postgres_storage.py -q` — live development checks using temporary tables; no real ledger rows are changed.
- `pnpm --filter @workspace/finance-bot run build` — public-page production build.