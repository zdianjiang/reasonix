# Feishu schedule integration environment

This isolated Docker environment runs only the headless Feishu Bot gateway.
Its configuration and schedule SQLite database stay under this directory and
never use a developer's normal Reasonix home.

## Prepare

```bash
cd testenv/feishu-schedule
cp .env.example .env
cp reasonix.toml.example reasonix.toml
mkdir -p state
```

Fill only these values when supplied:

- `FEISHU_BOT_APP_SECRET` and `DEEPSEEK_API_KEY` in `.env`;
- Feishu `app_id` and tester `open_id` in `reasonix.toml`.

The config uses Feishu WebSocket mode, so no public inbound webhook or Docker
port mapping is required.

### Live test configuration used by this environment

This directory is intentionally a **separate Bot integration-test home**:

- `REASONIX_HOME=state/reasonix`
- `REASONIX_STATE_HOME=state/reasonix`
- schedule SQLite: `state/reasonix/bot-scheduled-tasks.db`
- provider key file: `state/reasonix/.env`

Reasonix runtime loads normal provider API keys from `$REASONIX_HOME/.env`.
Feishu's app secret is still injected through the Bot process environment,
using the variable named by `bot.feishu.app_secret_env`.

For a first-time Feishu tester, the user's `open_id` is only known after the
tester sends a message to the Bot. During bootstrap you may set:

```toml
[bot.allowlist]
allow_all = true
```

After the first message appears in logs, copy the `open_id` into
`feishu_users`, `feishu_admins`, and `feishu_approvers`, then set
`allow_all = false`.

## Run and inspect

```bash
docker compose up --build -d
docker compose logs -f
docker compose exec reasonix-feishu-schedule \
  --config /config/reasonix.toml bot doctor --deep
```

Stop without deleting state:

```bash
docker compose down
```

Reset only the isolated test environment:

```bash
docker compose down
rm -rf state
```

If Docker Compose is not installed, run the equivalent foreground launcher:

```bash
./run.sh
```

For the host-binary path used during local Feishu validation:

```bash
cd /path/to/DeepSeek-Reasonix
set -a
. testenv/feishu-schedule/.env
set +a
REASONIX_HOME="$PWD/testenv/feishu-schedule/state/reasonix" \
REASONIX_STATE_HOME="$PWD/testenv/feishu-schedule/state/reasonix" \
/tmp/reasonix-feishu-schedule \
  --config "$PWD/testenv/feishu-schedule/reasonix.toml" \
  bot start --channels feishu --dir "$PWD"
```

Healthy startup logs should include:

```text
[bot-scheduler] started
feishu sdk websocket connected
```

## Test script in Feishu

1. Send: `请创建一个每 5 分钟发送“schedule e2e ok”的固定文本定时任务`.
2. Send `/schedule list` and record the task ID.
3. Verify one delivery arrives after the interval.
4. Send `/schedule pause <id>`; verify no later delivery.
5. Send `/schedule resume <id>`; verify delivery resumes.
6. Send `/schedule delete <id>`; verify it disappears from `/schedule list`.

AI-prompt schedule coverage:

1. Send: `每 5 分钟给我讲一个短笑话`.
2. Verify `/schedule list` reports `runs=<n>`, `last=<time>`,
   `next=<time>`, and no stale claim that no messages were sent.
3. Verify delivered short text plus trailing emoji arrives as one Feishu text
   message, not as a separate punctuation/emoji fragment.
4. Delete the schedule and verify the delete confirmation plus trailing
   success emoji is also one message.

Validation points:

- scheduled AI turns use an independent stable session `task:<schedule-id>`;
- scheduled task history is not injected into the user's interactive chat
  session;
- scheduled turns cannot create, pause, resume, or delete schedules;
- fixed-delay execution schedules the next run after the previous run finishes;
- consecutive failures retry after 30 seconds and auto-pause after three
  failures.

The database is `state/reasonix/bot-scheduled-tasks.db`. Never commit `.env`,
`reasonix.toml`, or `state/`.
