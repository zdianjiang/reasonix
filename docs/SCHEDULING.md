# Persistent schedules

Reasonix has two complementary asynchronous systems:

| System | Use it for | Lifetime |
| --- | --- | --- |
| `internal/jobs` | A user turn's one-off background `bash` or `task` work | Its controller session; cancelled when that session closes |
| `internal/schedule` | A future or recurring durable action | Application lifetime; persisted in SQLite |

## Architecture

`internal/schedule` owns task persistence, calendar calculation, retry policy,
and the due-task execution loop. It deliberately does not know about Bot
platforms, Desktop, or CLI. An integration registers an `Executor` identified
by `executor_kind`.

```
Bot / Desktop / CLI / API
            |
            v
    internal/schedule (SQLite, rules, retry, claim)
            |
            v
       Executor registry
       |              |
 bot_message       bot_agent
       |              |
 send chat      independent task:<id> Controller session
```

The Bot gateway starts and stops the application scheduler today, and registers
the `bot_message` and `bot_agent` executors. New frontends should create tasks
through the same service; they must not emulate schedules with `jobs`.

## Bot usage

Ask the Bot naturally to create a recurring action, or use the management
commands:

```
/schedule list
/schedule pause <id>
/schedule resume <id>
/schedule delete <id>
```

Supported schedules include `1h`, `30m`, `daily@09:00`, and
`weekly:mon@09:00`. The minimum interval is one minute. Calendar rules use the
timezone captured at creation. Tasks are limited to 50 per chat.

`fixed_text` uses `bot_message` and sends the stored text directly. The default
`ai_prompt` uses `bot_agent`: every schedule has a stable, independent
`task:<id>` session, so it neither reads nor pollutes the user's interactive
chat history. Scheduled turns cannot create or mutate schedules.

## Persistence and delivery

Schedules are stored in `$REASONIX_STATE_HOME/bot-scheduled-tasks.db`. Existing
`bot-scheduled-tasks.json` files are migrated on first startup. SQLite WAL and
the scheduler's cross-process execution lease prevent duplicate active runs.

Delivery is **at-least-once**: an unexpected crash after an external message is
accepted but before state persistence can cause a retry. Executors that cause
external side effects should use the task/execution id as an idempotency key.

After a failure, retry is attempted after 30 seconds. Three consecutive
failures pause the task and record the error and pause reason.

## Feishu Bot integration test plan

The repository includes an isolated Feishu schedule test environment under
`testenv/feishu-schedule/`. It keeps Bot config, secrets, and schedule state
separate from a developer's normal Reasonix home.

Use it for manual end-to-end validation:

1. Copy `.env.example` and `reasonix.toml.example`.
2. Put Feishu app credentials in `.env` / `reasonix.toml`.
3. Put the model provider key in `state/reasonix/.env`.
4. Start the Bot in Feishu WebSocket mode.
5. In Feishu, create/list/pause/resume/delete both `fixed_text` and
   `ai_prompt` schedules.

Expected healthy startup:

```text
[bot-scheduler] started
feishu sdk websocket connected
```

The test should verify persistence in
`state/reasonix/bot-scheduled-tasks.db`, independent `task:<schedule-id>`
sessions for scheduled AI turns, accurate `/schedule list` run metadata, and
single-message rendering for short replies with trailing emoji.
