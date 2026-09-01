# Telegram MCP Bridge

[![CI](https://github.com/ph4ntom-rev/telegram-mcp-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/ph4ntom-rev/telegram-mcp-bridge/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-3776AB.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Низколатентный и надежный мост между Telegram Bot API и MCP-клиентом (Codex, ChatGPT desktop/CLI или другим совместимым агентом). Проект не содержит собственного LLM: он дает агенту строго ограниченные инструменты для получения, подтверждения и отправки Telegram-сообщений.

## Почему два режима

| Профиль | MCP | Telegram ingress | Для чего |
|---|---|---|---|
| Локальный (рекомендуется для Codex) | `stdio` | long polling | Минимальная поверхность атаки, нет открытого порта, событие приходит практически сразу |
| Серверный | stateless Streamable HTTP | HTTPS webhook | Постоянно работающий удаленный сервис, reverse proxy/TLS |

SQLite в WAL-режиме является журналом истины. Webhook отвечает `200` только после commit. Long polling повышает offset только после durable insert. Между очередью и агентом используется `claim → lease → ack`, поэтому входящие сообщения доставляются как минимум один раз до подтверждения.

> MCP сам по себе не «будит» остановленную задачу Codex. Пока агент работает, он может долго ждать событие через `telegram_wait_updates`. Для круглосуточного автономного ответа нужен постоянно работающий agent loop/задача поверх этого MCP-сервера.

## Быстрый локальный запуск (Windows / PowerShell)

1. Создайте бота через `@BotFather` и сохраните token.
2. Клонируйте репозиторий, создайте окружение и установите зафиксированные зависимости:

   ```powershell
   git clone https://github.com/ph4ntom-rev/telegram-mcp-bridge.git
   Set-Location telegram-mcp-bridge
   py -m venv .venv
   .\.venv\Scripts\python.exe -m pip install --require-hashes -r requirements.lock
   .\.venv\Scripts\python.exe -m pip install --no-deps -e .
   Copy-Item .env.example .env
   ```

3. В `.env` заполните `TELEGRAM_BOT_TOKEN` и числовые allowlist. `TELEGRAM_ALLOWED_USER_IDS` разрешает автора входящего события, а `TELEGRAM_ALLOWED_CHAT_IDS` разрешает чат и на прием, и на отправку. Если заполнены оба списка, событие должно пройти обе проверки. Username не используется как авторизация.
4. Если у бота раньше был webhook, явно отключите его без удаления накопленных сообщений:

   ```powershell
   .\.venv\Scripts\telegram-mcp.exe webhook delete --env-file .env
   ```

5. Проверьте соединение:

   ```powershell
   .\.venv\Scripts\telegram-mcp.exe doctor --env-file .env
   ```

6. Добавьте stdio-сервер в Codex. Самый безопасный вариант — через Settings → MCP servers → Add server, указав абсолютные пути:

   ```text
   command: C:\absolute\path\telegram-mcp-bridge\.venv\Scripts\telegram-mcp.exe
   arguments: serve --transport stdio --env-file C:\absolute\path\telegram-mcp-bridge\.env
   ```

   То же через `config.toml` показано в [`codex-config.toml.example`](codex-config.toml.example).

После перезапуска Codex инструменты появятся под сервером `telegram`.

## MCP-инструменты

- `telegram_wait_updates`: атомарно арендует следующую пачку событий и может ждать до 30 секунд.
- `telegram_ack_updates`: подтверждает события только с правильным lease token.
- `telegram_release_updates`: освобождает пачку после временной ошибки.
- `telegram_reply`: отвечает на сохраненное событие; это безопаснее ручного `chat_id`.
- `telegram_send_text`: отправляет только в чат, явно указанный в `TELEGRAM_ALLOWED_CHAT_IDS`.
- `telegram_edit_text`, `telegram_delete_message`, `telegram_typing`, `telegram_answer_callback`.
- `telegram_peek_updates`, `telegram_list_chats`, `telegram_bridge_status`: диагностические read-only инструменты.

Любая логическая отправка требует `idempotency_key`, например `reply:824991:0`. Повтор того же ключа с тем же payload возвращает сохраненный результат; тот же ключ с другим payload отклоняется.

## Гарантии доставки

| Участок | Гарантия |
|---|---|
| Telegram → durable inbox | effectively-once по `(bot_key, update_id)` |
| Durable inbox → MCP consumer | at-least-once до корректного ack |
| MCP → Telegram `sendMessage` | cached idempotency при известном результате; `uncertain` при неоднозначном сетевом исходе |

Telegram Bot API не принимает idempotency key. Если Telegram получил сообщение, но TCP-ответ потерялся, мост не может отличить отправку от неотправки. Он помечает операцию `uncertain` и **не повторяет ее автоматически**, чтобы не создавать скрытые дубликаты.

## Server / webhook профиль

1. Сгенерируйте два независимых секрета (не используйте bot token):

   ```powershell
   py -c "import secrets; print(secrets.token_urlsafe(32)); print(secrets.token_urlsafe(32))"
   ```

2. В `.env` задайте:

   ```dotenv
   BRIDGE_TRANSPORT=http
   TELEGRAM_INGRESS_MODE=webhook
   MCP_AUTH_TOKEN=<первый секрет, минимум 32 символа>
   TELEGRAM_WEBHOOK_SECRET=<второй секрет>
   PUBLIC_BASE_URL=https://bridge.example.com
   MCP_HOST=127.0.0.1
   MCP_ALLOWED_HOSTS=bridge.example.com
   ```

3. Поднимите приложение за reverse proxy с TLS, затем зарегистрируйте webhook:

   ```powershell
   .\.venv\Scripts\telegram-mcp.exe serve --transport http --env-file .env
   .\.venv\Scripts\telegram-mcp.exe webhook set --env-file .env
   ```

MCP endpoint: `https://bridge.example.com/mcp`; webhook: `https://bridge.example.com/telegram/webhook`; health: `/healthz` и `/readyz`. Для remote MCP Codex передает `Authorization: Bearer ...` через `bearer_token_env_var`.

Production-требования: TLS 1.2+, один process worker для SQLite, локальный (не сетевой/NFS) диск, явный Host allowlist, фильтрация официальных Telegram CIDR на reverse proxy и резервное копирование через SQLite backup API. Для горизонтального масштабирования замените SQLite на серверную БД/очередь.

### Docker Compose

После заполнения `.env` серверный профиль можно запустить так:

```powershell
docker compose up --build -d
```

Контейнер непривилегированный, его корневая файловая система read-only, база хранится в отдельном volume. Порт публикуется только на `127.0.0.1:8000`; внешний TLS reverse proxy должен находиться перед ним. Не запускайте несколько реплик с одним SQLite volume.

## Безопасные ограничения

- Ingress и outbound проверяют числовые allowlist; пустой ingress allowlist не запускается без явного `TELEGRAM_ALLOW_ALL=true`.
- Входящее событие никогда не расширяет право отправки. Даже при `TELEGRAM_ALLOW_ALL=true` outbound закрыт, пока chat ID явно не добавлен в `TELEGRAM_ALLOWED_CHAT_IDS`.
- Нет универсального `call_api`, произвольных URL, SQL или файловых путей.
- Вложения возвращаются только как безопасные метаданные и непрозрачные Telegram `file_id`; мост намеренно не скачивает произвольные файлы.
- Plain text используется по умолчанию; форматированный текст длиннее 4096 символов отклоняется, чтобы не разорвать Telegram entities.
- Webhook проверяет `X-Telegram-Bot-Api-Secret-Token` через constant-time comparison и ограничивает body до 1 MiB.
- Bot token находится в URL Telegram API, поэтому исключения, ответы и access-логи его не содержат.
- Telegram-текст помечается `untrusted_content=true`: он не должен управлять секретами, allowlist или системными действиями агента.
- HTTP MCP всегда требует bearer token и включает Host/Origin protection.

Полный threat model и результаты повторной проверки находятся в [`AUDIT.md`](AUDIT.md).

## Проверка проекта

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\mypy.exe src
.\.venv\Scripts\pytest.exe --cov
.\.venv\Scripts\bandit.exe -c pyproject.toml -q -r src
.\.venv\Scripts\pip-audit.exe --require-hashes -r requirements.lock
```

Те же проверки автоматически выполняются в GitHub Actions на Python 3.10 и 3.12. Зафиксированные runtime-зависимости устанавливаются с обязательной проверкой SHA-256 из `requirements.lock`.

Локальный synthetic benchmark (без запросов к Telegram):

```powershell
.\.venv\Scripts\python.exe benchmarks\latency.py
```

## Настройки

Все доступные переменные и безопасные значения по умолчанию документированы в [`.env.example`](.env.example). Секреты не передавайте в аргументах командной строки и не коммитьте `.env`. Терминальные записи outbox (`sent`, `dead`, `uncertain`) сохраняются как idempotency ledger: контролируйте размер диска, не удаляйте их вручную без осознанного отказа от защиты ключей.

Операционные меры, backup/restore и реакция на утечку описаны в [`SECURITY.md`](SECURITY.md); подтвержденные проверки и остаточные ограничения — в [`AUDIT.md`](AUDIT.md).
