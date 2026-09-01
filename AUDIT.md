# Отчет о глубокой проверке

Дата среза: **2026-08-17**  
Версия: **1.0.0**  
Область: Python source, MCP surface, Telegram transport, SQLite lifecycle, HTTP/webhook, CLI, конфигурация, зависимости, контейнер и документация.

Это инженерный security/reliability audit с независимым повторным чтением кода и adversarial-тестами, а не сертифицированный внешний pentest.

## Что исследовано

Архитектура сверена с актуальными первичными источниками:

- Codex поддерживает локальный MCP `stdio` и remote Streamable HTTP: [OpenAI Codex MCP](https://developers.openai.com/codex/mcp).
- Реализация использует стабильный Python MCP SDK v2 и stateless HTTP: [официальный MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk).
- Семантика update, long polling, webhook secret и Bot API методов проверена по [Telegram Bot API](https://core.telegram.org/bots/api), [webhook guide](https://core.telegram.org/bots/webhooks) и [FAQ/rate limits](https://core.telegram.org/bots/faq).
- WAL, single-writer и durability trade-off проверены по [SQLite WAL](https://www.sqlite.org/wal.html).

Выбранный быстрый локальный профиль — `stdio + long polling`: нет публичного порта, persistent HTTP/2-клиент, Telegram long poll до 50 секунд возвращается сразу при событии. Серверный профиль — stateless Streamable HTTP + webhook за TLS proxy.

## Threat model

Проверялись следующие противники и отказы:

- неизвестный Telegram user/chat, подмена username, событие с несовпадающим вложенным chat ID;
- prompt injection и чрезмерные/невалидные JSON/Unicode payload;
- повторные, конфликтующие и пришедшие вне порядка `update_id`;
- поддельный webhook, слабый secret, DNS rebinding, неверный Host/Origin, body exhaustion;
- неавторизованный MCP-клиент и попытка вызвать произвольный Bot API/URL/path;
- утечка bot token через URL, exception chain, access log или Telegram description;
- timeout/reset/429, неоднозначный результат send, повтор unsafe mutation;
- crash/cancel во время SQLite transaction, HTTP send, shutdown или lease;
- два конкурентных claim, restart с еще не истекшим lease, ACL revoke во время send;
- заполнение очереди/диска и рост idempotency ledger.

## Подтвержденные свойства

| Свойство | Реализация и проверка |
|---|---|
| Durable ingress | Webhook возвращает 2xx только после commit; polling offset растет только в одной транзакции с insert. |
| Dedup | Первичный ключ `(bot_key, update_id)`; одинаковый payload дает duplicate, иной fingerprint дает conflict без замены оригинала. |
| Порядок | Consumer cursor — локальный монотонный `event_id`, а не Telegram `update_id`, который не обязан оставаться последовательным. |
| Consumer delivery | Atomic claim + opaque batch token + lease + idempotent ack; до ack гарантия at-least-once. |
| Outbound | Explicit chat ACL, payload fingerprint и обязательный idempotency key. Известный `sent` replay не делает второй HTTP-запрос. |
| Неоднозначный send | Transport failure после начала запроса и restart активного send переходят в `uncertain`, без скрытого retry. |
| Конкурентность | Один SQLite writer, `BEGIN IMMEDIATE`, WAL, busy timeout; exact-ID outbox claim выигрывает только у одного worker. |
| Отзыв доступа | ACL replacement/revoke атомарно блокирует новые claim и quarantines существующий outbox. Ingress никогда не расширяет outbound ACL. |
| Секреты | Token redaction покрывает URL, exception, raw pattern и server response; HTTP access log отключен. |
| HTTP boundary | Bearer auth, stateless MCP, Host/Origin protection, webhook secret constant-time, строгий JSON и byte cap. |
| API surface | Нет generic `call_api`, произвольной загрузки URL/файла, SQL или filesystem path. |

## Найденные и устраненные дефекты

В ходе проверки были воспроизведены и закрыты:

1. Bot token мог попасть в exception cause/Telegram error description — исключения теперь полностью санитизируются без опасной цепочки.
2. Невалидный элемент успешного `getUpdates` мог быть молча отброшен — batch теперь отклоняется целиком, offset не меняется.
3. Unsafe edit/delete/webhook/callback mutation могла автоматически повторяться после неоднозначного исхода — retry ограничен безопасными случаями.
4. Malformed HTTP 2xx send считался «не отправленным» — после начала запроса это `uncertain`.
5. Большой `retry_after` не обновлял общий cooldown — rate limiter теперь штрафует все 429.
6. Несколько актуальных Telegram Bot API update-вариантов не извлекали actor/chat — parser и adversarial matrix расширены.
7. Пустые allowlist могли пройти fail-open, а OR-политика ослабляла две заданные границы — теперь deny-by-default и пересечение заполненных списков.
8. Входящий разрешенный user мог навсегда авторизовать произвольный outbound chat — durable write ACL создается только из явной конфигурации.
9. Удаленный из env chat сохранял старое право в SQLite — startup выполняет атомарный replace/revoke.
10. Revoked pending/sending outbox мог занимать всю bounded queue — он атомарно переводится в `dead`/`uncertain`.
11. Cancel во время ожидающего `BEGIN IMMEDIATE` мог оставить orphan transaction; cancel `close()` — worker thread. Оба lifecycle path shielded и проверены failpoint-тестами.
12. Restart оставлял неистекшие inbox/outbox leases зависшими — startup сразу освобождает inherited inbox и quarantines inherited send.
13. Финальный `message_id` мог не записаться из-за истекших часов lease — exact current token остается владельцем до атомарного recovery/reclaim.
14. Webhook оставался активен в polling/disabled mode — route теперь вообще не монтируется вне webhook mode, а любой заданный secret валидируется.
15. `doctor` запускал mutable crash recovery рядом с живым server — диагностика теперь только открывает хранилище для чтения состояния и не меняет leases/ACL.
16. Второй server process с той же БД мог ошибочно выполнить crash recovery живого первого — cross-platform advisory lock теперь берется до открытия/восстановления и освобождается только после cancellation-safe shutdown.
17. JSON `NaN`/`Infinity`, duplicate keys, reserved webhook path, unsafe host/auth characters и относительный DB path получили строгую валидацию.
18. Разбиение текста могло повредить Unicode grapheme — plain-text chunking проверен на emoji, ZWJ и combining sequences.

## Верификация

Повторный независимый предрелизный прогон выполнен **2026-09-01** на Python 3.10.6. Результаты совпали с исходным аудитом; дополнительно добавлен CI для Python 3.10/3.12 и проверена чистота публикуемого дерева на секреты.

Финальный clean-room прогон выполняется из отдельного virtual environment:

```text
pytest --cov --cov-branch: 201 passed, branch coverage 85.50%
ruff check .: clean
ruff format --check .: clean
mypy --strict src/telegram_mcp: clean (15 modules)
pip check: clean
pip-audit --local --skip-editable: no known vulnerabilities
bandit -c pyproject.toml -r src: no unsuppressed findings; generated-placeholder SQL reviewed manually
wheel build + install in fresh venv + CLI/import smoke: passed
Docker Compose YAML/config: parsed; фактическая сборка image не выполнялась, потому что Docker отсутствует в среде аудита
```

Тестовая матрица включает dedup/conflict, две SQLite connections, 20 конкурентных exact-ID claims, expired/restarted leases, cancellation failpoints, ACL revoke, idempotency conflict/replay, ambiguous send, 429 cooldown, Telegram schema variants, hostile JSON/Unicode, webhook auth/mode, MCP auth/metadata, runtime lifecycle, CLI и logging redaction.

### Synthetic latency

Windows, SQLite `WAL + synchronous=FULL`, 500 итераций, без Telegram network и model latency:

| Операция | p50 | p95 | p99 | mean |
|---|---:|---:|---:|---:|
| Durable ingest | 1.515 ms | 1.856 ms | 1.974 ms | 1.566 ms |
| Claim + ACK | 2.619 ms | 3.249 ms | 3.599 ms | 2.724 ms |
| Полный durable round trip | 4.168 ms | 5.018 ms | 5.436 ms | 4.290 ms |

Это измеряет только собственный overhead моста. Фактическое время ответа включает Telegram transport и работу модели. Long polling не ждет окончания timeout при появлении update.

## Остаточные ограничения

- Абсолютная exactly-once отправка невозможна без idempotency primitive со стороны Telegram. `uncertain` требует проверки человеком/агентом.
- MCP пассивен: остановленную задачу Codex сервер сам не пробудит. Для 24/7 нужен постоянно работающий agent loop.
- SQLite-профиль рассчитан на один process и локальный диск. Горизонтальное масштабирование требует другой durable coordination layer.
- Outbox tombstones автоматически не удаляются, иначе старые idempotency keys снова смогут отправиться. Нужен disk monitoring и осознанная retention policy.
- Poison inbox event при постоянном release может голодать очередь; оператор должен ACK/разобрать его, а не бесконечно освобождать.
- Вложения доступны только как проверенные метаданные/`file_id`; download pipeline намеренно не входит в эту версию.
- TLS, Telegram source-CIDR filtering, DDoS protection и backup scheduling остаются обязанностью внешней инфраструктуры.
- Ни тесты, ни статический анализ не доказывают отсутствие всех дефектов; после обновления SDK/Bot API аудит следует повторить.
