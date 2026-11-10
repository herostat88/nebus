# Асинхронный процессинг платежей

Тестовое задание: FastAPI, Pydantic v2, SQLAlchemy 2 async, PostgreSQL,
RabbitMQ/FastStream, Alembic. Внешний платёжный шлюз эмулируется: реальных
списаний, банковских подключений и сторонних API в проекте нет.

## Запуск одной командой

Из каталога проекта, нужны только Docker Engine и Docker Compose v2.24+:

```bash
docker compose up --build -d --wait
```

Команда собирает образы, запускает PostgreSQL и RabbitMQ, автоматически
применяет миграции и поднимает API, consumer и демонстрационный приёмник webhook.
Python на хосте, ручное создание БД, установка библиотек и подготовка `.env`
не требуются. Первое скачивание образов требует доступа к Docker Hub и PyPI.

- API: `http://127.0.0.1:8010`.
- Просмотр полученных webhook: `http://127.0.0.1:8011/events`.
- RabbitMQ Management: `http://127.0.0.1:15673`.

Все опубликованные порты привязаны к loopback. PostgreSQL и AMQP-порт
доступны только в Docker-сети. Томам БД и брокера перезапуск не вредит.

**Значения по умолчанию намеренно публичные, только для локальной демонстрации:**
API key `development-api-key-change-me`, пользователь RabbitMQ `payments`,
пароль RabbitMQ `development-mq-password`. Это не действующие внешние ключи.
Для любого нелокального запуска обязательны свои секреты и сетевые ограничения.

## Проверка API

Создать платёж:

```bash
curl -i http://127.0.0.1:8010/api/v1/payments \
  -H 'X-API-Key: development-api-key-change-me' \
  -H 'Idempotency-Key: example-order-001' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"125.50","currency":"RUB","description":"Demo order","metadata":{"order_id":"example-order-001"},"webhook_url":"http://webhook:8080/hook"}'
```

Ответ `202 Accepted`:

```json
{"payment_id":"<UUID>","status":"pending","created_at":"<ISO-8601 timestamp>"}
```

В исходном ТЗ название обязательного заголовка идемпотентности потерялось
при форматировании; используется `Idempotency-Key`. `X-API-Key` обязателен
для всех HTTP-эндпоинтов основного API, включая health и OpenAPI.
Неавторизованной страницы документации нет.

```bash
curl http://127.0.0.1:8010/api/v1/payments/PAYMENT_UUID \
  -H 'X-API-Key: development-api-key-change-me'

curl http://127.0.0.1:8011/events \
  -H 'X-API-Key: development-api-key-change-me'

curl http://127.0.0.1:8010/openapi.json \
  -H 'X-API-Key: development-api-key-change-me'
```

Сумма хранится как `NUMERIC(18,2)` и передаётся строкой, а не binary float.
Разрешены только положительные суммы и RUB/USD/EUR. Метаданные ограничены
16 KiB, описание — 1000 символами. Эти ограничения являются контрактом API/схемы,
а не параметрами инфраструктуры.

Повтор того же тела с тем же ключом возвращает тот же ID и текущее состояние,
не создаёт второе событие. Изменение тела при том же ключе даёт `409`.
Проверка защищена уникальным индексом PostgreSQL и корректна при конкурентных
запросах. Ключи не истекают: заданием не определена политика их удаления.

## Архитектура и гарантии

```text
POST -> PostgreSQL transaction [payment + outbox]
                           |
                 outbox publisher task
                           |
                RabbitMQ payments.new
                           |
                   one consumer handler
                           |
          gateway -> commit result -> webhook
                           |
       success: commit delivery -> ACK
       failure: commit attempt + scheduled outbox -> ACK
                           |
           next attempt / payments.dlx -> payments.dlq
```

### Outbox

Запись платежа и начального события атомарны. API не подключается к RabbitMQ:
недоступность брокера не теряет принятые платежи. Publisher работает фоновой
задачей **в процессе consumer**, отдельного сервиса и второго consumer нет.

Publisher читает только наступившие события с `FOR UPDATE SKIP LOCKED`.
Сообщения persistent, обменники и quorum-очереди durable. Публикация выполняется
с publisher confirms и `mandatory`; ошибка маршрутизации не считается успехом.
`published_at` устанавливается только после успешного подтверждения.
При недоступном RabbitMQ outbox не удаляется, следующая попытка откладывается.
Для публикации нет лимита в три попытки: иначе длительный сбой брокера терял бы
ещё не доставленные события. Лимит в три относится к обработке платежа/webhook.

Сбой после broker confirm, но до коммита `published_at`, может повторить публикацию.
Стабильный `event_id` и проверка сохранённого номера попытки делают такой повтор
безопасным для обработчика. **Гарантия — at-least-once, не exactly-once.**

### Consumer и retry

Один обработчик получает сообщение, эмулирует шлюз, фиксирует результат,
уведомляет клиента и подтверждает сообщение вручную после коммита состояния.
Prefetch равен единице. Блокировка строки платежа защищает также от случайного
запуска нескольких consumer и параллельных дублей.

Эмулятор ждёт 2–5 секунд; распределение исходов — 90% succeeded / 10% failed.
Результат детерминирован ID, поэтому повтор того же вызова после сбоя возвращает
тот же исход. `failed` — нормальный бизнес-результат: о нём тоже отправляется
webhook, но платёж не пробуется повторно «до успеха».

Результат шлюза коммитится **до webhook**: клиент может сразу проверить его
через GET. Если упал webhook, завершённый платёж повторно не обрабатывается шлюзом.
Счётчик попыток и отложенное событие сохраняются одной транзакцией в PostgreSQL.
В памяти не хранятся ни retry-счётчики, ни таймеры. Задержки переживают рестарт.

По умолчанию **три попытки всего**, не «первая плюс три повтора»:
первая сразу, вторая после 1 секунды, третья после 2 секунд. Отложенная
доставка реализована `outbox.available_at`, без блокирующего сна в consumer
и без плагинов RabbitMQ. После третьей ошибки создаётся outbox-событие
`payments.dead`, которое подтверждённо публикуется в `payments.dlx`/`payments.dlq`.
Сбой брокера на этом этапе также не теряет DLQ-событие.

Некорректные сообщения отправляются в DLQ сразу через native dead-letter
exchange. Сбой БД, мешающий даже записать retry, приводит к NACK/requeue с
короткой задержкой. `BROKER_DELIVERY_LIMIT=1000` заменяет встроенный лимит
в 20 redelivery: кратковременный сбой БД не должен преждевременно остановить
обработку. После исчерпания инфраструктурного лимита сообщение сохраняется
в DLQ и требует ручного разбора; это отдельный лимит, не три попытки webhook.
Используется положительное значение: AMQP-кодек текущей версии не кодирует -1.
Невалидные события явно отклоняются, а не бесконечно requeue. Для native DLX
включён режим at-least-once с `reject-publish`; переполнение не удаляет старые
сообщения, а возвращает ошибку publisher. Вместимость очередей ограничивается
`BROKER_QUEUE_MAX_LENGTH`. Для production нужны алерты по backlog и outbox.

`attempts`, `webhook_delivered_at`, `dead_lettered_at` и `last_error` видны в GET.
Отказ доставки webhook **не меняет succeeded на failed**: ошибка уведомления
не отменяет состоявшуюся оплату. Если технически не удаётся получить результат
шлюза, платёж может остаться pending с событием в DLQ — он требует разбора,
а не выдуманного результата списания. Автоматического redrive из DLQ нет.

### Webhook

Тело содержит ID, сумму строкой, валюту, статус, описание, метаданные и даты.
`X-Webhook-Id` стабилен для платежа, `X-Webhook-Signature` — HMAC-SHA256
от неизменных байтов тела с `WEBHOOK_SECRET`.

Даже при успешной отправке процесс может упасть до записи delivery в БД.
Поэтому получатель должен дедуплицировать webhook по ID, сохраняя отметку
в одной транзакции со своими побочными эффектами. Демоприёмник показывает
проверку подписи и дедупликацию в памяти, **не является production-реализацией**.

Есть таймаут, лимит тела ответа и запрет следования redirect. Для защиты от
SSRF принимаются только явно разрешённые `WEBHOOK_ALLOWED_HOSTS` и порты,
без credentials и fragment в URL. По умолчанию разрешён только демосервис
`webhook`. Список должен контролироваться оператором, а не пользователем API.
Для production дополнительно нужны HTTPS, контроль DNS/egress и сетевой запрет
metadata/private endpoints: allowlist имени сам по себе не заменяет эти меры.
Запросы webhook не используют HTTP-прокси из окружения.
Полные URL callback и его тело не пишутся в логи; request-логи HTTPX/HTTPCore
отключены. Аутентифицированный локальный healthcheck также игнорирует прокси,
чтобы не передавать `API_KEY` за пределы контейнера.

## Конфигурация

Эксплуатационные параметры читаются из окружения и необязательного `.env`;
полный список — в `.env.example`. При отсутствии файла Compose использует
явно демонстрационные значения, чтобы запуск оставался одной командой.

- Ключи: `API_KEY`, `WEBHOOK_SECRET`.
- БД и брокер: `POSTGRES_*`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`; полные
  `DATABASE_URL`/`RABBITMQ_URL` можно переопределить. Спецсимволы в пароле
  полного URI должны быть percent-encoded.
- Порты хоста: `API_PORT`, `WEBHOOK_PORT`, `RABBITMQ_MANAGEMENT_PORT`.
- Шлюз: `GATEWAY_DELAY_MIN/MAX`, `GATEWAY_SUCCESS_RATE`.
- Повторы: `MAX_ATTEMPTS`, `RETRY_BASE_SECONDS`, `INFRA_RETRY_DELAY`.
- Outbox: `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE`, `PUBLISH_TIMEOUT`,
  `OUTBOX_RETRY_MAX_SECONDS`.
- Брокер: `RABBITMQ_PREFIX`, `BROKER_HEARTBEAT`, `BROKER_DELIVERY_LIMIT`,
  `BROKER_QUEUE_MAX_LENGTH`, `SHUTDOWN_TIMEOUT`, `CONSUMER_STOP_GRACE_PERIOD`.
- Webhook: `WEBHOOK_ALLOWED_HOSTS`, `WEBHOOK_ALLOWED_PORTS`, `WEBHOOK_TIMEOUT`,
  `WEBHOOK_MAX_RESPONSE_BYTES`.
- Пул: `DB_POOL_SIZE`, `DB_MAX_OVERFLOW`; логи: `LOG_LEVEL`.

Миграции выполняет единственный one-shot сервис `migrate`, API и consumer
ждут его успешного завершения. Зависимости Python зафиксированы в
`requirements.lock` и `requirements-test.lock`. Контейнер приложения не работает
от root. Реальные `.env` исключены из Git и Docker build context.

Команды обслуживания:

```bash
docker compose logs -f api consumer
docker compose restart consumer
docker compose down
```

`down` сохраняет тома. `down -v` удаляет данные текущего Compose-проекта;
использовать только для осознанного сброса локального стенда.

## Автоматические тесты

```bash
docker compose --profile test run --build --rm tests
```

В одной команде запускаются отдельные `postgres_test`, `rabbitmq_test` и
контейнер pytest. Основные БД, очереди и тома не затрагиваются. Тесты отказываются
работать с БД, имя которой не заканчивается `_test`. Миграции проверяются на
настоящем PostgreSQL; SQLite вместо него не используется. Проверки брокера идут
через настоящий RabbitMQ/FastStream, а внешние gateway/webhook подменяются.
Каждый broker-тест использует собственное пространство имён очередей.

Покрыты создание/чтение и авторизация; конкурентная идемпотентность и 409;
валидация денег/валюты/URL; атомарный rollback payment+outbox; SKIP LOCKED;
ошибка публикации и unroutable message; повтор после сбоя между broker confirm
и коммитом; дубли consumer; бизнес-отказ шлюза; retry/DLQ после трёх ошибок;
сохранение результата до callback; отмена обработчика после обработки шлюза;
подпись и стабильность webhook; запрет redirect; native DLQ для poison message.
Отдельно проверены общий дедлайн медленного webhook, лимит ответа, более 20
инфраструктурных redelivery, отсутствие URL-токенов в логах и обход прокси
локальным healthcheck.

Статический анализ:

```bash
docker compose --profile test run --rm tests ruff check --no-cache src tests migrations
docker compose --profile test run --rm tests ruff format --check src tests migrations
```

## Осознанные ограничения

Тестовое не претендует на production-платёжную систему. Здесь один инстанс
PostgreSQL и RabbitMQ, нет HA, backup/PITR, TLS termination, аутентификации
разных клиентов, rate limiting, метрик/трейсинга и политики архивирования outbox.
Строка платежа удерживается ограниченное время во время I/O шлюза/webhook:
это упрощает доказательство корректности при одном consumer. Для высокой
пропускной способности нужны lease/fencing и отдельный этап доставки webhook.

Для реального провайдера обязательны его идемпотентный API и reconciliation
неопределённых результатов. Локальная транзакция не может обеспечить
exactly-once одновременно с внешним списанием и HTTP-уведомлением.

## Источники решений

- [SQLAlchemy: async sessions](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html).
- [RabbitMQ: publisher confirms](https://www.rabbitmq.com/docs/confirms).
- [RabbitMQ: dead-letter exchanges](https://www.rabbitmq.com/docs/dlx).
- [FastStream: manual acknowledgement](https://faststream.ag2.ai/latest/getting-started/acknowledgement/).
