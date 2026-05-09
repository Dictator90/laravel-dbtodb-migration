# mb4it/laravel-dbtodb-migration

[English](README.md) | Русский

Пакет для Laravel 12: Artisan-команда, которая копирует строки из одного настроенного подключения к БД в одну или несколько целевых таблиц другого подключения. Запуски описываются именованными миграциями в `config/dbtodb_mapping.php`: поддерживаются чтение чанками, опциональная keyset-пагинация, отдельные карты колонок и трансформации для каждой целевой таблицы, фильтры, `upsert`/`insert`, строгая валидация, JSON-отчёты и опциональные профильные логи.

## Требования

- PHP `^8.2`
- Laravel `^12.0`

## Установка

```bash
composer require mb4it/laravel-dbtodb-migration
php artisan vendor:publish --tag=dbtodb-migration-config
```

После публикации настройте подключения к базам данных, которые используются в миграциях, в `config/database.php`.

## Поддерживаемые целевые драйверы

Для этих Laravel-драйверов целевого подключения поддерживаются чтение метаданных таблиц, проверка обязательных колонок и автоматическое определение primary key:

- `sqlite`
- `pgsql`
- `mysql`
- `mariadb`
- `sqlsrv`

Префиксы таблиц из конфигурации целевого подключения учитываются при чтении метаданных.

## Быстрый старт

Этот сценарий подходит, когда нужно перенести несколько колонок из одной исходной таблицы в одну целевую таблицу. Если не передавать `--migration`, команда использует `migrations.default`, поэтому для простого запуска не нужны `--source` и `--target`, если они уже указаны в конфиге.

1. Установите пакет и опубликуйте конфиг:

    ```bash
    composer require mb4it/laravel-dbtodb-migration
    php artisan vendor:publish --tag=dbtodb-migration-config
    ```

2. Добавьте подключения к исходной и целевой БД в `config/database.php`, например `legacy_mysql` и `pgsql_app`.

3. Отредактируйте `config/dbtodb_mapping.php` и замените пример на миграцию `default`:

    ```php
    return [
        'migrations' => [
            'default' => [
                'source' => 'legacy_mysql',
                'target' => 'pgsql_app',

                'tables' => [
                    // исходная таблица => целевая таблица => исходная колонка => целевая колонка
                    'legacy_users' => [
                        'users' => [
                            'id' => 'id',
                            'email' => 'email',
                        ],
                    ],
                ],
            ],
        ],
    ];
    ```

4. Сначала выполните безопасную проверку. `--dry-run` читает данные, применяет фильтры/трансформации, проверяет целевые колонки в строгом режиме и пишет только JSON-отчёт:

    ```bash
    php artisan db:to-db --dry-run
    ```

5. Если отчёт корректный, запустите реальный перенос:

    ```bash
    php artisan db:to-db
    ```

6. Для больших таблиц лучше перейти на полный синтаксис таблицы и явно указать `keyset_column` и размер `chunk`.

## Насколько полна документация

Документация описывает текущий публичный формат конфигурации: установку, драйверы, быстрый старт, именованные миграции, шаги, CLI-опции, настройки runtime/памяти, автоматические преобразования типов, профильные логи, синхронизацию sequence/auto-increment, фильтры, трансформации колонок, fan-out/fan-in маршрутизацию и важные особенности поведения.

Перед запуском на production-данных рекомендуется проверить схему целевых таблиц, выполнить `--dry-run`, оставить `strict` включённым, если нет осознанной причины отключить строгую проверку, и сохранить JSON-отчёт для аудита или отладки.

Старый верхнеуровневый формат `tables` / `columns` / `filters` больше не поддерживается. Все запуски нужно описывать через `migrations.<name>`.

## Именованные миграции

Добавляйте любые миграции внутри верхнеуровневого ключа `migrations`. Выберите нужную миграцию через `--migration=name`.

```php
'migrations' => [
    'default' => [
        'source' => 'legacy_mysql',
        'target' => 'pgsql_app',
        'tables' => [
            'legacy_users' => [
                'users' => ['id' => 'id', 'email' => 'email'],
            ],
        ],
    ],

    'catalog' => [
        'source' => 'legacy_mysql',
        'target' => 'pgsql_app',
        'tables' => [
            'top_banners' => [
                'catalog_banners' => [
                    'link' => 'link',
                ],
                'catalog_banners_2' => [
                    'name' => 'name',
                ],
            ],
        ],
    ],
],
```

```bash
php artisan db:to-db --migration=catalog
php artisan db:to-db --migration=catalog --source=legacy_mysql --target=pgsql_app
php artisan db:to-db --migration=catalog --dry-run
```

`--source` и `--target` остаются переопределениями для локальной отладки, но обычные миграции лучше описывать с `source` и `target` в конфиге.

## Полный синтаксис таблицы

Короткий синтаксис подходит для простой карты колонок. Полный синтаксис нужен для source/target-фильтров, трансформаций, ключей upsert, выбора операции, дедупликации, обработки ошибок отдельных строк или настроек чтения исходной таблицы. Поддерживаемые операции: `upsert`, `insert`, `truncate_insert`. `truncate_insert` очищает каждую целевую таблицу один раз за запуск перед первой записью.

```php
'migrations' => [
    'catalog' => [
        'source' => 'legacy_mysql',
        'target' => 'pgsql_app',

        'tables' => [
            'top_banners' => [
                'source' => [
                    'filters' => [
                        ['column' => 'active', 'operator' => '=', 'value' => 1],
                    ],
                    'chunk' => 1000,
                    'keyset_column' => 'id',
                ],

                'targets' => [
                    'catalog_banners' => [
                        'columns' => [
                            'id' => 'id',
                            'link' => 'link',
                        ],
                        'transforms' => [
                            'link' => ['trim', 'null_if_empty'],
                        ],
                        // Необязательные target-фильтры выполняются для каждого target
                        // после source-фильтров. Используйте их, чтобы направить
                        // в этот target только часть уже прочитанных source-строк.
                        'filters' => [
                            ['column' => 'link', 'operator' => 'not_null'],
                        ],
                        'upsert_keys' => ['id'],
                        'operation' => 'upsert',
                        'deduplicate' => ['keys' => ['id'], 'strategy' => 'last'],
                        'on_row_error' => 'fail', // fail|skip_row
                    ],
                ],
            ],
        ],
    ],
],
```

## Упорядоченные шаги

Миграция может содержать `steps`. Без `--step` команда выполняет все шаги по порядку; с `--step=name` — только один шаг.

```php
'migrations' => [
    'catalog' => [
        'source' => 'legacy_mysql',
        'target' => 'pgsql_app',
        'steps' => [
            'dimensions' => [
                'tables' => [
                    'legacy_brands' => ['brands' => ['id' => 'id', 'name' => 'name']],
                ],
            ],
            'facts' => [
                'tables' => [
                    'legacy_products' => ['products' => ['id' => 'id', 'brand_id' => 'brand_id']],
                ],
            ],
        ],
    ],
],
```

```bash
php artisan db:to-db --migration=catalog --step=dimensions
```

## Опции команды

| Опция | По умолчанию | Описание |
| --- | --- | --- |
| `--migration=` | `default` | Именованная миграция из `dbtodb_mapping.migrations`. |
| `--tables=` | все исходные таблицы выбранной миграции/шага | Список исходных таблиц через запятую. |
| `--step=` | все шаги | Запустить один шаг выбранной миграции. |
| `--source=` | `source` из миграции | Переопределить исходное подключение. |
| `--target=` | `target` из миграции | Переопределить целевое подключение. |
| `--dry-run` | выключено | Проверить и прочитать данные без записи. |
| `--continue-on-error` | выключено | Продолжать после ошибок отдельных pipelines. |
| `--report-file=` | файл с timestamp в `storage/logs` | Записать JSON-отчёт. |
| `--profile` | выключено | Писать тайминги в канал `dbtodb_mapping.profile_logging`. |

## Runtime и память

Глобальные значения находятся в `runtime.defaults`; миграция может переопределить их через `migrations.{name}.runtime.defaults`, а исходная таблица — через `chunk`, `transaction_mode` и `keyset_column` в своём блоке `source`.

| Ключ | По умолчанию | Назначение |
| --- | --- | --- |
| `runtime.defaults.chunk` | `500` | Количество строк, читаемых за один чанк. |
| `runtime.defaults.max_rows_per_upsert` | `500` | Ограничение строк в одном `insert`/`upsert`. Для PostgreSQL большие запросы также автоматически дробятся по лимиту 65535 placeholders. |
| `runtime.defaults.transaction_mode` | `'batch'` | `batch` оборачивает запись чанка в транзакцию. `atomic` оборачивает весь pipeline, но все targets должны использовать одно подключение. |
| `runtime.memory.memory_log_every_chunks` | `0` | Если `>0`, логирует потребление памяти каждые N чанков. |
| `runtime.memory.force_gc_every_chunks` | `20` | Вызывает `gc_collect_cycles()` каждые N чанков; `0` отключает. |
| `runtime.profile_slow_chunk_seconds` | `5.0` | Чанки медленнее этого значения помечаются `slow: true` в профильных логах. |
| `runtime.cli_memory_limit` | не задано | Если задано, передаётся в `ini_set('memory_limit', ...)` при старте команды. |

`keyset_column` включает keyset-пагинацию. Колонка должна быть доступна в source SELECT, сортируемой и желательно уникальной/монотонно возрастающей. Иначе строки могут пропускаться или дублироваться. Без `keyset_column` используется offset-пагинация.

## Автоматические трансформации

После пользовательских трансформаций пакет приводит значения к типам целевых колонок. Настройка находится в `auto_transforms`:

```php
'auto_transforms' => [
    'enabled' => true,
    'bool' => true,
    'integer' => true,
    'float' => true,
    'json' => true,
    'date' => true,
    'datetime' => true,
    'string' => false,
    'empty_string_to_null' => true,
    'json_invalid' => 'keep', // keep|null|fail
    'bool_columns' => [
        'users' => ['is_admin', 'is_active'],
    ],
],
```

Пустые строки превращаются в `null` только для nullable-колонок и только при включённом `empty_string_to_null`. Невалидные JSON-строки по умолчанию сохраняются как есть; для более строгого поведения используйте `null` или `fail`.

## Профильные логи и синхронизация sequence

```php
'profile_logging' => env('DB_TO_DB_LOG_CHANNEL', 'db_to_db'),

'sync_serial_sequences' => true,
'sync_serial_sequence_tables' => [
    'users',
    ['table' => 'orders', 'column' => 'order_id'],
],
```

- `--profile` пишет события старта/завершения, тайминги pipeline/chunk и снимки памяти в канал `profile_logging`. Канал нужно определить в `config/logging.php`.
- `sync_serial_sequences` после успешной записи выравнивает auto-increment / serial / identity счётчики. Поддерживаются `pgsql`, `mysql`/`mariadb`, `sqlite`, `sqlsrv`. В `--dry-run` синхронизация не выполняется.

## Фильтры

Source-фильтры поддерживают операторы `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not_in`, `like`, `not_like`, `null`, `not_null`, `between`, `not_between`, `exists_in`, `where_column`, `date`, `year`, `month`. Фильтры можно задавать простым ассоциативным массивом, списком правил, вложенными группами `and`/`or` или PHP callable, который получает `Illuminate\Database\Query\Builder`.

Source- и target-фильтры выполняются на разных этапах:

1. **Source-фильтры** применяются к SQL-запросу до чтения строк из source БД. Они уменьшают общий набор входных строк для всего pipeline исходной таблицы.
2. **Target-фильтры** выполняются в PHP отдельно для каждого target после того, как строка прошла source-фильтры, но до маппинга колонок и transforms target. Они не запрашивают target-таблицу и не видят уже преобразованные target-значения; проверяется исходная строка, прочитанная из source. Используйте target-фильтры для fan-out/маршрутизации: например, всех активных пользователей писать в `users`, а только source-строки с `is_admin = 1` — в `admins`.

Target-фильтры используют тот же DSL там, где его можно выполнить над PHP-массивом строки. `exists_in` работает только на SQL-стороне и для target-фильтров отклоняется валидатором. Callable-фильтры для builder предназначены только для source.

Пример target/PHP-фильтра. В target-описании эти правила фильтруют только для этого target строки, уже выбранные через `source.filters`:

```php
'targets' => [
    'paid_order_exports' => [
        'columns' => ['id' => 'id', 'status' => 'status', 'total' => 'total'],
        'filters' => [
            ['or' => [
                ['column' => 'status', 'operator' => '=', 'value' => 'paid'],
                ['column' => 'customer_email', 'operator' => 'like', 'value' => '%@example.com'],
            ]],
            ['column' => 'created_at', 'operator' => 'month', 'value' => 5],
            ['column' => 'total', 'operator' => 'where_column', 'value' => 'paid_total', 'comparison' => '<='],
        ],
    ],
],
```

## Трансформации колонок

Трансформации задаются по **целевой колонке** внутри target-описания.

```php
'targets' => [
    'users' => [
        'columns' => [
            'legacy_id' => 'id',
            'email_address' => 'email',
            'first_name' => 'name',
            'status_code' => 'status',
        ],
        'transforms' => [
            'email' => ['trim', 'lower', 'null_if_empty'],
            'name' => [
                ['rule' => 'from_columns', 'columns' => ['first_name', 'last_name'], 'separator' => ' '],
                ['rule' => 'default', 'value' => 'Anonymous'],
            ],
            'status' => ['rule' => 'map', 'map' => ['A' => 'active', 'B' => 'blocked'], 'default' => 'new'],
            'id' => ['rule' => 'cast', 'type' => 'int'],
        ],
    ],
],
```

Строковые правила: `trim`, `null_if_empty`, `zero_date_to_null`, `lower`, `upper`, `slug`.

Массивы-правила: `default`, `static`, `map`/`enum`, `replace`, `regex_replace`, `substr`, `lower`, `upper`, `slug`, `date_format`, `cast`, `concat`, `coalesce`, `from_columns`, `lookup`, `invoke`.

Closures и `invoke` получают текущий value, всю исходную строку, исходную колонку, целевую колонку и целевую таблицу: `(mixed $value, array $sourceRow, ?string $sourceColumn, ?string $targetColumn, ?string $targetTable) => mixed`. Двухаргументная форма `fn ($value, $row) => ...` также поддерживается.

`lookup` кэширует результаты на время одного запуска команды. Кэш растёт линейно от количества разных lookup-значений.

## Маршрутизация

### Fan-out: один source → несколько targets

```php
'tables' => [
    'legacy_users' => [
        'targets' => [
            'users' => [
                'columns' => ['id' => 'id', 'email' => 'email', 'is_admin' => 'is_admin'],
                'upsert_keys' => ['id'],
                'operation' => 'upsert',
            ],
            'admins' => [
                'columns' => ['id' => 'id', 'email' => 'email'],
                'filters' => [
                    ['column' => 'is_admin', 'operator' => '=', 'value' => 1],
                ],
                'operation' => 'insert',
            ],
        ],
    ],
],
```

Каждый target читает ту же исходную строку, но применяет свои фильтры, трансформации и операцию. Если один source пишет в несколько targets, каждый target должен объявить непустой `columns` map.

### Fan-in: несколько sources → один target

Для записи разных исходных таблиц в одну целевую таблицу удобно использовать `static`-трансформации, чтобы пометить источник строки.

```php
'tables' => [
    'legacy_users' => [
        'targets' => [
            'audit_log' => [
                'columns' => [
                    'id' => 'ref_id',
                    'name' => 'source_table',
                ],
                'transforms' => [
                    'source_table' => ['rule' => 'static', 'value' => 'users'],
                ],
                'operation' => 'insert',
            ],
        ],
    ],
],
```

Трансформации выполняются только для колонок, которые появились после column map. Поэтому для `static` нужно сначала связать любую исходную колонку с целевой, а затем заменить значение литералом.

## Важное поведение

- **Отсутствующая исходная колонка.** Если колонка из `columns` отсутствует в строке чанка, целевая колонка пропускается. В `strict` режиме обязательная целевая колонка без значения вызовет ошибку.
- **Дедупликация target.** `deduplicate => ['keys' => [...], 'strategy' => 'first'|'last']` схлопывает payload-строки по целевым колонкам перед записью.
- **Ошибки отдельных строк.** `on_row_error => 'skip_row'` после ошибки batch-записи переключается на построчную запись; неудачные строки считаются `skipped`.
- **Upsert-дедупликация.** Внутри одного чанка строки с одинаковыми `upsert_keys` дедуплицируются по принципу last-wins до SQL `UPSERT`.
- **`truncate_insert`.** Целевая таблица очищается один раз за запуск перед первой записью.
- **`atomic`.** Все targets pipeline должны использовать одно подключение; для разных подключений используйте `batch`.
- **`dry-run`.** Читает source, применяет фильтры/трансформации и strict-валидацию, но не открывает write-транзакцию и не запускает `sync_serial_sequences`.
- **`continue-on-error`.** Ошибки одного pipeline попадают в JSON-отчёт и таблицу результата, остальные pipelines продолжают выполняться.
- **Strict mode.** При `strict = true` каждая целевая колонка из `columns` должна существовать, а обязательные колонки должны получить значение.
- **PostgreSQL placeholder limit.** Слишком большие insert/upsert batches автоматически дробятся по лимиту 65535 placeholders.

## Разработка пакета

```bash
composer install
vendor/bin/phpunit
```

## Лицензия

MIT. См. [LICENSE](LICENSE).
