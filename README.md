# Database Synchronizer

Простой Docker-инструмент для синхронизации PostgreSQL баз в режиме **ADD-ONLY**.

## Возможности

- Только добавление (не удаляет и не перезаписывает данные)
- Режим `dry-run` для безопасного просмотра
- Режим `--schema-only` (перенос только структуры, без данных)

## Быстрый старт

### 1. Создайте `.env`:

```env
SOURCE_URL=postgresql://user:password@host:5432/source_db
TARGET_URL=postgresql://user:password@host:5432/target_db
BATCH_SIZE=1000
```
> **Примечание:** 
> Если базы находятся на той же машине, используйте `host.docker.internal` (Windows/Mac) или `172.17.0.1` (Linux).
> #### Пример для Windows:
> SOURCE_URL=postgresql://postgres:postgres@host.docker.internal:5432/test_db
> TARGET_URL=postgresql://postgres:postgres@host.docker.internal:5432/prod_db

### 2. Соберите образ
```
python run.py build
```

### 3. Просмотрите будущие изменения
```
python run.py dry-run
```
### 4. Выполните синхронизацию
```
python run.py sync
```
## Режимы синхронизации
```
# Просмотр изменений (схема + данные)
python run.py dry-run
```

```
# Полная синхронизация (схема + данные)
python run.py sync	
```

```
# Просмотр только изменений схемы
python run.py dry-run --schema-only	
```

```
# Синхронизация только схемы (без данных)
python run.py sync --schema-only	
```
## Ручной запуск
```
# Режим просмотра
docker compose run --rm sync --dry-run

# Режим просмотра только схемы
docker compose run --rm sync --dry-run --schema-only

# Cинхронизация
docker compose run --rm sync --no-dry-run

# Синхронизация только схемы
docker compose run --rm sync --no-dry-run --schema-only
```

## CLI
```
python sync_db.py --source <SOURCE_URL> --target <TARGET_URL> [--dry-run|--no-dry-run] [--batch-size 1000] [--schema-only]
```