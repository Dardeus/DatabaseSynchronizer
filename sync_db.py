#!/usr/bin/env python3
import json
import os
import sys
import argparse
import logging
import time
from typing import Any

from sqlalchemy import create_engine, MetaData, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import DBAPIError


def get_args_from_env(args):
    if os.getenv('SOURCE_URL') and not args.source:
        args.source = os.getenv('SOURCE_URL')
    if os.getenv('TARGET_URL') and not args.target:
        args.target = os.getenv('TARGET_URL')
    if os.getenv('BATCH_SIZE') and not args.batch_size:
        args.batch_size = int(os.getenv('BATCH_SIZE'))
    return args


class DatabaseSynchronizer:
    def __init__(self, source_url: str, target_url: str, dry_run: bool = True, schema_only: bool = False):
        self.source_engine = create_engine(source_url)
        self.target_engine = create_engine(target_url)
        self.dry_run = dry_run
        self.schema_only = schema_only
        self.logger = self._setup_logger()
        self.batch_size = 1000

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('db_sync')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _reflect_schema(self, engine: Engine) -> MetaData:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        return metadata

    def _reflect_schema_with_conn(self, conn: Any) -> MetaData:
        metadata = MetaData()
        metadata.reflect(bind=conn)
        return metadata

    def _quote_ident(self, name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _column_definition(self, col) -> str:
        col_def = f'{self._quote_ident(col.name)} {col.type}'
        if not col.nullable:
            col_def += ' NOT NULL'
        if col.unique and not col.primary_key:
            col_def += ' UNIQUE'
        return col_def

    def _table_create_sql(self, source_table) -> str:
        cols_def = []
        pk_cols = []

        for col in source_table.columns:
            cols_def.append(self._column_definition(col))
            if col.primary_key:
                pk_cols.append(self._quote_ident(col.name))

        pk_clause = f", PRIMARY KEY ({', '.join(pk_cols)})" if pk_cols else ""
        return (
            f"CREATE TABLE IF NOT EXISTS {self._quote_ident(source_table.name)} "
            f"({', '.join(cols_def)}{pk_clause})"
        )

    def _log_execute(self, conn: Any, statement: str, params: Any = None) -> None:
        if self.dry_run:
            self.logger.info(f"[DRY RUN] {statement}")
        else:
            if params:
                conn.execute(text(statement), params)
            else:
                conn.execute(text(statement))

    def _foreign_key_exists(self, target_conn: Any, table_name: str, fk_name: str) -> bool:
        insp = inspect(target_conn)
        for fk in insp.get_foreign_keys(table_name):
            if fk.get("name") == fk_name:
                return True
        return False

    def _add_foreign_keys(self, target_conn: Any, source_meta: MetaData, target_meta: MetaData) -> None:
        self.logger.info("Добавление внешних ключей...")

        fk_created = 0
        processed = set()

        for source_table in source_meta.tables.values():
            table_name = source_table.name
            if table_name not in target_meta.tables:
                continue

            for col in source_table.columns:
                for fk in col.foreign_keys:
                    fk_name = f"fk_{table_name}_{col.name}"
                    sig = (table_name, col.name, fk.column.table.name, fk.column.name)

                    if sig in processed:
                        continue

                    if self._foreign_key_exists(target_conn, table_name, fk_name):
                        self.logger.info(f"FK {fk_name} уже существует, пропуск")
                        processed.add(sig)
                        continue

                    ref_table = self._quote_ident(fk.column.table.name)
                    ref_col = self._quote_ident(fk.column.name)

                    fk_sql = f"""
                        ALTER TABLE {self._quote_ident(table_name)}
                        ADD CONSTRAINT {self._quote_ident(fk_name)}
                        FOREIGN KEY ({self._quote_ident(col.name)})
                        REFERENCES {ref_table}({ref_col})
                    """

                    if fk.ondelete:
                        fk_sql += f" ON DELETE {fk.ondelete}"

                    fk_sql += " DEFERRABLE INITIALLY DEFERRED"

                    try:
                        self._log_execute(target_conn, fk_sql)
                        fk_created += 1
                        processed.add(sig)
                    except DBAPIError as e:
                        if getattr(e.orig, "pgcode", None) == "42710":
                            self.logger.info(f"FK {fk_name} уже существует, пропуск")
                            processed.add(sig)
                            continue
                        raise

        if not self.dry_run and fk_created:
            self.logger.info(f"Добавлено {fk_created} внешних ключей")

        self.logger.info("Добавление внешних ключей завершено")

    def run_sync(self) -> None:
        start_total = time.time()
        self.logger.info("Запуск синхронизации")

        with self.target_engine.connect() as target_conn:
            trans = target_conn.begin()
            try:
                source_meta = self._reflect_schema(self.source_engine)
                target_meta = self._reflect_schema_with_conn(target_conn)

                self.logger.info("Создание отсутствующих таблиц...")
                start_tables = time.time()

                for source_table in source_meta.tables.values():
                    table_name = source_table.name
                    if table_name in target_meta.tables:
                        continue

                    create_stmt = self._table_create_sql(source_table)
                    self._log_execute(target_conn, create_stmt)

                self.logger.info(f"Создание таблиц завершено за {time.time() - start_tables:.2f} сек")

                target_meta = self._reflect_schema_with_conn(target_conn)

                self.logger.info("Добавление отсутствующих колонок...")
                start_columns = time.time()

                for table_name, source_table in source_meta.tables.items():
                    if table_name not in target_meta.tables:
                        continue

                    target_table = target_meta.tables[table_name]
                    existing_cols = {c.name for c in target_table.columns}

                    for column in source_table.columns:
                        if column.name in existing_cols:
                            continue

                        alter_stmt = (
                            f"ALTER TABLE {self._quote_ident(table_name)} "
                            f"ADD COLUMN {self._quote_ident(column.name)} {column.type}"
                        )

                        if column.server_default is not None:
                            alter_stmt += f" DEFAULT {column.server_default.arg}"

                        self._log_execute(target_conn, alter_stmt)

                self.logger.info(f"Добавление колонок завершено за {time.time() - start_columns:.2f} сек")

                target_meta = self._reflect_schema_with_conn(target_conn)

                self._add_foreign_keys(target_conn, source_meta, target_meta)

                target_meta = self._reflect_schema_with_conn(target_conn)

                if not self.schema_only:
                    self.logger.info("Синхронизация данных...")
                    start_data = time.time()
                    with self.source_engine.connect() as source_conn:
                        for source_table in source_meta.tables.values():
                            table_name = source_table.name

                            if table_name not in target_meta.tables:
                                self.logger.warning(
                                    f"Таблица {table_name} отсутствует в целевой БД, синхронизация данных пропущена")
                                continue

                            pk_columns = [c.name for c in source_table.primary_key.columns]
                            if not pk_columns:
                                self.logger.warning(
                                    f"Таблица {table_name} не имеет первичного ключа, синхронизация данных пропущена")
                                continue

                            source_rows = source_conn.execute(source_table.select()).fetchall()
                            if not source_rows:
                                continue

                            rows = []
                            for row in source_rows:
                                row_dict = dict(row._mapping)
                                for col_name, value in row_dict.items():
                                    col_obj = source_table.columns[col_name]
                                    if isinstance(col_obj.type, JSONB) and isinstance(value, dict):
                                        row_dict[col_name] = json.dumps(value)
                                rows.append(row_dict)

                            if self.dry_run:
                                target_pks = set()
                                target_table = target_meta.tables[table_name]
                                pk_cols = [target_table.c[pk] for pk in pk_columns]
                                stmt = target_table.select().with_only_columns(*pk_cols)

                                for row in target_conn.execute(stmt):
                                    target_pks.add(tuple(row))

                                new_rows = 0
                                for row_dict in rows:
                                    pk_value = tuple(row_dict[pk] for pk in pk_columns)
                                    if pk_value not in target_pks:
                                        new_rows += 1

                                if new_rows:
                                    self.logger.info(f"[DRY RUN] Будет вставлено {new_rows} строк в {table_name}")
                            else:
                                col_names = ", ".join(self._quote_ident(k) for k in rows[0].keys())
                                placeholders = ", ".join([f":{k}" for k in rows[0].keys()])
                                pk_names = ", ".join(self._quote_ident(pk) for pk in pk_columns)

                                insert_sql = f"""
                                    INSERT INTO {self._quote_ident(table_name)} ({col_names})
                                    VALUES ({placeholders})
                                    ON CONFLICT ({pk_names}) DO NOTHING
                                """
                                inserted = 0
                                for i in range(0, len(rows), self.batch_size):
                                    batch = rows[i:i + self.batch_size]
                                    result = target_conn.execute(text(insert_sql), batch)
                                    inserted += result.rowcount

                                if inserted:
                                    self.logger.info(f"Вставлено {inserted} строк в {table_name}")

                    self.logger.info(f"Синхронизация данных завершена за {time.time() - start_data:.2f} сек")

                else:
                    self.logger.info("Синхронизация данных пропущена (режим --schema-only)")

                self._report_orphaned_objects(source_meta, target_meta)

                if self.dry_run:
                    trans.rollback()
                    self.logger.info("DRY RUN: транзакция откачена, изменения не применены.")
                else:
                    trans.commit()
                    self.logger.info("Транзакция успешно завершена.")

            except Exception as e:
                trans.rollback()
                self.logger.error(f"Синхронизация не удалась, транзакция откачена: {e}")
                raise

        self.logger.info(f"Полная синхронизация завершена за {time.time() - start_total:.2f} сек")

    def _report_orphaned_objects(self, source_meta: MetaData, target_meta: MetaData) -> None:
        self.logger.info("Проверка объектов, присутствующих только в целевой БД...")

        orphaned_tables = set(target_meta.tables.keys()) - set(source_meta.tables.keys())
        if orphaned_tables:
            self.logger.warning(f"Таблицы только в целевой БД: {orphaned_tables}")

        orphaned_columns = {}
        for table_name in set(target_meta.tables.keys()) & set(source_meta.tables.keys()):
            source_cols = {c.name for c in source_meta.tables[table_name].columns}
            target_cols = {c.name for c in target_meta.tables[table_name].columns}
            extra = target_cols - source_cols
            if extra:
                orphaned_columns[table_name] = list(extra)

        if orphaned_columns:
            self.logger.warning(f"Колонки только в целевой БД: {orphaned_columns}")


def main():
    parser = argparse.ArgumentParser(description='Синхронизация баз данных PostgreSQL')
    parser.add_argument('-s', '--source', help='URL исходной БД')
    parser.add_argument('-t', '--target', help='URL целевой БД')
    parser.add_argument('-d', '--dry-run', action='store_true', default=True,
                        help='Режим просмотра')
    parser.add_argument('-n', '--no-dry-run', action='store_false', dest='dry_run',
                        help='Режим синхронизации')
    parser.add_argument('-b', '--batch-size', type=int, default=1000,
                        help='Размер пакета для вставки строк')
    parser.add_argument('--schema-only', action='store_true',
                        help='Синхронизировать только схему')
    args = parser.parse_args()
    args = get_args_from_env(args)

    if not args.source or not args.target:
        parser.error("Необходимо указать SOURCE и TARGET (через CLI или переменные окружения)")

    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    syncer = DatabaseSynchronizer(args.source, args.target, dry_run=args.dry_run, schema_only=args.schema_only)
    syncer.batch_size = args.batch_size

    print("\n" + "=" * 70)
    print(f"Исходная БД: {args.source}")
    print(f"Целевая БД:  {args.target}")
    print(f"Режим: {'DRY RUN' if args.dry_run else 'SYNC'}")
    print(f"Режим синхронизации: {'только схема' if args.schema_only else 'схема + данные'}")
    print("=" * 70 + "\n")

    try:
        syncer.run_sync()
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
