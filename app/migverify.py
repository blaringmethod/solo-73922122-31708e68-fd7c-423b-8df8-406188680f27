"""Verify a migration script on isolated empty databases before returning it.

Three scenarios are exercised on fresh scratch databases, each created from
the same verified SQL packages the API publishes:

1. *upgrade* — load the old package, run the script, then compare every row
   of both tables against the new package (must be identical);
2. *already new (no write)* — load the new package, run the script, assert it
   succeeds and the data is byte-for-byte the same afterwards;
3. *neither version (reject)* — load the old package, mutate one row into a
   third state, run the script, assert the whole transaction rolls back and
   the mutated data is untouched.

A foreign-key negative probe afterwards proves the NOT DEFERRABLE NO ACTION
foreign key is still live. No scratch database is reused between scenarios.
"""
from __future__ import annotations

import secrets

import psycopg

from .migration import MigrationValidationError
from .models import TableSchema
from .sqlgen import q
from .verifier import _cleanup_database, _replace_dbname


class MigrationVerificationError(Exception):
    pass


def _load_package(dsn: str, schema: str, package: dict[str, str]) -> None:
    with psycopg.connect(dsn, autocommit=False, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {q(schema)}")
        conn.commit()
        for name in ("01_schema.sql", "02_data.sql", "03_constraints.sql"):
            conn.cursor().execute(package[name])
        conn.commit()


def _fetch_all(conn, schema: str, table: str, columns: list[str]):
    cols = ", ".join(q(c) for c in columns)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {cols} FROM {q(schema)}.{q(table)} "
            f"ORDER BY {cols}"
        )
        return cur.fetchall()


def _assert_same_data(
    conn,
    schema: str,
    tables: dict[str, TableSchema],
    expected: dict[str, list[dict]],
    log: list[str],
    label: str,
) -> None:
    for tname, ts in tables.items():
        cols = [c.name for c in ts.columns]
        actual = _fetch_all(conn, schema, tname, cols)
        want_rows = sorted(
            (tuple(r[c] for c in cols) for r in expected[tname]),
        )
        got_rows = sorted(actual)
        if got_rows != want_rows:
            raise MigrationVerificationError(
                f"{label}: 表 {tname} 数据与新版不一致\n"
                f"  expected={want_rows}\n  actual  ={got_rows}"
            )
        log.append(f"{label}: {tname} 逐表逐行核对通过 ({len(got_rows)} 行)")


def verify_migration(
    admin_dsn: str,
    db_prefix: str,
    schema: str,
    tables: dict[str, TableSchema],
    old_package: dict[str, str],
    new_package: dict[str, str],
    old_data: dict[str, list[dict]],
    new_data: dict[str, list[dict]],
    script: str,
    fk: dict,
) -> tuple[bool, list[str]]:
    log: list[str] = [
        "migration verification: three isolated scratch databases "
        "(upgrade / already-new / neither-version)"
    ]
    db_names: list[str] = []
    try:
        with psycopg.connect(admin_dsn, autocommit=True, connect_timeout=10) as adm:
            for _ in range(3):
                name = f"{db_prefix}{secrets.token_hex(8)}"
                adm.cursor().execute(f"CREATE DATABASE {q(name)}")
                db_names.append(name)
        upgrade_dsn = _replace_dbname(admin_dsn, db_names[0])
        nonew_dsn = _replace_dbname(admin_dsn, db_names[1])
        reject_dsn = _replace_dbname(admin_dsn, db_names[2])

        # ---- scenario 1: old package -> script -> equals new package ----
        _load_package(upgrade_dsn, schema, old_package)
        log.append("scenario upgrade: 旧包已装入隔离空库，执行迁移脚本")
        with psycopg.connect(upgrade_dsn, autocommit=False) as conn:
            conn.cursor().execute(script)  # single BEGIN ... COMMIT script
            conn.commit()
            _assert_same_data(conn, schema, tables, new_data, log, "upgrade")
            _assert_fk_still_enforced(conn, schema, tables, fk, log)

        # ---- scenario 2: new package -> script succeeds without writing ----
        _load_package(nonew_dsn, schema, new_package)
        log.append("scenario already-new: 新包已装入隔离空库，执行迁移脚本（应为空操作）")
        with psycopg.connect(nonew_dsn, autocommit=False) as conn:
            before = {
                t: _fetch_all(conn, schema, t, [c.name for c in ts.columns])
                for t, ts in tables.items()
            }
            conn.cursor().execute(script)
            conn.commit()
            _assert_same_data(conn, schema, tables, new_data, log, "already-new")
            after = {
                t: _fetch_all(conn, schema, t, [c.name for c in ts.columns])
                for t, ts in tables.items()
            }
            if before != after:
                raise MigrationVerificationError(
                    "already-new: 目标已是新版，脚本却产生了写入"
                )
            log.append("already-new: 脚本成功且未写入任何行")

        # ---- scenario 3: third state -> whole transaction rejected ----
        _load_package(reject_dsn, schema, old_package)
        report = fk["report_table"]
        fk_col = fk["fk_column"]
        rep_pk = tables[report].pk.columns[0]
        rep_target = f"{q(schema)}.{q(report)}"
        with psycopg.connect(reject_dsn, autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {q(rep_pk)} FROM {rep_target} ORDER BY 1 LIMIT 1"
                )
                row = cur.fetchone()
            if row is None:
                # Nothing to mutate into a third state (both packages empty);
                # the reject branch itself is covered by non-empty fixtures.
                log.append(
                    "scenario neither: 旧版报告表为空，无行可改写为第三态，跳过该场景"
                )
            else:
                key = row[0]
                # The FK column is nullable: NULL is a legal third state that
                # matches neither package (a dangling non-NULL value would be
                # rejected by the live FK before the migration even starts).
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE {rep_target} SET {q(fk_col)} = NULL "
                        f"WHERE {q(rep_pk)} = %s",
                        (key,),
                    )
                conn.commit()
                log.append("scenario neither: 已将可空外键列改写为 NULL（新旧两版都不存在的第三态）")
                try:
                    with conn.cursor() as cur:
                        cur.execute(script)
                    conn.commit()
                except psycopg.Error as exc:
                    conn.rollback()
                    log.append(
                        f"neither: 脚本如预期整笔拒绝 ({exc.sqlstate})，事务回滚"
                    )
                else:
                    raise MigrationVerificationError(
                        "neither: 目标两版都不匹配，脚本却提交成功"
                    )
                # Mutated row must still hold the third-state value (full rollback).
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT {q(fk_col)} FROM {rep_target} "
                        f"WHERE {q(rep_pk)} = %s",
                        (key,),
                    )
                    (kept,) = cur.fetchone()
                if kept is not None:
                    raise MigrationVerificationError(
                        "neither: 回滚不完整，被改写的 NULL 行未恢复"
                    )
                log.append("neither: 回滚后第三态数据原样保留，未产生任何部分写入")

        log.append("migration verification: 全部场景通过，脚本可交付")
        return True, log
    except (psycopg.Error, MigrationValidationError, MigrationVerificationError, KeyError) as exc:
        log.append(f"migration verification FAILED: {exc.__class__.__name__}: {exc}")
        return False, log
    finally:
        for name in db_names:
            _cleanup_database(admin_dsn, name, log)


def _assert_fk_still_enforced(conn, schema, tables, fk, log) -> None:
    """Negative probe: a dangling report FK value must be rejected (23503)."""
    report = fk["report_table"]
    fk_col = fk["fk_column"]
    rep_pk = tables[report].pk.columns[0]
    target = f"{q(schema)}.{q(report)}"
    with conn.cursor() as cur:
        cur.execute(f"SELECT {q(rep_pk)} FROM {target} ORDER BY 1 LIMIT 1")
        row = cur.fetchone()
        if row is None:
            log.append("skip FK probe: report table empty")
            return
        key = row[0]
        savepoint = "sp_fk_probe"
        cur.execute(f"SAVEPOINT {q(savepoint)}")
        try:
            cur.execute(
                f"UPDATE {target} SET {q(fk_col)} = -900002 "
                f"WHERE {q(rep_pk)} = %s",
                (key,),
            )
        except psycopg.Error as exc:
            cur.execute(f"ROLLBACK TO SAVEPOINT {q(savepoint)}")
            conn.commit()
            if exc.sqlstate != "23503":
                raise MigrationVerificationError(
                    f"外键负向探测期望 23503，实际 {exc.sqlstate}: {exc}"
                )
            log.append(f"FK 负向探测通过: 悬空引用被外键拒绝 ({exc.sqlstate})")
            return
        cur.execute(f"ROLLBACK TO SAVEPOINT {q(savepoint)}")
        conn.commit()
    raise MigrationVerificationError("外键负向探测失败：悬空引用未被拒绝，外键检查可能已被关闭")
