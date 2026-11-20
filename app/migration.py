"""Incremental migration script generation between two succeeded sample jobs.

Scope (explicitly enforced, nothing outside it is silently accepted):

* exactly two tables, each with a single-column integer primary key and no
  other key (UNIQUE) constraints;
* one nullable, NOT DEFERRABLE, ON DELETE NO ACTION single-column foreign key
  from the *report* table to the *registry* table, and no other foreign keys;
* both jobs describe the identical structure (same columns, types, order,
  nullability, primary key and foreign key).

The generated PostgreSQL script runs in a single transaction and:

1. takes ``SHARE ROW EXCLUSIVE`` locks on both tables up front, so a
   concurrent writer cannot insert a row between the check and the update
   (statement/lock timeouts are set so a blocked script fails rather than
   waiting forever);
2. materialises the full old/new snapshots in ``TEMP`` tables and compares
   every column of every row with ``EXCEPT ALL`` — a complete-data check of
   both tables inside the same transaction;
3. succeeds without writing anything when the target already equals the new
   version, performs the rendered INSERT/UPDATE/DELETE set only when it
   equals the old version, and rejects the whole transaction otherwise.

Foreign-key checks are never disabled: statement order keeps the live
NOT DEFERRABLE NO ACTION foreign key satisfied at every step.
"""
from __future__ import annotations

from dataclasses import dataclass

from .models import TableSchema
from .sqlgen import q

#: Lock acquired on both tables for the whole transaction. It conflicts with
#: every row-writing lock (ROW EXCLUSIVE), so no concurrent INSERT/UPDATE/
#: DELETE can run between the check and the DML. Reads remain allowed.
_TABLE_LOCK = "SHARE ROW EXCLUSIVE"
_LOCK_TIMEOUT = "5s"
_STATEMENT_TIMEOUT = "60s"


class MigrationValidationError(Exception):
    """The two jobs do not form a supported old/new migration pair."""


@dataclass(frozen=True)
class TableDiff:
    inserts: list[dict]
    updates: list[dict]
    deletes: list[dict]

    @property
    def counts(self) -> dict[str, int]:
        return {
            "insert": len(self.inserts),
            "update": len(self.updates),
            "delete": len(self.deletes),
        }


# --------------------------------------------------------------------------- #
# Structural validation
# --------------------------------------------------------------------------- #
def _check_table_shape(ts: TableSchema, problems: list[str]) -> None:
    label = ts.full_name
    pk = ts.pk
    if pk is None:
        problems.append(f"{label}: 缺少单列整数主键")
    elif len(pk.columns) != 1:
        problems.append(f"{label}: 主键必须是单列，实际为 {len(pk.columns)} 列复合主键")
    else:
        pk_col = ts.column(pk.columns[0])
        if pk_col.pg_type not in ("smallint", "integer", "bigint"):
            problems.append(f"{label}: 主键列必须是整数类型，实际为 {pk_col.pg_type}")
    other_uk = [u for u in ts.unique_constraints if not u.is_primary]
    if other_uk:
        problems.append(
            f"{label}: 除主键外不允许其他键约束，发现唯一键 "
            f"{', '.join(u.name for u in other_uk)}"
        )


def _validate_pair(
    old: dict, new: dict
) -> tuple[str, str, dict[str, TableSchema]]:
    problems: list[str] = []
    if old["schema_name"] != new["schema_name"]:
        raise MigrationValidationError(
            f"两个作业的 schema 不同: {old['schema_name']} != {new['schema_name']}"
        )
    if set(old["schemas"]) != set(new["schemas"]):
        raise MigrationValidationError(
            f"两版表集合不同: 旧版 {sorted(old['schemas'])} / 新版 {sorted(new['schemas'])}"
        )
    old_tables: dict[str, TableSchema] = old["schemas"]
    new_tables: dict[str, TableSchema] = new["schemas"]
    if len(old_tables) != 2:
        raise MigrationValidationError(
            f"增量迁移仅处理登记表和报告表两张表，实际 {len(old_tables)} 张: "
            f"{sorted(old_tables)}"
        )

    for name, ts in old_tables.items():
        _check_table_shape(ts, problems)
        _check_table_shape(new_tables[name], problems)

    # Identical structure across versions: columns (name/type/order/nullable),
    # primary key and foreign-key definition.
    for name, old_ts in old_tables.items():
        new_ts = new_tables[name]
        old_cols = [(c.name, c.pg_type, c.ordinal, c.nullable) for c in old_ts.columns]
        new_cols = [(c.name, c.pg_type, c.ordinal, c.nullable) for c in new_ts.columns]
        if old_cols != new_cols:
            problems.append(
                f"{old_ts.full_name}: 两版列结构不一致 "
                f"旧 {old_cols} / 新 {new_cols}"
            )
        old_pk = [u for u in old_ts.unique_constraints if u.is_primary]
        new_pk = [u for u in new_ts.unique_constraints if u.is_primary]
        if [u.columns for u in old_pk] != [u.columns for u in new_pk]:
            problems.append(f"{old_ts.full_name}: 两版主键定义不一致")
        if [f.model_dump() for f in old_ts.foreign_keys] != [
            f.model_dump() for f in new_ts.foreign_keys
        ]:
            problems.append(f"{old_ts.full_name}: 两版外键定义不一致")

    # Exactly one FK overall: report -> registry, nullable single column,
    # NOT DEFERRABLE, ON DELETE NO ACTION; the registry has no FK at all.
    fk_owners = {name: ts.foreign_keys for name, ts in old_tables.items()}
    total_fk = sum(len(v) for v in fk_owners.values())
    if total_fk != 1:
        owners = {n: len(v) for n, v in fk_owners.items()}
        raise MigrationValidationError(
            f"两表之间必须恰好存在一条外键，实际 {total_fk} 条 ({owners})"
        )
    report = next(n for n, fks in fk_owners.items() if fks)
    registry = next(n for n in fk_owners if n != report)
    fk = fk_owners[report][0]

    if fk.ref_table != registry:
        problems.append(
            f"报告表 {report} 的外键必须引用登记表 {registry}，实际引用 {fk.ref_table}"
        )
    if len(fk.columns) != 1 or len(fk.ref_columns) != 1:
        problems.append(f"外键 {fk.name}: 必须是单列外键")
    if not fk.not_deferrable:
        problems.append(f"外键 {fk.name}: 必须不可延迟 (NOT DEFERRABLE)")
    if fk.on_delete != "NO ACTION":
        problems.append(f"外键 {fk.name}: ON DELETE 必须为 NO ACTION，实际 {fk.on_delete}")
    fk_col = old_tables[report].column(fk.columns[0])
    if not fk_col.nullable:
        problems.append(f"外键列 {fk_col.name} 必须可空")
    ref_pk = old_tables[registry].pk
    if ref_pk is None or fk.ref_columns != ref_pk.columns:
        problems.append(
            f"外键 {fk.name}: 必须引用登记表 {registry} 的主键列"
        )
    if problems:
        raise MigrationValidationError("; ".join(problems))
    return registry, report, old_tables


# --------------------------------------------------------------------------- #
# Row-level diff keyed by the single-column integer primary key
# --------------------------------------------------------------------------- #
def _diff_table(
    ts: TableSchema, old_rows: list[dict], new_rows: list[dict]
) -> TableDiff:
    pk_col = ts.pk.columns[0]
    old_by_key = {r[pk_col]: r for r in old_rows}
    new_by_key = {r[pk_col]: r for r in new_rows}
    if len(old_by_key) != len(old_rows):
        raise MigrationValidationError(f"{ts.full_name}: 旧版数据存在重复主键")
    if len(new_by_key) != len(new_rows):
        raise MigrationValidationError(f"{ts.full_name}: 新版数据存在重复主键")

    inserts = [new_by_key[k] for k in new_by_key.keys() - old_by_key.keys()]
    deletes = [old_by_key[k] for k in old_by_key.keys() - new_by_key.keys()]
    updates = [
        new_by_key[k]
        for k in new_by_key.keys() & old_by_key.keys()
        if new_by_key[k] != old_by_key[k]
    ]
    # Deterministic statement order.
    inserts.sort(key=lambda r: r[pk_col])
    deletes.sort(key=lambda r: r[pk_col])
    updates.sort(key=lambda r: r[pk_col])
    return TableDiff(inserts=inserts, updates=updates, deletes=deletes)


# --------------------------------------------------------------------------- #
# SQL rendering helpers
# --------------------------------------------------------------------------- #
def _lit(value) -> str:
    return "NULL" if value is None else str(int(value))


def _full_name(schema: str, table: str) -> str:
    return f"{q(schema)}.{q(table)}"


def _column_names(ts: TableSchema) -> str:
    return ", ".join(q(c.name) for c in ts.columns)


def _values_aliases(ts: TableSchema) -> str:
    # Neutral inner aliases: reusing the target column names here would make
    # the outer SELECT resolve "col" to its own output column instead of the
    # VALUES column, silently breaking the typed comparison.
    return ", ".join(f"c{i}" for i in range(len(ts.columns)))


def _values_row(row: dict, ts: TableSchema) -> str:
    return "(" + ", ".join(_lit(row[c.name]) for c in ts.columns) + ")"


def _typed_values_select(ts: TableSchema) -> str:
    # VALUES column aliases only accept names, so typing happens via an
    # explicit ::cast on every column of the outer SELECT.
    return ", ".join(
        f"c{i}::{c.pg_type} AS {q(c.name)}"
        for i, c in enumerate(ts.columns)
    )


def _render_snapshot(
    temp_name: str, ts: TableSchema, rows: list[dict]
) -> list[str]:
    out = [f"DROP TABLE IF EXISTS {q(temp_name)};"]
    if not rows:
        # Empty snapshot: create the temp table from the target's structure so
        # the EXCEPT ALL comparison still has a typed relation to work with.
        out.append(
            f"CREATE TEMP TABLE {q(temp_name)} (LIKE {_full_name(ts.schema_name, ts.name)});"
        )
        return out
    out.append(
        f"CREATE TEMP TABLE {q(temp_name)} AS "
        f"SELECT {_typed_values_select(ts)} FROM (VALUES"
    )
    out.append(",\n".join("        " + _values_row(r, ts) for r in rows))
    out.append(f"    ) AS v({_values_aliases(ts)});")
    return out


def _render_identity_check(
    temp_name: str, ts: TableSchema
) -> list[str]:
    """Full-data identity: true when target and snapshot are exactly equal."""
    target = _full_name(ts.schema_name, ts.name)
    cols = ", ".join(q(c.name) for c in ts.columns)
    return [
        f"IF NOT EXISTS (SELECT {cols} FROM {target} "
        f"EXCEPT ALL SELECT {cols} FROM {q(temp_name)}) "
        f"AND NOT EXISTS (SELECT {cols} FROM {q(temp_name)} "
        f"EXCEPT ALL SELECT {cols} FROM {target}) THEN",
    ]


def _render_insert(ts: TableSchema, rows: list[dict]) -> list[str]:
    if not rows:
        return []
    cols = ", ".join(q(c.name) for c in ts.columns)
    target = _full_name(ts.schema_name, ts.name)
    return [
        f"INSERT INTO {target} ({cols}) VALUES",
        ",\n".join("        " + _values_row(r, ts) for r in rows) + ";",
    ]


def _render_updates(ts: TableSchema, rows: list[dict]) -> list[str]:
    pk_col = ts.pk.columns[0]
    target = _full_name(ts.schema_name, ts.name)
    out: list[str] = []
    for row in rows:
        sets = [
            f"{q(c.name)} = {_lit(row[c.name])}"
            for c in ts.columns
            if c.name != pk_col
        ]
        if not sets:
            continue
        out.append(
            f"UPDATE {target} SET {', '.join(sets)} "
            f"WHERE {q(pk_col)} = {_lit(row[pk_col])};"
        )
    return out


def _render_deletes(ts: TableSchema, rows: list[dict]) -> list[str]:
    pk_col = ts.pk.columns[0]
    target = _full_name(ts.schema_name, ts.name)
    return [
        f"DELETE FROM {target} WHERE {q(pk_col)} = {_lit(row[pk_col])};"
        for row in rows
    ]


def _render_dml_block(
    reg_ts: TableSchema,
    rep_ts: TableSchema,
    reg_diff: TableDiff,
    rep_diff: TableDiff,
) -> tuple[list[str], bool]:
    """Render every DML statement in an order that keeps the live, NOT
    DEFERRABLE, ON DELETE NO ACTION foreign key satisfied at every step:

    1. UPDATE report rows in place (keys stay present in the registry);
    2. INSERT new registry rows (new parent keys exist before any child use);
    3. UPDATE registry rows (the report FK column is never changed here, so
       existing references remain valid);
    4. INSERT new report rows (all referenced parent keys now exist);
    5. DELETE report rows that disappear (releases references first);
    6. DELETE registry rows that disappear (only unreferenced rows remain).

    The order is derived from the structural rule that the report's FK is
    single-column, nullable and NO ACTION; constraints are never disabled.
    """
    sections: list[tuple[str, list[str]]] = [
        ("-- 1) report rows updated in place", _render_updates(rep_ts, rep_diff.updates)),
        ("-- 2) new registry rows", _render_insert(reg_ts, reg_diff.inserts)),
        ("-- 3) registry rows updated in place", _render_updates(reg_ts, reg_diff.updates)),
        ("-- 4) new report rows", _render_insert(rep_ts, rep_diff.inserts)),
        ("-- 5) report rows removed (release FK references first)", _render_deletes(rep_ts, rep_diff.deletes)),
        ("-- 6) registry rows removed (now unreferenced; NO ACTION holds)", _render_deletes(reg_ts, reg_diff.deletes)),
    ]
    lines: list[str] = []
    for header, stmts in sections:
        if stmts:
            lines.append(header)
            lines.extend(stmts)
    changed = any(
        (d.inserts or d.updates or d.deletes) for d in (reg_diff, rep_diff)
    )
    return lines, changed


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #
def build_migration(old_art: dict, new_art: dict) -> dict:
    """Build the migration script and its summary from two job artifacts."""
    registry, report, schemas = _validate_pair(old_art, new_art)
    reg_ts = schemas[registry]
    rep_ts = schemas[report]

    reg_diff = _diff_table(reg_ts, old_art["data"][registry], new_art["data"][registry])
    rep_diff = _diff_table(rep_ts, old_art["data"][report], new_art["data"][report])
    changes = {
        registry: reg_diff.counts,
        report: rep_diff.counts,
    }
    schema = old_art["schema_name"]
    script = _render_script(
        schema, registry, report, reg_ts, rep_ts,
        old_art, new_art, reg_diff, rep_diff,
    )
    return {
        "schema_name": schema,
        "registry_table": registry,
        "report_table": report,
        "changes": changes,
        "script": script,
    }


def _render_script(
    schema: str,
    registry: str,
    report: str,
    reg_ts: TableSchema,
    rep_ts: TableSchema,
    old_art: dict,
    new_art: dict,
    reg_diff: TableDiff,
    rep_diff: TableDiff,
) -> str:
    reg_target = _full_name(schema, registry)
    rep_target = _full_name(schema, report)
    lines: list[str] = [
        "-- ============================================================",
        "-- Incremental in-place migration: old sample -> new sample",
        f"-- registry table: {schema}.{registry} (single-column integer PK)",
        f"-- report table:   {schema}.{report} (nullable NOT DEFERRABLE "
        "ON DELETE NO ACTION FK)",
        "-- Single transaction; FK checks stay enabled for every statement.",
        "-- Lock mode makes the check-then-update window invisible to writers.",
        "-- ============================================================",
        "BEGIN;",
        f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}';",
        f"SET LOCAL statement_timeout = '{_STATEMENT_TIMEOUT}';",
        # Lock both tables first: any concurrent INSERT/UPDATE/DELETE now blocks
        # until this transaction ends, so it cannot land between check and DML.
        f"LOCK TABLE {reg_target}, {rep_target} IN {_TABLE_LOCK} MODE;",
        "DO $migration$",
        "DECLARE",
        "    v_old_reg   boolean;",
        "    v_old_rep   boolean;",
        "    v_new_reg   boolean;",
        "    v_new_rep   boolean;",
        "BEGIN",
    ]

    # ---- materialise both full snapshots of both tables (old/new) ----
    for temp, ts, art in (
        ("tmp_old_reg", reg_ts, old_art),
        ("tmp_old_rep", rep_ts, old_art),
        ("tmp_new_reg", reg_ts, new_art),
        ("tmp_new_rep", rep_ts, new_art),
    ):
        for ln in _render_snapshot(temp, ts, art["data"][ts.name]):
            lines.append("    " + ln)

    # ---- complete-data identity checks inside the same transaction ----
    lines.append("")
    lines.append("    -- full-data comparison (every column, every row)")
    for temp, ts in (
        ("tmp_old_reg", reg_ts),
        ("tmp_old_rep", rep_ts),
        ("tmp_new_reg", reg_ts),
        ("tmp_new_rep", rep_ts),
    ):
        check_lines = _render_identity_check(temp, ts)
        var = {
            "tmp_old_reg": "v_old_reg",
            "tmp_old_rep": "v_old_rep",
            "tmp_new_reg": "v_new_reg",
            "tmp_new_rep": "v_new_rep",
        }[temp]
        lines.append("    " + check_lines[0])
        lines.append(f"        {var} := true;")
        lines.append("    ELSE")
        lines.append(f"        {var} := false;")
        lines.append("    END IF;")

    lines += [
        "",
        "    IF v_new_reg AND v_new_rep THEN",
        "        -- Target already is the new version: success without writes.",
        f"        RAISE NOTICE 'migration skipped: {schema}.{registry} and "
        f"{schema}.{report} already match the new version';",
        "    ELSIF v_old_reg AND v_old_rep THEN",
        "        -- Target matches the old version exactly: apply the diff.",
    ]

    # ---- DML ordered across both tables to keep the live FK satisfied ----
    dml_lines, has_changes = _render_dml_block(
        reg_ts, rep_ts, reg_diff, rep_diff
    )
    if not has_changes:
        lines.append("        -- old and new versions contain identical rows; nothing to do")
    for ln in dml_lines:
        lines.append("        " + ln)

    lines += [
        "        -- Re-check the complete target data against the new snapshot.",
        "        IF EXISTS (SELECT "
        + ", ".join(q(c.name) for c in reg_ts.columns)
        + f" FROM {reg_target} EXCEPT ALL SELECT "
        + ", ".join(q(c.name) for c in reg_ts.columns)
        + " FROM " + q("tmp_new_reg") + ")",
        "            OR EXISTS (SELECT "
        + ", ".join(q(c.name) for c in reg_ts.columns)
        + " FROM " + q("tmp_new_reg") + " EXCEPT ALL SELECT "
        + ", ".join(q(c.name) for c in reg_ts.columns)
        + f" FROM {reg_target})",
        "            OR EXISTS (SELECT "
        + ", ".join(q(c.name) for c in rep_ts.columns)
        + f" FROM {rep_target} EXCEPT ALL SELECT "
        + ", ".join(q(c.name) for c in rep_ts.columns)
        + " FROM " + q("tmp_new_rep") + ")",
        "            OR EXISTS (SELECT "
        + ", ".join(q(c.name) for c in rep_ts.columns)
        + " FROM " + q("tmp_new_rep") + " EXCEPT ALL SELECT "
        + ", ".join(q(c.name) for c in rep_ts.columns)
        + f" FROM {rep_target}) THEN",
        "            RAISE EXCEPTION 'migration failed: post-change data does "
        "not match the new version' USING ERRCODE = 'integrity_constraint_violation';",
        "        END IF;",
        "        RAISE NOTICE 'migration applied: old -> new';",
        "    ELSE",
        "        -- Neither version matches: reject the whole transaction.",
        f"        RAISE EXCEPTION 'migration rejected: target data matches neither "
        f"the old nor the new version of {schema}.{registry}/{schema}.{report}' "
        "USING ERRCODE = 'integrity_constraint_violation';",
        "    END IF;",
        "END $migration$;",
        "COMMIT;",
        "",
    ]
    return "\n".join(lines)
