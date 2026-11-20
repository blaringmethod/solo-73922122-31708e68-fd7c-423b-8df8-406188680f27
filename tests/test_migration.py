"""Tests for the incremental registry/report migration feature.

Covers:

* structural validation (shape, FK properties, two-version identity);
* row-level diff and FK-safe DML rendering;
* the HTTP API (status checks, duplicate ids, 422/409 paths);
* end-to-end isolated-database verification of upgrade / already-new /
  neither-version, including a live foreign-key negative probe;
* concurrency: a writer is blocked by the migration lock and cannot land a
  row between the check and the update.
"""
from __future__ import annotations

import json
import threading

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.jobs import STORE, run_job
from app.main import app
from app.migration import (
    MigrationValidationError,
    _diff_table,
    build_migration,
)
from app.migverify import verify_migration
from app.models import GenerateRequest, JobStatus

client = TestClient(app)


def _load(name):
    raw = json.load(open(f"/workspace/examples/{name}"))
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def _run(name):
    jid = STORE.create()
    run_job(jid, GenerateRequest(**_load(name)))
    job = STORE.get(jid)
    assert job.status == JobStatus.SUCCEEDED, job.conflict
    return jid


@pytest.fixture(scope="module")
def old_new_jobs():
    old_id = _run("request_registry_report_old.json")
    new_id = _run("request_registry_report_new.json")
    return old_id, new_id


# --------------------------------------------------------------------------- #
# Structural validation
# --------------------------------------------------------------------------- #
def _clone(art):
    import copy

    return copy.deepcopy(art)


def test_pair_must_have_exactly_two_tables(old_new_jobs):
    old_id, new_id = old_new_jobs
    old_art, new_art = _clone(STORE.artifacts(old_id)), _clone(STORE.artifacts(new_id))
    extra_name = next(iter(old_art["schemas"]))
    for art in (old_art, new_art):
        art["schemas"]["extra_table"] = art["schemas"][extra_name]
        art["data"]["extra_table"] = []
    with pytest.raises(MigrationValidationError, match="两张表"):
        build_migration(old_art, new_art)


def test_rejects_composite_pk(old_new_jobs):
    old_id, new_id = old_new_jobs
    old_art, new_art = _clone(STORE.artifacts(old_id)), _clone(STORE.artifacts(new_id))
    for art in (old_art, new_art):
        ts = art["schemas"]["dw_registry"]
        ts.unique_constraints[0].columns = ["reg_id", "region_code"]
    with pytest.raises(MigrationValidationError, match="单列"):
        build_migration(old_art, new_art)


def test_rejects_extra_unique_constraint(old_new_jobs):
    old_id, new_id = old_new_jobs
    old_art, new_art = _clone(STORE.artifacts(old_id)), _clone(STORE.artifacts(new_id))
    from app.models import UniqueConstraint

    for art in (old_art, new_art):
        art["schemas"]["dw_registry"].unique_constraints.append(
            UniqueConstraint(
                name="extra_uk", columns=["load_batch"], is_primary=False
            )
        )
    with pytest.raises(MigrationValidationError, match="其他键约束"):
        build_migration(old_art, new_art)


def test_rejects_when_structure_differs_between_versions(old_new_jobs):
    old_id, new_id = old_new_jobs
    old_art, new_art = _clone(STORE.artifacts(old_id)), _clone(STORE.artifacts(new_id))
    new_art["schemas"]["dw_registry"].columns[0].pg_type = "bigint"
    with pytest.raises(MigrationValidationError, match="列结构不一致"):
        build_migration(old_art, new_art)


def test_rejects_deferrable_or_wrong_on_delete(old_new_jobs):
    old_id, new_id = old_new_jobs
    # deferrable
    old_art, new_art = _clone(STORE.artifacts(old_id)), _clone(STORE.artifacts(new_id))
    for art in (old_art, new_art):
        art["schemas"]["dw_report"].foreign_keys[0].not_deferrable = False
    with pytest.raises(MigrationValidationError, match="不可延迟"):
        build_migration(old_art, new_art)
    # wrong ON DELETE
    old_art, new_art = _clone(STORE.artifacts(old_id)), _clone(STORE.artifacts(new_id))
    for art in (old_art, new_art):
        art["schemas"]["dw_report"].foreign_keys[0].on_delete = "CASCADE"
    with pytest.raises(MigrationValidationError, match="ON DELETE"):
        build_migration(old_art, new_art)


def test_rejects_non_nullable_fk_column(old_new_jobs):
    old_id, new_id = old_new_jobs
    old_art, new_art = _clone(STORE.artifacts(old_id)), _clone(STORE.artifacts(new_id))
    for art in (old_art, new_art):
        art["schemas"]["dw_report"].column("reg_id").nullable = False
    with pytest.raises(MigrationValidationError, match="可空"):
        build_migration(old_art, new_art)


def test_rejects_pair_with_two_foreign_keys(old_new_jobs):
    old_id, new_id = old_new_jobs
    old_art, new_art = _clone(STORE.artifacts(old_id)), _clone(STORE.artifacts(new_id))
    extra = old_art["schemas"]["dw_report"].foreign_keys[0].model_copy(deep=True)
    extra.name = "second_fk"
    for art in (old_art, new_art):
        art["schemas"]["dw_registry"].foreign_keys.append(extra)
    with pytest.raises(MigrationValidationError, match="恰好存在一条外键"):
        build_migration(old_art, new_art)


# --------------------------------------------------------------------------- #
# Diff + rendering
# --------------------------------------------------------------------------- #
def test_diff_classifies_insert_update_delete(old_new_jobs):
    old_id, new_id = old_new_jobs
    old_art, new_art = STORE.artifacts(old_id), STORE.artifacts(new_id)
    ts = old_art["schemas"]["dw_report"]
    diff = _diff_table(ts, old_art["data"]["dw_report"], new_art["data"]["dw_report"])
    assert diff.counts == {"insert": 5, "update": 0, "delete": 4}
    # same input -> empty diff
    same = _diff_table(ts, old_art["data"]["dw_report"], old_art["data"]["dw_report"])
    assert same.counts == {"insert": 0, "update": 0, "delete": 0}


def test_script_is_single_transaction_with_locks_and_keeps_fk(old_new_jobs):
    old_id, new_id = old_new_jobs
    plan = build_migration(STORE.artifacts(old_id), STORE.artifacts(new_id))
    script = plan["script"]
    assert script.count("BEGIN;") == 1 and script.rstrip().endswith("COMMIT;")
    assert "SHARE ROW EXCLUSIVE" in script
    assert "LOCK TABLE" in script
    # FK stays live: never disabled, never made deferrable. Check executable
    # lines only (the header comment mentions deferrability descriptively).
    executable = "\n".join(
        ln for ln in script.splitlines() if not ln.lstrip().startswith("--")
    )
    assert "DISABLE TRIGGER" not in executable
    assert "DEFERRABLE" not in executable
    assert "SET CONSTRAINTS" not in executable
    # full data comparison on both tables, both directions
    assert script.count("EXCEPT ALL") >= 8
    assert "tmp_old_reg" in script and "tmp_new_rep" in script
    # explicit reject branch
    assert "migration rejected" in script
    assert "already match the new version" in script


# --------------------------------------------------------------------------- #
# HTTP API
# --------------------------------------------------------------------------- #
def test_migration_endpoint_happy_path(old_new_jobs):
    old_id, new_id = old_new_jobs
    r = client.post("/migrations", json={"old_job_id": old_id, "new_job_id": new_id})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["registry_table"] == "dw_registry"
    assert body["report_table"] == "dw_report"
    assert set(body["changes"]) == {"dw_registry", "dw_report"}
    assert "BEGIN;" in body["script"] and body["script"].rstrip().endswith("COMMIT;")
    assert any("全部场景通过" in line for line in body["verify_log"])
    assert r.headers["X-Migration-Verify"] == "passed"


def test_migration_rejects_same_job_id(old_new_jobs):
    old_id, _ = old_new_jobs
    r = client.post("/migrations", json={"old_job_id": old_id, "new_job_id": old_id})
    assert r.status_code == 422


def test_migration_unknown_job_404():
    r = client.post(
        "/migrations", json={"old_job_id": "nope1", "new_job_id": "nope2"}
    )
    assert r.status_code == 404


def test_migration_rejects_unfinished_job(old_new_jobs):
    old_id, _ = old_new_jobs
    pending = STORE.create()  # PENDING, never run
    r = client.post("/migrations", json={"old_job_id": pending, "new_job_id": old_id})
    assert r.status_code == 409


def test_migration_rejects_unsupported_cyclic_jobs():
    # The cyclic demo tables have composite PKs/UKs and multiple FKs; use two
    # distinct jobs so the request passes the same-id guard and reaches the
    # structural validation.
    jid1 = _run("request_cyclic_four_tables.json")
    jid2 = _run("request_cyclic_four_tables.json")
    r = client.post("/migrations", json={"old_job_id": jid1, "new_job_id": jid2})
    assert r.status_code == 422


def test_delete_referenced_registry_row_keeps_fk_satisfied(old_new_jobs, verify_dsn):
    """Deleting a still-referenced registry row must work without disabling
    the NO ACTION FK: report deletes run first, registry deletes last."""
    import copy

    from app.sqlgen import render_data

    old_id, _ = old_new_jobs
    old_art = STORE.artifacts(old_id)
    old_pkg = STORE.package(old_id)
    new_art = copy.deepcopy(old_art)
    registry_rows = old_art["data"]["dw_registry"]
    report_rows = old_art["data"]["dw_report"]
    # Pick a registry key that is actually referenced, then drop it together
    # with every report referencing it; null the nullable FK on one remaining
    # report row. Data-driven so solver ordering/randomness cannot break it.
    referenced = next(
        r["reg_id"]
        for r in registry_rows
        if any(rr.get("reg_id") == r["reg_id"] for rr in report_rows)
    )
    new_art["data"]["dw_registry"] = [
        r for r in registry_rows if r["reg_id"] != referenced
    ]
    keep = {r["reg_id"] for r in new_art["data"]["dw_registry"]}
    reports = copy.deepcopy(report_rows)
    reports[0]["reg_id"] = None
    reports[0]["metric"] = 77
    new_art["data"]["dw_report"] = [
        r for r in reports if r["reg_id"] in keep or r["reg_id"] is None
    ]
    plan = build_migration(old_art, new_art)
    assert plan["changes"]["dw_registry"]["delete"] == 1
    # Statement order: every report DELETE precedes every registry DELETE, so
    # the NO ACTION FK never sees a still-referenced parent removal.
    text = plan["script"]
    report_deletes = [
        i for i, ln in enumerate(text.splitlines())
        if ln.strip().startswith('DELETE FROM "public"."dw_report"')
    ]
    registry_deletes = [
        i for i, ln in enumerate(text.splitlines())
        if ln.strip().startswith('DELETE FROM "public"."dw_registry"')
    ]
    assert report_deletes and registry_deletes
    assert max(report_deletes) < min(registry_deletes)
    new_pkg = dict(old_pkg)
    new_pkg["02_data.sql"] = render_data(
        new_art["schemas"], ["dw_registry", "dw_report"], new_art["data"]
    )
    fk = old_art["schemas"]["dw_report"].foreign_keys[0]
    ok, log = verify_migration(
        verify_dsn,
        "sample_verify_",
        "public",
        old_art["schemas"],
        old_pkg,
        new_pkg,
        old_art["data"],
        new_art["data"],
        plan["script"],
        {
            "report_table": "dw_report",
            "registry_table": "dw_registry",
            "fk_column": fk.columns[0],
        },
    )
    assert ok, log


# --------------------------------------------------------------------------- #
# End-to-end verification behaviour, checked independently of the endpoint
# --------------------------------------------------------------------------- #
def test_empty_table_snapshot_branch(old_new_jobs, verify_dsn):
    """Both versions empty: the LIKE-based zero-row snapshot branch must still
    compare correctly and succeed without writes."""
    import copy

    from app.sqlgen import render_data

    old_id, _ = old_new_jobs
    old_art = STORE.artifacts(old_id)
    empty_art = copy.deepcopy(old_art)
    for t in empty_art["data"]:
        empty_art["data"][t] = []
    plan = build_migration(empty_art, empty_art)
    assert "LIKE " in plan["script"]  # zero-row snapshot rendering
    # A package whose data file is also empty, so the loaded target matches
    # the zero-row snapshots the script compares against.
    empty_pkg = dict(STORE.package(old_id))
    empty_pkg["02_data.sql"] = render_data(
        empty_art["schemas"], list(empty_art["schemas"]), empty_art["data"]
    )
    fk = old_art["schemas"]["dw_report"].foreign_keys[0]
    ok, log = verify_migration(
        verify_dsn,
        "sample_verify_",
        "public",
        old_art["schemas"],
        empty_pkg,
        empty_pkg,
        empty_art["data"],
        empty_art["data"],
        plan["script"],
        {
            "report_table": "dw_report",
            "registry_table": "dw_registry",
            "fk_column": fk.columns[0],
        },
    )
    assert ok, log


def test_verifier_three_scenarios(old_new_jobs, verify_dsn):
    old_id, new_id = old_new_jobs
    old_art, new_art = STORE.artifacts(old_id), STORE.artifacts(new_id)
    plan = build_migration(old_art, new_art)
    fk = old_art["schemas"]["dw_report"].foreign_keys[0]
    ok, log = verify_migration(
        verify_dsn,
        "sample_verify_",
        plan["schema_name"],
        old_art["schemas"],
        STORE.package(old_id),
        STORE.package(new_id),
        old_art["data"],
        new_art["data"],
        plan["script"],
        {
            "report_table": "dw_report",
            "registry_table": "dw_registry",
            "fk_column": fk.columns[0],
        },
    )
    assert ok, log
    joined = "\n".join(log)
    assert "upgrade" in joined and "already-new" in joined and "neither" in joined
    assert "23503" in joined  # FK negative probe
    assert "23000" in joined  # reject branch raise


def test_concurrent_writer_cannot_sneak_between_check_and_update(
    old_new_jobs, fresh_verify_database
):
    """The migration lock must block a concurrent INSERT until commit.

    The delivered script ends with COMMIT; here it is split at that boundary
    so the test can observe the lock while the migration transaction is still
    open (the exact state the script is in between its check and its commit).
    """
    old_id, new_id = old_new_jobs
    dsn, _ = fresh_verify_database
    old_pkg = STORE.package(old_id)
    plan = build_migration(STORE.artifacts(old_id), STORE.artifacts(new_id))
    head, _, _ = plan["script"].rpartition("COMMIT;")

    with psycopg.connect(dsn) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")
        conn.commit()
        for f in ("01_schema.sql", "02_data.sql", "03_constraints.sql"):
            conn.execute(old_pkg[f])
        conn.commit()

    writer_error = []
    writer_started = threading.Event()

    def writer():
        try:
            with psycopg.connect(dsn) as wconn:
                wconn.execute("SET lock_timeout = '4s'")
                writer_started.set()
                # Blocks while the migration holds SHARE ROW EXCLUSIVE.
                wconn.execute(
                    "INSERT INTO public.dw_registry VALUES (999, 9, 9999)"
                )
                wconn.commit()
        except psycopg.Error as exc:
            writer_error.append(exc.sqlstate)

    with psycopg.connect(dsn) as conn:
        # Everything up to (but excluding) the final COMMIT: locks are held.
        conn.execute(head)
        t = threading.Thread(target=writer)
        t.start()
        assert writer_started.wait(5)
        # Give the blocked writer time to queue on the relation lock.
        t.join(timeout=1.5)
        assert t.is_alive(), "writer should be blocked by the migration lock"
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                FROM pg_locks l
                JOIN pg_catalog.pg_class c ON c.oid = l.relation
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname IN ('dw_registry', 'dw_report')
                  AND l.mode = 'ShareRowExclusiveLock'
                  AND l.granted
                """
            )
            assert cur.fetchone()[0] == 2
        conn.commit()  # release: the queued writer may now proceed
        t.join(timeout=15)

    # Writer either waited and then succeeded after release, or hit
    # lock_timeout — but it never executed concurrently with the check.
    assert writer_error in ([], ["55P03"])
    with psycopg.connect(dsn) as conn:
        n = conn.execute("SELECT count(*) FROM public.dw_registry").fetchone()[0]
    new_count = len(STORE.artifacts(new_id)["data"]["dw_registry"])
    if not writer_error:
        assert n == new_count + 1  # writer's row landed strictly after commit
    else:
        assert n == new_count
