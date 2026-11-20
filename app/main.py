"""FastAPI application.

Endpoints
----------
GET  /healthz                 liveness
GET  /schemas/{schema}/tables list ordinary base tables (catalog read only)
GET  /schemas/{schema}/tables/{table}/structure
                              full supported-structure view + rejections
POST /jobs                    create a generation/verification job
GET  /jobs/{job_id}           status, conflict report, logs
GET  /jobs/{job_id}/download  zip package, available only after SUCCEEDED
POST /migrations              incremental old->new migration SQL for two
                              succeeded registry/report jobs, verified on an
                              isolated empty database before it is returned
GET  /docs                    OpenAPI Swagger UI
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response

from .config import settings
from .db import source_connection
from .introspect import (
    IntrospectionError,
    introspect_tables,
    list_base_tables,
)
from .jobs import STORE, package_zip, run_job
from .migration import MigrationValidationError, build_migration
from .migverify import verify_migration
from .models import (
    GenerateRequest,
    JobStatus,
    MigrationRequest,
    MigrationResult,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(
    title="数仓关联样本生成服务",
    version="1.0.0",
    description=(
        "只读源库表结构（不读行数据），基于 z3 生成同时满足指定行数、整数闭区间与"
        "主键/唯一键/可空/MATCH SIMPLE/FULL 复合及循环外键约束的合成样本，"
        "并在隔离空库实际装载验证通过后才开放 SQL 包下载。"
    ),
    lifespan=lifespan,
)


@app.get("/healthz", tags=["ops"])
def healthz():
    return {"status": "ok"}


@app.get("/schemas/{schema}/tables", tags=["catalog"])
def list_tables(schema: str):
    """List ordinary base tables; non-base relations are flagged, not read."""
    try:
        with source_connection(settings.source_dsn) as conn:
            base, others = list_base_tables(conn, schema)
    except Exception as exc:
        raise HTTPException(502, f"无法连接源库: {exc.__class__.__name__}: {exc}")
    return {
        "schema": schema,
        "base_tables": base,
        "excluded_non_base_relations": others,
        "max_selectable_tables": settings.max_tables,
        "max_rows_per_table": settings.max_rows_per_table,
    }


@app.get("/schemas/{schema}/tables/{table}/structure", tags=["catalog"])
def table_structure(schema: str, table: str):
    """Return the supported structural view, or explicit rejections."""
    try:
        with source_connection(settings.source_dsn) as conn:
            found = introspect_tables(conn, schema, [table])
    except IntrospectionError as exc:
        return JSONResponse(
            status_code=422,
            content={
                "table": f"{schema}.{table}",
                "supported": False,
                "rejections": [
                    {"object": o, "reason": r} for o, r in exc.rejections
                ],
            },
        )
    except Exception as exc:
        raise HTTPException(502, f"无法连接源库: {exc.__class__.__name__}: {exc}")
    ts = found[table]
    return {"table": f"{schema}.{table}", "supported": True, "structure": ts.model_dump()}


@app.post("/jobs", tags=["jobs"], status_code=202)
def create_job(req: GenerateRequest, background: BackgroundTasks):
    if not req.tables:
        raise HTTPException(422, "tables 不能为空")
    if len({t.table for t in req.tables}) != len(req.tables):
        raise HTTPException(422, "请求中存在重复表")
    if len(req.tables) > settings.max_tables:
        raise HTTPException(
            422, f"至多选择 {settings.max_tables} 张表（不含外键闭包扩展）"
        )
    job_id = STORE.create()
    background.add_task(run_job, job_id, req)
    return {"job_id": job_id, "status": JobStatus.PENDING.value}


@app.get("/jobs/{job_id}", tags=["jobs"])
def get_job(job_id: str):
    job = STORE.get(job_id)
    if job is None:
        raise HTTPException(404, "job 不存在或已过期")
    return job.model_dump()


@app.get("/jobs/{job_id}/download", tags=["jobs"])
def download_job(job_id: str):
    job = STORE.get(job_id)
    if job is None:
        raise HTTPException(404, "job 不存在或已过期")
    if job.status != JobStatus.SUCCEEDED:
        raise HTTPException(
            409,
            f"作业尚未验证成功（当前状态 {job.status.value}）；失败作业不提供部分包下载",
        )
    package = STORE.package(job_id)
    if package is None:
        raise HTTPException(409, "SQL 包不可用")
    data = package_zip(package)
    return Response(
        content=data,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="sample_{job_id}.zip"',
            "X-Contains-Real-Patient-Data": "false",
        },
    )


@app.post("/migrations", tags=["migrations"])
def create_migration(req: MigrationRequest):
    """Build a verified single-transaction old->new migration script.

    Both inputs must be SUCCEEDED jobs whose two tables form the supported
    registry/report pair (single-column integer PKs; one nullable
    NOT DEFERRABLE ON DELETE NO ACTION FK). The returned script is only
    handed out after it has been exercised on isolated empty databases for
    the upgrade, already-new and neither-version cases.
    """
    if req.old_job_id == req.new_job_id:
        raise HTTPException(422, "旧作业号与新作业号不能相同")

    jobs = {}
    for label, jid in (("old", req.old_job_id), ("new", req.new_job_id)):
        job = STORE.get(jid)
        if job is None:
            raise HTTPException(404, f"{label}_job_id 对应的作业不存在或已过期: {jid}")
        if job.status != JobStatus.SUCCEEDED:
            raise HTTPException(
                409,
                f"{label}_job_id 作业尚未成功（状态 {job.status.value}）；"
                "仅接受验证成功的作业",
            )
        jobs[label] = job

    old_art = STORE.artifacts(req.old_job_id)
    new_art = STORE.artifacts(req.new_job_id)
    old_pkg = STORE.package(req.old_job_id)
    new_pkg = STORE.package(req.new_job_id)
    if old_art is None or new_art is None or old_pkg is None or new_pkg is None:
        raise HTTPException(409, "作业的 SQL 包或结构化产物不可用")

    try:
        plan = build_migration(old_art, new_art)
    except MigrationValidationError as exc:
        raise HTTPException(422, f"不支持的迁移结构: {exc}")

    # FK metadata needed for the post-migration negative probe.
    report = plan["report_table"]
    registry = plan["registry_table"]
    fk = old_art["schemas"][report].foreign_keys[0]
    ok, vlog = verify_migration(
        settings.verify_dsn,
        settings.verify_db_prefix,
        plan["schema_name"],
        old_art["schemas"],
        old_pkg,
        new_pkg,
        old_art["data"],
        new_art["data"],
        plan["script"],
        {
            "report_table": report,
            "registry_table": registry,
            "fk_column": fk.columns[0],
        },
    )
    if not ok:
        raise HTTPException(
            409,
            "迁移脚本在隔离空库验证失败，拒绝交付（无部分脚本）",
            headers={"X-Migration-Verify": "failed"},
        )

    result = MigrationResult(
        old_job_id=req.old_job_id,
        new_job_id=req.new_job_id,
        schema_name=plan["schema_name"],
        registry_table=registry,
        report_table=report,
        changes=plan["changes"],
        script=plan["script"],
        verify_log=vlog,
    )
    return JSONResponse(
        result.model_dump(),
        headers={"X-Migration-Verify": "passed"},
    )
