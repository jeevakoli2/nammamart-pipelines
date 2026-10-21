"""
dags/nammamart_elt_jay.py
-------------------------
NammaMart ELT pipeline (Part B + C + D + F-audit).  Owner: jay

  extract_* (5 + increment) ──► load_raw ──► prepare ──► stg_customers ──► stg_products ──► stg_stores
        ──► stg_orders ──► stg_orders_merge_increment ──► stg_payments ──► dq_results
        ──► mart_* (7 tables) ──► run_audit (all_done)

  RAW      raw_orders, raw_payments, raw_customers, raw_products, raw_stores   (text, append-only, never cleaned)
  STAGING  stg_*   SQL only: types, standardise, dedupe, PII masking, DQ flags (dq_status / failure_reason)
  MART     mart_*  SQL only: answers the six business questions

Every transformation is a .sql file in include/sql/ (one Airflow task per file).
Schedule: 09:00 IST, Monday-Saturday.  Same schedule, retries and timeout as the ETL DAG.
"""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, "/usr/local/airflow/include")

import pandas as pd
import pendulum

try:  # Airflow 3
    from airflow.sdk import Param, dag, get_current_context, task
except ImportError:  # Airflow 2.x fallback
    from airflow.decorators import dag, task
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context

import nammamart_common as nc

DAG_ID = "nammamart_elt_jay"

default_args = {
    "owner": nc.OWNER,
    "retries": 2,
    "retry_delay": timedelta(seconds=int(os.environ.get("NAMMAMART_RETRY_DELAY_SEC", 60))),  # env override only for tests
    "execution_timeout": timedelta(minutes=10),
}


def _as_of() -> datetime:
    return datetime.fromisoformat(os.environ["NAMMAMART_AS_OF"]) if os.environ.get("NAMMAMART_AS_OF") else datetime.now()


def run_sql_file(con, filename: str, **variables) -> None:
    sql = (nc.SQL_DIR / filename).read_text()
    variables.setdefault("as_of", _as_of().strftime("%Y-%m-%d %H:%M:%S"))
    for k, v in variables.items():
        sql = sql.replace("{{" + k + "}}", str(v))
    con.execute(sql)


STG = {"stg_customers": "customers", "stg_products": "products", "stg_stores": "stores",
       "stg_orders": "orders", "stg_payments": "payments"}


@dag(
    dag_id=DAG_ID,
    description="NammaMart ELT: load raw as-is, transform in the warehouse with SQL",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"include_increment": Param(False, type="boolean",
                                       description="Also load orders_*_increment.csv (MERGE into stg_orders)")},
    tags=["nammamart", "elt", nc.OWNER],
)
def nammamart_elt_jay():
    # ------------------------------------------------------------------ Extract (parallel)
    def make_extract(logical: str):
        @task(task_id=f"extract_{logical}")
        def extract():
            f = nc.DATA_DIR / nc.FILES[logical]
            rows = len(nc.read_raw(f))  # raises FileNotFoundError when the source file is missing
            return {"table": f"raw_{logical}", "file": f.name, "path": str(f), "rows": rows}
        return extract()

    @task(task_id="extract_increment")
    def extract_increment():
        ctx = get_current_context()
        if not ctx["params"].get("include_increment"):
            return {"files": [], "rows": 0}
        files = sorted(nc.DATA_DIR.glob(nc.INCREMENT_GLOB))
        out = [{"table": "raw_orders", "file": f.name, "path": str(f), "rows": len(nc.read_raw(f))} for f in files]
        return {"files": out, "rows": sum(o["rows"] for o in out)}

    ex = [make_extract(k) for k in nc.FILES]
    inc = extract_increment()

    # ------------------------------------------------------------------ Load raw (as-is)
    @task(task_id="load_raw")
    def load_raw(extracts, increment):
        run_id = get_current_context()["run_id"]
        loads = list(extracts) + list(increment["files"])
        con = nc.get_con()
        loaded = {}
        try:
            for e in loads:
                run_sql_file(con, "elt_00_load_raw.sql", table=e["table"], path=e["path"],
                             source_file=e["file"], run_id=run_id)
                loaded[e["file"]] = e["rows"]
        finally:
            con.close()
        return {"rows_loaded": int(sum(loaded.values())), "files": loaded}

    @task(task_id="prepare")
    def prepare(_raw):
        """Macros, audit tables and the DQ catalog table (metadata only - no data transformation)."""
        con = nc.get_con()
        try:
            nc.ensure_audit_tables(con)
            run_sql_file(con, "elt_01_macros.sql")
            cat = pd.DataFrame(nc.CATALOG.values())
            con.register("_cat", cat)
            con.execute("CREATE OR REPLACE TABLE dq_check_catalog AS SELECT * FROM _cat")
            con.unregister("_cat")
        finally:
            con.close()
        return {"checks": len(nc.CATALOG)}

    # ------------------------------------------------------------------ SQL tasks (one per file)
    def sql_task(task_id: str, sql_file: str, count_table: str | None = None):
        @task(task_id=task_id)
        def run(_prev):
            ctx = get_current_context()
            con = nc.get_con()
            try:
                run_sql_file(con, sql_file, run_id=ctx["run_id"], dag_id=DAG_ID)
                out = {}
                if count_table:
                    out["rows"] = con.execute(f"SELECT count(*) FROM {count_table}").fetchone()[0]
                    if count_table in STG:
                        out["failed"] = con.execute(
                            f"SELECT count(*) FROM {count_table} WHERE dq_status = 'FAILED'").fetchone()[0]
                return out
            finally:
                con.close()
        return run

    @task(task_id="stg_orders_merge_increment")
    def stg_orders_merge_increment(_prev):
        """Incremental step: MERGE the increment file(s) into stg_orders (insert new, update corrected)."""
        ctx = get_current_context()
        con = nc.get_con()
        try:
            stats = con.execute("""
                SELECT count(*) FILTER (WHERE t.order_id IS NULL) AS to_insert,
                       count(*) FILTER (WHERE t.order_id IS NOT NULL AND s.dq_status <> 'FAILED') AS to_update,
                       count(*) FILTER (WHERE t.order_id IS NOT NULL AND s.dq_status <> 'FAILED'
                                        AND t.order_status <> s.order_status) AS status_changed
                FROM (SELECT * FROM v_orders_prepared WHERE _src_rank > 1
                      QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _source_file DESC, dup_rank) = 1) s
                LEFT JOIN stg_orders t ON t.order_id = s.order_id""").fetchone() if _view_exists(con) else (0, 0, 0)
            run_sql_file(con, "elt_21_stg_orders_merge_increment.sql", run_id=ctx["run_id"], dag_id=DAG_ID)
            rows = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
            failed = con.execute("SELECT count(*) FROM stg_orders WHERE dq_status = 'FAILED'").fetchone()[0]
            failed += con.execute("SELECT count(*) FROM stg_orders_rejected_corrections").fetchone()[0]
        finally:
            con.close()
        return {"rows": rows, "failed": failed, "merge_inserted": int(stats[0]), "merge_updated": int(stats[1]),
                "merge_status_changed": int(stats[2])}

    def _view_exists(con) -> bool:
        return con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'v_orders_prepared'"
                           ).fetchone()[0] > 0

    t_prev = prepare(load_raw(ex, inc))
    # staging order matters: orders/payments validate foreign keys against the (already validated) masters
    s_cust = sql_task("stg_customers", "elt_10_stg_customers.sql", "stg_customers")(t_prev)
    s_prod = sql_task("stg_products", "elt_11_stg_products.sql", "stg_products")(s_cust)
    s_store = sql_task("stg_stores", "elt_12_stg_stores.sql", "stg_stores")(s_prod)
    s_view = sql_task("orders_prepared_view", "elt_19_view_orders_prepared.sql")(s_store)
    s_orders = sql_task("stg_orders", "elt_20_stg_orders.sql", "stg_orders")(s_view)
    s_merge = stg_orders_merge_increment(s_orders)
    s_pay = sql_task("stg_payments", "elt_30_stg_payments.sql", "stg_payments")(s_merge)
    dq = sql_task("dq_results", "elt_50_dq_results.sql", "dq_results")(s_pay)

    marts = [("mart_daily_store_kpis", "elt_40_mart_daily_store_kpis.sql"),
             ("mart_category_margin", "elt_41_mart_category_margin.sql"),
             ("mart_delivery_sla", "elt_42_mart_delivery_sla.sql"),
             ("mart_payment_reconciliation", "elt_43_mart_payment_reconciliation.sql"),
             ("mart_top_customers", "elt_44_mart_top_customers.sql"),
             ("mart_revenue_by_payment_mode", "elt_45_mart_revenue_by_payment_mode.sql"),
             ("mart_rejected_rows", "elt_46_mart_rejected_rows.sql")]
    prev, mart_tasks = dq, []
    for name, f in marts:  # sequential: DuckDB has a single writer
        prev = sql_task(name, f, name)(prev)
        mart_tasks.append(prev)

    # ------------------------------------------------------------------ Run audit
    @task(task_id="run_audit", trigger_rule="all_done")
    def run_audit():
        c = get_current_context()
        run_id, ti = c["run_id"], c["ti"]
        steps = (["extract_" + k for k in nc.FILES] + ["extract_increment", "load_raw", "prepare", "stg_customers",
                 "stg_products", "stg_stores", "orders_prepared_view", "stg_orders", "stg_orders_merge_increment",
                 "stg_payments", "dq_results"] + [m for m, _ in marts])
        res = dict(zip(steps, ti.xcom_pull(task_ids=steps)))
        ok = all(res[s] is not None for s in steps)
        extracted = sum((res[s] or {}).get("rows", 0) for s in steps if s.startswith("extract_"))
        loaded = (res["load_raw"] or {}).get("rows_loaded", 0)
        quarantined = sum((res[s] or {}).get("failed", 0)
                          for s in ["stg_customers", "stg_products", "stg_stores", "stg_payments"])
        quarantined += (res["stg_orders_merge_increment"] or res["stg_orders"] or {}).get("failed", 0)
        start = c["dag_run"].start_date
        start = start.replace(tzinfo=None) if start else datetime.now()
        con = nc.get_con()
        try:
            nc.ensure_audit_tables(con)
            con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
            con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)",
                        [run_id, DAG_ID, start, datetime.now(), extracted, loaded, quarantined,
                         "SUCCESS" if ok else "FAILED"])
        finally:
            con.close()
        if not ok:
            raise RuntimeError(f"Pipeline run incomplete; tasks without a result: {[s for s in steps if res[s] is None]}")
        return {"status": "SUCCESS"}

    audit = run_audit()
    for upstream in [*ex, inc, s_cust, s_prod, s_store, s_view, s_orders, s_merge, s_pay, dq, *mart_tasks]:
        upstream >> audit


nammamart_elt_jay()
