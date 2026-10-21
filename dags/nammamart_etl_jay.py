"""
dags/nammamart_etl_jay.py
-------------------------
NammaMart ETL pipeline (Part A + C + D + F-audit).  Owner: jay

  extract_orders ─────┬──► validate_customers ─┐
  extract_customers ──┤    validate_products  ─┼──► validate_orders ─┐
  extract_products ───┤    validate_stores    ─┘                     │
  extract_stores ─────┤                                              ├──► transform ──► load_warehouse
  extract_payments ───┴──────────────► validate_payments ───────────┘          │
                                                                               ▼
                                                      reconcile_payments ──► report ──► run_audit (all_done)

Rules
  * Critical check failure  -> row goes to include/quarantine/jay/<file>_failed_<run_id>.csv  (+ etl_quarantine table)
  * Warning check failure   -> row loads, is logged in dq_results
  * Load is idempotent: dims are full-refresh, the fact is an upsert on order_id
    (an older source version never overwrites a newer one -> increment corrections survive re-runs)
  * Schedule: 09:00 IST, Monday-Saturday
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

DAG_ID = "nammamart_etl_jay"
IST = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": nc.OWNER,
    "retries": 2,                               # transient failures (locked DB, slow disk) heal themselves
    "retry_delay": timedelta(seconds=int(os.environ.get("NAMMAMART_RETRY_DELAY_SEC", 60))),  # env override only for tests        # 3 attempts inside ~3 min: still far ahead of the 09:30 stand-up
    "execution_timeout": timedelta(minutes=10),  # a hung task must not eat the SLA window
}


def _ctx():
    c = get_current_context()
    return c["run_id"], c["params"], c


def _as_of() -> datetime:
    return datetime.fromisoformat(os.environ["NAMMAMART_AS_OF"]) if os.environ.get("NAMMAMART_AS_OF") else datetime.now()


def _work_dir(run_id: str) -> Path:
    p = nc.TMP_DIR / DAG_ID / nc.safe_run_id(run_id)
    p.mkdir(parents=True, exist_ok=True)
    return p


@dag(
    dag_id=DAG_ID,
    description="NammaMart ETL: extract -> validate/quarantine -> transform -> load star schema",
    schedule="0 9 * * 1-6",                                   # 09:00 Mon-Sat ...
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),  # ... interpreted in IST because start_date is tz-aware
    catchup=False,  # the report is a point-in-time snapshot of the current files; back-filling missed mornings would just re-load the same files
    max_active_runs=1,
    default_args=default_args,
    params={"include_increment": Param(False, type="boolean",
                                       description="Also load orders_*_increment.csv (new orders + status corrections)")},
    tags=["nammamart", "etl", nc.OWNER],
)
def nammamart_etl_jay():
    # ------------------------------------------------------------------ Extract (5 parallel tasks)
    def make_extract(logical: str):
        @task(task_id=f"extract_{logical}")
        def extract():
            run_id, params, _ = _ctx()
            frames = []
            files = [nc.DATA_DIR / nc.FILES[logical]]
            if logical == "orders" and params.get("include_increment"):
                files += sorted(nc.DATA_DIR.glob(nc.INCREMENT_GLOB))
            for rank, f in enumerate(files, start=1):
                df = nc.read_raw(f)                      # raises FileNotFoundError if the file is missing -> retries
                nc.check_schema(df, logical, f.name)     # raises on schema drift -> fails loudly
                df["_source_file"], df["_src_rank"] = f.name, rank
                frames.append(df)
            raw = pd.concat(frames, ignore_index=True)
            out = _work_dir(run_id) / f"raw_{logical}.pkl"
            raw.to_pickle(out)
            return {"file": logical, "rows": int(len(raw)), "path": str(out)}
        return extract()

    ex = {k: make_extract(k) for k in nc.FILES}

    # ------------------------------------------------------------------ Validate (+ quarantine)
    def _finish_validation(logical, passed, failed, results, run_id, rows_in, extra_cols=None):
        wd = _work_dir(run_id)
        passed.to_pickle(wd / f"passed_{logical}.pkl")
        failed.to_pickle(wd / f"failed_{logical}.pkl")
        pd.DataFrame(results).to_pickle(wd / f"dq_{logical}.pkl")
        qdf = failed.copy()
        if logical == "customers":  # never write raw PII into the repo-side quarantine folder
            qdf["full_name"] = nc.mask_name(qdf["full_name"])
            qdf["email"], qdf["phone"] = nc.mask_email(qdf["email"]), nc.mask_phone(qdf["phone"])
        src_cols = nc.EXPECTED_COLUMNS[logical]
        keep = src_cols + [c for c in ["_source_file"] if c in qdf.columns] + ["failure_reason", "failure_description"]
        nc.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        qpath = nc.QUARANTINE_DIR / f"{logical}_failed_{nc.safe_run_id(run_id)}.csv"
        qdf[keep].to_csv(qpath, index=False)
        return {"file": logical, "rows_in": int(rows_in), "passed": int(len(passed)), "failed": int(len(failed)),
                "quarantine_file": str(qpath)}

    @task(task_id="validate_customers")
    def validate_customers(x):
        run_id, _, _ = _ctx()
        raw = pd.read_pickle(x["path"])
        p, f, r = nc.validate_customers(raw, _as_of())
        return _finish_validation("customers", p, f, r, run_id, len(raw))

    @task(task_id="validate_products")
    def validate_products(x):
        run_id, _, _ = _ctx()
        raw = pd.read_pickle(x["path"])
        p, f, r = nc.validate_products(raw)
        return _finish_validation("products", p, f, r, run_id, len(raw))

    @task(task_id="validate_stores")
    def validate_stores(x):
        run_id, _, _ = _ctx()
        raw = pd.read_pickle(x["path"])
        p, f, r = nc.validate_stores(raw)
        return _finish_validation("stores", p, f, r, run_id, len(raw))

    @task(task_id="validate_orders")
    def validate_orders(x, c, pr, st):
        run_id, _, _ = _ctx()
        wd = _work_dir(run_id)
        raw = pd.read_pickle(x["path"])
        valid_c = set(pd.read_pickle(wd / "passed_customers.pkl")["customer_id"])
        valid_p = set(pd.read_pickle(wd / "passed_products.pkl")["product_id"])
        valid_s = set(pd.read_pickle(wd / "passed_stores.pkl")["store_id"])
        con = nc.get_con()
        try:
            nc.ensure_audit_tables(con)
            prev = con.execute("""SELECT rows_checked FROM dq_results WHERE dag_id = ? AND run_id <> ?
                                  AND check_name = 'orders.order_id_not_null'
                                  ORDER BY checked_at DESC LIMIT 1""", [DAG_ID, run_id]).fetchone()
        finally:
            con.close()
        p, f, r = nc.validate_orders(raw, valid_c, valid_p, valid_s, _as_of(),
                                     previous_volume=int(prev[0]) if prev else None)
        return _finish_validation("orders", p, f, r, run_id, len(raw))

    @task(task_id="validate_payments")
    def validate_payments(x_pay, x_ord):
        run_id, _, _ = _ctx()
        raw = pd.read_pickle(x_pay["path"])
        order_ids = set(pd.read_pickle(x_ord["path"])["order_id"].astype("string").str.strip().dropna())
        p, f, r = nc.validate_payments(raw, order_ids, _as_of())
        out = _finish_validation("payments", p, f, r, run_id, len(raw))
        # reconciliation needs ALL de-duplicated payments, not just the clean ones
        pd.concat([p, f]).to_pickle(_work_dir(run_id) / "payments_all.pkl")
        return out

    v_c = validate_customers(ex["customers"])
    v_p = validate_products(ex["products"])
    v_s = validate_stores(ex["stores"])
    v_o = validate_orders(ex["orders"], v_c, v_p, v_s)
    v_pay = validate_payments(ex["payments"], ex["orders"])

    # ------------------------------------------------------------------ Transform
    @task(task_id="transform")
    def transform(v_c, v_p, v_s, v_o, v_pay):
        run_id, _, _ = _ctx()
        wd = _work_dir(run_id)
        cust = pd.read_pickle(wd / "passed_customers.pkl")
        prod = pd.read_pickle(wd / "passed_products.pkl")
        store = pd.read_pickle(wd / "passed_stores.pkl")
        orders = pd.read_pickle(wd / "passed_orders.pkl")

        # --- dimensions: de-duplicated (dup_rank == 1 is all that survives validation), standardised, PII masked
        dim_customer = pd.DataFrame({
            "customer_id": cust["customer_id"], "full_name_masked": nc.mask_name(cust["full_name"]),
            "email_masked": nc.mask_email(cust["email"]), "phone_masked": nc.mask_phone(cust["phone"]),
            "city": cust["city"], "area": cust["area"], "pincode": cust["pincode"],
            "signup_date": cust["signup_parsed"].dt.date, "loyalty_tier": cust["loyalty_tier"],
            "is_active": cust["is_active"].astype("string").str.strip()})
        dim_product = pd.DataFrame({
            "product_id": prod["product_id"], "product_name": prod["product_name"], "category": prod["category"],
            "sub_category": prod["sub_category"], "brand": prod["brand"], "mrp": prod["mrp"],
            "cost_price": prod["cost_price"], "unit": prod["unit"],
            "is_perishable": prod["is_perishable"].astype("string").str.strip(), "launch_date": prod["launch_date"]})
        dim_store = pd.DataFrame({
            "store_id": store["store_id"], "store_name": store["store_name"],
            "zone": store["zone"].fillna("Unknown"), "area": store["area"], "pincode": store["pincode"],
            "open_date": store["open_date"], "manager_name": store["manager_name"],
            "capacity_orders_per_day": store["capacity_orders_per_day"]})

        # --- fact: latest source version of every order wins (increment corrections override the base file)
        o = orders.sort_values(["order_id", "_src_rank"], kind="stable").drop_duplicates("order_id", keep="last")
        o = o.merge(dim_product[["product_id", "cost_price"]], on="product_id", how="left")
        delivered = o["order_status"] == "DELIVERED"
        fact = pd.DataFrame({
            "order_id": o["order_id"], "order_ts": o["order_ts_parsed"], "order_date": o["order_ts_parsed"].dt.date,
            "customer_id": o["customer_id"], "store_id": o["store_id"], "product_id": o["product_id"],
            "quantity": o["quantity"].astype("int64"), "unit_price": o["unit_price"],
            "discount_pct": o["discount_pct"], "order_amount": o["order_amount"],
            "payment_mode": o["payment_mode"], "order_status": o["order_status"], "channel": o["channel"],
            "delivery_minutes": o["delivery_minutes"],
            "net_revenue": o["order_amount"].where(delivered, 0.0),
            "gross_margin": (o["order_amount"] - o["quantity"] * o["cost_price"]).round(2).where(delivered, 0.0),
            "within_15_min": (delivered & (o["delivery_minutes"] <= 15)),
            "source_file": o["_source_file"], "src_rank": o["_src_rank"].astype("int64")})
        for name, df in [("dim_customer", dim_customer), ("dim_product", dim_product), ("dim_store", dim_store),
                         ("fact_orders", fact)]:
            df.to_pickle(wd / f"t_{name}.pkl")
        return {"fact_orders": int(len(fact)), "dim_customer": int(len(dim_customer)),
                "dim_product": int(len(dim_product)), "dim_store": int(len(dim_store))}

    # ------------------------------------------------------------------ Load (idempotent)
    @task(task_id="load_warehouse")
    def load_warehouse(t):
        run_id, _, _ = _ctx()
        wd = _work_dir(run_id)
        now = datetime.now()
        con = nc.get_con()
        try:
            nc.ensure_audit_tables(con)
            # dimensions: full refresh inside one transaction (re-run = same result)
            con.execute("BEGIN")
            for name in ["dim_customer", "dim_product", "dim_store"]:
                df = pd.read_pickle(wd / f"t_{name}.pkl")
                con.register("_df", df)
                con.execute(f"CREATE TABLE IF NOT EXISTS etl_{name} AS SELECT * FROM _df WHERE false")
                con.execute(f"DELETE FROM etl_{name}")
                con.execute(f"INSERT INTO etl_{name} SELECT * FROM _df")
                con.unregister("_df")
            # fact: upsert on order_id; never let an older source version overwrite a newer one
            fact = pd.read_pickle(wd / "t_fact_orders.pkl")
            fact["_run_id"], fact["_loaded_at"] = run_id, now
            con.register("_f", fact)
            con.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM _f WHERE false")
            con.execute("""CREATE TABLE IF NOT EXISTS etl_order_status_history (
                              order_id VARCHAR, old_status VARCHAR, new_status VARCHAR, changed_in_run VARCHAR,
                              changed_at TIMESTAMP)""")
            inserted = con.execute("SELECT count(*) FROM _f WHERE order_id NOT IN (SELECT order_id FROM etl_fact_orders)").fetchone()[0]
            con.execute("""INSERT INTO etl_order_status_history
                           SELECT f.order_id, e.order_status, f.order_status, ?, now()
                           FROM _f f JOIN etl_fact_orders e USING (order_id)
                           WHERE f.src_rank >= e.src_rank AND f.order_status <> e.order_status""", [run_id])
            updated = con.execute("""SELECT count(*) FROM _f f JOIN etl_fact_orders e USING (order_id)
                                     WHERE f.src_rank >= e.src_rank AND f.order_status <> e.order_status""").fetchone()[0]
            con.execute("""DELETE FROM etl_fact_orders WHERE order_id IN
                           (SELECT f.order_id FROM _f f JOIN etl_fact_orders e USING (order_id) WHERE f.src_rank >= e.src_rank)""")
            con.execute("INSERT INTO etl_fact_orders SELECT * FROM _f WHERE order_id NOT IN (SELECT order_id FROM etl_fact_orders)")
            con.unregister("_f")

            # quarantine + dq_results (idempotent per run)
            con.execute("""CREATE TABLE IF NOT EXISTS etl_quarantine (
                              run_id VARCHAR, source_file VARCHAR, row_key VARCHAR, failure_reason VARCHAR,
                              quarantined_at TIMESTAMP)""")
            con.execute("DELETE FROM etl_quarantine WHERE run_id = ?", [run_id])
            qrows = []
            for logical in nc.FILES:
                failed = pd.read_pickle(wd / f"failed_{logical}.pkl")
                for _, r in failed.iterrows():
                    for reason in r["failure_reason"].split("; "):
                        qrows.append((run_id, r.get("_source_file", nc.FILES[logical]), str(r[nc.KEY[logical]]), reason, now))
            if qrows:
                con.executemany("INSERT INTO etl_quarantine VALUES (?,?,?,?,?)", qrows)
            dq_rows = []
            for logical in nc.FILES:
                dq_rows += pd.read_pickle(wd / f"dq_{logical}.pkl").to_dict("records")
            nc.write_dq_results(con, dq_rows, run_id, DAG_ID)
            con.execute("COMMIT")
            counts = {k: con.execute(f"SELECT count(*) FROM etl_{k}").fetchone()[0]
                      for k in ["fact_orders", "dim_customer", "dim_product", "dim_store"]}
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return {"rows_loaded": int(sum(t.values())), "fact_inserted": int(inserted), "fact_status_updated": int(updated),
                "table_counts": counts}

    # ------------------------------------------------------------------ Reconcile
    @task(task_id="reconcile_payments")
    def reconcile_payments(_loaded):
        run_id, _, _ = _ctx()
        wd = _work_dir(run_id)
        orders_raw = pd.read_pickle(wd / "raw_orders.pkl")
        all_ids = set(orders_raw["order_id"].astype("string").str.strip().dropna())
        # clean orders = what is in the fact table now (latest version of each passed order)
        con = nc.get_con()
        try:
            fact = con.execute("SELECT order_id, order_status, order_amount, payment_mode FROM etl_fact_orders").df()
            pays = pd.read_pickle(wd / "payments_all.pkl")
            rec = nc.reconcile(fact, all_ids, pays)
            rec["run_id"], rec["reconciled_at"] = run_id, datetime.now()
            con.register("_r", rec)
            con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM _r")  # full snapshot: re-run safe
            con.unregister("_r")
        finally:
            con.close()
        return {"mismatch_rows": int(len(rec)), "unreconciled_amount": float(rec["unreconciled_amount"].sum().round(2))}

    # ------------------------------------------------------------------ Report
    @task(task_id="report")
    def report(_rec):
        con = nc.get_con()
        try:
            con.execute("""CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                SELECT f.order_date, s.zone, count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS net_revenue
                FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
                WHERE f.order_status = 'DELIVERED' GROUP BY 1, 2 ORDER BY 1, 2""")
            con.execute("""CREATE OR REPLACE TABLE etl_category_margin AS
                SELECT p.category, round(sum(f.net_revenue), 2) AS net_revenue,
                       round(sum(f.gross_margin), 2) AS gross_margin,
                       round(100 * sum(f.gross_margin) / nullif(sum(f.net_revenue), 0), 2) AS margin_pct
                FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
                WHERE f.order_status = 'DELIVERED' GROUP BY 1 ORDER BY 2 DESC""")
            zone = con.execute("SELECT zone, round(sum(net_revenue),2) AS net_revenue, sum(delivered_orders) AS orders "
                               "FROM etl_daily_zone_revenue GROUP BY 1 ORDER BY 2 DESC").df()
            cat = con.execute("SELECT * FROM etl_category_margin").df()
        finally:
            con.close()
        print("=== ETL net revenue by zone (all days) ===\n" + zone.to_string(index=False))
        print("=== ETL category margin ===\n" + cat.to_string(index=False))
        return {"zone_rows": int(len(zone)), "total_net_revenue": float(zone["net_revenue"].sum())}

    # ------------------------------------------------------------------ Run audit (always runs)
    @task(task_id="run_audit", trigger_rule="all_done")
    def run_audit():
        run_id, _, c = _ctx()
        ti = c["ti"]
        steps = ["extract_orders", "extract_payments", "extract_customers", "extract_products", "extract_stores",
                 "validate_customers", "validate_products", "validate_stores", "validate_orders",
                 "validate_payments", "transform", "load_warehouse", "reconcile_payments", "report"]
        res = dict(zip(steps, ti.xcom_pull(task_ids=steps)))
        ok = all(res[s] is not None for s in steps)
        extracted = sum((res[s] or {}).get("rows", 0) for s in steps if s.startswith("extract_"))
        quarantined = sum((res[s] or {}).get("failed", 0) for s in steps if s.startswith("validate_"))
        loaded = (res["load_warehouse"] or {}).get("rows_loaded", 0)
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
        if not ok:  # the audit row is written first; now make the run RED so the UI tells the truth
            failed = [s for s in steps if res[s] is None]
            raise RuntimeError(f"Pipeline run incomplete; tasks without a result: {failed}")
        return {"status": "SUCCESS"}

    t = transform(v_c, v_p, v_s, v_o, v_pay)
    ld = load_warehouse(t)
    rc = reconcile_payments(ld)
    rp = report(rc)
    audit = run_audit()
    # audit hangs off every task, so it runs (all_done) whether the run succeeded or not
    for upstream in [*ex.values(), v_c, v_p, v_s, v_o, v_pay, t, ld, rc, rp]:
        upstream >> audit


nammamart_etl_jay()
