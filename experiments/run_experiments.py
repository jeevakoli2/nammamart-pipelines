"""
experiments/run_experiments.py
------------------------------
Reproduces the Part F observability experiments by running BOTH DAGs with `airflow dags test`
against a scratch copy of the data + warehouse (nothing in include/ is touched; "restore the original
file" is therefore automatic).

Usage (inside the Astro container or any env with airflow + duckdb + pandas):
    python experiments/run_experiments.py 2 3 5        # run selected experiments
    python experiments/run_experiments.py all
Environment: AIRFLOW_HOME / AIRFLOW__CORE__DAGS_FOLDER must already point at this project's dags.
"""
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "include" / "data" / "nammamart"
WORK = Path(os.environ.get("EXP_WORK", "/tmp/nammamart_exp"))
BASE_WH = Path(os.environ.get("EXP_BASE_WH", WORK / "wh_base.duckdb"))   # warehouse after one happy-path run of each DAG
AF_DB = Path(os.environ["AIRFLOW_HOME"]) / "airflow.db"
DAGS = ["nammamart_etl_jay", "nammamart_elt_jay"]
AS_OF = "2026-10-20 09:00:00"


def run_dag(dag_id, data_dir, wh, conf=None, as_of=AS_OF, retry_sec=3):
    env = dict(os.environ, NAMMAMART_DATA_DIR=str(data_dir), NAMMAMART_WAREHOUSE=str(wh),
               NAMMAMART_AS_OF=as_of, NAMMAMART_RETRY_DELAY_SEC=str(retry_sec), PYTHONWARNINGS="ignore")
    cmd = ["airflow", "dags", "test", dag_id] + (["--conf", json.dumps(conf)] if conf else [])
    p = subprocess.run(cmd, env=env, capture_output=True, text=True, cwd=ROOT)
    return p.returncode, p.stdout + p.stderr, task_states(dag_id)


def task_states(dag_id):
    con = sqlite3.connect(AF_DB)
    rid = con.execute("select id from dag_run where dag_id=? order by id desc limit 1", (dag_id,)).fetchone()[0]
    rows = con.execute("""select t.task_id, t.state, t.try_number, t.start_date, t.end_date
                          from task_instance t where t.dag_id=? and t.run_id=(select run_id from dag_run where id=?)
                          order by t.start_date""", (dag_id, rid)).fetchall()
    run_state = con.execute("select state from dag_run where id=?", (rid,)).fetchone()[0]
    return {"run_state": run_state, "tasks": rows}


def fresh(n, files=None):
    d = WORK / f"exp{n}"
    shutil.rmtree(d, ignore_errors=True)
    shutil.copytree(SRC, d / "data")
    wh = d / "wh.duckdb"
    shutil.copy(BASE_WH, wh)
    return d / "data", wh


def q(wh, sql):
    c = duckdb.connect(str(wh), read_only=True)
    try:
        return c.execute(sql).df()
    finally:
        c.close()


def summarize(label, results, wh):
    print(f"\n----- {label}")
    for dag, (rc, log, st) in results.items():
        bad = [(t[0], t[1], t[2]) for t in st["tasks"] if t[1] != "success"]
        print(f"{dag}: run={st['run_state']}  non-success tasks={bad}")
    try:
        print(q(wh, "select dag_id, status, rows_extracted, rows_loaded, rows_quarantined from pipeline_runs order by start_time desc limit 2").to_string(index=False))
    except Exception as e:
        print("pipeline_runs:", e)


def both(data, wh, conf=None, as_of=AS_OF):
    return {d: run_dag(d, data, wh, conf, as_of) for d in DAGS}


def counts(wh):
    return q(wh, """select
        (select count(*) from etl_fact_orders) etl_fact, (select count(*) from stg_orders) elt_stg_orders,
        (select count(*) from raw_orders) raw_orders, (select count(*) from raw_customers) raw_customers,
        (select round(sum(net_revenue),2) from etl_fact_orders where order_status='DELIVERED') etl_rev,
        (select round(sum(net_revenue),2) from mart_daily_store_kpis) elt_rev""").to_string(index=False)


def dq_hits(wh, like="%"):
    return q(wh, f"""select dag_id, check_name, rows_failed, status from dq_results
        where run_id in (select run_id from pipeline_runs order by start_time desc limit 2)
          and rows_failed > 0 and check_name like '{like}' order by check_name, dag_id""").to_string(index=False)


def exp2():
    data, wh = fresh(2)
    print("before:\n", counts(wh))
    res = both(data, wh)
    summarize("EXP2 re-run (2nd run of each DAG on same warehouse)", res, wh)
    print("after:\n", counts(wh))


def exp3():
    data, wh = fresh(3)
    print("before:\n", counts(wh))
    res = both(data, wh, conf={"include_increment": True})
    summarize("EXP3 incremental load", res, wh)
    print("after:\n", counts(wh))
    print(q(wh, "select order_status, count(*) n from etl_fact_orders where order_id in (select order_id from raw_orders where _source_file like '%increment%') group by 1").to_string(index=False))
    print("dup order_ids ETL:", q(wh, "select count(*) - count(distinct order_id) d from etl_fact_orders").iloc[0, 0],
          "ELT:", q(wh, "select count(*) - count(distinct order_id) d from stg_orders where dq_status<>'FAILED'").iloc[0, 0])
    print(q(wh, "select * from etl_order_status_history limit 3").to_string(index=False))
    # re-run without the param: corrections must survive
    res = both(data, wh)
    print("after re-run WITHOUT increment param:\n", counts(wh))
    print(q(wh, "select count(*) returned_corrections from etl_fact_orders where order_id in ('ORD000017','ORD000271','ORD000332') and order_status='RETURNED'").to_string(index=False),
          q(wh, "select count(*) returned_corrections from stg_orders where order_id in ('ORD000017','ORD000271','ORD000332') and order_status='RETURNED'").to_string(index=False))


def exp4():
    data, wh = fresh(4)
    (data / "products.csv").rename(data / "products_RENAMED.csv")
    res = both(data, wh)
    summarize("EXP4 missing products.csv (retries=2)", res, wh)
    for dag, (rc, log, st) in res.items():
        print(dag, [(t[0], t[1], t[2], str(t[3])[11:19], str(t[4])[11:19]) for t in st["tasks"]
                    if t[1] != "success" or t[0] == "extract_products"])


def exp5():
    data, wh = fresh(5)
    o = pd.read_csv(data / "orders.csv", dtype=str, keep_default_na=False)
    tmpl = o[o.order_status == "DELIVERED"].iloc[0].copy()
    rows = []
    for i, (col, val) in enumerate([("customer_id", ""), ("quantity", "-2"), ("store_id", "S99"),
                                    ("order_status", "SHIPPED"), ("order_ts", "2027-03-01 10:00:00")]):
        r = tmpl.copy(); r["order_id"] = f"ORD9000{i}"; r[col] = val
        if col == "quantity":
            r["order_amount"] = str(round(-2 * float(r["unit_price"]) * (1 - float(r["discount_pct"]) / 100), 2))
        rows.append(r)
    pd.concat([o, pd.DataFrame(rows)]).to_csv(data / "orders.csv", index=False)
    res = both(data, wh)
    summarize("EXP5 five new bad rows", res, wh)
    print(q(wh, "select source_file,row_key,failure_reason from etl_quarantine where row_key like 'ORD9000%' order by 2").to_string(index=False))
    print(q(wh, "select order_id, dq_status, failure_reason from stg_orders where order_id like 'ORD9000%' order by 1").to_string(index=False))
    print(dq_hits(wh, "orders.%"))


def exp6():
    data, wh = fresh(6)
    s = (data / "orders.csv").read_text().replace("order_amount", "total", 1)
    (data / "orders.csv").write_text(s)
    res = both(data, wh)
    summarize("EXP6 schema change order_amount->total", res, wh)
    for dag, (rc, log, st) in res.items():
        err = [l for l in log.splitlines() if "Error" in l and "Schema" in l or "Binder Error" in l or "ValueError" in l]
        print(dag, "first error line:", (err[0][:230] if err else "none"))
        print(dag, "task states:", [(t[0], t[1]) for t in st["tasks"] if t[1] != "success"])
    print(q(wh, "select count(*) n from raw_orders").to_string(index=False), "(raw_orders rows; unchanged = nothing loaded silently)")


def exp7():
    data, wh = fresh(7)
    o = pd.read_csv(data / "orders.csv", dtype=str, keep_default_na=False)
    o.head(50).to_csv(data / "orders.csv", index=False)
    res = both(data, wh)
    summarize("EXP7 volume: only 50 orders", res, wh)
    print(dq_hits(wh, "orders.volume_drop"))
    print(counts(wh))


def exp8():
    for variant in ("a_unit_price_only", "b_unit_price_and_amount"):
        data, wh = fresh(8)
        o = pd.read_csv(data / "orders.csv", dtype=str, keep_default_na=False)
        m = o.store_id == "S03"
        o.loc[m, "unit_price"] = (o.loc[m, "unit_price"].astype(float) * 100).astype(str)
        if variant.startswith("b"):
            o.loc[m, "order_amount"] = (o.loc[m, "order_amount"].astype(float) * 100).round(2).astype(str)
        o.to_csv(data / "orders.csv", index=False)
        before = q(wh, "select store_id, round(sum(net_revenue),2) rev from mart_daily_store_kpis where store_id='S03' group by 1").iloc[0, 1]
        res = both(data, wh)
        after = q(wh, "select round(sum(net_revenue),2) from mart_daily_store_kpis where store_id='S03'").iloc[0, 0]
        tot_b = q(BASE_WH, "select round(sum(net_revenue),2) from mart_daily_store_kpis").iloc[0, 0]
        tot_a = q(wh, "select round(sum(net_revenue),2) from mart_daily_store_kpis").iloc[0, 0]
        summarize(f"EXP8 distribution drift ({variant}) store S03 x100", res, wh)
        print(f"S03 revenue before {before} -> after {after};  total before {tot_b} -> after {tot_a}")
        print(dq_hits(wh, "orders.amount_formula"))


def exp9():
    data, wh = fresh(9)
    res = both(data, wh, as_of="2026-10-30 09:00:00")   # latest order is 30 days old
    summarize("EXP9a freshness: latest order ~30 days old", res, wh)
    print(dq_hits(wh, "orders.data_not_stale"))
    data, wh = fresh(9)
    res = both(data, wh, as_of="2026-10-01 10:00:00")   # latest order < 1 day old
    summarize("EXP9b freshness control: latest order < 1 day old", res, wh)
    print(q(wh, """select dag_id, check_name, rows_failed, status from dq_results where check_name='orders.data_not_stale'
                   and run_id in (select run_id from pipeline_runs order by start_time desc limit 2)""").to_string(index=False))


if __name__ == "__main__":
    WORK.mkdir(parents=True, exist_ok=True)
    if not BASE_WH.exists():
        sys.exit(f"Create the baseline warehouse first: run both DAGs once with NAMMAMART_WAREHOUSE={BASE_WH}")
    todo = sys.argv[1:] or ["all"]
    exps = {"2": exp2, "3": exp3, "4": exp4, "5": exp5, "6": exp6, "7": exp7, "8": exp8, "9": exp9}
    for k in (exps if todo == ["all"] else todo):
        exps[str(k)]()
