import duckdb, pandas as pd
pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 20)
c = duckdb.connect("/usr/local/airflow/include/warehouse_jay.duckdb", read_only=True)
q = lambda s: c.execute(s).df().to_string(index=False)

print("== pipeline_runs")
print(q("select dag_id, status, rows_extracted, rows_loaded, rows_quarantined from pipeline_runs order by start_time"))

print("\n== dq_results: checks with failures (latest run of each DAG)")
print(q("""select dag_id, check_name, severity, rows_checked, rows_failed, status
           from dq_results
           where rows_failed > 0
             and run_id in (select max_by(run_id, start_time) from pipeline_runs group by dag_id)
           order by dag_id, check_name"""))

print("\n== ETL reconciliation")
print(q("select mismatch_reason, count(*) n, round(sum(unreconciled_amount),2) amount from etl_payment_reconciliation group by 1 order by 1"))

print("\n== ELT reconciliation")
print(q("select mismatch_reason, count(*) n, round(sum(unreconciled_amount),2) amount from mart_payment_reconciliation group by 1 order by 1"))

print("\n== revenue check (should be identical)")
print(q("select 'ETL' src, round(sum(net_revenue),2) revenue from etl_fact_orders union all select 'ELT', round(sum(net_revenue),2) from mart_daily_store_kpis"))
