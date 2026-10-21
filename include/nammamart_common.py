"""
include/nammamart_common.py
---------------------------
Shared helpers for the NammaMart ETL (dags/nammamart_etl_jay.py) and
ELT (dags/nammamart_elt_jay.py) pipelines.

Single source of truth for:
  * paths + warehouse connection (with retry, because DuckDB allows one writer at a time)
  * the data-quality CHECK CATALOG (name, dimension, severity, description)
  * value standardisation + PII masking (pandas side; the SQL side mirrors it in
    include/sql/elt_01_macros.sql)
  * the pandas validators used by the ETL pipeline (the ELT pipeline applies the same
    rules in SQL: include/sql/elt_1x / elt_2x / elt_3x files)
  * pipeline_runs / dq_results writers
"""
from __future__ import annotations

import os
import re
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

OWNER = "jay"
INCLUDE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("NAMMAMART_DATA_DIR", INCLUDE_DIR / "data" / "nammamart"))
QUARANTINE_DIR = INCLUDE_DIR / "quarantine" / OWNER
TMP_DIR = INCLUDE_DIR / "tmp" / OWNER
SQL_DIR = INCLUDE_DIR / "sql"
WAREHOUSE = Path(os.environ.get("NAMMAMART_WAREHOUSE", INCLUDE_DIR / f"warehouse_{OWNER}.duckdb"))

FILES = {  # logical name -> file name
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}
INCREMENT_GLOB = "orders_*_increment.csv"

EXPECTED_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
               "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date",
                  "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit",
                 "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
               "capacity_orders_per_day"],
}
KEY = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id",
       "products": "product_id", "stores": "store_id"}

VALID_STATUS = ["DELIVERED", "CANCELLED", "RETURNED"]
VALID_PAYMENT_MODE = ["UPI", "Card", "Cash on Delivery", "Wallet"]
VALID_PAYMENT_STATUS = ["SUCCESS", "FAILED", "REFUNDED"]
PAYMENT_MODE_MAP = {"upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery",
                    "cod": "Cash on Delivery", "cashh": "Cash on Delivery", "wallet": "Wallet"}
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
AMOUNT_TOLERANCE = 0.011
VOLUME_DROP_THRESHOLD = 0.5   # warn if orders volume drops by more than 50% vs previous run
STALE_HOURS = 24

# --------------------------------------------------------------------------------------
# DQ check catalog: (check_name, table, dimension, severity, description, dataset_level)
# severity: Critical -> row is quarantined / FAILED ; Warning -> row loads, but is logged
# --------------------------------------------------------------------------------------
_C, _W = "Critical", "Warning"
DQ_CHECKS = [
    # ---- orders
    ("orders.order_id_not_null", "orders", "Completeness", _C, "order_id is missing", False),
    ("orders.customer_id_not_null", "orders", "Completeness", _C, "customer_id is missing", False),
    ("orders.delivered_has_minutes", "orders", "Completeness", _C, "DELIVERED order has no delivery_minutes", False),
    ("orders.order_id_unique", "orders", "Uniqueness", _C, "order_id appears more than once in the file", False),
    ("orders.status_valid", "orders", "Validity", _C, "order_status not in DELIVERED/CANCELLED/RETURNED", False),
    ("orders.payment_mode_valid", "orders", "Validity", _C, "payment_mode not in UPI/Card/Cash on Delivery/Wallet", False),
    ("orders.order_ts_valid", "orders", "Validity", _C, "order_ts is missing or not a valid timestamp", False),
    ("orders.discount_range", "orders", "Validity", _C, "discount_pct outside 0-100", False),
    ("orders.quantity_positive", "orders", "Accuracy", _C, "quantity <= 0", False),
    ("orders.amount_formula", "orders", "Accuracy", _C, "order_amount != quantity x unit_price x (1 - discount/100)", False),
    ("orders.delivery_minutes_lt_120", "orders", "Accuracy", _C, "delivery_minutes >= 120 (impossible)", False),
    ("orders.delivery_minutes_typical", "orders", "Accuracy", _W, "delivery_minutes outside the normal 8-35 range (but < 120)", False),
    ("orders.customer_fk", "orders", "Consistency", _C, "customer_id not found in valid customers", False),
    ("orders.product_fk", "orders", "Consistency", _C, "product_id not found in valid products", False),
    ("orders.store_fk", "orders", "Consistency", _C, "store_id not found in valid stores", False),
    ("orders.status_casing", "orders", "Consistency", _W, "order_status casing/whitespace fixed (e.g. 'delivered')", False),
    ("orders.payment_mode_variant", "orders", "Consistency", _W, "payment_mode spelling variant mapped (e.g. 'upi', 'cod')", False),
    ("orders.ts_not_future", "orders", "Timeliness", _C, "order_ts is in the future", False),
    ("orders.data_not_stale", "orders", "Timeliness", _W, "latest order_ts is more than 1 day old at run time", True),
    ("orders.volume_drop", "orders", "Completeness", _W, "row volume dropped >50% vs the previous run", True),
    # ---- payments
    ("payments.payment_id_unique", "payments", "Uniqueness", _C, "payment_id appears more than once", False),
    ("payments.status_valid", "payments", "Validity", _C, "payment_status not in SUCCESS/FAILED/REFUNDED", False),
    ("payments.payment_mode_valid", "payments", "Validity", _C, "payment_mode not in allowed list", False),
    ("payments.amount_positive", "payments", "Accuracy", _C, "SUCCESS payment with amount_paid <= 0", False),
    ("payments.order_exists", "payments", "Consistency", _C, "payment refers to an order_id that does not exist", False),
    ("payments.payment_mode_variant", "payments", "Consistency", _W, "payment_mode spelling variant mapped", False),
    ("payments.gateway_ref_present", "payments", "Completeness", _W, "non-COD payment has no gateway_ref", False),
    ("payments.payment_ts_not_future", "payments", "Timeliness", _C, "payment_ts is in the future", False),
    # ---- customers
    ("customers.customer_id_unique", "customers", "Uniqueness", _C, "customer_id appears more than once", False),
    ("customers.city_not_null", "customers", "Completeness", _W, "city is missing", False),
    ("customers.email_format", "customers", "Validity", _W, "email is not a valid address", False),
    ("customers.phone_format", "customers", "Validity", _W, "phone is not 10 digits", False),
    ("customers.pincode_format", "customers", "Validity", _W, "pincode is not 6 digits", False),
    ("customers.email_unique", "customers", "Uniqueness", _W, "email shared by more than one customer", False),
    ("customers.loyalty_tier_casing", "customers", "Consistency", _W, "loyalty_tier casing fixed (e.g. 'BRONZE')", False),
    ("customers.signup_not_future", "customers", "Timeliness", _C, "signup_date is in the future", False),
    # ---- products
    ("products.product_id_unique", "products", "Uniqueness", _C, "product_id appears more than once (superseded '(old)' rows lose)", False),
    ("products.category_not_null", "products", "Completeness", _C, "category is missing", False),
    ("products.mrp_positive", "products", "Accuracy", _C, "mrp <= 0", False),
    ("products.cost_le_mrp", "products", "Accuracy", _C, "cost_price > mrp", False),
    # ---- stores
    ("stores.store_id_unique", "stores", "Uniqueness", _C, "store_id appears more than once", False),
    ("stores.zone_not_null", "stores", "Completeness", _W, "zone is missing (loaded as 'Unknown')", False),
    ("stores.capacity_positive", "stores", "Accuracy", _W, "capacity_orders_per_day <= 0", False),
    ("stores.pincode_format", "stores", "Validity", _W, "pincode is not 6 digits", False),
]
CATALOG = {c[0]: dict(check_name=c[0], table_name=c[1], dimension=c[2], severity=c[3],
                      description=c[4], dataset_level=c[5]) for c in DQ_CHECKS}


# --------------------------------------------------------------------------------------
# Warehouse helpers
# --------------------------------------------------------------------------------------
def get_con(retries: int = 20, wait: float = 3.0) -> duckdb.DuckDBPyConnection:
    """Open the warehouse. DuckDB allows a single writer process, and both DAGs are scheduled
    at 09:00, so retry on a lock instead of failing the task immediately."""
    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    last = None
    for _ in range(retries):
        try:
            return duckdb.connect(str(WAREHOUSE))
        except duckdb.IOException as exc:  # lock held by the other DAG
            last = exc
            time.sleep(wait)
    raise last


def safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(run_id))


def ensure_audit_tables(con) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
            rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP)""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
            rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR)""")


def dq_status(severity: str, failed: int) -> str:
    if failed == 0:
        return "PASSED"
    return "FAILED" if severity == "Critical" else "WARNING"


def write_dq_results(con, rows: list[dict], run_id: str, dag_id: str) -> None:
    """Idempotent per (run_id, dag_id, check_name): delete then insert."""
    ensure_audit_tables(con)
    if not rows:
        return
    df = pd.DataFrame(rows)
    df["run_id"], df["dag_id"], df["checked_at"] = run_id, dag_id, datetime.now()
    df = df[["run_id", "dag_id", "check_name", "dimension", "severity", "rows_checked", "rows_failed",
             "status", "checked_at"]]
    con.register("_dq_df", df)
    con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ? AND check_name IN "
                "(SELECT check_name FROM _dq_df)", [run_id, dag_id])
    con.execute("INSERT INTO dq_results SELECT * FROM _dq_df")
    con.unregister("_dq_df")


# --------------------------------------------------------------------------------------
# Standardisation + masking (pandas)
# --------------------------------------------------------------------------------------
def read_raw(path: Path) -> pd.DataFrame:
    """Read a CSV with every column as text and empty strings as NULL."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    return df.replace(r"^\s*$", pd.NA, regex=True)


def check_schema(df: pd.DataFrame, logical: str, filename: str) -> None:
    missing = [c for c in EXPECTED_COLUMNS[logical] if c not in df.columns]
    if missing:
        raise ValueError(f"Schema drift in {filename}: missing columns {missing}; got {list(df.columns)}")


def _strip(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def std_payment_mode(s: pd.Series) -> pd.Series:
    t = _strip(s)
    mapped = t.str.lower().map(PAYMENT_MODE_MAP)
    return mapped.where(mapped.notna(), t)


def std_status(s: pd.Series) -> pd.Series:
    return _strip(s).str.upper()


def mask_email(e: pd.Series) -> pd.Series:
    e = _strip(e)
    ok = e.str.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+").fillna(False)
    masked = e.str.split("@").str[0].str[:2] + "***@" + e.str.split("@").str[1]
    return masked.where(ok, "***").where(e.notna(), pd.NA)


def mask_phone(p: pd.Series) -> pd.Series:
    p = _strip(p)
    ok = p.str.fullmatch(r"\d{10}").fillna(False)
    masked = p.str[:2] + "******" + p.str[-2:]
    return masked.where(ok, "**********").where(p.notna(), pd.NA)


def mask_name(n: pd.Series) -> pd.Series:
    n = _strip(n)
    return n.str.replace(r"(\w)\w*", r"\1***", regex=True)  # same rule as the SQL macro mask_name()


# --------------------------------------------------------------------------------------
# Validator framework
# --------------------------------------------------------------------------------------
class Validator:
    """Apply catalog checks to a dataframe; collect per-row reasons + per-check results."""

    def __init__(self, df: pd.DataFrame, table: str):
        self.df, self.table = df, table
        self.crit = {i: [] for i in df.index}
        self.warn = {i: [] for i in df.index}
        self.results: list[dict] = []

    def apply(self, name: str, mask: pd.Series | None, dataset_failed: int | None = None, rows_checked=None):
        meta = CATALOG[name]
        if mask is not None:
            mask = mask.fillna(False).astype(bool)
            failed = int(mask.sum())
            target = self.crit if meta["severity"] == "Critical" else self.warn
            for i in mask[mask].index:
                target[i].append(name)
            checked = len(self.df)
        else:  # dataset-level check
            failed, checked = int(dataset_failed or 0), rows_checked or 1
        self.results.append(dict(check_name=name, dimension=meta["dimension"], severity=meta["severity"],
                                 rows_checked=checked, rows_failed=failed,
                                 status=dq_status(meta["severity"], failed)))

    def finish(self):
        df = self.df.copy()
        df["failure_reason"] = [";  ".join(self.crit[i]).replace(";  ", "; ") for i in df.index]
        df["warning_reason"] = [";  ".join(self.warn[i]).replace(";  ", "; ") for i in df.index]
        failed = df[df["failure_reason"] != ""].copy()
        passed = df[df["failure_reason"] == ""].copy()
        failed["failure_description"] = failed["failure_reason"].map(
            lambda r: " | ".join(CATALOG[x]["description"] for x in r.split("; ")))
        return passed, failed, self.results


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format=TS_FORMAT, errors="coerce")


def _dup_rank(df: pd.DataFrame, key: str, group_extra: list[str] | None = None, order_cols=None) -> pd.Series:
    d = df
    if order_cols:
        d = df.sort_values(order_cols, kind="stable")
    return d.groupby([key] + (group_extra or []), dropna=False).cumcount().reindex(df.index) + 1


# ---- orders ---------------------------------------------------------------------------
def validate_orders(raw: pd.DataFrame, valid_customers: set, valid_products: set, valid_stores: set,
                    as_of: datetime, previous_volume: int | None = None):
    """raw has the 13 source columns plus _source_file, _src_rank (1 = base, 2+ = increment)."""
    d = raw.copy().reset_index(drop=True)
    for c in ["order_id", "customer_id", "store_id", "product_id", "channel"]:
        d[c] = _strip(d[c])
    d["status_raw"], d["payment_mode_raw"] = _strip(d["order_status"]), _strip(d["payment_mode"])
    d["order_status"], d["payment_mode"] = std_status(d["order_status"]), std_payment_mode(d["payment_mode"])
    d["order_ts_parsed"] = _ts(d["order_ts"])
    for c in ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"]:
        d[c] = _num(d[c])
    d["dup_rank"] = _dup_rank(d, "order_id", ["_source_file"])

    v = Validator(d, "orders")
    v.apply("orders.order_id_not_null", d["order_id"].isna())
    v.apply("orders.customer_id_not_null", d["customer_id"].isna())
    v.apply("orders.delivered_has_minutes", (d["order_status"] == "DELIVERED") & d["delivery_minutes"].isna())
    v.apply("orders.order_id_unique", d["dup_rank"] > 1)
    v.apply("orders.status_valid", ~d["order_status"].isin(VALID_STATUS))
    v.apply("orders.payment_mode_valid", ~d["payment_mode"].isin(VALID_PAYMENT_MODE))
    v.apply("orders.order_ts_valid", d["order_ts_parsed"].isna())
    v.apply("orders.discount_range", (d["discount_pct"] < 0) | (d["discount_pct"] > 100))
    v.apply("orders.quantity_positive", d["quantity"] <= 0)
    calc = (d["quantity"] * d["unit_price"] * (1 - d["discount_pct"] / 100)).round(2)
    v.apply("orders.amount_formula", (calc - d["order_amount"]).abs() > AMOUNT_TOLERANCE)
    v.apply("orders.delivery_minutes_lt_120", d["delivery_minutes"] >= 120)
    v.apply("orders.delivery_minutes_typical",
            ((d["delivery_minutes"] < 8) | (d["delivery_minutes"] > 35)) & (d["delivery_minutes"] < 120))
    v.apply("orders.customer_fk", d["customer_id"].notna() & ~d["customer_id"].isin(valid_customers))
    v.apply("orders.product_fk", ~d["product_id"].isin(valid_products))
    v.apply("orders.store_fk", ~d["store_id"].isin(valid_stores))
    v.apply("orders.status_casing", (d["status_raw"] != d["order_status"]) & d["order_status"].isin(VALID_STATUS))
    v.apply("orders.payment_mode_variant",
            (d["payment_mode_raw"] != d["payment_mode"]) & d["payment_mode"].isin(VALID_PAYMENT_MODE))
    v.apply("orders.ts_not_future", d["order_ts_parsed"] > pd.Timestamp(as_of))
    # dataset-level checks
    latest = d["order_ts_parsed"][d["order_ts_parsed"] <= pd.Timestamp(as_of)].max()
    stale = int(pd.isna(latest) or (pd.Timestamp(as_of) - latest) > pd.Timedelta(hours=STALE_HOURS))
    v.apply("orders.data_not_stale", None, dataset_failed=stale)
    drop = int(previous_volume is not None and previous_volume > 0
               and (previous_volume - len(d)) / previous_volume > VOLUME_DROP_THRESHOLD)
    v.apply("orders.volume_drop", None, dataset_failed=drop)
    passed, failed, results = v.finish()
    return passed, failed, results


# ---- payments -------------------------------------------------------------------------
def validate_payments(raw: pd.DataFrame, all_order_ids: set, as_of: datetime):
    d = raw.copy().reset_index(drop=True)
    for c in ["payment_id", "order_id", "payment_status"]:
        d[c] = _strip(d[c])
    d["payment_status"] = d["payment_status"].str.upper()
    d["payment_mode_raw"] = _strip(d["payment_mode"])
    d["payment_mode"] = std_payment_mode(d["payment_mode"])
    d["amount_paid"] = _num(d["amount_paid"])
    d["payment_ts_parsed"] = _ts(d["payment_ts"])
    d["dup_rank"] = _dup_rank(d, "payment_id")
    v = Validator(d, "payments")
    v.apply("payments.payment_id_unique", d["dup_rank"] > 1)
    v.apply("payments.status_valid", ~d["payment_status"].isin(VALID_PAYMENT_STATUS))
    v.apply("payments.payment_mode_valid", ~d["payment_mode"].isin(VALID_PAYMENT_MODE))
    v.apply("payments.amount_positive", (d["payment_status"] == "SUCCESS") & (d["amount_paid"] <= 0))
    v.apply("payments.order_exists", ~d["order_id"].isin(all_order_ids))
    v.apply("payments.payment_mode_variant",
            (d["payment_mode_raw"] != d["payment_mode"]) & d["payment_mode"].isin(VALID_PAYMENT_MODE))
    v.apply("payments.gateway_ref_present", (d["payment_mode"] != "Cash on Delivery") & d["gateway_ref"].isna())
    v.apply("payments.payment_ts_not_future", d["payment_ts_parsed"] > pd.Timestamp(as_of))
    return v.finish()


# ---- customers ------------------------------------------------------------------------
def validate_customers(raw: pd.DataFrame, as_of: datetime):
    d = raw.copy().reset_index(drop=True)
    for c in ["customer_id", "email", "phone", "pincode", "city", "area", "full_name"]:
        d[c] = _strip(d[c])
    d["tier_raw"] = _strip(d["loyalty_tier"])
    d["loyalty_tier"] = d["tier_raw"].str.title()
    d["signup_parsed"] = pd.to_datetime(d["signup_date"], format="%Y-%m-%d", errors="coerce")
    d["dup_rank"] = _dup_rank(d, "customer_id")
    v = Validator(d, "customers")
    v.apply("customers.customer_id_unique", d["dup_rank"] > 1)
    v.apply("customers.city_not_null", d["city"].isna())
    v.apply("customers.email_format", ~d["email"].str.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+").fillna(False))
    v.apply("customers.phone_format", ~d["phone"].str.fullmatch(r"\d{10}").fillna(False))
    v.apply("customers.pincode_format", ~d["pincode"].str.fullmatch(r"\d{6}").fillna(False))
    first = d[d["dup_rank"] == 1]
    shared = first["email"].dropna()
    shared = set(shared[shared.duplicated(keep=False)])
    v.apply("customers.email_unique", (d["dup_rank"] == 1) & d["email"].isin(shared))
    v.apply("customers.loyalty_tier_casing", d["tier_raw"] != d["loyalty_tier"])
    v.apply("customers.signup_not_future", d["signup_parsed"] > pd.Timestamp(as_of))
    return v.finish()


# ---- products -------------------------------------------------------------------------
def validate_products(raw: pd.DataFrame):
    d = raw.copy().reset_index(drop=True)
    for c in ["product_id", "product_name", "category"]:
        d[c] = _strip(d[c])
    for c in ["mrp", "cost_price"]:
        d[c] = _num(d[c])
    d["_superseded"] = d["product_name"].str.contains(r"\(old\)", regex=True).fillna(False)
    d["dup_rank"] = _dup_rank(d, "product_id", order_cols=["_superseded"])
    v = Validator(d, "products")
    v.apply("products.product_id_unique", d["dup_rank"] > 1)
    v.apply("products.category_not_null", d["category"].isna())
    v.apply("products.mrp_positive", d["mrp"] <= 0)
    v.apply("products.cost_le_mrp", d["cost_price"] > d["mrp"])
    return v.finish()


# ---- stores ---------------------------------------------------------------------------
def validate_stores(raw: pd.DataFrame):
    d = raw.copy().reset_index(drop=True)
    for c in ["store_id", "zone", "pincode"]:
        d[c] = _strip(d[c])
    d["capacity_orders_per_day"] = _num(d["capacity_orders_per_day"])
    d["dup_rank"] = _dup_rank(d, "store_id")
    v = Validator(d, "stores")
    v.apply("stores.store_id_unique", d["dup_rank"] > 1)
    v.apply("stores.zone_not_null", d["zone"].isna())
    v.apply("stores.capacity_positive", d["capacity_orders_per_day"] <= 0)
    v.apply("stores.pincode_format", ~d["pincode"].str.fullmatch(r"\d{6}").fillna(False))
    return v.finish()


# --------------------------------------------------------------------------------------
# Payment reconciliation (ELT implements the same rules in include/sql/elt_43_*.sql)
# --------------------------------------------------------------------------------------
RECON_COLUMNS = ["order_id", "payment_id", "order_status", "order_amount", "payment_status", "amount_paid",
                 "order_payment_mode", "gateway_payment_mode", "mismatch_reason", "unreconciled_amount"]


def reconcile(orders_clean: pd.DataFrame, orders_all_ids: set, payments_all: pd.DataFrame) -> pd.DataFrame:
    """One row per mismatch reason.

    orders_clean : orders that passed every critical check (latest version of each order)
    orders_all_ids : every order_id seen in the source files (clean or rejected)
    payments_all : de-duplicated payments as received by the gateway (standardised, not DQ-filtered)
    """
    pay = payments_all[payments_all["dup_rank"] == 1][
        ["payment_id", "order_id", "payment_status", "amount_paid", "payment_mode"]]
    m = orders_clean[["order_id", "order_status", "order_amount", "payment_mode"]].merge(
        pay, on="order_id", how="left", suffixes=("", "_pay"))
    has, st, ps = m["payment_id"].notna(), m["order_status"], m["payment_status"]
    money = st.isin(["DELIVERED", "RETURNED"])
    diff = (m["order_amount"] - m["amount_paid"]).abs()
    rules = [
        ("MISSING_PAYMENT", ~has & money, m["order_amount"]),
        ("PAYMENT_FOR_CANCELLED_ORDER", has & (st == "CANCELLED"), m["amount_paid"].abs()),
        ("PAYMENT_FAILED", has & (st == "DELIVERED") & (ps == "FAILED"), m["order_amount"]),
        ("STATUS_MISMATCH", has & (st == "DELIVERED") & (ps == "REFUNDED"), m["amount_paid"].abs()),
        ("RETURNED_NOT_REFUNDED", has & (st == "RETURNED") & (ps != "REFUNDED"), m["amount_paid"].abs()),
        ("AMOUNT_MISMATCH", has & money & ps.isin(["SUCCESS", "REFUNDED"]) & (diff > AMOUNT_TOLERANCE), diff),
        ("PAYMENT_MODE_MISMATCH", has & money & (m["payment_mode"] != m["payment_mode_pay"]), 0.0),
    ]
    parts = []
    for reason, mask, unrec in rules:
        d = m[mask.fillna(False)]
        if d.empty:
            continue
        parts.append(pd.DataFrame({
            "order_id": d["order_id"], "payment_id": d["payment_id"], "order_status": d["order_status"],
            "order_amount": d["order_amount"], "payment_status": d["payment_status"], "amount_paid": d["amount_paid"],
            "order_payment_mode": d["payment_mode"], "gateway_payment_mode": d["payment_mode_pay"],
            "mismatch_reason": reason,
            "unreconciled_amount": unrec.loc[d.index] if hasattr(unrec, "loc") else unrec}))
    orph = pay[~pay["order_id"].isin(set(orders_clean["order_id"]))]
    if len(orph):
        parts.append(pd.DataFrame({
            "order_id": orph["order_id"], "payment_id": orph["payment_id"], "order_status": None,
            "order_amount": None, "payment_status": orph["payment_status"], "amount_paid": orph["amount_paid"],
            "order_payment_mode": None, "gateway_payment_mode": orph["payment_mode"],
            "mismatch_reason": orph["order_id"].map(lambda x: "ORPHAN_PAYMENT_ORDER_REJECTED"
                                                    if x in orders_all_ids else "ORPHAN_PAYMENT_NO_ORDER"),
            "unreconciled_amount": orph["amount_paid"].abs()}))
    if not parts:
        return pd.DataFrame(columns=RECON_COLUMNS)
    return pd.concat(parts, ignore_index=True)[RECON_COLUMNS]
