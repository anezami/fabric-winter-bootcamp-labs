# Fixed validator cell: read-only contract checks shared by every bootcamp notebook.
"""Fixed read-only checks for the tiny bootcamp fixture; not transformation code.

Each stage notebook calls validate_bootcamp(spark, stage) after its generated
cells. The expected state is derived from the landed CSV files: until
customer_changes.csv is uploaded every layer must hold the initial load;
afterwards every layer must hold the changed state. bootcamp_snapshot()
captures a layer's rows and Delta versions so a rerun can prove that it
changed nothing.
"""

from collections import Counter as _bootcamp_counter
from datetime import datetime as _bootcamp_datetime, timedelta as _bootcamp_timedelta
from decimal import Decimal as _bootcamp_decimal, ROUND_HALF_UP as _bootcamp_round_half_up


_BOOTCAMP_SOURCES = {
    "customers_source": ("initial/customers.csv", 6),
    "products": ("initial/products.csv", 4),
    "stores": ("initial/stores.csv", 2),
    "orders": ("initial/orders.csv", 7),
    "order_lines": ("initial/order_lines.csv", 9),
    "customer_changes_source": ("changes/customer_changes.csv", 3),
}
_BOOTCAMP_ATTRIBUTES = ("customer_id", "full_name", "email", "city", "loyalty_tier")
_BOOTCAMP_OPEN_END = _bootcamp_datetime(9999, 12, 31)
_BOOTCAMP_AUDIT_TYPES = {"_ingested_at": "timestamp", "_source_file": "string", "_batch_id": "string"}
_BOOTCAMP_HISTORY_TYPES = {
    "customer_sk": "bigint", **{key: "string" for key in _BOOTCAMP_ATTRIBUTES},
    "valid_from": "timestamp", "valid_to": "timestamp", "is_current": "boolean",
    "source_commit_version": "bigint",
}
_BOOTCAMP_SILVER_TABLES = {
    "dim_product": ("products", {"list_price": "decimal(18,2)"}),
    "dim_store": ("stores", {}),
    "orders": ("orders", {"order_ts": "timestamp"}),
    "order_lines": ("order_lines", {
        "line_number": "int", "quantity": "int",
        "unit_price": "decimal(18,2)", "discount": "decimal(6,4)",
    }),
}
_BOOTCAMP_SILVER_MARKS = {
    ("bronze.products", "silver.dim_product"),
    ("bronze.stores", "silver.dim_store"),
    ("bronze.orders", "silver.orders"),
    ("bronze.order_lines", "silver.order_lines"),
    ("bronze.customers_current", "silver.customer_history"),
}
_BOOTCAMP_GOLD_MARKS = {
    ("silver.customer_history", "gold.dim_customer"),
    ("silver.dim_product", "gold.dim_product"),
    ("silver.dim_store", "gold.dim_store"),
    ("silver.orders", "gold.fact_sales"),
    ("silver.order_lines", "gold.fact_sales"),
}
_BOOTCAMP_LAYER_TABLES = {
    "bronze": ("bronze.customers_source", "bronze.products", "bronze.stores",
               "bronze.orders", "bronze.order_lines", "bronze.customers_current"),
    "silver": ("silver.dim_product", "silver.dim_store", "silver.orders",
               "silver.order_lines", "silver.customer_history", "lab_ops.cdf_watermarks"),
    "gold": ("gold.dim_customer", "gold.dim_product", "gold.dim_store",
             "gold.dim_date", "gold.fact_sales", "lab_ops.cdf_watermarks"),
}


def _bootcamp_rows(frame):
    """Collect a small Spark DataFrame as a list of plain dictionaries."""
    return [row.asDict() for row in frame.collect()]


def _bootcamp_bag(rows):
    """Turn rows into a multiset so that comparisons ignore row order."""
    return _bootcamp_counter(tuple(sorted(row.items())) for row in rows)


def _bootcamp_equal(actual, expected, label):
    """Assert that actual and expected rows match exactly, ignoring order."""
    assert _bootcamp_bag(actual) == _bootcamp_bag(expected), f"{label}: rows/values differ from the spec"


def _bootcamp_table(spark, name, types=None):
    """Read a table and, when types are given, assert its exact column types."""
    frame = spark.table(name)
    if types is not None:
        actual = {field.name: field.dataType.simpleString() for field in frame.schema.fields}
        assert actual == types, f"{name}: expected types {types}, got {actual}"
    return _bootcamp_rows(frame)


def _bootcamp_version(spark, name):
    """Return the latest Delta commit version of a table."""
    return spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").first()["version"]


def _bootcamp_cdf_enabled(spark, name):
    """Assert that a table is stored as Delta with Change Data Feed enabled."""
    detail = spark.sql(f"DESCRIBE DETAIL {name}").first().asDict()
    assert detail["format"] == "delta", f"{name}: expected Delta storage"
    assert detail["properties"].get("delta.enableChangeDataFeed") == "true", (
        f"{name}: CDF must be enabled"
    )


def _bootcamp_source(spark, name):
    """Read a landed CSV as strings with Bronze's minimal cleansing applied."""
    path, count = _BOOTCAMP_SOURCES[name]
    rows = []
    for row in _bootcamp_rows(
        spark.read.option("header", True).option("inferSchema", False)
        .csv(f"Files/bootcamp/{path}")
    ):
        clean = {key: (value.strip() or None) if isinstance(value, str) else value
                 for key, value in row.items()}
        if any(value is not None for value in clean.values()):
            rows.append(clean)
    assert len(rows) == count, f"{path}: expected {count} immutable source rows"
    return rows


def bootcamp_changes_landed(spark):
    """Return True once customer_changes.csv has been uploaded to OneLake Files."""
    try:
        _bootcamp_source(spark, "customer_changes_source")
    except Exception as error:
        if "PATH_NOT_FOUND" in str(error) or "Path does not exist" in str(error):
            return False
        raise
    return True


def _bootcamp_expected_state(spark, changed):
    """Return the expected keyed customer state before or after the change batch."""
    state = {row["customer_id"]: row for row in _bootcamp_source(spark, "customers_source")}
    if changed:
        columns = tuple(next(iter(state.values())))
        for event in _bootcamp_source(spark, "customer_changes_source"):
            if event["operation"] == "D":
                del state[event["customer_id"]]
            else:
                state[event["customer_id"]] = {key: event[key] for key in columns}
    return list(state.values())


def _bootcamp_expected_history(spark, changed):
    """Return the expected SCD2 customer versions, without surrogate keys."""
    events = {}
    if changed:
        events = {row["customer_id"]: row for row in _bootcamp_source(spark, "customer_changes_source")}
    expected = []
    for customer in _bootcamp_source(spark, "customers_source"):
        event = events.get(customer["customer_id"])
        expected.append({
            **{key: customer[key] for key in _BOOTCAMP_ATTRIBUTES},
            "valid_from": _bootcamp_datetime.fromisoformat(customer["effective_at"]),
            "valid_to": _bootcamp_datetime.fromisoformat(event["effective_at"]) if event
            else _BOOTCAMP_OPEN_END,
            "is_current": event is None,
        })
    for event in events.values():
        if event["operation"] in ("U", "I"):
            expected.append({
                **{key: event[key] for key in _BOOTCAMP_ATTRIBUTES},
                "valid_from": _bootcamp_datetime.fromisoformat(event["effective_at"]),
                "valid_to": _BOOTCAMP_OPEN_END, "is_current": True,
            })
    return expected


def _bootcamp_check_history(spark, changed):
    """Check SCD2 keys, validity bounds, source commit versions and attributes."""
    rows = _bootcamp_table(spark, "silver.customer_history", _BOOTCAMP_HISTORY_TYPES)
    keys = [row["customer_sk"] for row in rows]
    assert None not in keys and len(keys) == len(set(keys)), "SCD2 keys must be unique/non-null"
    source_version = _bootcamp_version(spark, "bronze.customers_current")
    assert all(row["source_commit_version"] is not None
               and 0 <= row["source_commit_version"] <= source_version for row in rows), (
        "SCD2 rows need source commit versions within Bronze history"
    )
    for customer in {row["customer_id"] for row in rows}:
        versions = [row["source_commit_version"] for row in
                    sorted((row for row in rows if row["customer_id"] == customer),
                           key=lambda row: row["valid_from"])]
        assert versions == sorted(set(versions)), f"{customer}: commit versions must increase per version"
    expected = _bootcamp_expected_history(spark, changed)
    projection = [{key: row[key] for key in expected[0]} for row in rows]
    _bootcamp_equal(projection, expected, "SCD2 attributes, validity, and retained history")
    return rows


def _bootcamp_check_marks(spark, pairs):
    """Assert one current watermark per source/target pair that this layer owns."""
    rows = _bootcamp_table(spark, "lab_ops.cdf_watermarks", {
        "source_table": "string", "target_table": "string", "last_processed_version": "bigint",
    })
    counts = _bootcamp_counter((row["source_table"], row["target_table"]) for row in rows)
    assert set(counts) <= _BOOTCAMP_SILVER_MARKS | _BOOTCAMP_GOLD_MARKS, "Unexpected watermark pairs"
    assert all(count == 1 for count in counts.values()), "Duplicate watermark pairs"
    for source, target in sorted(pairs):
        assert counts[(source, target)] == 1, f"Missing watermark {source} -> {target}"
    for row in rows:
        if (row["source_table"], row["target_table"]) in pairs:
            _bootcamp_cdf_enabled(spark, row["source_table"])
            assert row["last_processed_version"] == _bootcamp_version(spark, row["source_table"]), (
                f"{row['source_table']} -> {row['target_table']}: watermark is not current"
            )


def _bootcamp_check_bronze(spark, changed):
    """Check raw string tables, audit lineage, Bronze CDF and the keyed customer state."""
    for name, (path, _) in _BOOTCAMP_SOURCES.items():
        if name == "customer_changes_source" and not changed:
            continue
        expected = _bootcamp_source(spark, name)
        types = {key: "string" for key in expected[0]}
        types.update(_BOOTCAMP_AUDIT_TYPES)
        actual = _bootcamp_table(spark, f"bronze.{name}", types)
        assert all(row["_ingested_at"] and row["_batch_id"]
                   and row["_source_file"] and row["_source_file"].endswith(path)
                   for row in actual), f"bronze.{name}: missing/wrong audit lineage"
        _bootcamp_equal([{key: row[key] for key in expected[0]} for row in actual],
                        expected, f"bronze.{name}")
        _bootcamp_cdf_enabled(spark, f"bronze.{name}")
    expected = _bootcamp_expected_state(spark, changed)
    types = {key: "string" for key in expected[0]}
    types.update(_BOOTCAMP_AUDIT_TYPES)
    state = _bootcamp_table(spark, "bronze.customers_current", types)
    _bootcamp_equal([{key: row[key] for key in expected[0]} for row in state],
                    expected, "Keyed Bronze customer state")
    _bootcamp_cdf_enabled(spark, "bronze.customers_current")


def _bootcamp_check_silver(spark, changed):
    """Check typed Silver tables, SCD2 history, Silver CDF and Bronze-to-Silver watermarks."""
    for table, (source, typed) in _BOOTCAMP_SILVER_TABLES.items():
        expected = _bootcamp_source(spark, source)
        types = {key: typed.get(key, "string") for key in expected[0]}
        for row in expected:
            for key, kind in typed.items():
                if kind == "timestamp":
                    row[key] = _bootcamp_datetime.fromisoformat(row[key])
                elif kind == "int":
                    row[key] = int(row[key])
                else:
                    row[key] = _bootcamp_decimal(row[key])
        _bootcamp_equal(_bootcamp_table(spark, f"silver.{table}", types), expected, table)
        _bootcamp_cdf_enabled(spark, f"silver.{table}")
    history = _bootcamp_check_history(spark, changed)
    _bootcamp_cdf_enabled(spark, "silver.customer_history")
    _bootcamp_check_marks(spark, _BOOTCAMP_SILVER_MARKS)
    return history


def _bootcamp_check_gold(spark, changed):
    """Check the Gold star against Silver and the sources, plus Silver-to-Gold watermarks."""
    history = _bootcamp_check_silver(spark, changed)
    _bootcamp_equal(
        _bootcamp_table(spark, "gold.dim_customer", _BOOTCAMP_HISTORY_TYPES),
        history, "Gold customer history",
    )
    for table in ("dim_product", "dim_store"):
        frame = spark.table(f"silver.{table}")
        types = {field.name: field.dataType.simpleString() for field in frame.schema.fields}
        _bootcamp_equal(_bootcamp_table(spark, f"gold.{table}", types), _bootcamp_rows(frame), table)

    orders = {row["order_id"]: row for row in _bootcamp_source(spark, "orders")}
    expected = []
    for line in _bootcamp_source(spark, "order_lines"):
        order = orders[line["order_id"]]
        when = _bootcamp_datetime.fromisoformat(order["order_ts"])
        versions = [row for row in history if row["customer_id"] == order["customer_id"]
                    and row["valid_from"] <= when < row["valid_to"]]
        assert len(versions) == 1, "Order must match exactly one as-of customer version"
        expected.append({
            "order_id": line["order_id"], "line_number": int(line["line_number"]),
            "date_key": int(when.strftime("%Y%m%d")), "customer_sk": versions[0]["customer_sk"],
            "product_id": line["product_id"], "store_id": order["store_id"],
            "channel": order["channel"], "quantity": int(line["quantity"]),
            "unit_price": _bootcamp_decimal(line["unit_price"]),
            "discount": _bootcamp_decimal(line["discount"]),
            "net_amount": (int(line["quantity"]) * _bootcamp_decimal(line["unit_price"])
                           * (1 - _bootcamp_decimal(line["discount"]))).quantize(
                               _bootcamp_decimal("0.01"), rounding=_bootcamp_round_half_up),
        })
    types = {
        "order_id": "string", "line_number": "int", "date_key": "int",
        "customer_sk": "bigint", "product_id": "string", "store_id": "string",
        "channel": "string", "quantity": "int", "unit_price": "decimal(18,2)",
        "discount": "decimal(6,4)", "net_amount": "decimal(18,2)",
    }
    _bootcamp_equal(_bootcamp_table(spark, "gold.fact_sales", types), expected, "Sales facts")
    dates = [_bootcamp_datetime.fromisoformat(row["order_ts"]).date() for row in orders.values()]
    day, end = min(dates), max(dates)
    calendar = []
    while day <= end:
        calendar.append({
            "date_key": int(day.strftime("%Y%m%d")), "date": day,
            "year": day.year, "month": day.month,
            "day_of_week": (day.weekday() + 1) % 7 + 1,
        })
        day += _bootcamp_timedelta(days=1)
    _bootcamp_equal(_bootcamp_table(spark, "gold.dim_date", {
        "date_key": "int", "date": "date", "year": "int", "month": "int", "day_of_week": "int",
    }), calendar, "Date dimension")
    _bootcamp_check_marks(spark, _BOOTCAMP_GOLD_MARKS)


def _bootcamp_check_mlv(spark):
    """Check both materialized lake views against Silver and confirm their definitions."""
    orders = {row["order_id"]: row for row in _bootcamp_table(spark, "silver.orders")}
    groups = {}
    for line in _bootcamp_table(spark, "silver.order_lines"):
        order = orders[line["order_id"]]
        key = int(order["order_ts"].strftime("%Y%m%d")), order["store_id"], order["channel"]
        group = groups.setdefault(key, {
            "date_key": key[0], "store_id": key[1], "channel": key[2],
            "sales_lines": 0, "units": 0, "net_amount": _bootcamp_decimal("0.00"),
        })
        group["sales_lines"] += 1
        group["units"] += line["quantity"]
        group["net_amount"] += (line["quantity"] * line["unit_price"] * (1 - line["discount"])).quantize(
            _bootcamp_decimal("0.01"), rounding=_bootcamp_round_half_up)
    _bootcamp_equal(_bootcamp_table(spark, "gold.sales_daily", {
        "date_key": "int", "store_id": "string", "channel": "string",
        "sales_lines": "bigint", "units": "bigint", "net_amount": "decimal(18,2)",
    }), list(groups.values()), "MLV groups and totals")
    columns = ("customer_sk", *_BOOTCAMP_ATTRIBUTES, "valid_from")
    current = [{key: row[key] for key in columns}
               for row in _bootcamp_table(spark, "silver.customer_history") if row["is_current"]]
    _bootcamp_equal(_bootcamp_table(spark, "gold.customer_current", {
        key: _BOOTCAMP_HISTORY_TYPES[key] for key in columns
    }), current, "MLV current customers")
    for name in ("gold.sales_daily", "gold.customer_current"):
        statement = spark.sql(f"SHOW CREATE MATERIALIZED LAKE VIEW {name}").first()[0]
        assert "MATERIALIZED LAKE VIEW" in statement.upper(), f"{name}: not a materialized lake view"
        print(f"{name}: materialized lake view definition confirmed")


def bootcamp_snapshot(spark, layer):
    """Capture a layer's rows and Delta versions so a rerun can prove it changed nothing."""
    names = list(_BOOTCAMP_LAYER_TABLES[layer])
    if layer == "bronze" and bootcamp_changes_landed(spark):
        names.append("bronze.customer_changes_source")
    return {name: (_bootcamp_version(spark, name), _bootcamp_bag(_bootcamp_table(spark, name)))
            for name in names}


def validate_bootcamp(spark, stage):
    """Run the fixed contract checks for one stage: bronze, silver, gold or mlv."""
    checks = {"bronze": _bootcamp_check_bronze, "silver": _bootcamp_check_silver,
              "gold": _bootcamp_check_gold}
    if stage not in (*checks, "mlv"):
        raise ValueError(f"Unknown bootcamp stage: {stage}")
    changed = bootcamp_changes_landed(spark)
    if stage == "mlv":
        _bootcamp_check_mlv(spark)
    else:
        checks[stage](spark, changed)
    state = "after the customer change batch" if changed else "initial load"
    print(f"PASS: bootcamp {stage} contract, {state} (fixed read-only checks)")
