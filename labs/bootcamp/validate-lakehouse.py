"""Fixed read-only checks for the tiny bootcamp fixture; not transformation code."""

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
_BOOTCAMP_HISTORY_TYPES = {
    "customer_sk": "bigint", **{key: "string" for key in _BOOTCAMP_ATTRIBUTES},
    "valid_from": "timestamp", "valid_to": "timestamp", "is_current": "boolean",
    "source_commit_version": "bigint",
}


def _bootcamp_rows(frame):
    return [row.asDict() for row in frame.collect()]


def _bootcamp_equal(actual, expected, label):
    def bag(rows):
        return _bootcamp_counter(tuple(sorted(row.items())) for row in rows)

    assert bag(actual) == bag(expected), f"{label}: rows/values differ from the spec"


def _bootcamp_table(spark, name, types=None):
    frame = spark.table(name)
    if types is not None:
        actual = {field.name: field.dataType.simpleString() for field in frame.schema.fields}
        assert actual == types, f"{name}: expected types {types}, got {actual}"
    return _bootcamp_rows(frame)


def _bootcamp_cdf_enabled(spark, name):
    detail = spark.sql(f"DESCRIBE DETAIL {name}").first().asDict()
    assert detail["format"] == "delta", f"{name}: expected Delta storage"
    assert detail["properties"].get("delta.enableChangeDataFeed") == "true", (
        f"{name}: CDF must be enabled"
    )


def _bootcamp_source(spark, name):
    path, count = _BOOTCAMP_SOURCES[name]
    rows = _bootcamp_rows(
        spark.read.option("header", True).option("inferSchema", False)
        .csv(f"Files/bootcamp/{path}")
    )
    assert len(rows) == count, f"{path}: expected {count} immutable source rows"
    return rows


def _bootcamp_history(spark, changed):
    rows = _bootcamp_table(spark, "silver.customer_history", _BOOTCAMP_HISTORY_TYPES)
    keys = [row["customer_sk"] for row in rows]
    assert None not in keys and len(keys) == len(set(keys)), "SCD2 keys must be unique/non-null"
    assert all(row["source_commit_version"] is not None and row["source_commit_version"] >= 0
               for row in rows), "SCD2 rows need source commit versions"
    source_version = spark.sql("DESCRIBE HISTORY bronze.customers_current LIMIT 1").first()["version"]
    assert all(row["source_commit_version"] <= source_version for row in rows), (
        "SCD2 source commit version exceeds Bronze history"
    )
    if not changed:
        assert all(row["source_commit_version"] == source_version for row in rows), (
            "SCD2 bootstrap source versions must match the captured snapshot"
        )
    expected = []
    events = {row["customer_id"]: row for row in _bootcamp_source(spark, "customer_changes_source")}
    for customer in _bootcamp_source(spark, "customers_source"):
        event = events.get(customer["customer_id"]) if changed else None
        expected.append({
            **{key: customer[key] for key in _BOOTCAMP_ATTRIBUTES},
            "valid_from": _bootcamp_datetime.fromisoformat(customer["effective_at"]),
            "valid_to": _bootcamp_datetime.fromisoformat(event["effective_at"]) if event
            else _bootcamp_datetime(9999, 12, 31),
            "is_current": event is None,
        })
    if changed:
        for event in events.values():
            if event["operation"] in ("U", "I"):
                expected.append({
                    **{key: event[key] for key in _BOOTCAMP_ATTRIBUTES},
                    "valid_from": _bootcamp_datetime.fromisoformat(event["effective_at"]),
                    "valid_to": _bootcamp_datetime(9999, 12, 31), "is_current": True,
                })
    projection = [{key: row[key] for key in expected[0]} for row in rows]
    _bootcamp_equal(projection, expected, "SCD2 attributes, validity, and retained history")
    _bootcamp_cdf_enabled(spark, "silver.customer_history")
    return rows


def _bootcamp_silver(spark):
    casts = {
        "dim_product": ("products", {"list_price": "decimal(18,2)"}),
        "dim_store": ("stores", {}),
        "orders": ("orders", {"order_ts": "timestamp"}),
        "order_lines": ("order_lines", {
            "line_number": "int", "quantity": "int",
            "unit_price": "decimal(18,2)", "discount": "decimal(6,4)",
        }),
    }
    for table, (source, typed) in casts.items():
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


def _bootcamp_gold(spark, changed):
    history = _bootcamp_history(spark, changed)
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


def bootcamp_snapshot(spark):
    """Capture persisted state around replay without invoking generated functions."""
    names = ("bronze.customers_current", "silver.customer_history",
             "gold.dim_customer", "gold.fact_sales", "lab_ops.cdf_watermarks")
    return {
        name: _bootcamp_counter(tuple(sorted(row.items())) for row in _bootcamp_table(spark, name))
        for name in names
    }


def bootcamp_changes_started(spark):
    """Detect persisted events, including an interrupted partial application."""
    initial = _bootcamp_source(spark, "customers_source")
    current = _bootcamp_table(spark, "bronze.customers_current")
    columns = initial[0].keys()
    before = _bootcamp_counter(tuple(row[key] for key in columns) for row in initial)
    after = _bootcamp_counter(tuple(row[key] for key in columns) for row in current)
    return before != after


def _bootcamp_changed_state(spark):
    expected = {row["customer_id"]: row for row in _bootcamp_source(spark, "customers_source")}
    columns = tuple(next(iter(expected.values())))
    for event in _bootcamp_source(spark, "customer_changes_source"):
        if event["operation"] == "D":
            del expected[event["customer_id"]]
        else:
            expected[event["customer_id"]] = {key: event[key] for key in columns}
    actual = _bootcamp_table(spark, "bronze.customers_current")
    _bootcamp_equal([{key: row[key] for key in columns} for row in actual],
                    list(expected.values()), "Changed Bronze state")


def validate_bootcamp(spark, stage, after_changes=False):
    """Run unchanged after the corresponding generated lab cells in Fabric."""
    if stage not in ("bronze", "silver", "gold", "cdf", "mlv"):
        raise ValueError(f"Unknown bootcamp stage: {stage}")
    if stage == "bronze":
        for name, (path, _) in _BOOTCAMP_SOURCES.items():
            expected = _bootcamp_source(spark, name)
            types = {key: "string" for key in expected[0]}
            types.update({"_ingested_at": "timestamp", "_source_file": "string", "_batch_id": "string"})
            actual = _bootcamp_table(spark, f"bronze.{name}", types)
            assert all(row["_ingested_at"] and row["_batch_id"]
                       and row["_source_file"] and row["_source_file"].endswith(path)
                       for row in actual), f"bronze.{name}: missing/wrong audit lineage"
            _bootcamp_equal([{key: row[key] for key in expected[0]} for row in actual],
                            expected, f"bronze.{name}")
        state = _bootcamp_table(spark, "bronze.customers_current")
        source = _bootcamp_source(spark, "customers_source")
        if after_changes:
            _bootcamp_changed_state(spark)
        else:
            _bootcamp_equal([{key: row[key] for key in source[0]} for row in state],
                            source, "Initial keyed Bronze state")
        _bootcamp_cdf_enabled(spark, "bronze.customers_current")
    elif stage == "silver":
        _bootcamp_silver(spark)
        _bootcamp_history(spark, changed=after_changes)
        marks = _bootcamp_table(spark, "lab_ops.cdf_watermarks", {
            "source_table": "string", "target_table": "string", "last_processed_version": "bigint",
        })
        if after_changes:
            assert len(marks) == 2, "Completed incremental state needs both watermark pairs"
        else:
            assert len(marks) in (1, 2), "Silver needs a bootstrap watermark"
        expected_pairs = {("bronze.customers_current", "silver.customer_history")}
        if len(marks) == 2:
            expected_pairs.add(("silver.customer_history", "gold.dim_customer"))
        assert {(row["source_table"], row["target_table"]) for row in marks} == expected_pairs, (
            "Silver has duplicate or unexpected watermark pairs"
        )
        bronze_marks = [row for row in marks if row["source_table"] == "bronze.customers_current"
                        and row["target_table"] == "silver.customer_history"]
        assert len(bronze_marks) == 1, "Silver needs exactly one Bronze-to-Silver watermark"
        mark = bronze_marks[0]
        assert (mark["source_table"], mark["target_table"]) == (
            "bronze.customers_current", "silver.customer_history"
        ), "Silver bootstrap has the wrong watermark pair"
        version = spark.sql("DESCRIBE HISTORY bronze.customers_current LIMIT 1").first()["version"]
        assert mark["last_processed_version"] == version, "Silver bootstrap watermark is not current"
    elif stage in ("gold", "cdf"):
        _bootcamp_silver(spark)
        _bootcamp_gold(spark, changed=stage == "cdf" or after_changes)
        marks = _bootcamp_table(spark, "lab_ops.cdf_watermarks", {
            "source_table": "string", "target_table": "string", "last_processed_version": "bigint",
        })
        pairs = {("bronze.customers_current", "silver.customer_history"),
                 ("silver.customer_history", "gold.dim_customer")}
        assert len(marks) == 2 and {(row["source_table"], row["target_table"])
                                  for row in marks} == pairs, "Expected exactly two watermark pairs"
        for mark in marks:
            source = mark["source_table"]
            _bootcamp_cdf_enabled(spark, source)
            version = spark.sql(f"DESCRIBE HISTORY {source} LIMIT 1").first()["version"]
            assert mark["last_processed_version"] == version, f"{source}: watermark is not current"
        if stage == "cdf":
            _bootcamp_changed_state(spark)
    else:
        expected = {}
        for row in _bootcamp_table(spark, "gold.fact_sales"):
            key = row["date_key"], row["store_id"], row["channel"]
            if key not in expected:
                expected[key] = dict(zip(("date_key", "store_id", "channel"), key))
                expected[key].update(sales_lines=0, units=0, net_amount=_bootcamp_decimal("0.00"))
            group = expected[key]
            group["sales_lines"] += 1
            group["units"] += row["quantity"]
            group["net_amount"] += row["net_amount"]
        _bootcamp_equal(_bootcamp_table(spark, "gold.sales_daily", {
            "date_key": "int", "store_id": "string", "channel": "string",
            "sales_lines": "bigint", "units": "bigint", "net_amount": "decimal(18,2)",
        }), list(expected.values()), "MLV groups and totals")
        spark.sql("SHOW CREATE MATERIALIZED LAKE VIEW gold.sales_daily").show(truncate=False)
    print(f"PASS: bootcamp {stage} contract (fixed read-only checks)")
