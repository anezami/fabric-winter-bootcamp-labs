# Lab 4: Materialized lake view

**8 minutes.** Prerequisite: Gold and CDF checks pass.

Open `nb_bootcamp_mlv`. The goal is a real Fabric MLV serving daily sales by
date, store and channel, not merely a query returning the same numbers.

## 1. Generate and execute

```text
Use spark-cli and section 7 of specs/bootcamp-lakehouse.spec.md.
Save a short MLV plan and numbered tasks under specs/plans/.
Generate new Spark SQL/PySpark code creating gold.sales_daily as a
MATERIALIZED LAKE VIEW, not a table or ordinary view. Implement the exact
grain, columns/types, six FAIL constraints and source aggregate.
Define any needed imports locally and preserve the validator/final check.
Do not alter the spec, fixtures or checks, or claim incremental refresh
without Fabric run evidence. Show code/write set, explain explicit CDF
versus Fabric-managed refresh, then stop for approval.
```

Approve, save and execute using
[Lab 1's execution prompt](01-bronze.md#2-execute-approved-code), or run the
approved cells manually. Do not overlap execution paths.

## 2. Verify and show the result

```python
validate_bootcamp(spark, "mlv")
```

The fixed check compares every group and value and displays
`SHOW CREATE MATERIALIZED LAKE VIEW`. Inspect that definition, its six
`ON MISMATCH FAIL` constraints and **Materialized lake views -> Manage** for
lineage/refresh details. Report refresh strategy only when Fabric exposes it.

```sql
SELECT date_key, store_id, channel, sales_lines, units, net_amount
FROM gold.sales_daily
ORDER BY date_key, store_id, channel;
```

Also show C003's two versions in `silver.customer_history` and explain why
its October sales still use the original version.

## Definition of done

- Actual `PASS: bootcamp mlv contract (fixed read-only checks)`.
- Real MLV DDL with six FAIL constraints and inspected lineage from
  `gold.fact_sales`.
- Seven groups reconcile individually, totalling nine sales lines,
  16 units and 516.73 net sales.
- Generated code, check output, daily sales and customer history are saved
  or visible in your notebooks. Tick the completed tasks.

If MLV support is unavailable, record the stage as incomplete. A checked
aggregate SELECT is useful diagnosis but is not an MLV pass. Follow
[repair and recovery](recovery.md).

## Finish the live build

Stop after the 45-minute build. Show the pipeline dependency graph.
An optional full serial replay can run afterward using
[the setup execution guidance](setup.md#4-execute-during-the-labs); preserve
customer history, keys, facts and watermarks. Stop sessions you started.

**Optional next steps:** [Project Osmos](05-osmos-replay.md) in a fresh
approved target, or [semantic model and Fabric IQ](06-semantic-model-fabric-iq.md)
over this completed Gold layer.
