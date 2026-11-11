# Lab 2: Silver from the Bronze change feed

**9 minutes.** Prerequisite: Lab 1 passes.

Open `nb_bootcamp_silver`. It has its own description, validator and fixed
final check; no Python state carries over from Bronze, so imports and helpers
must be defined here.

## 1. Generate and review

```text
Use spark-cli and spec sections 3 and 5 of specs/bootcamp-lakehouse.spec.md.
Save a short Silver plan and numbered tasks under specs/plans/.
Generate NEW PySpark/Spark SQL code for nb_bootcamp_silver that reads only
Bronze and writes only silver plus its five lab_ops.cdf_watermarks rows.
Define process_bronze_to_silver() and call it once at the end:
- Create the five typed Silver tables once, empty, with CDF enabled.
- For each Bronze source, read its change feed from watermark + 1 (version 0
  on the first run) through a captured current version, ignore
  update_preimage, keep the latest change per key, apply a keyed MERGE
  (insert, update, delete), then advance the watermark.
- customer_history is SCD2 from bronze.customers_current: an update expires
  the open version and inserts a new one, an insert opens a version, a delete
  only expires; valid_from/valid_to come from effective_at; customer_sk is a
  unique BIGINT; source_commit_version is the Bronze commit version.
- Stop on unavailable history; a run without new versions commits nothing.
Define all imports and helpers here. Start the cell with a # comment header
and give every function a docstring. Replace only the placeholder; keep the
fixed cells. Do not copy supplied transformations or edit the spec, CSVs or
validator. Explain the grain and SCD2 policy, show the code and write set in
lh_bootcamp, then stop for my approval.
```

Approve the types, SCD2 validity policy, CDF reads and write set. Inspect every
cell, including the fixed checks: no Gold or MLV code belongs here. Use
[Lab 1's execution prompt](01-bronze.md#2-execute-approved-code) to save and
run this notebook, or run approved cells manually.

## 2. Verify and explain

The shell's unchanged final check runs:

```python
validate_bootcamp(spark, "silver")
before = bootcamp_snapshot(spark, "silver")
process_bronze_to_silver()
assert bootcamp_snapshot(spark, "silver") == before, "Rerunning process_bronze_to_silver() changed silver"
print("PASS: silver rerun is a no-op")
```

Inspect the outcome, not just the code:

```sql
SELECT customer_id, customer_sk, city, loyalty_tier,
       valid_from, valid_to, is_current, source_commit_version
FROM silver.customer_history
ORDER BY customer_id, valid_from;
```

Explain why an attribute change closes a version instead of overwriting it,
and why facts will join on a surrogate key valid at the order timestamp.

## Definition of done

- Actual `PASS: bootcamp silver contract, initial load (fixed read-only checks)`
  and `PASS: silver rerun is a no-op`.
- Typed products, stores, orders and order lines match every source value.
- Six customer versions have unique BIGINT keys, exact validity bounds,
  current flags and Bronze source commit versions.
- CDF is enabled on all five Silver tables; the five Bronze-to-Silver
  watermarks equal the Bronze versions.
- Code is documented. Save output and tick your tasks.

On failure, send Copilot the assertion or error, approve its patch to the
generated code and rerun the notebook. Never recreate history, reset
watermarks or change expected results; see [repair and recovery](recovery.md).

**Next:** [Lab 3 - Gold from the Silver change feed](03-gold-cdf.md).
