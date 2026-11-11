# Lab 2: Silver from the Bronze change feed

**7 minutes.** Prerequisite: Lab 1 passes.

Open `nb_bootcamp_silver`. It has its own description, validator, fixed
baseline cell and fixed check cell; session variables and temporary views do
not carry over from Bronze, so declare what you need here.

## 1. Generate and review

```text
Use spark-cli and spec sections 3 and 5 of specs/bootcamp-lakehouse.spec.md.
Save a short Silver plan and numbered tasks under specs/plans/.
Generate NEW Spark SQL for nb_bootcamp_silver as %%sql cells that read only
Bronze and write only silver plus its five lab_ops.cdf_watermarks rows.
Follow the Native Spark SQL rules in section 3:
- Create the five typed Silver tables once, empty, with CDF enabled, and
  lab_ops.cdf_watermarks if it does not exist.
- One cell per Bronze source and Silver target. Declare session variables
  for the version range (from = watermark + 1, or 0 on the first run; to =
  the source's latest change version), read table_changes() through to,
  ignore update_preimage, keep the latest change per key, apply a keyed MERGE
  (insert, update, delete) whose source query is inline in USING (...) (a
  MERGE cannot read a temporary view over a Delta table), then advance the
  watermark with a MERGE that commits nothing when it already matches.
- customer_history is SCD2 from bronze.customers_current: a MERGE closes the
  current version of each changed or deleted customer, then an INSERT opens
  the new versions; valid_from/valid_to come from effective_at; customer_sk
  is a unique BIGINT; source_commit_version is the Bronze commit version.
- Stop on unavailable history; a run without new versions commits nothing.
Every cell starts with %%sql, then -- Stage: silver - <what the cell does>,
then at least one statement; use PySpark only if SQL cannot do a step and
say why in the header. Replace only the placeholder; keep the fixed cells.
Do not copy supplied transformations or edit the spec, CSVs or validator.
Explain the grain and SCD2 policy, show the code and write set in
lh_bootcamp, then stop for my approval.
```

Approve the types, SCD2 validity policy, CDF reads and write set. Inspect
every cell: each generated cell is `%%sql` with a `-- Stage: silver - …`
header, and the fixed baseline and check cells are Silver-specific too, so no
Gold or MLV code belongs here. Use
[Lab 1's execution prompt](01-bronze.md#2-execute-approved-code) to save and
run this notebook once, or run approved cells manually.

## 2. Verify and explain

The shell's fixed baseline and check cells wrap your generated cells:

```python
bootcamp_begin(spark, "silver")       # fixed baseline cell, before the generated cells
# ... your NEW %%sql cells run here ...
validate_bootcamp(spark, "silver")    # fixed check cell, after the generated cells
```

If time remains, an optional second **Run all** in the warm session lets the
baseline find Silver current, so the check proves that your cells committed
nothing.

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
  and `PASS: bootcamp silver was built or brought to contract by this run`.
  An optional second **Run all** in the warm session prints
  `PASS: bootcamp silver was already current and the rerun changed nothing`.
- Typed products, stores, orders and order lines match every source value.
- Six customer versions have unique BIGINT keys, exact validity bounds,
  current flags and Bronze source commit versions.
- CDF is enabled on all five Silver tables; the five Bronze-to-Silver
  watermarks equal the Bronze versions.
- Code is documented native Spark SQL. Save output and tick your tasks.

On failure, send Copilot the assertion or error, approve its patch to the
generated code and rerun the notebook. Never recreate history, reset
watermarks or change expected results; see [repair and recovery](recovery.md).

**Next:** [Lab 3 - Gold from the Silver change feed](03-gold-cdf.md).
