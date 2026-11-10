# Lab 2: Silver and SCD2 history

**12 minutes.** Prerequisite: Lab 1 passes.

Open `nb_bootcamp_silver`. Its context is independent of Bronze; imports
and helpers must be defined here.

## 1. Generate and review

```text
Use spark-cli and section 5 of specs/bootcamp-lakehouse.spec.md.
Save a short Silver plan and numbered tasks under specs/plans/.
Generate new PySpark/Spark SQL cells for typed Silver products, stores,
orders, order_lines and customer_history. Implement exact source values,
types, unique surrogate keys, validity bounds, is_current and
source_commit_version. Enable Silver history CDF before changes.
Bootstrap the Bronze-to-Silver watermark durably when creating history.
An existing target without its watermark is an error.
Do not apply the customer event batch yet. Define all imports/helpers
locally. Never reset history or watermarks, or re-enable CDF on reruns.
Preserve the validator and final fixed check. Do not alter spec/CSV/checks.
Explain grain and the SCD2 policy, show code and the write set in lh_bootcamp,
then stop for approval.
```

Approve types, history policy and the write set. Use
[Lab 1's execution prompt](01-bronze.md#2-execute-approved-code) to save and
run this notebook, or run approved cells manually.

## 2. Verify and explain

Run the shell's unchanged check:

```python
validate_bootcamp(spark, "silver", after_changes=bootcamp_changes_started(spark))
```

Inspect the initial dimension:

```sql
SELECT customer_id, customer_sk, city, loyalty_tier,
       valid_from, valid_to, is_current
FROM silver.customer_history
ORDER BY customer_id, valid_from;
```

Explain why an attribute change closes a version instead of overwriting it,
and why facts will join on a surrogate key valid at the order timestamp.

## Definition of done

- Actual `PASS: bootcamp silver contract (fixed read-only checks)`.
- Typed products, stores, orders and order lines match every source value.
- Six initial customer versions have unique BIGINT keys, exact validity
  bounds and current flags; history CDF is enabled.
- One durable Bronze-to-Silver bootstrap watermark matches the captured
  Bronze version. Save output and tick your tasks.

On completed-build replay, the fixed `after_changes` check validates retained
history, not a reset to six bootstrap rows. If blocked, use
[repair and recovery](recovery.md); never recreate history to hide a failure.

**Next:** [Lab 3 - Gold and CDF](03-gold-cdf.md).
