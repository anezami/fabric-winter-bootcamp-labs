# Lab 3: Gold from the Silver change feed

**9 minutes.** Prerequisite: Lab 2 passes.

Open `nb_bootcamp_gold`. It reads only Silver; session variables and
temporary views do not carry over from other notebooks, so declare what you
need here. Keep the fixed cells unchanged.

## 1. Generate and review

```text
Use spark-cli and spec sections 3, 6 and 8 of
specs/bootcamp-lakehouse.spec.md. Save a short Gold plan and numbered tasks
under specs/plans/. Generate NEW Spark SQL for nb_bootcamp_gold as %%sql
cells that read only silver and write only gold plus its five
lab_ops.cdf_watermarks rows. Follow the Native Spark SQL rules in section 3:
- Create the five documented Gold tables once, with table and column
  comments and the exact columns/types.
- For each Silver source, declare session variables for the version range
  (from = watermark + 1, or 0 on the first run; to = the source's latest
  change version), read table_changes() through to, ignore update_preimage
  and write that subquery inline in every statement that reads it.
- dim_customer, dim_product and dim_store apply the latest change per key
  with a keyed MERGE (dim_customer keyed on customer_sk); dim_date adds the
  missing days between the first and last order date.
- fact_sales recomputes only the orders touched by the orders/order_lines
  changes (keep their ids in an ARRAY session variable) at order-line grain,
  joins the customer version valid at order_ts, rounds net_amount per line,
  upserts with one MERGE and removes lines deleted from Silver with WHEN NOT
  MATCHED BY SOURCE ... THEN DELETE.
- Advance each watermark only after its target write; stop on unavailable
  history; a run without new versions commits nothing.
Every cell starts with %%sql, then -- Stage: gold - <what the cell does>,
then at least one statement; use PySpark only if SQL cannot do a step and
say why in the header. Replace only the placeholder; do not copy supplied
transformations or edit the spec, CSVs or validator. Show the code and write
set, explain replay safety, then stop for my approval.
```

Before approving, check the CDF reads, update-preimage filter, MERGE keys and
watermark ordering. Review every cell: each generated cell is `%%sql` with a
`-- Stage: gold - …` header, Gold reconciles against Silver, never reads
Bronze and contains no Silver-building or MLV code; the fixed baseline and
check cells are Gold-specific too. Use
[Lab 1's execution prompt](01-bronze.md#2-execute-approved-code) to save and
run this notebook twice, or run approved cells manually.

## 2. Verify

The shell's fixed baseline and check cells wrap your generated cells:

```python
bootcamp_begin(spark, "gold")         # fixed baseline cell, before the generated cells
# ... your NEW %%sql cells run here ...
validate_bootcamp(spark, "gold")      # fixed check cell, after the generated cells
```

On the second **Run all** the baseline finds Gold current, so the check
proves that your cells committed nothing.

Query the star:

```sql
SELECT d.date, f.store_id, f.channel, c.city, SUM(f.net_amount) AS net_amount
FROM gold.fact_sales f
JOIN gold.dim_date d ON d.date_key = f.date_key
JOIN gold.dim_customer c ON c.customer_sk = f.customer_sk
GROUP BY d.date, f.store_id, f.channel, c.city
ORDER BY d.date;
```

Output equality alone cannot prove the method used CDF: keep the generated
code and the consumed version ranges (the `lab_ops.cdf_watermarks` rows) as
evidence.

## Definition of done

- Actual `PASS: bootcamp gold contract, initial load (fixed read-only checks)`
  and `PASS: bootcamp gold was built or brought to contract by this run`;
  the second **Run all** adds
  `PASS: bootcamp gold was already current and the rerun changed nothing`.
- Gold dimensions equal Silver; `dim_date` covers 2026-10-10 to 2026-10-16.
- `fact_sales` has 9 lines, 16 units and 516.73 net sales with as-of
  customer keys.
- The five Silver-to-Gold watermarks equal the Silver versions; the consumed
  version ranges are in the generated code and the watermark rows.
- Gold tables and columns carry comments; code is documented native Spark
  SQL. Save evidence and tick your tasks.

On failure, have Copilot patch only the generated code, approve and rerun.
Never reset watermarks, overwrite history or relax the contract to pass; see
[repair and recovery](recovery.md). Do not run the full pipeline until Lab 4.

**Next:** [Lab 4 - Materialized lake views and the change batch](04-materialized-view.md).
