# Lab 3: Gold from the Silver change feed

**8 minutes.** Prerequisite: Lab 2 passes.

Open `nb_bootcamp_gold`. It reads only Silver; session variables and
temporary views do not carry over from other notebooks, so the generated
cells declare what they need. Keep the fixed cells unchanged.

## 1. Generate and run

Paste:

```text
Use the spark-cli skill.
Goal: build Gold in lh_bootcamp.
Read Silver through table_changes().
Generate NEW native Spark SQL as %%sql cells in nb_bootcamp_gold from spec
sections 3, 6 and 8.
Create only gold tables and their lab_ops watermarks.
Validate with the fixed check and report its row counts.
Preserve Bronze and Silver.
Explain replay safety and the as-of customer join.
Show the cells and the exact write set. Stop for my code approval.
```

**Decision 7 · approve the Gold code.** Check:

- Each generated cell is `%%sql` with a `-- Stage: gold - …` header.
- It reads Silver only through `table_changes()` from each watermark + 1,
  ignores `update_preimage`, never reads Bronze and contains no
  Silver-building or MLV code; the fixed baseline and check cells are
  Gold-specific too.
- Every MERGE source is inline in `USING (...)`; `dim_customer` is keyed on
  `customer_sk`. Every MERGE uses the stable key from the write-pattern table
  in spec section 3, so a rerun with no new changes commits nothing, and
  there is no `mergeSchema` or schema auto-merge.
- `fact_sales` recomputes only the touched orders, joins the customer version
  valid at `order_ts` and removes deleted lines with
  `WHEN NOT MATCHED BY SOURCE`.
- Each watermark advances only after its target write.

Reply `change: <what>` or `approve`: as in Lab 1, approval saves the cells and
runs this notebook once, or gives you the cells to run manually.

## 2. Verify

The shell's fixed baseline and check cells wrap your generated cells:

```python
bootcamp_begin(spark, "gold")         # fixed baseline cell, before the generated cells
# ... your NEW %%sql cells run here ...
validate_bootcamp(spark, "gold")      # fixed check cell, after the generated cells
```

If time remains, an optional second **Run all** in the warm session lets the
baseline find Gold current, so the check proves that your cells committed
nothing.

**Decision 8 · accept the result.** Query the star:

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
evidence. Compare with the definition of done and accept by pasting the
[Lab 4](04-materialized-view.md) prompt.

## Definition of done

- Actual `PASS: bootcamp gold contract, initial load (fixed read-only checks)`
  and `PASS: bootcamp gold was built or brought to contract by this run`.
  An optional second **Run all** in the warm session prints
  `PASS: bootcamp gold was already current and the rerun changed nothing`.
- Gold dimensions equal Silver; `dim_date` covers 2026-10-10 to 2026-10-16.
- `fact_sales` has 9 lines, 16 units and 516.73 net sales with as-of
  customer keys.
- The five Silver-to-Gold watermarks equal the Silver versions; the consumed
  version ranges are in the generated code and the watermark rows.
- The `bootcamp gold rows validated: ...` line lists the row count of every
  table the check read.
- Gold tables and columns carry comments; code is documented native Spark
  SQL. Save evidence and tick your tasks.

On failure, reply `repair: <actual error>`, approve the patch and rerun.
Never reset watermarks, overwrite history or relax the contract to pass; see
[repair and recovery](recovery.md). Do not run the full pipeline until Lab 4.

**Next:** [Lab 4 - Materialized lake views and the change batch](04-materialized-view.md).
