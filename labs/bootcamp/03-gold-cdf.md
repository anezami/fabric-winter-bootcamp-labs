# Lab 3: Gold from the Silver change feed

**9 minutes.** Prerequisite: Lab 2 passes.

Open `nb_bootcamp_gold`. It reads only Silver; define all imports and
CDF/watermark helpers here, because no Python state carries over. Keep the
fixed cells unchanged.

## 1. Generate and review

```text
Use spark-cli and spec sections 3, 6 and 8 of
specs/bootcamp-lakehouse.spec.md. Save a short Gold plan and numbered tasks
under specs/plans/. Generate NEW PySpark/Spark SQL code for nb_bootcamp_gold
that reads only silver and writes only gold plus its five
lab_ops.cdf_watermarks rows. Define process_silver_to_gold() and call it
once at the end:
- Create the five documented Gold tables once, with table and column
  comments and the exact columns/types.
- Read each Silver source's change feed from watermark + 1 (version 0 on the
  first run) through a captured current version, ignoring update_preimage,
  and print the consumed version ranges.
- dim_customer, dim_product and dim_store apply the latest change per key
  with a keyed MERGE (dim_customer keyed on customer_sk); dim_date adds the
  missing days between the first and last order date.
- fact_sales recomputes only the orders touched by the orders/order_lines
  changes at order-line grain, joins the customer version valid at order_ts,
  rounds net_amount per line, upserts with MERGE and removes lines deleted
  from Silver.
- Advance each watermark only after its target write; stop on unavailable
  history; a run without new versions commits nothing.
Start the cell with a # comment header and give every function a docstring.
Replace only the placeholder; do not copy supplied transformations or edit
the spec, CSVs or validator. Show the code and write set, explain replay
safety, then stop for my approval.
```

Before approving, check the CDF reads, update-preimage filter, MERGE keys and
watermark ordering. Review every cell: Gold reconciles against Silver, never
reads Bronze and contains no Silver-building or MLV code. Use
[Lab 1's execution prompt](01-bronze.md#2-execute-approved-code) to save and
run this notebook, or run approved cells manually.

## 2. Verify

The unchanged final check validates Gold and replays the processor:

```python
validate_bootcamp(spark, "gold")
before = bootcamp_snapshot(spark, "gold")
process_silver_to_gold()
assert bootcamp_snapshot(spark, "gold") == before, "Rerunning process_silver_to_gold() changed gold"
print("PASS: gold rerun is a no-op")
```

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
code and printed version ranges as evidence.

## Definition of done

- Actual `PASS: bootcamp gold contract, initial load (fixed read-only checks)`
  and `PASS: gold rerun is a no-op`.
- Gold dimensions equal Silver; `dim_date` covers 2026-10-10 to 2026-10-16.
- `fact_sales` has 9 lines, 16 units and 516.73 net sales with as-of
  customer keys.
- The five Silver-to-Gold watermarks equal the Silver versions; consumed
  version ranges are printed.
- Gold tables and columns carry comments; code is documented. Save evidence
  and tick your tasks.

On failure, have Copilot patch only the generated code, approve and rerun.
Never reset watermarks, overwrite history or relax the contract to pass; see
[repair and recovery](recovery.md). Do not run the full pipeline until Lab 4.

**Next:** [Lab 4 - Materialized lake views and the change batch](04-materialized-view.md).
