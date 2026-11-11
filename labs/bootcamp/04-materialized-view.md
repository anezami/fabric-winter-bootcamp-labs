# Lab 4: Materialized lake views and the change batch

**15 minutes.** Prerequisite: Labs 1-3 pass.

Open `nb_bootcamp_mlv`. Its description, validator and fixed check are
already present. The goal is two real Fabric materialized lake views over
Silver, not merely queries returning the same numbers. Then a customer change
file arrives and one pipeline run carries it through every layer.

## 1. Generate and execute

```text
Use spark-cli and section 7 of specs/bootcamp-lakehouse.spec.md.
Save a short MLV plan and numbered tasks under specs/plans/. Generate NEW
Spark SQL for nb_bootcamp_mlv that reads only silver and defines
gold.sales_daily and gold.customer_current with CREATE OR REPLACE
MATERIALIZED LAKE VIEW, not tables or ordinary views. Implement the exact
grouping, columns/types and ON MISMATCH FAIL constraints. Start the cell with
a # comment header and comment each view. Replace only the placeholder; do not
copy supplied transformations or edit the spec, CSVs or validator. Do not
claim incremental refresh without Fabric run evidence. Show the code and
write set, explain explicit CDF versus Fabric-managed refresh, then stop for
my approval.
```

Approve, save and execute using
[Lab 1's execution prompt](01-bronze.md#2-execute-approved-code), or run the
approved cells manually. Do not overlap execution paths.

The unchanged final check runs:

```python
validate_bootcamp(spark, "mlv")
```

It reconciles **every group and value** of `gold.sales_daily` and every row
of `gold.customer_current` to Silver, and confirms both definitions with
`SHOW CREATE MATERIALIZED LAKE VIEW`. Inspect the `ON MISMATCH FAIL`
constraints. Once the initial check passes, start the next step immediately.

## 2. Land the change batch and run the pipeline

Ask Copilot:

```text
Upload data/bootcamp/changes/customer_changes.csv to
Files/bootcamp/changes/ in lh_bootcamp and verify its bytes. Then check that
no job is running and trigger pl_bootcamp_build once. Monitor it to
completion and report each notebook's status. Do not edit any notebook.
```

The pipeline runs Bronze, Silver, Gold and MLV in order. Every fixed check
then expects the changed state, and each processor's rerun must still be a
no-op.

**Timing:** reserve about 10-11 minutes on F2 for four serial notebook jobs;
do not assume session reuse. Start it as soon as the MLV check passes.
While it runs, inspect saved definitions, existing output and the pipeline
dependency graph only. Do not start interactive Spark queries or another job.

## 3. Inspect the completed build

After the pipeline completes, open **Manage materialized lake views** to show
lineage. Report refresh strategy only when Fabric exposes it. Query the views
and customer history:

```sql
SELECT date_key, store_id, channel, sales_lines, units, net_amount
FROM gold.sales_daily
ORDER BY date_key, store_id, channel;
```

```sql
SELECT customer_id, city, loyalty_tier, valid_from, valid_to, is_current
FROM silver.customer_history
WHERE customer_id IN ('C002', 'C003', 'C007')
ORDER BY customer_id, valid_from;
```

Explain why C003's October sales still use its original version.

## Definition of done

- Actual `PASS: bootcamp mlv contract, initial load (fixed read-only checks)`
  with both view definitions confirmed and lineage inspected.
- `gold.sales_daily`: seven groups reconcile individually, totalling nine
  sales lines, 16 units and 516.73 net sales.
- The change file is uploaded and verified; one `pl_bootcamp_build` run
  succeeds and every stage reports
  `PASS: bootcamp <stage> contract, after the customer change batch (fixed read-only checks)`;
  the Bronze, Silver and Gold reruns are still no-ops.
- C003 has an expired and a current version (Amsterdam, Gold), C007 one
  current version and C002 no current version but retained history, in both
  `silver.customer_history` and `gold.dim_customer`; `gold.customer_current`
  has six customers.
- Generated code and check output are saved in your notebooks. Tick the
  completed tasks.

On failure, give Copilot the actual error and repair the generated code in
the failing notebook only, keeping the checks fixed. Rerun the pipeline once;
Bronze and Silver no-op if already applied. If materialized lake views are
unavailable, label the stage "query checked; materialized lake view not
created"; that is not a pass. Follow [repair and recovery](recovery.md).

## Finish the live build

Stop at the end of the 45-minute build, even if a stage needs labelled
[recovery](recovery.md). Show the pipeline dependency graph and see
[the setup execution guidance](setup.md#5-generate-approve-and-execute) for
later reruns. Stop Spark sessions you started.

**Optional next steps:** [Project Osmos](05-osmos-replay.md) in a fresh
approved target, or [semantic model and Fabric IQ](06-semantic-model-fabric-iq.md)
over this completed Gold layer.
