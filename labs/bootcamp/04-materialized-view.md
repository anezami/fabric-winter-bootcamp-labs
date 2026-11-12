# Lab 4: Materialized lake views and the change batch

**17 minutes.** Prerequisite: Labs 1-3 pass.

Open `nb_bootcamp_mlv`. Its description, validator and fixed check cell are
already present. The goal is two real Fabric materialized lake views over
Silver, not merely queries returning the same numbers. Then a customer change
file arrives and one pipeline run carries it through every layer.

## 1. Generate and run the materialized lake views

Paste:

```text
Use the spark-cli skill. Generate NEW native Spark SQL as %%sql cells for
nb_bootcamp_mlv from spec section 7. Explain how materialized lake view
refresh differs from explicit CDF. Show the cells and the exact write set.
Stop for my code approval.
```

**Decision 9 · approve the MLV code.** Check that each generated cell is
`%%sql` with a `-- Stage: mlv - …` header and that it reads only `silver` and
defines `gold.sales_daily` and `gold.customer_current` with
`CREATE OR REPLACE MATERIALIZED LAKE VIEW` (not tables or ordinary views),
with the exact grouping, columns and `ON MISMATCH FAIL` constraints. Reply
`change: <what>` or `approve` to save and run it once: the views are replaced
on every run, so there is no rerun proof. The shell's fixed check cell runs
after your generated cells:

```python
validate_bootcamp(spark, "mlv")       # fixed check cell, after the generated cells
```

It reconciles **every group and value** of `gold.sales_daily` and every row
of `gold.customer_current` to Silver, and confirms both definitions with
`SHOW CREATE MATERIALIZED LAKE VIEW`. Inspect the `ON MISMATCH FAIL`
constraints. Once the initial check passes, start the next step immediately.

## 2. Land the change batch and run the pipeline

Paste:

```text
Prepare the change batch: confirm that all four notebooks passed, no job is
running and Files/bootcamp/changes/ in lh_bootcamp is empty. Stop for my
go/no-go. On go, upload data/bootcamp/changes/customer_changes.csv there,
verify its bytes, trigger pl_bootcamp_build once and report each notebook's
status. Do not edit any notebook.
```

**Decision 10 · change-batch go/no-go.** A landed file is immutable and the
pipeline carries it through every layer, so reply `go` only when the preflight
is clean; otherwise `stop` and fix the failing stage first.

The pipeline runs Bronze, Silver, Gold and MLV in order. Every fixed check now
expects the changed state. Each baseline cell finds a layer that has not seen
the change batch yet, so each check ends with
`PASS: bootcamp <stage> was built or brought to contract by this run`. If
time remains afterwards, **Run all** one notebook again: its check must print
`PASS: bootcamp <stage> was already current and the rerun changed nothing`.

**Timing:** allow about 10-13 minutes on F2 for four serial notebook jobs;
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

For pipeline child output, open the child notebook run in Fabric Monitor.
Do not assume the manual notebook-job exit-value endpoint exposes pipeline
children. Report job status separately from any stdout actually retrieved;
an output-retrieval failure must not trigger another pipeline run.

**Decision 11 · accept the result.** Compare with the definition of done, or
label any stage that needed recovery.

## Definition of done

- Actual `PASS: bootcamp mlv contract, initial load (fixed read-only checks)`
  with both view definitions confirmed and lineage inspected.
- `gold.sales_daily`: seven groups reconcile individually, totalling nine
  sales lines, 16 units and 516.73 net sales.
- The change file is uploaded and verified; one `pl_bootcamp_build` run
  succeeds and every stage reports
  `PASS: bootcamp <stage> contract, after the customer change batch (fixed read-only checks)`.
- C003 has an expired and a current version (Amsterdam, Gold), C007 one
  current version and C002 no current version but retained history, in both
  `silver.customer_history` and `gold.dim_customer`; `gold.customer_current`
  has six customers.
- Generated code and check output are saved in your notebooks. Tick the
  completed tasks.

On failure, reply `repair: <actual error>` to repair the generated code in
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
