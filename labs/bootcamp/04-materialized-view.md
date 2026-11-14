# Lab 4: Materialized lake views and the change batch

**17 minutes · session minutes 43–60.** Prerequisite: Labs 1-3 pass.

| Goal | Start | Open | Finish signal |
|---|---|---|---|
| Serve reconciled results and carry one change batch through every layer | Bronze, Silver and Gold initial checks passed | `nb_bootcamp_mlv`, then `pl_bootcamp_build` | Both MLVs verified; every pipeline stage has change-batch PASS |

**Watch, work, check:** watch the two serving definitions, approve yours and
run MLV once. As soon as it passes, make decision 10 and launch your pipeline.
Aim to launch by session minute 47; this is a pacing target, not a runtime
guarantee. If a prerequisite failed, stop and get help instead of skipping it.

Open `nb_bootcamp_mlv`. Its description, validator and fixed check cell are
already present. The goal is two real Fabric materialized lake views over
Silver, not merely queries returning the same numbers. Then a customer change
file arrives and one pipeline run carries it through every layer.

## 1. Generate and run the materialized lake views

Paste:

```text
Use the spark-cli skill.
Goal: build the materialized lake views in lh_bootcamp.
Read Silver.
Generate NEW native Spark SQL as %%sql cells in nb_bootcamp_mlv from spec
section 7.
Create only gold.sales_daily and gold.customer_current.
Validate with the fixed check and report its row counts.
Preserve Bronze, Silver and the other Gold tables.
Explain how materialized lake view refresh differs from explicit CDF.
Show the cells and the exact write set. Stop for my code approval.
```

**Decision 9 · approve the MLV code.** Check that each generated cell is
`%%sql` with a `-- Stage: mlv - …` header and that it reads only `silver` and
defines `gold.sales_daily` and `gold.customer_current` with
`CREATE OR REPLACE MATERIALIZED LAKE VIEW` (not tables or ordinary views),
with the exact grouping, columns and `ON MISMATCH FAIL` constraints. Reply
`change: <what>` or `approve` to save and run it once. The definitions are
recreated on rerun and their exact schema is still checked; this is not
evidence of incremental refresh or unchanged Delta versions. MLV has no
baseline/rerun cell. Its fixed contract check runs after your generated cells:

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
Goal: land the customer change batch and run the pipeline once.
Confirm that all four notebooks passed, no job is running and
Files/bootcamp/changes/ in lh_bootcamp is empty.
Stop for my go/no-go.
On go, upload data/bootcamp/changes/customer_changes.csv there, verify its
bytes and trigger pl_bootcamp_build once.
Validate by reporting each notebook's status.
Preserve every notebook: edit none.
```

**Decision 10 · change-batch go/no-go.** A landed file is immutable and the
pipeline carries it through every layer, so reply `go` only when the preflight
is clean; otherwise `stop` and fix the failing stage first.

The pipeline runs Bronze, Silver, Gold and MLV in order. Every fixed check now
expects the changed state. Bronze, Silver and Gold have baseline cells; on
this first change-batch run they also report
`PASS: bootcamp <stage> was built or brought to contract by this run`.
MLV reports its contract PASS and validated row counts only.
As optional after-session work, **Run all** a Bronze, Silver or Gold notebook
with no new input: its check must print
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
  with both view definitions confirmed and lineage inspected; the
  `bootcamp mlv rows validated: ...` line gives the row count of each view.
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

## If blocked

On failure, wait for the pipeline and its child job to reach a terminal
state. Reply `repair: <actual error>` to repair the generated code in
the failing notebook only, keeping the checks fixed. Approve the patch and
explicitly approve one pipeline retry; do not launch a competing notebook.
Already-current Bronze, Silver and Gold stages must no-op. An error retrieving
output is not a failed execution and never authorizes a retry. If materialized lake views are
unavailable, label the stage "query checked; materialized lake view not
created"; that is not a pass. Follow [repair and recovery](recovery.md).

## Finish the live build

Stop at the end of the 45-minute build, even if a stage needs labelled
[recovery](recovery.md). Record completed stages, actual output and any
pending job ID; an unfinished job is not a PASS. Show the dependency graph and see
[the setup execution guidance](setup.md#5-generate-approve-and-execute) for
later reruns. Stop only sessions/jobs you started and confirm terminal status.

**Optional next steps:** [Project Osmos](05-osmos-replay.md) in a fresh
approved target, or [semantic model and Fabric IQ](06-semantic-model-fabric-iq.md)
over this completed Gold layer.
