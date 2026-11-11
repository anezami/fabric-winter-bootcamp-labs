# Lab 5: Project Osmos

**Optional, after the session; untimed.**

Ask Project Osmos to build the same lakehouse as one end-to-end task, then
compare its actual outputs against the same fixed contracts.

## 1. Prepare a fresh target

This lab starts from the [Lab 1 setup checkpoint](setup.md#4-wire-the-serial-pipeline):
staged CSVs and empty notebook implementations, with no built tables.
Do not overwrite your completed live build. For a separate replay, ask the
instructor to assign a fresh empty workspace with its dedicated active capacity.
Confirm the new target, then run only the setup portion of the
[Lab 1 initial prompt](01-bronze.md); stop before Bronze code generation.
Do not reset resources, create workspaces or change capacity yourself.

Confirm identity, assigned workspace, isolated schema-enabled `lh_bootcamp`,
active F2 capacity, five staged initial CSVs, four bound shells and their
serial pipeline. Do not overlap an Osmos task with other notebook/pipeline writers.

Use the public `project-osmos` skill from
[Skills for Fabric](https://github.com/microsoft/skills-for-fabric).
Verify it is installed, enabled and usable in your client. See
[Project Osmos setup](https://github.com/microsoft/project-osmos) for
prerequisites. Never send credentials in a prompt.

## 2. Submit a bounded task

```text
Use project-osmos. In my confirmed assigned workspace and fresh lh_bootcamp,
build the Zava sales lakehouse from Files/bootcamp/ using
specs/bootcamp-lakehouse.spec.md and its fixed validation contract.
Create raw Bronze tables with CDF, typed Silver tables and SCD2 customer
history fed by Bronze CDF, a documented Gold star maintained from Silver CDF
and current Silver rows, and gold.sales_daily and gold.customer_current as
Fabric materialized lake views over Silver.
Keep Files immutable. Preserve expired history and the initial fact snapshot.
Use durable per-pair watermarks, target writes before watermark advancement,
and replay-safe processing. Stop on missing history or unsupported features.
Then land data/bootcamp/changes/customer_changes.csv and run the pipeline once.
Use the four separate Bronze, Silver, Gold and MLV notebooks, each writing
only its own layer, and the existing serial pipeline. Write each as native
Spark SQL (%%sql cells with a -- Stage: comment header; PySpark only where
SQL cannot) and keep the fixed validator, baseline and check cells unchanged.
Do not rely on shared session state: every notebook runs in its own context.
Write only the specified objects in lh_bootcamp schemas bronze, silver,
gold and lab_ops. Do not change workspaces, capacities or permissions.
Show the plan and exact write set for my approval before execution.
```

Approve the plan and write set, then let the same task execute. Keep the
task ID for follow-up; inspect its status rather than submitting duplicates.

## 3. Check the result

Use the unchanged fixed check cell in each stage notebook, as in Labs 1-4.
Before the change file lands each stage prints its initial-load PASS; after
the pipeline run it prints
`PASS: bootcamp <stage> contract, after the customer change batch (fixed read-only checks)`.
Each check also reports either
`PASS: bootcamp <stage> was built or brought to contract by this run` or
`PASS: bootcamp <stage> was already current and the rerun changed nothing`.

Inspect real bounded reads, version ranges, watermark ordering, MLV DDL and
Fabric lineage. Do not infer the method from matching counts, or incremental
MLV refresh from a successful create.

## Definition of done

- The task actually completes and every fixed stage check passes, before and
  after the change batch.
- History, facts, keys and all ten watermarks meet the same contract as
  Labs 1-4; reruns without new input preserve them.
- Both real MLVs pass: `gold.sales_daily` has seven reconciled groups and six
  FAIL constraints, `gold.customer_current` six current customers, and
  lineage is inspected.
- Keep generated notebooks, task ID, output and an explanation of how
  Osmos approached the specification. Label unsupported/incomplete work.

Stop sessions started for this exercise. The
[semantic-model lab](06-semantic-model-fabric-iq.md) is a separate optional
task, not part of this Osmos request.

[Back to the lab overview](../../README.md).
