# Lab 5: Project Osmos

**Optional, after the session; untimed.**

Ask Project Osmos to build the same lakehouse as one end-to-end task, then
compare its actual outputs against the same fixed contracts.

## 1. Prepare a fresh target

This lab starts from the [Lab 1 setup checkpoint](setup.md#4-wire-the-serial-pipeline):
staged CSVs and empty notebook implementations, with no built tables.
Do not overwrite your completed live build. For a separate replay, ask the
instructor to assign a fresh empty workspace with its dedicated active capacity.
Confirm the new target, then run only the
[Lab 1 setup prompt](01-bronze.md); skip the Bronze prompt.
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

Submit one task. It follows the
[recommended outcome pattern](https://learn.microsoft.com/en-us/fabric/data-engineering/data-engineering-agent-best-practices):
goal, sources, transformations, outputs, validation and constraints.

```text
Use project-osmos.
Goal: build the isolated Zava sales lakehouse in my confirmed assigned
workspace and fresh lh_bootcamp, following specs/bootcamp-lakehouse.spec.md.
Read the CSVs in Files/bootcamp/initial/ and, after the first build, land and
read data/bootcamp/changes/customer_changes.csv in Files/bootcamp/changes/.
Apply raw Bronze with CDF; typed Silver with SCD2 customer history fed by
Bronze CDF; a documented Gold star maintained from Silver CDF and current
Silver rows.
Create or update the Bronze, Silver, Gold and lab_ops tables, and
gold.sales_daily and gold.customer_current as Fabric materialized lake views
over Silver. Use the four setup notebooks (Bronze, Silver, Gold, MLV), each
writing only its own layer, as native Spark SQL (%%sql cells with -- Stage:
comments; PySpark only where SQL cannot), with fixed validation in each.
Keep the fixed validator, baseline and check cells unchanged.
Write pattern: create each Bronze table once; merge Silver and Gold on stable
keys and retain expired customer versions; replace the two views; keep
schemas locked and stop on a schema mismatch.
Reruns: with no new input a rerun changes nothing; advance CDF watermarks only
after target writes; run the pipeline once after landing the change file.
Validate source counts, SCD2 state, dimension keys, fact grain and revenue,
watermarks, replay, and MLV totals and lineage, using only each stage's fixed
checks, even among unused definitions. Report row counts per table and
inserted, updated, deleted and unchanged counts per MERGE where Spark returns
them; rejected: none.
Preserve the OneLake CSVs and the setup pipeline wiring. Write only to
lh_bootcamp schemas bronze, silver, gold and lab_ops. Touch no other
workspace, capacity or permission. Add no data-quality quarantine. Do not rely
on shared session state or promise session reuse.
Confirm the plan and exact write set before execution; stop on a CDF
retention gap or unsupported MLV feature.
```

Approve the plan and write set, then let the same task execute. Keep the
task ID for follow-up; inspect its status rather than submitting duplicates.

**Capacity and cost.** Osmos consumes LLM tokens as well as Spark compute, so
start with this one bounded task in one workspace and run it alone. Measure
both: the Microsoft Fabric Capacity Metrics app for Spark and the Project
Osmos consumption page for the agent. A separate workspace on a shared
capacity gives no compute isolation, while a separately assigned capacity
does; that is why each participant has a dedicated F2. If a capacity is
shared, ask your instructor about on-demand (autoscale) billing for Spark. Its
capacity-unit limit bounds Spark compute, not LLM tokens.

## 3. Check the result

Use the unchanged fixed check cell in each stage notebook, as in Labs 1-4.
Before the change file lands each stage prints its initial-load PASS; after
the pipeline run it prints
`PASS: bootcamp <stage> contract, after the customer change batch (fixed read-only checks)`.
Only Bronze, Silver and Gold additionally report either
`PASS: bootcamp <stage> was built or brought to contract by this run` or
`PASS: bootcamp <stage> was already current and the rerun changed nothing`.
MLV reports its contract PASS and row counts, not unchanged Delta versions.

Inspect real bounded reads, version ranges, watermark ordering, MLV DDL and
Fabric lineage. Do not infer the method from matching counts, or incremental
MLV refresh from a successful create.

## Definition of done

- The task actually completes and every fixed stage check passes, before and
  after the change batch.
- History, facts, keys and all ten watermarks meet the same contract as
  Labs 1-4; reruns without new input preserve them.
- Osmos' reported row counts match the `rows validated` lines of the fixed
  checks.
- Both real MLVs pass: `gold.sales_daily` has seven reconciled groups and six
  FAIL constraints, `gold.customer_current` six current customers, and
  lineage is inspected.
- Keep generated notebooks, task ID, output and an explanation of how
  Osmos approached the specification. Label unsupported/incomplete work.

Stop sessions started for this exercise. The
[semantic-model lab](06-semantic-model-fabric-iq.md) is a separate optional
task, not part of this Osmos request.

[Back to the lab overview](../../README.md).
