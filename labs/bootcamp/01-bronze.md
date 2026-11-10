# Lab 1: create the lakehouse and Bronze

**10 minutes.** Prerequisite: the [pre-work ready check](pre-work.md#ready-check).

Start Copilot from the repo root with your assigned **empty workspace** on its
dedicated active F2 capacity. No lakehouse, uploaded files, notebooks or warm
Spark session are required yet. This slot includes [setup](setup.md), cold
startup and the generated Bronze ingestion.

## 1. Setup, generate and review

Replace the workspace placeholder and paste this initial prompt:

```text
Use the spark-cli skill and CLI. Read specs/bootcamp-lakehouse.spec.md,
labs/bootcamp/setup.md and the six local data/bootcamp CSV headers.
My assigned workspace is <assigned workspace name>. It starts empty,
with a dedicated F2 capacity already assigned. Run az account show;
discover and confirm the exact workspace, capacity Active status and contents.
Do not create/change workspaces, capacities or permissions. Stop on
unexpected contents; never reset an existing build or guess IDs.

Propose the architecture, short table-flow plan and numbered tasks from the
fixed contracts; save the plan/tasks under specs/plans/. Show the setup
write set below and target identity. Stop for my setup approval.

After setup approval, follow setup.md in order using Fabric skills/CLI:
1. Create lh_bootcamp with creationPayload.enableSchemas=true. Wait for
   completion, capture its actual ID and verify schema support.
2. Upload the six local CSVs to their specified Files/bootcamp paths;
   verify their bytes and keep them immutable.
3. Run prepare-artifacts.py shells with discovered workspace/lakehouse IDs;
   deploy the four bound empty notebooks with unchanged validator/checks.
4. Run prepare-artifacts.py pipeline with the actual notebook IDs; deploy
   pl_bootcamp_build with serial success-only dependencies, no schedule.
Capture setup evidence. Reconcile this setup's items on retry, skipping
identical files; stop on conflicts. Poll accepted operations, never duplicate
submissions. Do not run placeholders, pre-warm Spark or start the pipeline.

Then generate NEW PySpark cells for section 4 in nb_bootcamp_bronze: six raw
string tables with audit columns, using _metadata.file_path for _source_file,
and customers_current initialized once with CDF enabled. Do not apply
customer events yet. Do not rewrite existing raw snapshots, reset customer
state or re-enable CDF properties on reruns. Preserve the validator and
final fixed check; replace only the implementation placeholder.
Do not alter the spec, CSVs or validator or copy supplied transformations.
Show the code and exact write set and explain replay safety.
Stop for my code approval; setup approval does not authorize running transformations.
```

Approve the setup write set first: one lakehouse, six uploads, four empty
notebooks and their pipeline in your assigned workspace only. After setup,
open `nb_bootcamp_bronze` and review the newly generated code.
Approve only if writes stay in `lh_bootcamp`, source Files remain immutable,
raw values remain strings and no DQ-repair rules are invented.

## 2. Execute approved code

Use this execution prompt after **each** lab's approval:

```text
I approve only the code and write set just shown. Use the Fabric skill to
save these cells in the confirmed stage notebook, preserving the validator
and final fixed check, then trigger that notebook only.
Check for an existing job first; capture its ID and monitor it to completion
with actual validation output. Do not trigger the full pipeline while later
shells are unimplemented. Never overlap notebook/pipeline jobs or an
interactive writer. If notebook submission is unavailable, give me the cells
to run manually in that notebook instead; do not execute both paths.
Do not claim PASS without actual execution output.
```

For manual execution, paste approved cells between the validator and final
check, replace the placeholder and run them in order. Keep code and output.

## 3. Verify

Run the unchanged final check:

```python
validate_bootcamp(spark, "bronze", after_changes=bootcamp_changes_started(spark))
```

Re-run just the generated ingestion step and check again. It must not
duplicate rows, rewrite raw snapshots or reset customer state/CDF history.

## Definition of done

- Schema-enabled `lh_bootcamp`, six verified uploads, four correctly bound
  shells with fixed checks and an unscheduled serial pipeline; setup IDs saved.
- Actual `PASS: bootcamp bronze contract (fixed read-only checks)`.
- Six source tables: 6 customers, 4 products, 2 stores, 7 orders, 9 order
  lines and 3 events; every source value/type and audit path matches.
- Six initial `customers_current` rows with CDF enabled.
- Ingestion replay preserves the data. Save code/output and tick your tasks.

If setup fails, retain returned IDs, resume/poll the accepted operation and
reconcile only this approved setup. Do not duplicate creates or overwrite
conflicting items/files. Ask the instructor about access/capacity blocks.

If a job fails, get its actual notebook exception from the run snapshot in
Fabric Monitor. A generic job failure is not enough to diagnose it.
Follow [repair and recovery](recovery.md), without weakening the checks.

**Next:** [Lab 2 - Silver and SCD2](02-silver-scd2.md).
