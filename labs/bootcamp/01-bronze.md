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
labs/bootcamp/setup.md and the five local data/bootcamp/initial CSV headers.
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
2. Upload the five initial CSVs to Files/bootcamp/initial/; verify their
   bytes and keep them immutable. The change file waits for Lab 4.
3. Run prepare-artifacts.py shells with discovered workspace/lakehouse IDs;
   deploy the four bound empty notebooks with unchanged validator/checks.
4. Run prepare-artifacts.py pipeline with the actual notebook IDs; deploy
   pl_bootcamp_build with serial success-only dependencies, no schedule.
Capture setup evidence. Reconcile this setup's items on retry, skipping
identical files; stop on conflicts. Poll accepted operations, never duplicate
submissions. Do not run placeholders, pre-warm Spark or start the pipeline.

Then generate NEW PySpark code for spec sections 3 and 4 in
nb_bootcamp_bronze that writes only the bronze schema. Define load_bronze()
and call it once at the end:
- Create the five raw string tables once, with CDF enabled at creation, the
  minimal cleansing of section 3 and the audit columns, taking _source_file
  from _metadata.file_path. Never rewrite an existing raw table.
- Create customers_current once from customers_source, with CDF enabled.
- If Files/bootcamp/changes/customer_changes.csv exists, create
  customer_changes_source once and apply its U/I/D events to
  customers_current with keyed MERGE; skip the MERGE when nothing differs.
- A second call must commit nothing.
Start the cell with a # comment header and give every function a docstring.
Replace only the placeholder cell; keep the description, validator and final
fixed-check cells. Do not alter the spec, CSVs or validator or copy supplied
transformations. Show the code and exact write set and explain replay safety.
Stop for my code approval; setup approval does not authorize running transformations.
```

Approve the setup write set first: one lakehouse, five uploads, four empty
notebooks and their pipeline in your assigned workspace only. After setup,
open `nb_bootcamp_bronze` and review the newly generated code.
Approve only if it writes only `bronze`, source Files remain immutable, raw
values remain strings, no DQ-repair rules are invented and every cell and
function is documented.

## 2. Execute approved code

Use this execution prompt after **each** lab's approval:

```text
I approve only the code and write set just shown. Use the Fabric skill to
save these cells in the confirmed stage notebook, preserving the description,
validator and final fixed check, then trigger that notebook only.
Check for an existing job first; capture its ID and monitor it to completion
with actual validation output. Do not trigger the full pipeline while later
shells are unimplemented. Never overlap notebook/pipeline jobs or an
interactive writer. If notebook submission is unavailable, give me the cells
to run manually in that notebook instead; do not execute both paths.
Do not claim PASS without actual execution output.
```

For manual execution, paste the approved cells in place of the placeholder,
between the validator and the final check, and run all cells in order. Keep
code and output.

## 3. Verify

The shell's unchanged final check validates Bronze, calls `load_bronze()`
again and requires identical rows and Delta versions:

```python
validate_bootcamp(spark, "bronze")
before = bootcamp_snapshot(spark, "bronze")
load_bronze()
assert bootcamp_snapshot(spark, "bronze") == before, "Rerunning load_bronze() changed bronze"
print("PASS: bronze rerun is a no-op")
```

## Definition of done

- Schema-enabled `lh_bootcamp`, five verified uploads, four correctly bound
  shells with fixed checks and an unscheduled serial pipeline; setup IDs saved.
- Actual `PASS: bootcamp bronze contract, initial load (fixed read-only checks)`
  and `PASS: bronze rerun is a no-op`.
- Five raw tables: 6 customers, 4 products, 2 stores, 7 orders and 9 order
  lines as strings; every source value and audit path matches.
- CDF enabled on every Bronze table; `customers_current` matches the 6 source
  customers.
- Code is documented. Save code/output and tick your tasks.

If setup fails, retain returned IDs, resume/poll the accepted operation and
reconcile only this approved setup. Do not duplicate creates or overwrite
conflicting items/files. Ask the instructor about access/capacity blocks.

If a job fails, get its actual notebook exception from the run snapshot in
Fabric Monitor. A generic job failure is not enough to diagnose it.
Follow [repair and recovery](recovery.md), without weakening the checks.

**Next:** [Lab 2 - Silver and SCD2](02-silver-scd2.md).
