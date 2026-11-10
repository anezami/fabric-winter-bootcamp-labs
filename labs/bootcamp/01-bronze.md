# Lab 1: Bronze ingestion

**10 minutes.** Prerequisite: the [pre-work ready check](pre-work.md#ready-check).

Open `nb_bootcamp_bronze`. You will generate code to land six immutable
source snapshots and initialize a separate customer-state table for CDF.

## 1. Generate and review

Paste this into Copilot from the repository root:

```text
Use the public spark-cli skill. Read specs/bootcamp-lakehouse.spec.md and
the six data/bootcamp CSV headers. Confirm my signed-in identity, assigned
workspace and isolated lh_bootcamp before any Fabric write.
Propose the architecture from the business goal and fixed output contracts.
Save a short Bronze plan and numbered tasks under specs/plans/.

Generate new PySpark cells for section 4 in nb_bootcamp_bronze: six raw
string tables with audit columns, using _metadata.file_path for _source_file,
and customers_current initialized once with CDF enabled. Do not apply
customer events yet. Do not rewrite existing raw snapshots, reset customer
state or re-enable CDF properties on reruns. Preserve the validator and
final fixed check; replace only the implementation placeholder.
Do not alter the spec, CSVs or validator. Show the code and exact write set,
explain replay safety, and stop for my approval.
```

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

- Actual `PASS: bootcamp bronze contract (fixed read-only checks)`.
- Six source tables: 6 customers, 4 products, 2 stores, 7 orders, 9 order
  lines and 3 events; every source value/type and audit path matches.
- Six initial `customers_current` rows with CDF enabled.
- Ingestion replay preserves the data. Save code/output and tick your tasks.

If a job fails, get its actual notebook exception from the run snapshot in
Fabric Monitor. A generic job failure is not enough to diagnose it.
Follow [repair and recovery](recovery.md), without weakening the checks.

**Next:** [Lab 2 - Silver and SCD2](02-silver-scd2.md).
