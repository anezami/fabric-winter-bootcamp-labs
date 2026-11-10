# Lab 5: Project Osmos

**Optional, after the session; untimed.**

Ask Project Osmos to build the same lakehouse as one end-to-end task, then
compare its actual outputs against the same fixed contracts.

## 1. Prepare a fresh target

This lab starts from the [pre-work](pre-work.md) state: staged CSVs and empty
notebook implementations, with no built tables. Do not overwrite your
completed live build. If you have already finished Labs 1-4, ask the
instructor to arrange a fresh approved target first. Do not reset resources
or create another workspace yourself.

Confirm identity, assigned workspace, isolated schema-enabled `lh_bootcamp`,
active F2 capacity, six staged CSVs, four bound shells and their serial
pipeline. Do not overlap an Osmos task with other notebook/pipeline writers.

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
Create raw Bronze snapshots, typed Silver, SCD2 customer history, the Gold
star, bounded Bronze-to-Silver and Silver-to-Gold customer CDF processors,
and gold.sales_daily as a Fabric materialized lake view.
Keep Files immutable. Preserve expired history and the initial fact snapshot.
Use durable per-pair watermarks, target writes before watermark advancement,
and replay-safe processing. Stop on missing history or unsupported features.
Use the four independent notebooks and existing serial pipeline, with local
imports/helpers and unchanged validation in every context. Use no shared
Python globals. Validate each stage at its specified checkpoint and retain
the Gold-before-events check plus the final CDF/replay checks.
Write only the specified objects in lh_bootcamp schemas bronze, silver,
gold and lab_ops. Do not change workspaces, capacities or permissions.
Show the plan and exact write set for my approval before execution.
```

Approve the plan and write set, then let the same task execute. Keep the
task ID for follow-up; inspect its status rather than submitting duplicates.

## 3. Check the result

Use the unchanged validator at the same checkpoints as Labs 1-4. On the
completed build, the validator supports:

```python
for stage in ("bronze", "silver", "gold", "cdf"):
    validate_bootcamp(spark, stage, after_changes=True)
validate_bootcamp(spark, "mlv")
```

Run the CDF replay check in the Gold/CDF notebook where its two processor
functions are defined. Inspect real bounded reads, version ranges,
watermark ordering, MLV DDL and Fabric lineage. Do not infer the method from
matching counts, or incremental MLV refresh from a successful create.

## Definition of done

- The task actually completes and all fixed stage checks pass.
- History, facts, keys and watermarks meet the same contract as Lab 3;
  no-new-input replay preserves them.
- The real MLV has seven reconciled groups, six FAIL constraints and
  inspected lineage.
- Keep generated notebooks, task ID, output and an explanation of how
  Osmos approached the specification. Label unsupported/incomplete work.

Stop sessions started for this exercise. The
[semantic-model lab](06-semantic-model-fabric-iq.md) is a separate optional
task, not part of this Osmos request.

[Back to the lab overview](../../README.md).
