# Zava Lakehouse Bootcamp: participant guardrails

## Goal and workflow

Build the lakehouse defined in `specs/bootcamp-lakehouse.spec.md` from the
synthetic CSVs in `data/bootcamp/`. Follow:
**spec -> plan -> tasks -> implement -> verify**.

Before implementing a layer, write a short
`specs/plans/<layer>.plan.md` (engine, tables, steps, risks) and
`specs/plans/<layer>.tasks.md` (numbered checklist). Work one task at a time
and tick it off after its check passes.

AI proposes the design and generates new implementation cells. Explain
grain, keys, dependencies and the exact write set. Stop for participant
approval before executing any new code or patch. Do not copy prepared
transformations. If a business rule or target is unclear, stop and ask.

## Fabric boundaries

- Start from the participant's assigned empty workspace with its dedicated
  active F2 capacity already assigned. Pre-work is tools, clone, sign-in and
  read-only access checks, not lakehouse/files/notebook/pipeline creation or
  Spark warm-up.
- Confirm `az account show`, the assigned workspace/capacity and proposed
  `lh_bootcamp` before the first remote write. Discover IDs; never guess them.
- Use only that target. Never create/delete workspaces, resize/resume/pause
  capacities, change permissions, or touch another participant's resources.
- After setup approval, the initial Lab 1 prompt may create schema-enabled
  `lh_bootcamp`, upload the six CSVs, deploy four bound empty stage notebooks
  and wire the serial pipeline. Reconcile this setup's items on retry, skip
  identical uploads and stop on conflicts; never duplicate or reset resources.
  Obtain separate code approval before transformations. Table writes stay in `bronze`,
  `silver`, `gold` and `lab_ops`, and only on objects named in the spec.
- Keep OneLake CSVs immutable. Raw Bronze snapshots initialize once and
  preserve source values as strings plus audit columns.
- Run one workload at a time on F2. Check for an existing job before
  submission; monitor accepted jobs rather than submitting duplicates.
  Notebook jobs have independent contexts, not shared Python globals.
- Stop sessions you started when finished. Do not reset a lakehouse or drop
  tables to hide a failed check; request an explicit scoped repair decision.
- Use the public Skills for Fabric, including `spark-cli`, rather than
  inventing APIs. Use signed-in identity and runtime tokens; never paste or
  commit credentials. Keep local IDs, generated payloads and logs in `out/`.

## Data contracts

- Use exact spec columns, types, keys and grains. No extra quarantine or
  data-quality repair rules are part of this clean-data exercise.
- Preserve SCD2 history, surrogate keys and the initial fact snapshot.
  Read bounded CDF ranges after durable watermarks; write targets before
  advancing watermarks. Missing history or a missing watermark on an
  existing target is an error, never a reason to skip changes.
- Replays must preserve results. Do not generate no-op commits on CDF
  sources by rewriting raw snapshots or resetting properties.
- Keep `labs/bootcamp/validate-lakehouse.py`, the source CSVs and final
  fixed-check cells unchanged. Repair the generated implementation, not
  the specification or expected results, to obtain a pass.
- Report success only after actual execution and fixed PASS output.
  Matching table values alone do not prove CDF consumption or MLV creation.
  Keep version ranges, DDL, lineage and job results as applicable.

## Timing and optional work

The four live labs have a hard 45-minute limit, including setup and cold Spark
startup in Lab 1's 10-minute slot. Use
`labs/bootcamp/recovery.md` if blocked; distinguish generated, supplied and
incomplete stages. Supplied code is not evidence of live generation.

Use Project Osmos only when explicitly requested in its optional lab, in a
fresh approved target. Do not run it concurrently with notebook writers.
The optional semantic lab may create only the participant-approved model;
Gold stays read-only. No report, permissions or capacity changes are allowed.
