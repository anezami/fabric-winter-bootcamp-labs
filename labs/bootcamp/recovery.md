# Repair and recovery

Keep the fixed specification and checks. A failed stage is a debugging
exercise, not permission to change expected values or reset history.

## Recover setup safely

If the initial setup is interrupted, keep the returned item/operation IDs.
Ask Copilot to poll the accepted operation or rediscover only this approved
setup's items and bindings before retrying. Reuse matching items and skip
identical uploads; stop on conflicts. Do not duplicate creates, overwrite
files or reset another build. Ask the instructor about access/capacity blocks.

The [Lab 1 setup checkpoint](setup.md#4-wire-the-serial-pipeline) must be
complete before transformation recovery: supplied code cannot replace a
missing lakehouse, input files or notebook bindings.

## Repair your generated code

Wait for the current job to end and open its notebook run snapshot in Fabric
Monitor. Copy the actual assertion or exception, not just the generic REST
job failure reason. Ask Copilot:

```text
Fix only my generated implementation to satisfy the unchanged bootcamp spec
and validator. Use the actual error below and inspect persisted state.
Do not edit source CSVs/checks, drop tables, reset history or watermarks,
or create another workspace. Explain the root cause and show the smallest
patch and exact write set. Stop for approval before executing it.
```

Approve the patch, run only the affected stage and its unchanged checks, and
keep the error and result locally. Do not start a duplicate job while one is
queued or running.

## Use instructor-supplied recovery only explicitly

If blocked within the time box, ask the instructor for the relevant stage's
recovery cells. Record that stage as **supplied recovery**, not successful
live generation.

Before using them, confirm the same identity, workspace and lakehouse, and
stop any existing writer. Preserve your generated code/output. Replace only
the implementation cells in the existing notebook; keep the validator,
final checks, notebook ID and pipeline wiring. Recovery cells must include
their own imports and helpers.

Run that stage and its fixed checks, then continue downstream. Resume an
interrupted customer-event batch in Gold/CDF before full pipeline replay.
Never reseed a missing watermark for an existing target or discard history.
Malformed raw tables or unavailable CDF history need an explicit instructor
repair decision, not an automatic drop or reset.

## Stop on time

Stop the live build after 45 minutes. Show available sales, history and
lineage evidence, and label any incomplete stage. Keep completed work and
stop Spark sessions you started.

After a complete build, pipeline replay must preserve customer history,
surrogate keys, facts and watermark values. Compare
`bootcamp_snapshot(spark)` before and after replay, not only row counts.

[Back to the lab overview](../../README.md).
