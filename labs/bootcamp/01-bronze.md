# Lab 1: create the lakehouse and Bronze

**13 minutes · session minutes 15–28.** Prerequisite: the [pre-work ready check](pre-work.md#ready-check).

| Goal | Start | Open | Finish signal |
|---|---|---|---|
| Land a replayable source, without changing its meaning | Empty assigned workspace on dedicated active F2 | Copilot at repo root, then `nb_bootcamp_bronze` | Setup verified and Bronze initial-load PASS |

**Watch, work, check:** watch the instructor's setup approval, then use your
own workspace name. While your job runs, review the generated cells and wait;
do not launch another run. Your own PASS, not the instructor's, unlocks Lab 2.

Start Copilot from the repo root with your assigned **empty workspace** on its
dedicated active F2 capacity. No lakehouse, uploaded files, notebooks or warm
Spark session are required yet. This slot includes [setup](setup.md), cold
startup and the generated Bronze ingestion.

Every layer has its own short prompt. Each states its outcome the way Microsoft
recommends for [data engineering
agents](https://learn.microsoft.com/en-us/fabric/data-engineering/data-engineering-agent-best-practices):
goal, what to read, what to generate, what to create, how to validate and what
to preserve. At each **decision** Copilot stops and
waits for you; reply `approve`, `change: <what>` or `stop`. The rules live in
the [spec](../../specs/bootcamp-lakehouse.spec.md) (section 1 lists all eleven
decisions) and the [Copilot guardrails](../../.github/copilot-instructions.md),
so the prompts stay short.

## 1. Set up the lakehouse

Replace the workspace placeholder and paste:

```text
Use the spark-cli skill.
Goal: set up lh_bootcamp in my assigned workspace <assigned workspace name>,
which starts empty with a dedicated F2 capacity already assigned.
Run az account show; confirm workspace, capacity and contents.
Propose the plan and tasks from specs/bootcamp-lakehouse.spec.md, save them
under specs/plans/ and show the setup write set from labs/bootcamp/setup.md.
Stop for my setup approval.
Then run setup.md steps 2-4; validate the setup checkpoint.
Preserve other workspaces, capacities and permissions.
Do not generate Bronze code yet.
```

**Decision 1 · setup go/no-go.** Check that the identity, workspace and
capacity are yours, the workspace is empty, and the write set is exactly one
schema-enabled `lh_bootcamp`, five uploads, an empty `changes/` folder, four
empty notebooks and their pipeline, in your assigned workspace only. Then
reply `approve`. Setup approval does not authorize running transformations.

Copilot creates the lakehouse with `creationPayload.enableSchemas=true`,
uploads the five initial CSVs, deploys the bound shells with
`prepare-artifacts.py shells`, wires the pipeline with
`prepare-artifacts.py pipeline` and stops at the setup checkpoint.

## 2. Generate and run Bronze

```text
Use the spark-cli skill.
Goal: build Bronze in lh_bootcamp.
Read the five CSVs in Files/bootcamp/initial/.
Generate NEW native Spark SQL as %%sql cells in nb_bootcamp_bronze from spec
sections 3 and 4.
Create only bronze tables.
Validate with the fixed check and report its row counts.
Preserve the CSVs and the other notebooks.
Show the plan, cells and exact write set. Stop for my code approval.
```

**Decision 2 · approve the Bronze code.** Review Copilot's proposed cells
against this checklist. The notebook still contains its placeholder until
you approve; Copilot saves the new cells only after approval:

- Native Spark SQL: each cell starts with `%%sql` and a
  `-- Stage: bronze - …` header; any PySpark is justified in that header.
- Writes only `bronze`, keeps source Files immutable, preserves raw values as
  strings with the audit columns and invents no DQ-repair rules; CDF is
  enabled when each table is created.
- Reruns are safe and schemas stay locked: raw tables are created only if
  missing, change events are inserted only if new, and there is no
  `mergeSchema` or schema auto-merge. A header mismatch stops the run.
- `customers_current` is applied with a keyed MERGE whose source is written
  inline in `USING (...)`, never a temporary view.
- No Silver, Gold or MLV code or table references in any cell, including the
  fixed baseline and check cells.

Reply `change: <what>` to regenerate, or `approve`. Approval authorizes one
run: Copilot saves the cells between the fixed baseline cell and the final
fixed check cell, confirms no job is running, triggers only
`nb_bootcamp_bronze`, monitors it and shows the actual check output. The same
applies to every lab.

If notebook submission is unavailable, Copilot gives you the cells instead:
paste them in place of the placeholder, between the fixed baseline cell and
the final check, and run all cells in order. Do not execute both paths. Keep
code and output.

## 3. Verify

The shell's fixed validator, baseline and check cells are Python because they
read Delta history and assert exact values; they never change. The baseline
cell records whether Bronze is already current, and the final check validates
Bronze and, if you run the notebook again, proves the generated cells
committed nothing:

```python
bootcamp_begin(spark, "bronze")       # fixed baseline cell, before the generated cells
# ... your NEW %%sql cells run here ...
validate_bootcamp(spark, "bronze")    # fixed check cell, after the generated cells
```

**Decision 3 · accept the result.** Compare the output with the definition of
done below. Accept by pasting the [Lab 2](02-silver-scd2.md) prompt; otherwise
follow the failure notes. If time remains, an optional second **Run all** in
the warm session shows the rerun proof.

## Definition of done

- Schema-enabled `lh_bootcamp`, five verified uploads, the empty `changes/`
  folder, four correctly bound shells with fixed checks and an unscheduled
  serial pipeline; setup IDs saved.
- Actual `PASS: bootcamp bronze contract, initial load (fixed read-only checks)`
  and `PASS: bootcamp bronze was built or brought to contract by this run`.
  An optional second **Run all** in the warm session prints
  `PASS: bootcamp bronze was already current and the rerun changed nothing`.
- Five raw tables: 6 customers, 4 products, 2 stores, 7 orders and 9 order
  lines as strings; every source value and audit path matches.
- CDF enabled on every Bronze table; `customers_current` matches the 6 source
  customers.
- The `bootcamp bronze rows validated: ...` line lists the row count of every
  table the check read; no row is rejected.
- Code is documented native Spark SQL. Save code/output and tick your tasks.

## If blocked

If setup fails, retain returned IDs, resume/poll the accepted operation and
reconcile only this approved setup. Do not duplicate creates or overwrite
conflicting items/files. Ask the instructor about access/capacity blocks.

If a job fails, reply `repair: <actual error>`. Copilot fixes only the
generated code to satisfy the unchanged spec and validator, shows the patch
and write set, and stops at the code decision again; approve to rerun. The
same reply works in every lab. Get the actual notebook exception from the run
snapshot in Fabric Monitor; a generic job failure is not enough to diagnose
it. `Failed to decode multipart name` means a MERGE reads a temporary view
over a Delta table; the fix is to write that source inline in `USING (...)`.
Follow [repair and recovery](recovery.md), without weakening the checks.

**Next:** [Lab 2 - Silver and SCD2](02-silver-scd2.md).
Continue only after your PASS. If blocked, keep your error and follow the
walkthrough without running downstream layers.
