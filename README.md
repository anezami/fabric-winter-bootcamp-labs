# Zava Lakehouse Bootcamp

**Build a Microsoft Fabric lakehouse with AI: 15-minute introduction +
45-minute hands-on lab.**

Start from an **empty workspace with a dedicated capacity already assigned**.
The initial prompt creates `lh_bootcamp`, lands five initial synthetic CSVs
and wires four notebooks and a pipeline. Copilot generates each layer as
native Spark SQL: Bronze, Silver from the Bronze change feed, Gold from
the Silver change feed, and materialized lake views (MLVs) over Silver.
Lab 4 lands a customer change batch and runs the pipeline once. Learn Type 2
slowly changing dimensions (SCD2), Delta Change Data Feed (CDF) and durable
watermarks. You approve each implementation; fixed checks verify results.

## Get started

You need GitHub Copilot CLI, Git, Python 3.10+, Azure CLI, and an assigned
empty Fabric workspace on its **dedicated active F2 capacity**, with notebook
and MLV support.

```powershell
git clone https://github.com/anezami/fabric-winter-bootcamp-labs.git
cd fabric-winter-bootcamp-labs
az login
copilot
```

In Copilot, install the public Fabric skills if missing:

```text
/plugin marketplace add microsoft/skills-for-fabric
/plugin install fabric-skills@fabric-collection
```

Restart Copilot after installation. Complete the [pre-work](labs/bootcamp/pre-work.md)
**before the session**: tools, sign-in, access and local [starter CSVs](data/bootcamp/).
Create nothing in Fabric yet. [Lab 1's initial prompt](labs/bootcamp/01-bronze.md)
performs [setup](labs/bootcamp/setup.md) after approval, then generates
Bronze code for separate approval. Setup and cold Spark startup count in the
time box.

Read the [specification](specs/bootcamp-lakehouse.spec.md) and
[Copilot guardrails](.github/copilot-instructions.md).

## Follow the labs in order

For each lab: **plan -> generate -> review and approve -> run -> check**.
Each notebook, including its fixed checks, contains only its own layer's
code. Every notebook and cell starts with a comment. Replace only the
placeholder cell; keep the fixed validator, baseline and check cells.
Generated code or an accepted job is not a successful run.

| Minutes | Lab | Build | Definition of done |
|---|---|---|---|
| 0-15 | Introduction | Skills, design loop, medallion, SCD2, CDF, MLVs | Architecture and boundaries are clear. |
| 15-28 | [1. Setup + Bronze](labs/bootcamp/01-bronze.md) | Lakehouse, five CSVs, bound shells/pipeline; raw Delta with CDF and customer state | Setup verified; `bronze` initial-load PASS: source strings/audit intact; counts 6/4/2/7/9. |
| 28-35 | [2. Silver + SCD2](labs/bootcamp/02-silver-scd2.md) | Typed tables and customer history from the Bronze change feed | `silver` PASS: exact types/values, six customer versions with unique keys, CDF enabled, five watermarks. |
| 35-43 | [3. Gold from CDF](labs/bootcamp/03-gold-cdf.md) | Documented star schema from the Silver change feed | `gold` PASS: 9 facts, 16 units, 516.73 net sales; keys resolve; five Gold watermarks current. |
| 43-60 | [4. MLVs + changes](labs/bootcamp/04-materialized-view.md) | `gold.sales_daily` and `gold.customer_current` over Silver; land the change batch; run the pipeline once | `mlv` PASS: views reconcile with FAIL constraints; every stage reports the change-batch PASS. |

Use one Spark workload at a time. Run each notebook as it becomes ready;
run the pipeline only **after all four notebooks are implemented**.

## Finish

Show the daily-sales query, C003's expired and current versions, C002's
retained history, the watermarks and your validation output. Explain why
October sales keep their original customer version after the November change.

Stop the live build after 45 minutes. Keep notebooks and outputs, and stop
Spark sessions you started. If blocked, follow [repair and recovery](labs/bootcamp/recovery.md);
label supplied or incomplete stages. Never change fixtures or checks to force
a pass.

## Optional labs after the session

| Lab | Definition of done |
|---|---|
| [5. Project Osmos](labs/bootcamp/05-osmos-replay.md) | In a fresh approved target, the same contracts, change batch and both real MLV checks pass; retain actual task output. |
| [6. Semantic model + Fabric IQ](labs/bootcamp/06-semantic-model-fabric-iq.md) | Approve the AI-proposed model/measures, deploy and refresh it, then execute queries whose totals and daily groups reconcile to Gold. |

Keep source files immutable. Use only your confirmed workspace and lakehouse;
do not change capacities, permissions or another participant's resources.
Never commit credentials or environment-specific IDs.
