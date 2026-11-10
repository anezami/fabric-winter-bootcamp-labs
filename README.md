# Zava Lakehouse Bootcamp

**Build a Microsoft Fabric lakehouse with AI: 15-minute introduction +
45-minute hands-on lab.**

Start with six small, synthetic CSV files in OneLake. Use GitHub Copilot to
propose a design, generate the code, and build Bronze, Silver and Gold data
products. Learn Type 2 slowly changing dimensions (SCD2), Delta Change Data
Feed (CDF), durable watermarks and materialized lake views (MLVs).
You approve each implementation; fixed checks verify the results.

## Get started

You need GitHub Copilot CLI, Git, Python 3.10+, Azure CLI, and an assigned
Fabric workspace on an **active F2 capacity** with notebook and MLV support.

```powershell
git clone https://github.com/anezami/fabric-winter-bootcamp-labs.git
cd fabric-winter-bootcamp-labs
az login
copilot
```

In Copilot, install the public Fabric skills if not already available:

```text
/plugin marketplace add microsoft/skills-for-fabric
/plugin install fabric-skills@fabric-collection
```

Restart Copilot after installation. Complete the [pre-work](labs/bootcamp/pre-work.md)
**before the session**: confirm your identity and workspace, create an empty
schema-enabled `lh_bootcamp`, upload the [starter CSVs](data/bootcamp/), and
prepare the [four notebook shells and pipeline](labs/bootcamp/setup.md).
The shells contain checks, not transformation code.

Read the [specification](specs/bootcamp-lakehouse.spec.md) and
[Copilot guardrails](.github/copilot-instructions.md). Start Lab 1 only when
the pre-work ready check passes.

## Follow the labs in order

For each lab: **plan -> generate -> review and approve -> run -> check**.
Use its copyable prompt, replace only the notebook's implementation
placeholder, and retain the fixed validator and final check. Capture actual
output; generated code or an accepted job is not a successful run.

| Lab | Time | Build | Definition of done |
|---|---|---|---|
| [1. Bronze](labs/bootcamp/01-bronze.md) | 10 min | Raw Delta snapshots and customer state | `bronze` PASS: six source tables retain all values as strings with audit columns; counts 6/4/2/7/9/3; customer state has CDF; ingestion replay is safe. |
| [2. Silver + SCD2](labs/bootcamp/02-silver-scd2.md) | 12 min | Typed tables and customer history | `silver` PASS: exact types/values, six initial customer versions with unique keys and validity bounds, CDF enabled, bootstrap watermark recorded. |
| [3. Gold + CDF](labs/bootcamp/03-gold-cdf.md) | 15 min | Star schema and both incremental processors | `gold` then `cdf` PASS: 9 facts; 8 customer-history rows in Silver and Gold; update/insert/delete correct; bounded CDF reads shown; watermarks current; replay preserves data and keys. |
| [4. Materialized lake view](labs/bootcamp/04-materialized-view.md) | 8 min | Daily sales by date/store/channel | `mlv` PASS: a real MLV with 7 groups, 9 sales lines, 16 units and 516.73 net sales; every group reconciles; inspect DDL, FAIL constraints and lineage. |

Use one Spark workload at a time. Run each notebook as it becomes ready;
the serial pipeline is for replay **after all four notebooks are implemented**.
Do not run it against unfinished shells.

## Finish

Show the daily-sales query, C003's two customer versions, both watermarks and
your validation output. Explain why October sales retain their original
customer version after the November change.

Stop the live build after 45 minutes. Keep your notebooks and outputs, and
stop Spark sessions you started. If blocked, follow [repair and recovery](labs/bootcamp/recovery.md);
mark supplied or incomplete stages honestly. Never change the fixtures or
checks to force a pass.

## Optional labs after the session

| Lab | Definition of done |
|---|---|
| [5. Project Osmos](labs/bootcamp/05-osmos-replay.md) | In a fresh approved target, the same contracts, SCD2/CDF replay and real MLV checks pass; retain actual task output. |
| [6. Semantic model + Fabric IQ](labs/bootcamp/06-semantic-model-fabric-iq.md) | Approve the AI-proposed model/measures, deploy and refresh it, then execute queries whose totals and daily groups reconcile to Gold. |

Keep source files immutable. Use only your confirmed workspace and lakehouse;
do not change capacities, permissions or another participant's resources.
Never commit credentials or environment-specific IDs.
