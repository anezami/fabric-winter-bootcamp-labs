# Pre-work: get ready

Prepare tools and access before the session. Begin the live build with an
**empty workspace** and a dedicated active F2 capacity already assigned.
Lab 1 creates the lakehouse, stages the five initial CSVs and provisions
shells/pipeline; setup and cold Spark startup count inside the 45-minute build.

## 1. Check tools and access

You need:

- Git, Python 3.10+ and Azure CLI.
- GitHub Copilot CLI with an active Copilot account.
- Your assigned empty Fabric workspace, permission to create the lakehouse,
  notebooks and pipeline, and its dedicated active F2 capacity with
  Materialized Lake View support.
- The public [Skills for Fabric](https://github.com/microsoft/skills-for-fabric)
  bundle, including `spark-cli`.

From PowerShell:

```powershell
git clone https://github.com/anezami/fabric-winter-bootcamp-labs.git
cd fabric-winter-bootcamp-labs
az login
az account show
copilot
```

If your Fabric identity has no Azure subscription, use
`az login --allow-no-subscriptions`. Confirm the correct tenant and identity
with your instructor; do not paste tokens into chat.

Inside Copilot, check installed plugins. If the Fabric bundle is missing:

```text
/plugin marketplace add microsoft/skills-for-fabric
/plugin install fabric-skills@fabric-collection
```

Restart Copilot after installation and confirm `spark-cli` is available.
If you use another supported Copilot client, follow the public package's
client-specific setup. Do not copy CLI configuration into a different client.

## 2. Check access only

Ask Copilot to list and confirm the exact assigned workspace, its capacity
assignment and Active status, your identity and permission to create items.
Ask the instructor to confirm that the capacity is dedicated to your workspace
and that the runtime supports MLVs. Do not test access by creating items.

Check that the workspace is empty and the local [CSV files](../../data/bootcamp/)
(five initial files and one change file) are present. If the workspace
contains another build, stop and ask the instructor for the correct target.
Do not delete, reset or overwrite anything.

Do not create a lakehouse, upload CSVs, provision notebooks/pipelines, create
tables or start Spark during pre-work. Do not load prepared transformations.
The [Lab 1 setup prompt](01-bronze.md) performs [setup](setup.md) after
approval and selects each shell's unchanged stage sections from the
[validator](validate-lakehouse.py).

## Ready check

- GitHub Copilot, `spark-cli`, Python and Azure CLI are available; sign-in works.
- Correct identity and assigned **empty workspace**, with its dedicated F2
  capacity already assigned and Active.
- The five initial CSVs and the change CSV exist locally under
  `data/bootcamp/`; no upload is required yet.
- No lakehouse, notebooks, pipeline, data tables or Spark session exist yet.

If anything is blocked, ask the instructor before starting. Do not change
workspaces, capacities or permissions yourself.

**Next:** [Lab 1 - create the lakehouse and Bronze](01-bronze.md).
