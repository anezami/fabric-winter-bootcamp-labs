# Lab 6: Semantic model and Fabric IQ

**Optional, after the session; untimed.** Prerequisite: completed Gold and
MLV checks, including the change batch. No report is required.

AI proposes a model and measures from the business goal and actual Gold
schema. You approve the design before creation, then query the real model.

## 1. Confirm access and tools

Confirm your identity, assigned workspace and `lh_bootcamp`. Gold remains
read-only; only the approved semantic-model item may be created.

Check Power BI authoring licensing for your workspace (normally Pro/PPU),
workspace access, Direct Lake availability and the model-authoring endpoint.
An F2 capacity or GitHub Copilot subscription does not grant Power BI
authoring rights. If blocked, retain the design and mark creation incomplete;
do not change capacity, permissions or workspaces.

Use `semantic-model-authoring` for creation and `fabriciq` for read-only
querying. Fabric IQ cannot author a model. Prefer connected
`powerbi-modeling-mcp` for authoring; otherwise follow the authoring skill's
complete TMDL/Fabric REST route. Do not mix live MCP edits with stale local
TMDL. Load the skill's modeling and Direct Lake guidelines before building.

For GitHub Copilot CLI, follow Microsoft's
[Fabric IQ MCP setup](https://learn.microsoft.com/en-us/fabric/iq/connectors/fabric-iq-mcp).
Merge its HTTP server entry into your existing MCP configuration rather than
replacing other servers. The endpoint is
`https://fabriciq.svc.cloud.microsoft/v1/mcp/fabriciq`.
Use interactive OAuth, not pasted bearer tokens. Restart the CLI, run
`/mcp show FabricIQ`, sign in and verify actual connected tools.
Consent is subject to tenant policy; do not change that policy.

Use Microsoft 365 Copilot only if your instructor confirms an
organization-approved integration, required licensing and successful model
discovery/query in that client. A license alone is not a Fabric IQ connector.
Otherwise use the GitHub Copilot CLI path.

## 2. Approve an AI-proposed model

```text
Use semantic-model-authoring in my confirmed workspace. Inspect only
lh_bootcamp Gold tables and their actual types, grain and keys.
Business goal: explain Zava daily sales by date, store and channel, with
customer history interpreted correctly. Propose a model and explicit
measures; do not assume an approved measure list.
Prefer Direct Lake. Explain source mapping, relationships, filter direction,
date handling, formats, measure meanings and the trade-off between the Gold
star and an aggregate-only model.
If including customer history, join fact_sales.customer_sk to unique
dim_customer.customer_sk, not repeated customer_id. Preserve as-of attributes.
Do not relate gold.sales_daily to customer/product grain or double-count
facts and aggregates. Show the complete proposed model and exact item write set.
Keep Gold read-only. No report, permission or capacity changes.
Stop for approval before creating or deploying anything.
```

Approve metric definitions, not only names. Save the approved design and
equivalent Gold reconciliation queries.

Then ask Copilot to build the complete model, deploy once, verify the actual
item, refresh/frame it, inspect relationships and test every measure. Stop
on duplicate names or credential errors; do not delete/redeploy blindly.
Keep actual completion output. Never store tokens in model files.

## 3. Query through Fabric IQ

```text
Use fabriciq to discover my model by its actual name. Resolve ambiguity
with me. Read the full model schema, custom instructions and verified
answers; use relevant verified answers.
Using my approved measures, show daily sales by date, store and channel
for the fixture's order dates and explain the largest group.
Execute against the real model and cite it. Do not answer from memory or
earlier notebook output. Resolve stored entity values before named filters.
Explain any unavailable capability rather than inventing results.
```

Inspect actual query output. Multirow DAX queries use `ORDER BY`; each query
has one `EVALUATE`. Do not bypass model security or assume columns from this
guide without schema discovery.

## Definition of done

- The approved model exists, refresh succeeds and relationships match its
  intended grain.
- Every approved measure reconciles at total and daily-group levels to
  equivalent Gold SQL under identical filters.
- For approved net-sales semantics, results match `gold.sales_daily`.
  If customer analysis is supported, C003's historical sales use its
  original version, not its later city/tier.
- Retain the approved definitions, actual Fabric IQ output and comparison
  results. A plausible natural-language answer alone is not proof.

References: [Direct Lake](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-overview),
[licensing](https://learn.microsoft.com/en-us/fabric/enterprise/licenses),
[Fabric IQ capabilities](https://learn.microsoft.com/en-us/fabric/iq/connectors/fabric-iq-mcp).

[Back to the lab overview](../../README.md).
