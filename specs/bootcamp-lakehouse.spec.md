# Spec: AI-assisted Zava lakehouse

| Field | Contract |
|---|---|
| Goal | Explain daily Zava sales by date, store and channel while retaining customer history |
| Target | An assigned empty workspace with its dedicated active F2 capacity; create schema-enabled `lh_bootcamp` in Lab 1 |
| Engine | Four layer notebooks (Bronze, Silver, Gold, MLV) chained by Change Data Feed, and one serial Data Pipeline |
| Inputs | Five immutable initial CSVs (Lab 1) and one customer change CSV (Lab 4) under `Files/bootcamp/` |
| Acceptance | Unchanged [fixed validator](../labs/bootcamp/validate-lakehouse.py) plus execution, CDF and MLV evidence |

## 1. Authoring and execution

AI proposes the architecture and implementation from this business goal and
the fixed contracts below. The participant approves layer boundaries, table
flow, history policy, serving choices and each write set before execution.
Use short plans and numbered tasks under `specs/plans/`, as described in the
[guardrails](../.github/copilot-instructions.md).

Pre-work is tools, repository clone, sign-in and read-only access checks only.
Each participant receives an empty workspace with a dedicated capacity already
assigned and Active. No lakehouse, staged CSVs, notebooks, pipeline or warm
Spark session are required in advance.

The initial Lab 1 prompt confirms identity, workspace and capacity, shows the
setup write set and waits for approval. Use Fabric skills and CLI to create
`lh_bootcamp` with `creationPayload.enableSchemas=true`, wait for completion
and verify schema support. Upload the five initial CSVs to the paths below,
verifying exact bytes, then use the local builder to deploy
`nb_bootcamp_bronze`, `nb_bootcamp_silver`, `nb_bootcamp_gold` and
`nb_bootcamp_mlv`. Each shell holds a markdown description, the complete
validator, a fail-fast implementation placeholder and an unchanged final
check. Capture actual IDs and create their serial `pl_bootcamp_build`; do not
schedule it or run placeholders. See [Lab 1 setup](../labs/bootcamp/setup.md).

On retry, reconcile only items from the same confirmed setup; poll accepted
operations rather than duplicate submissions. Skip identical files and stop
on unexpected items, mismatched files or incompatible bindings. Never reset
or overwrite existing work. Setup approval is not code-execution approval.

Generate transformation code live. Each notebook contains only its own
layer's code: Bronze loads CSVs into CDF-enabled Bronze tables, Silver reads
Bronze Change Data Feed, Gold reads Silver Change Data Feed and the MLV
notebook defines materialized lake views over Silver. Every notebook starts
with a markdown description, every code cell starts with a `#` comment header
and every function has a docstring. Every notebook defines its own imports
and helpers; persistent state lives in Delta tables and watermarks.

For each lab: propose, generate, approve, save, run and verify. Keep code,
job status and actual check output. A submission is not completion.
Fix generated code after an error; do not edit the spec, fixtures, validator
or expected values to obtain a pass.

`pl_bootcamp_build` runs Bronze, Silver, Gold and MLV serially with
success-only dependencies. Trigger individual notebooks as they become
ready. Run the full pipeline only after all four implementations are ready;
Lab 4 uses one pipeline run to carry the customer change batch through every
layer. Use one execution path per stage: a notebook job or manual interactive
cells, never both concurrently. Include cold Spark startup; do not assume
session reuse.

The fixed checks derive the expected state from the landed files: before
`Files/bootcamp/changes/customer_changes.csv` exists every layer must hold the
initial load; afterwards every layer must hold the changed state. Each
Bronze, Silver and Gold final check snapshots its layer, calls the layer's
processor again and requires an identical snapshot (rows and Delta versions).

The live build is limited to 45 minutes, including lakehouse creation, CSV
uploads, shell/pipeline provisioning and cold startup in Lab 1's 10-minute
slot. If setup is blocked, ask the instructor; recovery code cannot replace
missing resources, uploads or bindings. For a blocked implementation, follow
[repair and recovery](../labs/bootcamp/recovery.md) and identify supplied or
incomplete stages. Optional Labs 5 and 6 are outside this time box.

## 2. Inputs

Lab 1 uploads the five initial `data/bootcamp/` CSVs into the newly created
lakehouse before generating Bronze ingestion. Lab 4 uploads the change file,
which represents new data arriving after the first build:

| OneLake path | Uploaded in | Grain | Rows |
|---|---|---|---|
| `Files/bootcamp/initial/customers.csv` | Lab 1 | one current customer | 6 |
| `Files/bootcamp/initial/products.csv` | Lab 1 | one product | 4 |
| `Files/bootcamp/initial/stores.csv` | Lab 1 | one store | 2 |
| `Files/bootcamp/initial/orders.csv` | Lab 1 | one order | 7 |
| `Files/bootcamp/initial/order_lines.csv` | Lab 1 | one order line | 9 |
| `Files/bootcamp/changes/customer_changes.csv` | Lab 4 | one customer event | 3 |

Headers and values must match the [checked-in CSVs](../data/bootcamp/).
Source files stay immutable after landing. Initial customer validity starts at
the source `effective_at`; orders are dated 10-16 October 2026.

The batch is effective at `2026-11-02T00:00:00`:

| Operation | Customer | Required outcome |
|---|---|---|
| U | C003 | Expire Utrecht/Silver; insert Amsterdam/Gold |
| I | C007 | Insert one current customer version |
| D | C002 | Remove current Bronze state; expire Silver history without removing it |

There is one business event per affected customer. Latest-per-key processing
is valid only for this bounded fixture, not a general all-changes SCD2
algorithm for multiple business events between watermarks.

## 3. Boundaries

- Use only the confirmed workspace and isolated `lh_bootcamp`. Do not
  create/delete workspaces or change capacities, permissions or other resources.
- After setup approval, the initial prompt may create `lh_bootcamp`, stage the
  five initial CSVs and provision the four named shells and their serial
  pipeline. Lab 4 may upload the change CSV. The organizer supplies the empty
  workspace and its dedicated active capacity.
- Create only the tables and views specified below in `bronze`, `silver`,
  `gold` and `lab_ops`. Keep OneLake files read-only after staging.
- Layer separation: the Bronze notebook writes only `bronze`; the Silver
  notebook reads `bronze` and writes `silver` plus its `lab_ops` watermarks;
  the Gold notebook reads `silver` and writes `gold` plus its `lab_ops`
  watermarks; the MLV notebook reads `silver` and defines views in `gold`.
- This is a lakehouse-building exercise, not a DQ-repair exercise. Bronze
  applies only minimal cleansing: trim string values, turn empty strings into
  null and drop rows whose values are all null. No quarantine, invented DQ
  rules, other row drops or unrelated transformations.
- CDF chains every layer: Bronze and Silver tables have CDF enabled, Silver
  consumes Bronze CDF and Gold consumes Silver CDF, each with a watermark per
  source/target pair. Gold tables need no CDF because nothing downstream
  reads them incrementally.
- Fixture assumptions: all source versions after a watermark are processed
  as one batch, so a stamp-then-delete collapses to the delete. Facts keep the
  initial sales; late-arriving or corrected facts, and history corrections
  that would re-key an existing fact, are outside this exercise.
- Use signed-in identity, discovered IDs and runtime tokens. Never commit
  credentials, local configuration or environment-specific identifiers.

## 4. Bronze

Create these Delta tables in `bronze`, all with CDF enabled
(`delta.enableChangeDataFeed = true`) when they are created:

- Raw string snapshots `customers_source`, `products`, `stores`, `orders`
  and `order_lines`, from the five initial CSVs.
- `customer_changes_source`: the raw change events, created only once
  `Files/bootcamp/changes/customer_changes.csv` has landed.
- `customers_current`: keyed current-state table initialized from
  `customers_source`, with the same string columns and audit columns.

Preserve every source column and value as STRING after the minimal cleansing
in section 3, with exactly these audit columns added:

| Column | Type | Requirement |
|---|---|---|
| `_ingested_at` | TIMESTAMP | populated ingestion timestamp |
| `_source_file` | STRING | clean file path ending in the corresponding CSV path |
| `_batch_id` | STRING | populated batch identifier |

Use the file source's `_metadata.file_path` for clean lineage. On Fabric
Runtime 2.0, `input_file_name()` can include query suffixes that fail this
contract.

Expose `load_bronze()` with no arguments. Raw tables are created once and
never rewritten; `customers_current` is the only mutable Bronze table.
Rerunning ingestion must not duplicate rows, replace raw snapshots, reset
changed state or re-enable properties that create new commits. Validate
existing tables rather than silently accepting an incompatible shape.

When the change file is present, apply it to `customers_current` with keyed
MERGE: `U` updates the row, `I` inserts it and `D` removes it. Stamp the D
event's `effective_at` before removing its state row so its delete image keeps
the business boundary, and remove it with `MERGE ... WHEN MATCHED THEN
DELETE`: Fabric Delta rejects subqueries in a `DELETE` condition. Skip the
MERGE when the table already matches the events, so a rerun commits nothing.

## 5. Silver and SCD2

Create these typed Delta tables in `silver`, all with CDF enabled:

- `dim_product(product_id, sku, product_name, category, list_price)`
- `dim_store(store_id, store_name, city, country)`
- `orders(order_id, customer_id, store_id, order_ts, channel)`
- `order_lines(order_id, line_number, product_id, quantity, unit_price, discount)`
- `customer_history(customer_sk, customer_id, full_name, email, city,
  loyalty_tier, valid_from, valid_to, is_current, source_commit_version)`

Prices are DECIMAL(18,2); `line_number` and `quantity` are INT; `discount`
is DECIMAL(6,4); `order_ts` is TIMESTAMP. Other source attributes are STRING.

Expose `process_bronze_to_silver()` with no arguments. Silver reads only
Bronze Change Data Feed: `products`, `stores`, `orders` and `order_lines`
feed the four typed tables, and `customers_current` feeds `customer_history`.
Apply inserts, updates and deletes with a keyed MERGE.

`customer_history` is a Type 2 dimension. Use BIGINT for surrogate keys and
source versions, TIMESTAMP for validity bounds, BOOLEAN for `is_current`, and
STRING for customer attributes. `customer_sk` is unique and non-null; each
customer has at most one current version. Use `9999-12-31 00:00:00` as the
open end. The initial load opens six current versions with
`valid_from = effective_at`.

On an update, expire the current version and insert the new version. On an
insert, insert one current version. On a delete, expire the current version
without replacement. Boundaries use the business event's `effective_at`, not
processing time. Keep expired attributes and surrogate keys; never reuse an
old key for a new version. `source_commit_version` is the Bronze commit
version that produced the row and increases across each customer's versions.

## 6. Gold and incremental processing

Create these documented Delta tables in `gold`:

| Table | Grain and content |
|---|---|
| `dim_customer` | All Silver customer-history versions, keyed on `customer_sk` |
| `dim_product` | One product; same columns and types as Silver |
| `dim_store` | One store; same columns and types as Silver |
| `dim_date` | Every date from first through last order date, inclusive |
| `fact_sales` | One row per order-line key, enriched with order attributes |

Customer columns and types match `silver.customer_history`.
`dim_date(date_key INT, date DATE, year INT, month INT, day_of_week INT)`
uses `yyyyMMdd` keys and Spark day-of-week numbering, 1=Sunday to 7=Saturday.

The fact schema is:

```text
order_id STRING, line_number INT, date_key INT, customer_sk BIGINT,
product_id STRING, store_id STRING, channel STRING, quantity INT,
unit_price DECIMAL(18,2), discount DECIMAL(6,4), net_amount DECIMAL(18,2)
```

Join the customer version where `valid_from <= order_ts < valid_to`.
Product/store identifiers are natural dimension keys. Compute net amount
as `quantity * unit_price * (1 - discount)`, rounded HALF_UP to two decimals
per line. Keep exactly nine facts, totalling 16 units and 516.73 net sales.
Add table descriptions and column comments to Gold.

Expose `process_silver_to_gold()` with no arguments. Gold reads only Silver
Change Data Feed. Dimensions apply the latest change per key with a keyed
MERGE; `dim_date` adds missing days; `fact_sales` recomputes only the orders
touched by the `orders` or `order_lines` change feed and removes lines
deleted from Silver.

### CDF and watermarks

Every Silver and Gold processor follows the same incremental contract. Read
the source's change feed from the version after the watermark (version 0 on
the first run) through a captured current version, ignore `update_preimage`,
keep the latest change per key, apply an idempotent keyed MERGE, and advance
the watermark only after the target write succeeds. Stop on unavailable
history; do not skip a CDF gap by advancing a watermark. A run without new
source versions commits nothing.

Store exactly one row per source/target pair in:

```text
lab_ops.cdf_watermarks(
    source_table STRING,
    target_table STRING,
    last_processed_version BIGINT
)
```

Silver owns five pairs: `bronze.products`, `bronze.stores`, `bronze.orders`,
`bronze.order_lines` and `bronze.customers_current` to their Silver targets.
Gold owns five pairs: `silver.customer_history`, `silver.dim_product` and
`silver.dim_store` to their Gold copies, and `silver.orders` and
`silver.order_lines` to `gold.fact_sales`. After each layer's run its
watermarks equal the current source versions.

Print the actual consumed version ranges. Matching output values alone do
not prove use of CDF.

After the change batch there are six current Bronze customers, eight Silver
history rows (six current, two expired), and the same eight rows in Gold.
All ten watermarks match their source versions.

## 7. Materialized lake views

Define two materialized lake views over Silver with
`CREATE OR REPLACE MATERIALIZED LAKE VIEW`, so that a rerun is safe. They are
actual Fabric materialized lake views, not tables or ordinary views.

`gold.sales_daily` joins `silver.orders` to `silver.order_lines` and groups by
`date_key`, `store_id` and `channel`. Use exactly:

```text
date_key INT, store_id STRING, channel STRING,
sales_lines BIGINT, units BIGINT, net_amount DECIMAL(18,2)
```

Use COUNT(*) for lines, the sum of quantity for units and the sum of per-line
amounts rounded to two places for net sales. Require non-null grouping keys,
positive line/unit counts and nonnegative sales using six `ON MISMATCH FAIL`
constraints. Do not silently drop mismatching rows. Every group must
reconcile to Silver: seven groups, nine lines, 16 units and 516.73 net sales.

`gold.customer_current` holds the current version of each customer from
`silver.customer_history`: `customer_sk`, `customer_id`, `full_name`, `email`,
`city`, `loyalty_tier` and `valid_from`, with an `ON MISMATCH FAIL` constraint
for non-null keys. It has six rows before and after the change batch.

The MLV notebook reads only Silver. Inspect `SHOW CREATE MATERIALIZED LAKE
VIEW`, Fabric lineage and any available refresh evidence. Claim incremental
MLV refresh only when Fabric reports that strategy; a create or full refresh
is not proof. A production design would schedule MLV refresh from lineage
instead of recreating the views.

## 8. Acceptance and replay

| Checkpoint | Required evidence |
|---|---|
| Pre-work | Tools and sign-in ready; assigned empty workspace with dedicated active capacity; five initial CSVs and one change CSV available locally |
| Lab 1 setup | Approved schema-enabled lakehouse, five exact uploads, four bound shells with fixed checks and an unscheduled serial pipeline; actual IDs and completion evidence |
| Bronze | Fixed `bronze` initial-load PASS and rerun no-op; source values, string types and audit lineage intact; CDF on every Bronze table |
| Silver | Fixed `silver` initial-load PASS and rerun no-op; typed values exact; six initial history versions; CDF on every Silver table; five current Bronze-to-Silver watermarks |
| Gold | Fixed `gold` initial-load PASS and rerun no-op; exact star, dates, as-of keys and facts; five current Silver-to-Gold watermarks; printed version ranges |
| MLV | Fixed `mlv` PASS; two actual MLV definitions with FAIL constraints; all groups and totals; current customers; inspected lineage |
| Change batch | Change CSV uploaded in Lab 4; one successful pipeline run; every stage reports the change-batch PASS and reruns stay no-ops |
| Documentation | Every notebook has a markdown description, every code cell a `#` comment header and every function a docstring |

Run the fixed checks in each notebook context. They detect whether the
change file has landed and print `PASS: bootcamp <stage> contract, initial
load (fixed read-only checks)` or `PASS: bootcamp <stage> contract, after the
customer change batch (fixed read-only checks)`. The Bronze, Silver and Gold
checks then print `PASS: <stage> rerun is a no-op`.

After the change batch and one pipeline run, C003 has an expired and a
current version (Amsterdam, Gold), C007 has one current version and C002 has
no current version but keeps its history, in both `silver.customer_history`
and `gold.dim_customer`; `gold.customer_current` has six rows. A partial change state
is not a valid checkpoint: repair and rerun the failing notebook, which
resumes from its persisted watermarks.

Idempotence means unchanged results: rerunning any processor without new
input leaves rows and Delta versions unchanged. Do not manufacture commits on
CDF sources. A table holding the right values is not proof of MLV creation,
nor are matching targets proof of CDF consumption.

## 9. Optional labs

[Project Osmos](../labs/bootcamp/05-osmos-replay.md) builds the same contracts
in a fresh approved target. Use separate layer notebooks and the same fixed
checks; do not overwrite a completed live build.

[Semantic model and Fabric IQ](../labs/bootcamp/06-semantic-model-fabric-iq.md)
uses existing Gold read-only. AI proposes a model and measures from the
business goal and actual schema; the participant approves before creation.
Only that semantic-model item may be created; no report is required.
Prefer Direct Lake. If using customer history, relate fact `customer_sk`
to unique dimension `customer_sk`, not repeated customer ID.

Discover authoring, licensing and client capabilities first. Verify actual
deployment, refresh and Fabric IQ query execution. Reconcile every approved
measure at total and daily-group levels against equivalent Gold queries
under identical filters. Missing access or features are incomplete outcomes,
not permission to change infrastructure or invent results.
