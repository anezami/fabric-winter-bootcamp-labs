# Spec: AI-assisted Zava lakehouse

| Field | Contract |
|---|---|
| Goal | Explain daily Zava sales by date, store and channel while retaining customer history |
| Target | The participant's confirmed workspace and schema-enabled `lh_bootcamp` |
| Engine | Four independent Fabric notebooks and one serial Data Pipeline |
| Inputs | Six immutable synthetic CSVs under `Files/bootcamp/` |
| Acceptance | Unchanged [fixed validator](../labs/bootcamp/validate-lakehouse.py) plus execution, CDF and MLV evidence |

## 1. Authoring and execution

AI proposes the architecture and implementation from this business goal and
the fixed contracts below. The participant approves layer boundaries, table
flow, history policy, serving choices and each write set before execution.
Use short plans and numbered tasks under `specs/plans/`, as described in the
[guardrails](../.github/copilot-instructions.md).

Pre-work provides `nb_bootcamp_bronze`, `nb_bootcamp_silver`,
`nb_bootcamp_gold_cdf` and `nb_bootcamp_mlv`, each with the complete validator,
a fail-fast implementation placeholder and an unchanged final check.
Generate transformation code live. Every notebook defines its own imports
and helpers; persistent state lives in Delta tables and watermarks.

For each lab: propose, generate, approve, save, run and verify. Keep code,
job status and actual check output. A submission is not completion.
Fix generated code after an error; do not edit the spec, fixtures, validator
or expected values to obtain a pass.

`pl_bootcamp_build` runs Bronze, Silver, Gold/CDF and MLV serially with
success-only dependencies. Trigger individual notebooks as they become
ready. Run the full pipeline only after all four implementations are ready.
Use one execution path per stage: a notebook job or manual interactive cells,
never both concurrently. A warm session does not guarantee job/session reuse.

The live build is limited to 45 minutes. Follow
[repair and recovery](../labs/bootcamp/recovery.md) if blocked, and identify
supplied or incomplete stages. Optional Labs 5 and 6 are outside this time box.

## 2. Inputs

| OneLake path | Grain | Rows |
|---|---|---|
| `Files/bootcamp/initial/customers.csv` | one current customer | 6 |
| `Files/bootcamp/initial/products.csv` | one product | 4 |
| `Files/bootcamp/initial/stores.csv` | one store | 2 |
| `Files/bootcamp/initial/orders.csv` | one order | 7 |
| `Files/bootcamp/initial/order_lines.csv` | one order line | 9 |
| `Files/bootcamp/changes/customer_changes.csv` | one customer event | 3 |

Headers and values must match the [checked-in CSVs](../data/bootcamp/).
Source files stay immutable. Initial customer validity starts at the source
`effective_at`; orders are dated 10-16 October 2026.

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
- Create only the tables/view specified below in `bronze`, `silver`, `gold`
  and `lab_ops`. Keep OneLake files read-only after staging.
- This is a lakehouse-building exercise: no quarantine, invented DQ rules,
  silent row drops or unrelated transformations.
- CDF covers customer changes only. Facts remain the initial sales snapshot;
  late-arriving or corrected facts are outside this exercise.
- Use signed-in identity, discovered IDs and runtime tokens. Never commit
  credentials, local configuration or environment-specific identifiers.

## 4. Bronze

Create six Delta source snapshots in `bronze`: `customers_source`,
`products`, `stores`, `orders`, `order_lines`, `customer_changes_source`.
Preserve every source column and value as STRING, with exactly these audit
columns added:

| Column | Type | Requirement |
|---|---|---|
| `_ingested_at` | TIMESTAMP | populated ingestion timestamp |
| `_source_file` | STRING | clean file path ending in the corresponding CSV path |
| `_batch_id` | STRING | populated batch identifier |

Use the file source's `_metadata.file_path` for clean lineage. On Fabric
Runtime 2.0, `input_file_name()` can include query suffixes that fail this
contract.

Create `bronze.customers_current` as a keyed state copy of the six source
customer columns. Enable CDF before applying any events. This state table is
the only mutable Bronze table; raw snapshots and raw events remain unchanged.

Initialize snapshots/state once. Rerunning ingestion must not duplicate
rows, replace audit snapshots, reset changed state or re-enable properties
that create new source commits. Validate existing tables rather than
silently accepting an incompatible shape.

In Lab 3, apply U/I using keyed MERGE. Stamp the D event's `effective_at`
before removing its state row so its delete image retains the business
boundary; use keyed `MERGE ... WHEN MATCHED THEN DELETE`. Fabric Delta
does not support subqueries in a DELETE condition. Skip already-applied
events rather than creating no-op source commits.

## 5. Silver and SCD2

Create these typed Delta tables in `silver`:

- `dim_product(product_id, sku, product_name, category, list_price)`
- `dim_store(store_id, store_name, city, country)`
- `orders(order_id, customer_id, store_id, order_ts, channel)`
- `order_lines(order_id, line_number, product_id, quantity, unit_price, discount)`
- `customer_history(customer_sk, customer_id, full_name, email, city,
  loyalty_tier, valid_from, valid_to, is_current, source_commit_version)`

Prices are DECIMAL(18,2); `line_number` and `quantity` are INT; `discount`
is DECIMAL(6,4); `order_ts` is TIMESTAMP. Other source attributes are STRING.
Static typed tables may be rebuilt idempotently; customer history must be
preserved.

For `customer_history`, use BIGINT for surrogate keys and source versions,
TIMESTAMP for validity bounds, BOOLEAN for `is_current`, and STRING for
customer attributes. Keys are unique and non-null. Each customer has at most
one current version. Use `9999-12-31 00:00:00` as the open end.

Bootstrap six current versions from a captured Bronze snapshot:
`valid_from = effective_at`, `source_commit_version = captured source version`.
Enable Silver history CDF before subsequent changes. Record the
Bronze-to-Silver bootstrap watermark when creating history.

On U, close the current version and insert the new version. On I, insert one
current version. On D, close the current version without replacement.
Boundaries use the business event's `effective_at`, not processing time.
Keep expired attributes and surrogate keys; never reuse an old key for a
new version. New versions record their source CDF commit version.

## 6. Gold and incremental processing

Create these Delta tables in `gold`:

| Table | Grain and content |
|---|---|
| `dim_customer` | All Silver customer-history versions, maintained from Silver CDF |
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
Initialize this snapshot once; customer events must not re-key its history.
Add table descriptions and column comments to Gold.

### CDF and watermarks

Process `bronze.customers_current -> silver.customer_history`, then
`silver.customer_history -> gold.dim_customer`.

Store exactly one row per pair in:

```text
lab_ops.cdf_watermarks(
    source_table STRING,
    target_table STRING,
    last_processed_version BIGINT
)
```

Bootstrap each target from a complete snapshot at a captured Delta version.
Bootstrap Bronze-to-Silver in Lab 2 and Silver-to-Gold in Lab 3 before events.
An existing target without its watermark is an error, not permission to
reset it or skip history.

For an incremental run, capture an upper source version and read the closed
range `[last_processed_version + 1, upper]`. Ignore `update_preimage` and
select the latest change per target key for this fixture. Use customer ID
for Bronze changes and customer surrogate key for Silver history changes.
Write the target idempotently, then advance its watermark. A retry between
target writes and watermark advancement must not duplicate versions.
If there is no new version, do not write. Stop on unavailable CDF history.

Expose `process_bronze_to_silver()` and `process_silver_to_gold()` with no
arguments for the fixed replay check. Print the actual consumed version
ranges. Matching output values alone do not prove use of CDF.

After the batch there are six current Bronze customers, eight Silver history
rows (six current, two expired), and the same eight rows in Gold. Both
watermarks match their captured source versions. Preserve rows, keys, facts
and watermark values when replaying without new input.

## 7. Materialized lake view

Create `gold.sales_daily` as an actual Fabric MATERIALIZED LAKE VIEW over
`gold.fact_sales`, not a table or ordinary view. Group by `date_key`,
`store_id`, `channel`. Use exactly:

```text
date_key INT, store_id STRING, channel STRING,
sales_lines BIGINT, units BIGINT, net_amount DECIMAL(18,2)
```

Use COUNT(*) for lines and sums for quantity/net amount. Require non-null
grouping keys, positive line/unit counts and nonnegative sales using six
`ON MISMATCH FAIL` constraints. Do not silently drop mismatching rows.

Every group must reconcile to the facts: seven groups, nine lines, 16 units
and 516.73 net sales. Inspect `SHOW CREATE MATERIALIZED LAKE VIEW`, Fabric
lineage and any available refresh evidence. Claim incremental MLV refresh
only when Fabric reports that strategy; a create/full refresh is not proof.

## 8. Acceptance and replay

| Checkpoint | Required evidence |
|---|---|
| Pre-work | Six exact inputs, empty schema-enabled target, four bound shells, serial pipeline |
| Bronze | Fixed `bronze` PASS; source values/types/audit intact; ingestion repeatable; state CDF enabled |
| Silver | Fixed `silver` PASS; typed values exact; six initial history versions; bootstrap watermark |
| Gold before events | Fixed `gold` PASS; exact star, dates, as-of keys and facts; both bootstrap watermarks |
| Customer events | Fixed `cdf` PASS; exact changed state/history; bounded reads and write-before-watermark ordering |
| No-new-input replay | `bootcamp_snapshot(spark)` identical before/after both CDF functions |
| MLV | Fixed `mlv` PASS; actual MLV DDL, six constraints, all groups and totals, inspected lineage |

Run fixed checks in each notebook context. Bronze/Silver final checks use
`after_changes=bootcamp_changes_started(spark)` on completed-build replay.
The initial Gold check runs only before events; the final CDF/replay checks
always run. Resume interrupted Gold/CDF processing before a full pipeline
replay. A partial event state is not a valid initial or completed checkpoint.

A completed-build pipeline replay must preserve `bootcamp_snapshot` results:
customer state/history, surrogate keys, facts and watermark values.
Idempotence means unchanged results; static dimension rewrites need not be
globally write-free. Do not manufacture commits on CDF sources.

## 9. Optional labs

[Project Osmos](../labs/bootcamp/05-osmos-replay.md) builds the same contracts
in a fresh approved target. Use independent notebooks and the same fixed
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
