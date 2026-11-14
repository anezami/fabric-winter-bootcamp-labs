# Lab 2: Silver from the Bronze change feed

**7 minutes · session minutes 28–35.** Prerequisite: Lab 1 passes.

| Goal | Start | Open | Finish signal |
|---|---|---|---|
| Give each customer version a stable key and preserve history | Bronze initial-load PASS | `nb_bootcamp_silver` | Silver PASS, six history versions and five current watermarks |

**Watch, work, check:** watch the key choice and one SCD2 close/open operation.
Choose your strategy, approve your cells and run only Silver. Wait for your
own PASS before Gold; do not copy another participant's keys or results.

Open `nb_bootcamp_silver`. It has its own description, validator, fixed
baseline cell and fixed check cell; session variables and temporary views do
not carry over from Bronze, so the generated cells declare what they need.

## 1. Choose the key, generate and run

Paste:

```text
Use the spark-cli skill.
Goal: build Silver in lh_bootcamp. First ask me to choose the customer_sk
strategy (decision 4).
Read Bronze through table_changes().
Generate NEW native Spark SQL as %%sql cells in nb_bootcamp_silver from spec
sections 3 and 5.
Create only silver tables and their lab_ops watermarks.
Validate with the fixed check and report its row counts.
Preserve Bronze.
Show the cells, the SCD2 validity rule and the exact write set. Stop for my
code approval.
```

**Decision 4 · choose the customer key.** Both options satisfy the contract
(a unique, non-null BIGINT); Gold copies the key unchanged. Reply `A` or `B`:

- **A:** `xxhash64(customer_id, valid_from)`: deterministic, so a rebuild
  reproduces the same keys.
- **B:** the current maximum `customer_sk` plus `row_number()`: a compact
  sequence whose values depend on processing order.

**Decision 5 · approve the Silver code.** Check:

- Each generated cell is `%%sql` with a `-- Stage: silver - …` header.
- It reads only Bronze through `table_changes()` from each watermark + 1,
  writes only `silver` and its five `lab_ops.cdf_watermarks` rows, and
  advances each watermark only after its target write.
- Every MERGE source is inline in `USING (...)`.
- Every MERGE uses the table's stable key from the write-pattern table in
  spec section 3, so a rerun with no new changes commits nothing, and there is
  no `mergeSchema` or schema auto-merge.
- SCD2: a MERGE closes the current version, then an INSERT opens the new one;
  `valid_from`/`valid_to` come from `effective_at`; an open version ends at
  `9999-12-31`; `customer_sk` follows your decision 4.
- No Gold or MLV code in any cell, including the fixed baseline and check
  cells.

Reply `change: <what>` or `approve`: as in Lab 1, approval saves the cells and
runs this notebook once, or gives you the cells to run manually.

## 2. Verify and explain

The shell's fixed baseline and check cells wrap your generated cells:

```python
bootcamp_begin(spark, "silver")       # fixed baseline cell, before the generated cells
# ... your NEW %%sql cells run here ...
validate_bootcamp(spark, "silver")    # fixed check cell, after the generated cells
```

If time remains, an optional second **Run all** in the warm session lets the
baseline find Silver current, so the check proves that your cells committed
nothing.

**Decision 6 · accept the result.** Compare the fixed output with the
definition of done; inspect the outcome, not just the code.

**Stretch inspection:** use this query only if the job has ended and time
remains in an existing session. Do not start Spark just for this query:

```sql
SELECT customer_id, customer_sk, city, loyalty_tier,
       valid_from, valid_to, is_current, source_commit_version
FROM silver.customer_history
ORDER BY customer_id, valid_from;
```

Explain why an attribute change closes a version instead of overwriting it,
and why facts will join on a surrogate key valid at the order timestamp.
Compare with the definition of done and accept by pasting the
[Lab 3](03-gold-cdf.md) prompt.

## Definition of done

- Actual `PASS: bootcamp silver contract, initial load (fixed read-only checks)`
  and `PASS: bootcamp silver was built or brought to contract by this run`.
  An optional second **Run all** in the warm session prints
  `PASS: bootcamp silver was already current and the rerun changed nothing`.
- Typed products, stores, orders and order lines match every source value.
- Six customer versions have unique BIGINT keys from your chosen strategy,
  exact validity bounds, current flags and Bronze source commit versions.
- CDF is enabled on all five Silver tables; the five Bronze-to-Silver
  watermarks equal the Bronze versions.
- The `bootcamp silver rows validated: ...` line lists the row count of every
  table the check read.
- Code is documented native Spark SQL. Save output and tick your tasks.

## If blocked

On failure, reply `repair: <actual error>`, approve the patch to the generated
code and rerun the notebook. Never recreate history, reset watermarks or
change expected results; see [repair and recovery](recovery.md).

**Next:** [Lab 3 - Gold from the Silver change feed](03-gold-cdf.md).
If Silver has not passed, repair it or watch without running Gold.
