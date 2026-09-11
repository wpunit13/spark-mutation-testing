# Integration Notes: spark-mutator WP-07 — End-to-End Survival/Kill Verification

## 1. Fixture Data

The fixture datasets are defined in `examples/pyspark-pipeline/tests/conftest.py`.

### `customers_df` Schema: `(customer_id: String, customer_name: String)`

| customer_id | customer_name | Description |
|---|---|---|
| `C1` | `Alice` | Matched customer |
| `C2` | `Bob` | Matched customer |

### `orders_df` Schema: `(order_id: Int, customer_id: String, amount: Int, status: String)`

Filter Predicate $P = (A \land B)$, where $A = (\text{amount} > 100)$ and $B = (\text{status} == \text{"COMPLETED"})$.

| order_id | customer_id | amount | status | Conjunct A ($>100$) | Conjunct B ($==\text{"COMPLETED"}$) | Join Match | Fixture Purpose |
|---|---|---|---|---|---|---|---|
| `1` | `C1` | 150 | `COMPLETED` | True | True | Yes (`C1`) | Baseline match, satisfies $A \land B$ |
| `2` | `C2` | 150 | `COMPLETED` | True | True | Yes (`C2`) | Baseline match, satisfies $A \land B$ |
| `3` | `C1` | 150 | `PENDING` | True | False | Yes (`C1`) | Violates only 2nd conjunct ($B$) |
| `4` | `C2` | 50 | `COMPLETED` | False | True | Yes (`C2`) | Violates only 1st conjunct ($A$) |
| `5` | `C99` | 150 | `COMPLETED` | True | True | No (unmatched) | Unmatched customer, satisfies $A \land B$ |
| `6` | `C98` | 150 | `COMPLETED` | True | True | No (unmatched) | Unmatched customer, satisfies $A \land B$ |

---

## 2. Pre-Run Predictions vs. Actual Observed Statuses

### Baseline Expected Output
- **Baseline Row Count**: `2`
- **Baseline Row Set**: `{(1, "C1", 150, "COMPLETED"), (2, "C2", 150, "COMPLETED")}`

### Prediction vs. Actual Table

| Mutant ID | Operator | mutationIndex | Description | Predicted Output Set | Predicted Count | Predicted Weak (`len == 2`) | Predicted Hardened (`set == {O1, O2}`) | Actual Weak Status | Actual Hardened Status | Match? |
|---|---|---|---|---|---|---|---|---|---|---|
| - | **Baseline** | - | Unmutated | `{O1, O2}` | 2 | **PASS** | **PASS** | **PASS** | **PASS** | Yes |
| `f284a3ec73285945` | `JOIN` | 0 | `INNER -> LEFT` | `{O1, O2, O5, O6}` | 4 | **KILLED** ($4 \ne 2$) | **KILLED** | **KILLED** | **KILLED** | Yes |
| `b737c6518f617706` | `JOIN` | 1 | `INNER -> CROSS` | `{O1, O2, O5, O6}` (×2 dup) | 8 | **KILLED** ($8 != 2$) | **KILLED** | **KILLED** | **KILLED** | Yes (post-fix) |
| `4c70456edb9512b3` | `JOIN` | 2 | `INNER -> ANTI` | `{O5, O6}` | 2 | **SURVIVED** ($2 == 2$) | **KILLED** (`{O5, O6} != {O1, O2}`) | **SURVIVED** | **KILLED** | **Yes (Proved)** |
| `9ba095ef3e59e507` | `FILTER` | 0 | `FILTER -> keep left conjunct` | `{O1, O2, O3}` | 3 | **KILLED** ($3 \ne 2$) | **KILLED** | **KILLED** | **KILLED** | Yes |
| `4dcb02ddbf0065f8` | `FILTER` | 1 | `FILTER -> keep right conjunct` | `{O1, O2, O4}` | 3 | **KILLED** ($3 \ne 2$) | **KILLED** | **KILLED** | **KILLED** | Yes |
| `bd9154351558d207` | `FILTER` | 2 | `FILTER -> FALSE` | `{}` | 0 | **KILLED** ($0 \ne 2$) | **KILLED** | **KILLED** | **KILLED** | Yes |
| `d6ae22f6b04c5610` | `FILTER` | 3 | `FILTER -> NOT(predicate)` | `{O3, O4}` | 2 | **SURVIVED** ($2 == 2$) | **KILLED** (`{O3, O4} != {O1, O2}`) | **SURVIVED** | **KILLED** | **Yes (Proved)** |

> **Note (post-fix):** the `INNER -> CROSS` mutation has since been corrected to drop the join predicate (see §3.3), so it now yields a Cartesian product and is **KILLED** in both suites (was previously SURVIVED as an "equivalent mutant"). Re-run `scripts/verify_e2e.py` on the corrected mutator confirms this.

---

## 3. Explanation of Prediction/Actual Analysis & Mechanics

1. **`JOIN 2` (`INNER -> ANTI`, Mutant `4c70456edb9512b3`)**:
   `LEFT ANTI` join outputs rows from `orders_df` that have no matching `customer_id` in `customers_df`. In our fixture, exactly two orders (`O5` and `O6`) have unmatched keys (`C99`, `C98`), and both satisfy the filter condition $(A \land B)$. Thus, the mutated output contains exactly 2 rows (`{O5, O6}`). The weak suite asserting `len(collect()) == 2` passes, allowing the mutant to **SURVIVE**. The hardened suite asserting `set(collect()) == {O1, O2}` detects that the row contents differ completely and **KILLS** the mutant.

2. **`FILTER 3` (`NOT(predicate)`, Mutant `d6ae22f6b04c5610`)**:
   Under predicate inversion, rows that previously satisfied $(A \land B)$ are rejected, and matched rows that violated $(A \land B)$ are admitted. In our fixture, exactly two matched rows (`O3` and `O4`) violate $(A \land B)$ (one violates $A$, one violates $B$). Thus, the inverted filter outputs exactly 2 rows (`{O3, O4}`). The weak suite row count assertion ($2 == 2$) passes, so the mutant **SURVIVES**. The hardened suite set comparison detects `{O3, O4} != {O1, O2}` and **KILLS** the mutant.

3. **`JOIN 1` (`INNER -> CROSS`, Mutant `b737c6518f617706`)**:
   `ShimImpl.mutateJoin` drops the join predicate for the CROSS mutation (`j.copy(joinType = Cross, condition = None)`), so `INNER -> CROSS` now yields a true Cartesian product (8 rows after the filter, deduplicated to `{O1, O2, O5, O6}`) instead of being silently equivalent to the `INNER` join. It is therefore **KILLED** in both suites. This was a follow-up fix applied after the initial WP-07 run.

4. **Logical Plan Coordinate Stability**:
   In `test_orders_weak.py`, using `len(build_report(...).collect()) == 2` ensures that the Catalyst LogicalPlan executed in both suites is identical in node structure and depth. When `df.count()` was used instead, Catalyst injected an extra `Aggregate` node at the root of the plan, shifting the tree depth by 1 and altering the derived `NodeCoordinate`. Using `len(collect()) == 2` preserves pure row-count-only semantics while ensuring coordinate and `mutantId` stability across independent test runs.

---

## 4. Scoping Mechanism

We chose the first mechanism allowed by TASK SPECIFICATION step 4:
- **Two variant config files**: `examples/pyspark-pipeline/weak.toml` (with `output_dir = "target/weak-reports"`) and `examples/pyspark-pipeline/hardened.toml` (with `output_dir = "target/hardened-reports"`).
- **Test path restriction**: `pytest tests/test_orders_weak.py` and `pytest tests/test_orders_hardened.py`.
- **Rationale**: Eliminates code duplication across example pipelines, maintains a single cohesive pipeline under test, and cleanly separates report outputs into distinct directories.

---

## 5. HARD RULE 10 Invocation

**Invoked**: Yes.

- **File modified**: `mutator-core/src/main/java/io/github/wpunit13/mutator/MutantRegistry.java`
- **Change**: In `clearActiveMutant(String mutantId)`, replaced the direct `state.compareAndSet(mutantId, null)` reference equality check with a loop comparing `state.get().equals(mutantId)` before `state.compareAndSet(current, null)`.
- **Reason**: Py4J creates a newly allocated `java.lang.String` instance on each bridged JVM method call. `AtomicReference.compareAndSet(expected, update)` performs reference identity comparison (`expected == current`). When `clearActiveMutant` was called with an equal string value allocated as a distinct heap object by Py4J, `compareAndSet` failed with `IllegalStateException: Expected to clear mutant id '...' but registry is ACTIVE for '...'`, leaving the registry permanently locked in `ACTIVE` state after the first mutant and causing all subsequent mutants to error. Comparing by value (`.equals`) before atomically updating preserves the exact method contract, exception types, and exception messages while allowing cross-language string bridging via Py4J.
