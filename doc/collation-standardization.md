# Database Collation & Charset Standardization

**Database:** `vaugouindb` · **Server:** MariaDB 11.2.3 (self-hosted) · **Tables:** 397

This document explains the current charset/collation fragmentation in `vaugouindb`, gives a
recommended single target, and lays out a safe, repeatable migration procedure for **all**
tables (the Wikidata `*_V1` tables and everything else).

---

## 1. Why this matters

Joining two string columns that have **different collations** fails hard:

```
#1267 - Illegal mix of collations (utf8mb4_general_ci,IMPLICIT)
        and (utf8mb4_unicode_ci,IMPLICIT) for operation '='
```

We hit this joining `T_WC_WIKIDATA_STATEMENT.ID_WIKIDATA` (unicode_ci) against
`T_WC_WIKIDATA_EPISODE_V1.ID_WIKIDATA` (general_ci). Every cross-family join currently needs a
manual `COLLATE` override to work. Standardizing the whole database on **one** charset + **one**
collation removes the entire class of error permanently.

There are actually **two independent problems**:

1. **Collation split** — `utf8mb4_general_ci` vs `utf8mb4_unicode_ci` (sort/equality rules differ).
2. **Charset split** — 8 legacy tables are still on `utf8mb3` / `latin1`, which physically
   **cannot store the full Unicode range** (no emoji, no astral-plane CJK). This is a data-capacity
   bug, not just a cosmetic one, and must be fixed regardless of the collation decision.

---

## 2. Current state (inventory)

| Collation | Charset | Tables | What they are |
|---|---|---:|---|
| `utf8mb4_general_ci` | utf8mb4 | **345** | Bulk of the schema: TMDB, T2S, IMDB, LBA, forms, users, **and the Wikidata `*_V1` tables** |
| `utf8mb4_unicode_ci` | utf8mb4 | **44** | The new Wikidata ETL pipeline (`STATEMENT`, `MOVIE`, `SERIE`, `PERSON`, `ITEM`, all value/qualifier/media tables) |
| `utf8mb3_unicode_ci` | **utf8mb3** | 5 | `T_WC_SHADE_SEL_HM_*` |
| `utf8mb3_general_ci` | **utf8mb3** | 2 | `T_WC_LBA_OPERATION_TAG`, `T_WC_SHADE_SEL_HM_NAME_BRAND` |
| `latin1_swedish_ci` | **latin1** | 1 | `T_WC_RANDOM_STRING` |

Regenerate this inventory any time:

```sql
SELECT TABLE_COLLATION, COUNT(*) AS nb
FROM   information_schema.TABLES
WHERE  TABLE_SCHEMA = 'vaugouindb'
  AND  TABLE_TYPE   = 'BASE TABLE'
GROUP  BY TABLE_COLLATION
ORDER  BY nb DESC;
```

Column-level check (a table's default collation does **not** guarantee every column matches —
columns can be pinned individually):

```sql
SELECT TABLE_NAME, COLUMN_NAME, COLLATION_NAME
FROM   information_schema.COLUMNS
WHERE  TABLE_SCHEMA = 'vaugouindb'
  AND  COLLATION_NAME IS NOT NULL
  AND  COLLATION_NAME <> 'utf8mb4_unicode_ci'   -- the chosen target
ORDER  BY TABLE_NAME, COLUMN_NAME;
```

---

## 3. Background: the collation options on MariaDB 11.2

| Collation | UCA version | Available since | Portability | Sorting quality |
|---|---|---|---|---|
| `utf8mb4_general_ci` | none (legacy heuristic) | always | universal | English-ok; wrong for many languages; `ß` ≠ `ss` |
| `utf8mb4_unicode_ci` | UCA 4.0.0 (2003) | always | universal (MySQL + any MariaDB) | correct multilingual; handles expansions/contractions |
| `utf8mb4_uca1400_ai_ci` | **UCA 14.0.0 (2021)** | MariaDB **10.10+** | **MariaDB ≥10.10 only** | best; matches future MariaDB default |

Two MariaDB-11 facts confirmed against the official docs:

- The server default collation for `utf8mb4` is **still `utf8mb4_general_ci`** in 11.x. That is the
  root cause of the drift: tables created without an explicit `COLLATE` clause inherited
  `general_ci`, while the ETL tables that *declared* `COLLATE utf8mb4_unicode_ci` did not.
- 11.x adds `@@character_set_collations`, which lets you override the per-charset default
  (e.g. `SET @@character_set_collations='utf8mb4=uca1400_ai_ci';`) so **new** tables inherit the
  modern collation without an explicit clause.

---

## 4. Recommendation

> **Target the whole database at `utf8mb4` / `utf8mb4_unicode_ci`.**

Rationale:

- **Strictly better than `general_ci`** for the multilingual content this database is full of
  (Wikidata labels in every language, TMDB titles, person names with diacritics). Do **not**
  standardize "down" to `general_ci` even though it is technically less migration work — it bakes
  the least-correct collation into the schema permanently.
- **Universally portable.** `unicode_ci` restores into any MySQL or any MariaDB. This protects your
  backup/restore and disaster-recovery path — important given the 11-day Wikidata ETL and the fact
  that the bulk-load step is the expensive thing to lose.
- **Already the collation of the actively-developed, correctness-critical subsystem** (the 44
  Wikidata ETL tables), so the most important tables stay put and act as the reference.

### When to choose `utf8mb4_uca1400_ai_ci` instead

Pick the newer UCA-14 collation **only if** all of these hold:

- You are certain this data will never be dumped into MySQL or a pre-10.10 MariaDB, **and**
- You want the schema to already match the future MariaDB 12+ default (avoid a second migration), **and**
- You actively sort large multilingual sets and want best-in-class ordering.

The migration mechanics below are **identical** — just substitute the collation name and set the
default via `@@character_set_collations` instead of the `ALTER DATABASE` clause.

### What *not* to do

- Don't keep `general_ci` as the global target.
- Don't leave any table on `utf8mb3` or `latin1` — upgrade their **charset** to `utf8mb4`
  regardless of the collation choice.
- Don't mix the new UCA-14 family with the older families; pick exactly one target and apply it everywhere.

> **Identifier columns (optional refinement):** the `ID_WIKIDATA` / Q-id columns are pure ASCII.
> A `*_bin` collation on just those join columns would give exact, faster, case-sensitive matches.
> This is a *later* optimization — not required to fix the `#1267` errors — and adds a third
> collation to manage, so it is out of scope for the standardization pass.

---

## 5. Migration methodology

The workhorse statement converts a table's charset **and** every string column's collation in one
rebuild:

```sql
ALTER TABLE `<table>`
  CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

- For tables already on `utf8mb4`, this only re-sorts/rebuilds indexes (no byte re-encoding).
- For `utf8mb3` / `latin1` tables, it **re-encodes** the stored bytes into UTF-8 — see §5.5 for the
  one caveat (double-encoding / mojibake).

### 5.0 Pre-flight (do all of these first)

1. **Full backup.** `mysqldump --single-transaction --routines --triggers vaugouindb > vaugouindb_pre_collation.sql`
   (or a filesystem/LVM snapshot). Verify it restores into a scratch DB before touching production.
2. **Test on a staging copy.** Run the entire procedure end-to-end on a clone first. Time the big
   tables there so you can size the maintenance window.
3. **Quiesce the ETL.** Pause the Wikidata crawler. The bulk-load step is resumable
   (`--start-step 110`), so a pause is safe — do not run a `CONVERT` against a table the ETL is
   writing to.
4. **Confirm the target collation is available** (relevant only if you choose UCA-14):
   ```sql
   SHOW COLLATION WHERE Charset = 'utf8mb4' AND Collation LIKE '%uca1400_ai_ci%';
   ```

### 5.1 Generate the ALTER statements from `information_schema`

Don't hand-write 350+ statements. Emit them, ordered smallest-first so early failures surface on
cheap tables:

```sql
SELECT CONCAT('ALTER TABLE `', TABLE_NAME,
              '` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;') AS stmt
FROM   information_schema.TABLES
WHERE  TABLE_SCHEMA = 'vaugouindb'
  AND  TABLE_TYPE   = 'BASE TABLE'
  AND  TABLE_COLLATION <> 'utf8mb4_unicode_ci'
ORDER  BY DATA_LENGTH ASC;
```

Copy the output into a script you can review, version, and run in batches.

### 5.2 Foreign keys — the one real gotcha

A foreign key requires **identical collation on both sides for string columns**. (Numeric FKs —
e.g. the many `ID_STATEMENT bigint` references in the Wikidata pipeline — are unaffected.)

The Wikidata pipeline *does* have **string-typed** FKs:

- `T_WC_WIKIDATA_STATEMENT.ID_PROPERTY` → `T_WC_WIKIDATA_PROPERTY_METADATA.ID_PROPERTY` (varchar)
- `T_WC_WIKIDATA_STATEMENT_QUALIFIER.ID_QUALIFIER_PROPERTY` → same parent

List every string FK column so you know which tables must move **together**:

```sql
SELECT k.TABLE_NAME, k.COLUMN_NAME,
       k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME,
       c.COLLATION_NAME
FROM   information_schema.KEY_COLUMN_USAGE k
JOIN   information_schema.COLUMNS c
       ON c.TABLE_SCHEMA = k.TABLE_SCHEMA
      AND c.TABLE_NAME   = k.TABLE_NAME
      AND c.COLUMN_NAME  = k.COLUMN_NAME
WHERE  k.TABLE_SCHEMA = 'vaugouindb'
  AND  k.REFERENCED_TABLE_NAME IS NOT NULL
  AND  c.COLLATION_NAME IS NOT NULL          -- string FK columns only
ORDER  BY k.TABLE_NAME;
```

Because we convert the **entire** database to one collation, every FK ends up matched again. To avoid
transient mismatches *during* the run, disable FK checks for the session:

```sql
SET FOREIGN_KEY_CHECKS = 0;
-- ... run all ALTER ... CONVERT statements ...
SET FOREIGN_KEY_CHECKS = 1;
```

After re-enabling, validate that no FK is left mismatched (the column-level query in §2 returning
zero rows is your proof).

### 5.3 Large tables — locking & online options

`CONVERT TO CHARACTER SET` is a **table rebuild** (`ALGORITHM=COPY`): it allows reads but **blocks
writes** for the duration. Small tables are instant; the heavy ones in this DB need planning:

- `T_WC_WIKIDATA_STATEMENT` and its value child tables
- `T_WC_WIKIDATA_ITEM_PROPERTY` (~11M rows)
- the IMDB import tables (`T_WC_IMDB_*`, tens of millions of rows)

Options for the big ones:

1. **Maintenance window + ETL paused** (simplest). Since the ETL is paused anyway and these tables
   aren't user-facing, a plain blocking `ALTER` is usually acceptable. Convert them last.
2. **Percona `pt-online-schema-change`** if you need zero write-downtime:
   ```bash
   pt-online-schema-change --alter "CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" \
     --execute D=vaugouindb,t=T_WC_WIKIDATA_STATEMENT
   ```
   (It builds a shadow table + triggers and backfills in chunks. Test on staging — it interacts
   with existing triggers and FKs, both of which this schema uses heavily.)

### 5.4 Index key-length pitfall (utf8mb3 → utf8mb4 only)

Going from 3 bytes/char to 4 bytes/char inflates index key bytes. A `VARCHAR(255)` unique index that
was 765 bytes under `utf8mb3` becomes 1020 bytes under `utf8mb4`. InnoDB's limit is 3072 bytes **with
`ROW_FORMAT=DYNAMIC`** (the default in MariaDB 11.2), so the 8 legacy tables here are well within
range. If a `CONVERT` ever errors with *"Specified key was too long"*, confirm the row format:

```sql
SELECT TABLE_NAME, ROW_FORMAT
FROM   information_schema.TABLES
WHERE  TABLE_SCHEMA = 'vaugouindb' AND ROW_FORMAT <> 'Dynamic';
-- fix if needed:  ALTER TABLE `<t>` ROW_FORMAT=DYNAMIC;
```

### 5.5 `latin1` / `utf8mb3` re-encoding caveat (double-encoding)

`CONVERT TO CHARACTER SET` correctly re-encodes bytes **only if the stored bytes really are in the
declared charset.** If UTF-8 bytes were previously shoved into a `latin1` column (classic mojibake),
a naïve convert doubles the corruption. Before converting `T_WC_RANDOM_STRING` (latin1) and the
`utf8mb3` tables, spot-check their content:

```sql
SELECT * FROM T_WC_RANDOM_STRING LIMIT 50;   -- random strings: expected pure ASCII → safe to convert
```

- If the data looks correct as displayed → plain `CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci` is right.
- If it shows mojibake (e.g. `Ã©` for `é`) → use the two-step *binary* fix instead:
  ```sql
  ALTER TABLE `<t>` MODIFY `<col>` BLOB;                       -- detach charset, keep raw bytes
  ALTER TABLE `<t>` MODIFY `<col>` <type> CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
  ```
  For this DB only `T_WC_RANDOM_STRING` and 7 `utf8mb3` tables are affected, and they hold simple
  reference/ASCII-ish data, so the plain convert is expected to be safe — but verify, don't assume.

### 5.6 Generated/virtual columns & fulltext indexes

`CONVERT` can fail on generated columns or need fulltext indexes rebuilt. None of the legacy/`*_V1`
tables use these, but if a future table does: drop the generated column / fulltext index, convert,
then recreate.

---

## 6. Make it permanent (stop the drift recurring)

Converting existing tables fixes today; these steps stop new tables from drifting tomorrow.

**Database default** (affects only tables created *after* this — existing tables still need §5):

```sql
ALTER DATABASE vaugouindb
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

**Server config** (`my.cnf` / `50-server.cnf`), so the instance default matches and survives restarts:

```ini
[mysqld]
character-set-server = utf8mb4
collation-server     = utf8mb4_unicode_ci
```

*(UCA-14 variant: omit `collation-server` and instead set
`character_set_collations = utf8mb4=uca1400_ai_ci`.)*

**Client / connection** — ensure tooling connects with the same collation so session-level
comparisons don't reintroduce a mismatch. In phpMyAdmin set the connection collation to
`utf8mb4_unicode_ci`; for app/CLI connections use `SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;`
(or the driver's charset option).

---

## 7. Suggested execution order

1. Backup + verify restore. Clone to staging.
2. Run the whole procedure on **staging**; time the big tables; fix any surprises.
3. Pause the ETL.
4. `SET FOREIGN_KEY_CHECKS = 0;`
5. Convert the **8 legacy charset tables first** (`utf8mb3` / `latin1` → `utf8mb4`) — highest value
   (data-capacity fix), smallest tables.
6. Convert the remaining `utf8mb4_general_ci` tables, smallest-first (§5.1 ordering). Batch them.
7. Convert / leave the 44 `unicode_ci` tables (already correct — they only appear in the list if a
   *column* was pinned differently).
8. The big tables (`STATEMENT`, `ITEM_PROPERTY`, IMDB imports) last, via window or `pt-osc`.
9. `SET FOREIGN_KEY_CHECKS = 1;`
10. Set the database default + server config + connection collation (§6).
11. **Verify** (below). Resume the ETL.

---

## 8. Verification (must return zero rows)

```sql
-- table-level
SELECT TABLE_NAME, TABLE_COLLATION
FROM   information_schema.TABLES
WHERE  TABLE_SCHEMA = 'vaugouindb'
  AND  TABLE_TYPE   = 'BASE TABLE'
  AND  TABLE_COLLATION <> 'utf8mb4_unicode_ci';

-- column-level (catches individually-pinned columns CONVERT might have missed)
SELECT TABLE_NAME, COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME
FROM   information_schema.COLUMNS
WHERE  TABLE_SCHEMA = 'vaugouindb'
  AND  COLLATION_NAME IS NOT NULL
  AND  COLLATION_NAME <> 'utf8mb4_unicode_ci';

-- charset sanity (no utf8mb3 / latin1 left anywhere)
SELECT TABLE_NAME, COLUMN_NAME, CHARACTER_SET_NAME
FROM   information_schema.COLUMNS
WHERE  TABLE_SCHEMA = 'vaugouindb'
  AND  CHARACTER_SET_NAME IS NOT NULL
  AND  CHARACTER_SET_NAME <> 'utf8mb4';
```

Then re-run the original cross-family join (the one that threw `#1267`) **without** any `COLLATE`
override — it should now succeed:

```sql
SELECT COUNT(*)
FROM   T_WC_WIKIDATA_STATEMENT  s
JOIN   T_WC_WIKIDATA_EPISODE_V1 e ON e.ID_WIKIDATA = s.ID_WIKIDATA
WHERE  s.ID_PROPERTY = 'P2079';
```

---

## 9. Rollback

- **Best:** restore the pre-migration `mysqldump` / snapshot (collation is a schema-wide property;
  partial undo is fiddly).
- **Per-table undo** during a phased run, if you must revert one table before completing:
  ```sql
  ALTER TABLE `<table>` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
  ```
  (Only meaningful for `utf8mb4` tables; for the `utf8mb3`/`latin1` originals, restore from backup —
  re-encoding is not cleanly reversible.)

---

## 10. TL;DR

- **One target everywhere: `utf8mb4` / `utf8mb4_unicode_ci`** (or `utf8mb4_uca1400_ai_ci` if you
  accept MariaDB-10.10+ lock-in for the future default).
- Fix the **charset** of the 8 `utf8mb3`/`latin1` tables too — that's a data-capacity fix, not cosmetic.
- Convert with `ALTER TABLE ... CONVERT TO CHARACTER SET ... COLLATE ...`, generated from
  `information_schema`, FK checks off, smallest tables first, big tables in a window or via `pt-osc`.
- Lock it in with `ALTER DATABASE` + `my.cnf` + connection collation so it never drifts again.
- Verify with the zero-row `information_schema` checks and the previously-failing join.
