# Wikidata V1 → V2 Coverage Gap — Analysis

**Database:** `vaugouindb` (MariaDB 11.2.3) · **Status:** under investigation · **First observed:** 2026-06-04

This document analyzes a coverage gap discovered between the two generations of Wikidata data held
in the same database, using the property **P2079 ("fabrication method")** as the probe. It explains
the two data models, the measured gap, the diagnostic method, the candidate root causes, and the
remediation options.

---

## 1. Two generations of Wikidata data

The database carries **two parallel representations** of Wikidata, built by different pipelines:

### V1 — legacy crawler (per-entity, live API)

- Property → item links stored generically in **`T_WC_WIKIDATA_ITEM_PROPERTY`**
  (`ID_WIKIDATA`, `ID_PROPERTY`, `ID_ITEM`, `DELETED`, …).
- Subjects typed via the legacy entity tables: `T_WC_WIKIDATA_MOVIE_V1`,
  `T_WC_WIKIDATA_SERIE_V1`, `T_WC_WIKIDATA_PERSON_V1`, etc.
- Labels in `T_WC_WIKIDATA_ITEM_V1`.
- Collation: **`utf8mb4_general_ci`**.
- Populated incrementally over a long period — it accumulates everything the crawler ever touched.

### V2 — dump-based ETL (normalized statement model)

- Statements in **`T_WC_WIKIDATA_STATEMENT`**, with the value split into typed child tables
  (`T_WC_WIKIDATA_ITEM_VALUE`, `_STRING_VALUE`, `_TIME_VALUE`, …) plus qualifiers and media tables.
- Subjects typed via `T_WC_WIKIDATA_MOVIE`, `T_WC_WIKIDATA_SERIE`, `T_WC_WIKIDATA_PERSON`,
  `T_WC_WIKIDATA_ITEM`.
- Collation: **`utf8mb4_unicode_ci`**.
- Populated by bulk-loading a single Wikidata **dump** (one `IMPORT_BATCH_ID`).

> Because V1 is `general_ci` and V2 is `unicode_ci`, every V1↔V2 join on a string key (e.g.
> `ID_WIKIDATA`) requires `COLLATE utf8mb4_unicode_ci` to avoid error `#1267`. See
> `doc/collation-standardization.md`.

---

## 2. The measured gap (probe: P2079)

P2079 is nominally "method/process/technique used to manufacture the item"; in this film/TV catalog
it functions as an **animation/production-technique** tag (values dominated by *traditional
animation*, *computer animation*, *stop-motion*, …).

| Metric | **V1** (`ITEM_PROPERTY`) | **V2** (`STATEMENT`/`ITEM_VALUE`) |
|---|---:|---:|
| Distinct subjects with P2079 | **6 287** (movies/series/persons) | **824** |
| Distinct values used | 48 | 52 |
| Statements / rows | — | 898 (no duplicates; `nb_items == nb_statements`) |

**V2 holds ~13 % of V1's P2079 subjects.** Note the value sets are *not* nested: V2 actually has a
few values V1 lacks (film-stock/process values such as *Technicolor*, *Cineon*, *VistaVision*,
*Schüfftan process*, *deepfake*), so there is churn in both directions — but the dominant story is
that **V2 is missing the bulk of the subject volume**.

### What was already ruled out (from earlier diagnostics on V2)

- **Not duplication** — `nb_items == nb_statements` for every value; the duplicate detector returned nothing.
- **Not hidden soft-deletes / validation drops** — all 898 V2 rows are `DELETED=0` (only 1 `deprecated` rank).
- **Not an unsupported property** — `IS_SUPPORTED=1` in `T_WC_WIKIDATA_PROPERTY_METADATA`.
- **Not a missing long tail** — V2 has 52 distinct values, *more* than V1's 48.
- **Not load recency** — V2 freshness is governed by the **dump file's date**, not by `DAT_CREAT`
  (the row-insert timestamp). `LAST_SYNC_AT` is `NULL`: no incremental sync has ever run.

So the gap is specifically a **deficit in subject/statement volume**, not categories, duplicates,
or filtering.

---

## 3. Diagnostic method — localizing the loss

The decisive question: for the V1 P2079 subjects that are absent from V2, **were the entities never
migrated to V2, or were the entities migrated but their P2079 statements not ingested?** These imply
different fixes.

The probe (query **G3** in the companion command) takes every distinct V1 P2079 subject and, per
subject, computes two booleans:

- `present_as_v2_entity` — does the QID exist in *any* V2 entity table (`MOVIE`/`SERIE`/`PERSON`/`ITEM`)?
- `has_p2079_in_v2` — does the QID have a P2079 statement in `T_WC_WIKIDATA_STATEMENT`?

Interpretation:

| `present_as_v2_entity` | `has_p2079_in_v2` | Diagnosis | Fix surface |
|---|---|---|---|
| ≈ 6 287 | ≈ 824 | Entities migrated, **statements not ingested** | V2 **statement ingestion** (property handling / dump parse / validation) |
| ≈ 824 | ≈ 824 | **Entities never reached V2** | V2 **entity scope / crawl coverage** |
| in between | ≈ 824 | Mixed — partial entity migration *and* statement loss | both |

Supporting queries:

- **G1** — raw V1 totals from `ITEM_PROPERTY` with a `DELETED` split (does V1 itself hide deleted rows?).
- **G2** — V1 subject breakdown by entity type; reproduces the "6 287" and surfaces any `nb_untyped`
  subjects (P2079 subjects not present in the three `*_V1` type tables — meaning V1 may hold *more*
  than 6 287).
- **G5** — a sample of V1 P2079 subjects entirely absent from V2, to eyeball what kind of entities
  are being lost.

---

## 4. Findings (2026-06-04 run)

The diagnostic was run. It **disproved the initial hypothesis** (which predicted entities largely
present in V2 with only statements missing). The reality is that the V1 and V2 P2079 populations
**barely overlap**, and the dominant problem is missing *entities*, not missing *statements*.

**G1 — V1 `ITEM_PROPERTY` P2079:** 6 287 rows, **5 769 distinct subjects**, **126 distinct values**
(the earlier "48" was a truncated view). All `DELETED=0`.

**G2 — V1 subjects by type:** 4 913 movies, 720 series, 24 persons, **119 untyped** (overlaps make
the sum slightly exceed 5 769). Overwhelmingly movies.

**G3 — the verdict:**

| Measure | Count | % of 5 769 |
|---|---:|---:|
| V1 P2079 subjects | 5 769 | 100 % |
| …present as a V2 entity at all (`MOVIE`/`SERIE`/`PERSON`/`ITEM`) | **1 265** | **22 %** |
| …with the P2079 statement in V2 | **701** | **12 %** |

Therefore **both gaps are real**:

- **Entity-coverage gap (dominant): 4 504 subjects (78 %) do not exist in V2 at all** — never migrated.
- **Statement-ingestion gap (secondary): of the 1 265 present in V2, 564 (45 %) lack the P2079 statement.**

**Venn reconciliation** with the earlier V2 total of 824 P2079 subjects: the V1 and V2 P2079 sets
overlap by only **701**. V2 holds `824 − 701 = 123` P2079 subjects V1 lacks (newer dump data); V1
holds `5 769 − 701 = 5 068` that V2 lacks. **The two are substantially different populations, not a
superset/subset.**

The G5 sample is dominated by **low-QID (i.e. notable) animated films** (Q29011, Q43051, Q36479,
Q134430…) absent from a V2 `MOVIE` table of ~1.18 M rows — so this is **not** a "V2 kept only obscure
entities" effect. V2's **entity scope for the film/animation domain is genuinely narrower/different**
than V1's.

### Candidate causes

**For the dominant entity-coverage gap (78 % absent):**

1. **V2 dump-ETL entity-selection filter.** The bulk-load likely only materializes entities matching
   a scope — e.g. an `instance of` (P31) class whitelist, a "must have an external ID (IMDB/TMDB)"
   rule, or an inclusion list — that excludes many animated films/shorts present in V1. *This is the
   primary thing to confirm, from the ETL code/config, not SQL.*
2. **QID divergence / merges.** Some V1 QIDs may be redirects/merged targets that don't match V2's
   canonical QIDs.

**For the secondary statement-ingestion gap (45 % of present entities):**

3. **Dump vs live divergence.** V1 = accumulated live-crawl history; V2 = one dump snapshot. P2079
   statements V1 gathered may simply not be in the dump.
4. **Parse/validation drop.** Statements in the dump but skipped during ETL (value-type mismatch,
   unresolved item value, validation rule). Check `IS_VALID` / `VALIDATION_ERROR` and ETL logs.

---

## 5. Remediation options (decide after G3)

- **If statement-ingestion gap:**
  - Re-run the V2 statement ETL for the affected property/properties (and ideally all properties),
    sourced from a current dump. The bulk-load step is **resumable/idempotent**
    (`--start-step 110`), so re-ingestion is low-risk.
  - Verify the ETL's property whitelist / `IS_SUPPORTED` handling includes P2079 (and audit other
    properties for the same gap — P2079 is just the probe).
  - Consider a **V1→V2 backfill**: migrate `T_WC_WIKIDATA_ITEM_PROPERTY` rows directly into
    `T_WC_WIKIDATA_STATEMENT` + `T_WC_WIKIDATA_ITEM_VALUE` for subjects already present as V2 entities.
- **If entity-coverage gap:** widen the V2 crawl/entity scope, then re-ingest statements.

In all cases, **re-test with more than one property** before declaring the ETL healthy — the P2079
deficit is almost certainly representative of a systemic ingestion shortfall, not a P2079-specific quirk.

---

## 6. Caveats / data-hygiene notes

- **`DAT_CREAT` ≠ dump date.** It is the row-insert time of the bulk-load (all P2079 rows share
  `2026-05-19`). True V2 freshness = the dump file's generation date. `LAST_SYNC_AT` is `NULL`.
- **Collation.** All V1↔V2 comparisons must `COLLATE utf8mb4_unicode_ci` until the database is
  standardized (`doc/collation-standardization.md`).
- **Counting unit.** Compare `COUNT(DISTINCT ID_WIKIDATA)` (subjects/items), not raw row counts —
  V2 statements and V1 rows are both ~1 per subject here, but don't assume that for other properties.
- **One deprecated statement** exists in V2 (`RANK='deprecated'`); exclude it from analytical counts
  where appropriate.

---

## 7. Companion query

The runnable diagnostic (G1/G2/G3 + G5 sample) is kept alongside this analysis; see the
project chat / ops notes for the `docker exec … mariadb` one-shot command. G3 is the block that
classifies the gap; G1/G2 characterize V1; G5 samples the lost subjects.

---

## 8. Root cause and fix (applied 2026-06-04)

### Root cause — subclass descendants are empty during pass1 detection

The dump ETL (`wikidata_dump_etl.py`) classifies an entity by matching its **direct P31** against
each root class **plus that root's P279 subclass descendants** (`classify_entity`). The pipeline runs
in three streaming passes over the dump (`wikidata_crawler.py`):

1. **pass1** runs with `class_roots_json=None`, so `movie_descendants` / `series_descendants` /
   `person_descendants` are **empty for the whole scan**. `classify_entity` is nonetheless called
   per entity during the scan → it matches **roots only** (`MOVIE_ROOTS = {Q11424, Q506240}`).
2. The descendant sets are computed **only after** the scan completes and written to
   `class_roots.jsonl`. But `core_entity_ids.txt` was already finalized in step 1 with roots-only logic.
3. **pass2** loads the complete descendants and *can* classify correctly — but it only processed
   entities already listed in pass1's roots-only `core_entity_ids.txt` (the
   `if entity_id in self.in_scope_entity_ids` gate).

**Consequence:** only entities whose **direct P31 is literally `film` (Q11424) or `television film`
(Q506240)** became core entities. Works typed with a *subclass* — `animated film` (Q202866),
`animated short film` (Q23739410), `short film` (Q24862), `feature film` (Q24869), anime film types,
etc. — were dropped, despite `class_roots.jsonl` correctly knowing they are films. A single streaming
pass fundamentally cannot classify on the fly, because an instance can appear in the dump *before* the
class entities that define its subclass chain.

This single defect produces **both** observed symptoms:
- the 78 % of P2079 subjects **absent from V2** (subclass-typed films never made the core set), and
- the 45 % of present-but-no-statement cases (those reached V2 only as referenced `ITEM` rows via the
  item_cache pass, which never emits claims).

### Fix — pass1 records a P31 sidecar, then classifies it with the complete graph

Detection is done in **pass1**, but *after* the scan, so it sees the complete subclass graph — and it
adds **no extra dump scan**:

- `wikidata_dump_etl.py` `process_item` (pass1): during the scan, write each item's
  `(id, P31-qids, has_imdb)` to a sidecar (`entity_class_input.tsv`) and build the P279 graph. No
  classification yet (descendants aren't complete mid-scan).
- `wikidata_dump_etl.py` `_classify_sidecar_and_write_core` (pass1, post-scan): with the graph now
  complete, stream the sidecar, classify each record (movie/series/person/season/episode/character,
  applying the persons-need-IMDb rule), and write the authoritative `core_entity_ids.txt` +
  `candidate_person_ids.txt`.
- `wikidata_dump_etl.py` `process_item` (pass2): revert to the cheap id-gate
  (`if entity_id not in self.in_scope_entity_ids`) against pass1's now-correct core set, classify, and
  emit. Combined with the run()-level fast-skip, pass2 full-parses **only the ~1M in-scope entities**
  instead of all 120M.
- `wikidata_crawler.py`: `step_run_item_cache` consumes pass1's `core_entity_ids.txt` again;
  `step_validate_pass2` no longer requires a pass2 core file.

A single streaming pass still cannot classify on the fly (an instance can appear before the classes
that define its subclass chain) — the sidecar defers classification to the moment the graph is
complete, which is the crux of the fix.

A standalone smoke test, **`tests/test_etl_smoke.py`**, builds a tiny synthetic dump in which the
subclass-typed movie and a descendant-typed character appear **before** their class definitions, runs
all three passes with no DB/network, and asserts the subclass-typed entities land in the core set and
are emitted. Run it with `python tests/test_etl_smoke.py` (exit 0 = pass).

### Confirming the root cause from V1 data

V1's `T_WC_WIKIDATA_ITEM_PROPERTY` retains every subject's P31. The missing P2079 subjects should be
dominated by *subclass* P31 classes, not the bare roots:

```sql
SELECT p31.ID_ITEM AS p31_class, it.LABEL_EN AS class_label,
       CASE WHEN p31.ID_ITEM IN ('Q11424','Q506240') THEN 'bare root (captured)'
            ELSE 'SUBCLASS (dropped by old pass1)' END AS status,
       COUNT(DISTINCT p31.ID_WIKIDATA) AS nb_missing
FROM   T_WC_WIKIDATA_ITEM_PROPERTY p79
JOIN   T_WC_WIKIDATA_ITEM_PROPERTY p31
       ON p31.ID_WIKIDATA = p79.ID_WIKIDATA AND p31.ID_PROPERTY = 'P31'
LEFT JOIN T_WC_WIKIDATA_ITEM it ON it.ID_WIKIDATA = p31.ID_ITEM COLLATE utf8mb4_unicode_ci
WHERE  p79.ID_PROPERTY = 'P2079'
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                   WHERE st.ID_WIKIDATA = p79.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                     AND st.ID_PROPERTY = 'P2079')
GROUP BY p31.ID_ITEM, it.LABEL_EN
ORDER BY nb_missing DESC
LIMIT 25;
```

### Re-running to apply

The fix changes pass2 and item_cache only; pass1 outputs from a prior run remain valid.

- If `/shared/pass1/*` and the dump source are still present: re-run from **step 104** (pass2) →
  item_cache → staging → bulk load. Saves one full dump scan.
- Otherwise: full re-run from **step 102** (pass1).

Expect a substantial increase in `T_WC_WIKIDATA_MOVIE`/`SERIE` row counts and in statement volume —
that is the corrected coverage. Re-validate with the G3 diagnostic: `present_as_v2_entity` and
`has_p2079_in_v2` should both jump toward the V1 figure of 5 769.

---

## 9. Entity-type coverage expansion (2026-06-04)

V1 had entity tables V2 lacked. Comparing `*_V1` tables to V2:

| V1 table | V2 equivalent | Status before | Action |
|---|---|---|---|
| `MOVIE_V1` | `T_WC_WIKIDATA_MOVIE` | present | — |
| `SERIE_V1` | `T_WC_WIKIDATA_SERIE` | present | — |
| `PERSON_V1` | `T_WC_WIKIDATA_PERSON` | present | — |
| `ITEM_V1` | `T_WC_WIKIDATA_ITEM` | present (referenced-item cache) | — |
| `SEASON_V1` | `T_WC_WIKIDATA_SEASON` | **missing** | **added** |
| `EPISODE_V1` | `T_WC_WIKIDATA_EPISODE` | **missing** | **added** |
| `CHARACTER_V1` | `T_WC_WIKIDATA_CHARACTER` | **missing** | **added** |

**Season, episode and character** are now first-class V2 entity types, wired through every layer
exactly like movie/serie/person:

- **`wikidata_dump_etl.py`** — new roots `SEASON_ROOTS={Q3464665}` (television series season),
  `EPISODE_ROOTS={Q21191270}` (television series episode), `CHARACTER_ROOTS={Q95074}` (fictional
  character); their P279 descendants are computed in pass1 and persisted to `class_roots.jsonl`;
  `classify_entity` returns the new classes; pass2 emits them via the `CLASS_TO_TABLE` map and emits
  their claims as statements.
- **`01_create_schema.sql`** — `T_WC_WIKIDATA_SEASON` / `_EPISODE` / `_CHARACTER` (same shape as MOVIE).
- **`02_staging_and_triggers.sql`** — matching `STG_*` staging tables.
- **`03_bulk_load_from_staging_FULL.sql`** — INSERT…ON DUPLICATE KEY UPDATE + `ROW_STATUS='LOADED'` blocks.
- **`load_staging_jsonl.py`** — `TableSpec`s for the three new pass2 JSONL files.
- **`wikidata_crawler.py`** — staging/target validation lists include the three new tables.

> **Root QIDs to verify.** The three new root classes are sensible defaults but worth a sanity check
> against your domain, since they drive coverage: `Q3464665` (television series season), `Q21191270`
> (television series episode), `Q95074` (fictional character). Subtypes (anime seasons/episodes,
> fictional humans/superheroes, etc.) are picked up automatically via the P279 descendant graph. Add
> more roots to the corresponding `*_ROOTS` set if you want broader capture (e.g. a generic
> `episode`/`season` class).

> **Scope note — characters.** This captures **all** fictional characters in the dump (book, comic,
> game, and screen), mirroring how the crawler captures all films/series. If you want to restrict to
> screen-relevant characters later, gate `character` emission the way persons are gated by IMDb.

### Live-DB DDL (now auto-applied)

`01_create_schema.sql` only runs on a fresh database, so the long-lived `vaugouindb` would otherwise
never get the new tables. This is handled by **`apply_to_live_db.sql`** (3 target + 3 staging tables,
all `CREATE TABLE IF NOT EXISTS`), which `wikidata_crawler.py` runs **automatically at the start of
step 108 (load staging) and step 110 (bulk load)** via `_apply_live_db_schema()` — idempotent, so it
is a no-op once the tables exist, and it covers runs started at either step. It can still be applied
by hand:

```bash
docker exec -i damp-vaugouin-com-mariadb-1 mariadb -uroot -p \
    --default-character-set=utf8mb4 vaugouindb < apply_to_live_db.sql
```

The table definitions (same pattern as the [live-schema-drift] runbook), for reference:

```sql
CREATE TABLE IF NOT EXISTS T_WC_WIKIDATA_SEASON (
    ID_ROW BIGINT NOT NULL AUTO_INCREMENT,
    ID_WIKIDATA VARCHAR(50) NOT NULL,
    LABEL_EN VARCHAR(500) DEFAULT NULL,
    DESCRIPTION_EN TEXT DEFAULT NULL,
    LABELS_JSON LONGTEXT DEFAULT NULL,
    DESCRIPTIONS_JSON LONGTEXT DEFAULT NULL,
    DELETED TINYINT(1) DEFAULT 0,
    DAT_CREAT DATETIME DEFAULT CURRENT_TIMESTAMP,
    TIM_UPDATED DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ID_ROW),
    UNIQUE KEY UK_T_WC_WIKIDATA_SEASON_ID_WIKIDATA (ID_WIKIDATA),
    KEY IDX_T_WC_WIKIDATA_SEASON_LABEL_EN (LABEL_EN(255)),
    KEY IDX_T_WC_WIKIDATA_SEASON_DELETED (DELETED)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS T_WC_WIKIDATA_EPISODE (
    ID_ROW BIGINT NOT NULL AUTO_INCREMENT,
    ID_WIKIDATA VARCHAR(50) NOT NULL,
    LABEL_EN VARCHAR(500) DEFAULT NULL,
    DESCRIPTION_EN TEXT DEFAULT NULL,
    LABELS_JSON LONGTEXT DEFAULT NULL,
    DESCRIPTIONS_JSON LONGTEXT DEFAULT NULL,
    DELETED TINYINT(1) DEFAULT 0,
    DAT_CREAT DATETIME DEFAULT CURRENT_TIMESTAMP,
    TIM_UPDATED DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ID_ROW),
    UNIQUE KEY UK_T_WC_WIKIDATA_EPISODE_ID_WIKIDATA (ID_WIKIDATA),
    KEY IDX_T_WC_WIKIDATA_EPISODE_LABEL_EN (LABEL_EN(255)),
    KEY IDX_T_WC_WIKIDATA_EPISODE_DELETED (DELETED)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS T_WC_WIKIDATA_CHARACTER (
    ID_ROW BIGINT NOT NULL AUTO_INCREMENT,
    ID_WIKIDATA VARCHAR(50) NOT NULL,
    LABEL_EN VARCHAR(500) DEFAULT NULL,
    DESCRIPTION_EN TEXT DEFAULT NULL,
    LABELS_JSON LONGTEXT DEFAULT NULL,
    DESCRIPTIONS_JSON LONGTEXT DEFAULT NULL,
    DELETED TINYINT(1) DEFAULT 0,
    DAT_CREAT DATETIME DEFAULT CURRENT_TIMESTAMP,
    TIM_UPDATED DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ID_ROW),
    UNIQUE KEY UK_T_WC_WIKIDATA_CHARACTER_ID_WIKIDATA (ID_WIKIDATA),
    KEY IDX_T_WC_WIKIDATA_CHARACTER_LABEL_EN (LABEL_EN(255)),
    KEY IDX_T_WC_WIKIDATA_CHARACTER_DELETED (DELETED)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

Because these new types change the entity set, the re-run must start at **pass1** (`--start-step
102`) so the subclass graph and `core_entity_ids` are rebuilt with the new roots — not from pass2.

---

## 10. Performance optimizations (2026-06-04)

The previous full run took ~12 days. Per-pass `run_summary.json` showed the split: **pass2 6.6 days
(53%)**, item_cache 1.9 days, pass1 1.15 days, staging+bulk+media ~2.3 days. pass2 ran at 210 ent/s
vs pass1's 1,208 ent/s because the old code full-parsed all 120M entities every pass while only ~1M
(pass2) / ~15M (item_cache) actually needed it.

Changes:

- **pass2 no longer full-parses 120M entities.** The pass1-sidecar redesign (§8) produces a correct
  core set *by id*, so a cheap top-level-id regex (`fast_entity_id`, anchored, with a safe
  no-match → don't-skip fallback) skips the full JSON parse for every Q-item the pass can't emit.
  pass2 now parses only the ~1M in-scope entities; item_cache only the ~15M referenced ones.
- **Classifier pools precomputed once** (`_build_pools`) instead of rebuilding six `set` unions per
  entity across 120M entities.
- **4 MB NDJSON write buffer** (`NDJSONWriter`) — collapses the write-syscall storm from emitting
  tens of millions of small rows on `/shared`, the likely dominant cost of pass2 emission.
- **Parallel bz2 decompression** (`indexed_bzip2`, `_open_parallel_bz2`) — stdlib `bz2` is
  single-threaded and is the per-pass read floor. Optional dependency: if absent it falls back to the
  stdlib decompressor. `pip install indexed_bzip2`; tune cores with `BZ2_PARALLELISM` (default = all).
- **Staging batch 100 → 1,000** (`load_staging_jsonl.py`) — 10× fewer round-trips in step 108.

Server-side (see `doc/mariadb-server-tuning.md`): raise `innodb_buffer_pool_size` to ~60–70% of RAM
(the highest-value DB change, helps every workload), plus the session-scoped load pragmas.

Expected effect: pass2 drops from ~6.6 days toward ~1.5 days (id-gate + buffering), item_cache and
pass1 also benefit from parallel decompression, and the bulk load benefits from the buffer pool —
targeting roughly **~5–6 days** total instead of ~12. Re-measure `run_summary.json` after the run.
