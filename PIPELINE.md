# PIPELINE.md

## Purpose

This document summarizes the ETL pipeline required to populate and update the MariaDB Wikidata tables from the weekly Wikidata JSON dump.

The design follows these principles:

- use the weekly JSON dump as the bulk source of truth
- avoid SPARQL for core ingestion
- populate the 2-level statement/value architecture
- keep `T_WC_WIKIDATA_ITEM` as a referenced-item cache
- do not create a foreign key from `T_WC_WIKIDATA_ITEM_VALUE.ID_ITEM` to `T_WC_WIKIDATA_ITEM`
- keep raw claims separate from resolved media resources
- classify movies / series / persons using a `P31/P279*` strategy derived from the dump
- support later incremental refreshes, but this document focuses on the weekly dump workflow

This document contains no SQL and no code. It is an ETL and architecture specification.

---

# Scope of the pipeline

The pipeline populates and updates these table families.

## Entity tables
- `T_WC_WIKIDATA_MOVIE`
- `T_WC_WIKIDATA_SERIE`
- `T_WC_WIKIDATA_PERSON`
- `T_WC_WIKIDATA_ITEM`

## Property metadata cache
- `T_WC_WIKIDATA_PROPERTY_METADATA`

## Statement layer
- `T_WC_WIKIDATA_STATEMENT`

## Typed value tables
- `T_WC_WIKIDATA_ITEM_VALUE`
- `T_WC_WIKIDATA_STRING_VALUE`
- `T_WC_WIKIDATA_EXTERNAL_ID_VALUE`
- `T_WC_WIKIDATA_MEDIA_VALUE`
- `T_WC_WIKIDATA_TIME_VALUE`
- `T_WC_WIKIDATA_QUANTITY_VALUE`

## Media resolution layer
- `T_WC_WIKIDATA_MEDIA_RESOURCE`
- `T_WC_WIKIDATA_MEDIA_RESOURCE_URL`
- `T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK`

---

# High-level pipeline strategy

The weekly Wikidata JSON dump should be treated as the authoritative bulk source.

The pipeline should be split into distinct stages so that:

1. property metadata is refreshed first
2. property-to-property subclass information is extracted
3. movie / series / person entities are identified with a `P31/P279*` classification strategy
4. their claims are extracted into the 2-level architecture
5. referenced items are selectively cached in `T_WC_WIKIDATA_ITEM`
6. media-related claims are queued for downstream resolution
7. resource resolution populates the media resolution tables

This should be implemented as a batch ETL workflow, not as ad hoc SPARQL queries.

---

# Main design decisions

## 1. Use the weekly JSON dump as the authoritative source
The dump is the primary bulk source for:
- item entities
- property entities
- claims
- datatypes
- statement ranks
- raw values

## 2. Keep `T_WC_WIKIDATA_ITEM` selective
`T_WC_WIKIDATA_ITEM` is not a mirror of all non-movie / non-series / non-person items.

It is a referenced-item cache.

Only items referenced through `T_WC_WIKIDATA_ITEM_VALUE` are candidates for insertion into `T_WC_WIKIDATA_ITEM`.

## 3. Do not require local item-cache presence for item-valued claims
A row in `T_WC_WIKIDATA_ITEM_VALUE` may reference a valid Wikidata `ID_ITEM` even if this Q-id is not yet present in `T_WC_WIKIDATA_ITEM`.

Therefore:

- `T_WC_WIKIDATA_ITEM_VALUE.ID_ITEM` remains a Wikidata identifier reference
- `T_WC_WIKIDATA_ITEM` remains a selective cache
- there should be no foreign key from `ITEM_VALUE.ID_ITEM` to `T_WC_WIKIDATA_ITEM`

## 4. Separate raw claims from resolved resources
The statement/value tables store raw Wikidata claims.

The media resolution tables store:
- derived resources
- derived URLs
- validation history

## 5. Process entities in a deterministic order
The ETL should run in this general sequence:

1. property metadata
2. subclass graph extraction
3. movie / series / person entity detection
4. statement extraction
5. typed value extraction
6. referenced-item cache construction
7. media resolution candidate extraction
8. media resolution

---

# Recommended P31/P279* classification strategy

## Why use P31/P279* instead of a flat empirical list
The ETL should not rely only on a manually maintained list of direct `instance of (P31)` values.

A more robust approach is:

- inspect direct `P31` values on the entity
- resolve whether each direct `P31` value belongs to a root class through the `subclass of (P279)` graph
- classify the entity according to root-class membership

This is the dump-based equivalent of SPARQL `P31/P279*`.

This approach gives you two advantages:
- you do not need to keep extending a long flat empirical list
- subclasses such as animated film or anime television series are automatically captured when they sit under the chosen root classes

## Root classes to use

### Movie roots
Use these roots for movie classification:

- `Q11424` = film
- `Q506240` = television film

Operational recommendation:
- treat `Q11424` as the main movie root
- keep `Q506240` explicitly included for safety, because TV movies may not always sit exactly where you expect in the subclass graph or may be modeled inconsistently

### Series roots
Use these roots for series classification:

- `Q5398426` = television series
- `Q1259759` = miniseries
- `Q526877` = web series

Operational recommendation:
- treat `Q5398426` as the main series root
- include `Q1259759` explicitly
- include `Q526877` if web-native series matter to your database

### Person roots
Use this root for persons:

- `Q5` = human

Operational recommendation:
- keep person classification simple: `P31 = Q5` or root membership under `Q5` if needed
- do not broaden this unless you deliberately want fictional humans, groups, or non-human beings

## Important exclusion note about television program
Do not use `Q15416` = television program as a general series root.

Reason:
- it is broader than television series
- it may include non-series television content such as programs, shows, news, reality content, and other broadcast material

If your business scope is truly “series”, `Q15416` is too broad for the core classifier.

## Expected practical effects
With this root-based strategy, the ETL should naturally include common subclasses such as:

### Movie-side examples
- animated film
- silent film
- short film
- anime film
- animated short film

### Series-side examples
- animated television series
- anime television series
- miniseries
- web series

These do not need to be hard-coded as a long direct-`P31` list if they are reachable through the subclass graph of the chosen roots.

---

# Global ETL stages

## Stage 0 – Input preparation

### Objective
Prepare the weekly dump for sequential processing.

### Tasks
- download the latest weekly Wikidata JSON dump
- decompress it into a streamable format
- process the dump line by line
- avoid loading the full dump into memory

### Output
A sequential entity stream suitable for:
- property processing
- item processing
- statement extraction

### Notes
The parser should distinguish between:
- property entities (`P...`)
- item entities (`Q...`)

---

## Stage 1 – Refresh property metadata cache

### Objective
Populate and update `T_WC_WIKIDATA_PROPERTY_METADATA` before statement routing.

### Why this stage comes first
Statement routing depends on the property datatype.
The ETL needs to know whether a property maps to:
- `item`
- `string`
- `external_id`
- `media`
- `time`
- `quantity`

### Input
All property entities from the dump.

### Tasks
For each property entity:
- extract property id
- extract labels
- extract descriptions
- extract official Wikidata datatype
- extract formatter metadata when available
- compute local `VALUE_TYPE`
- compute expected child table
- upsert the property metadata row

### Output
A refreshed `T_WC_WIKIDATA_PROPERTY_METADATA` table used by the later statement-routing stages.

### Important mapping logic
The property metadata cache must contain the local datatype mapping, for example:
- `wikibase-item` → `item`
- `string` → `string`
- `external-id` → `external_id`
- `commonsMedia` → `media`
- `time` → `time`
- `quantity` → `quantity`

### Additional notes
This table should be treated as a local cached copy of official Wikidata property metadata.

---

## Stage 2 – Build the subclass graph needed for classification

### Objective
Create the local classification support structure needed for `P31/P279*` evaluation.

### Why this stage is needed
Movie / series / person classification depends on knowing whether a direct `P31` value belongs to one of the chosen root classes through zero or more `subclass of (P279)` hops.

### Input
All item entities from the dump that have `subclass of (P279)` claims.

### Tasks
- extract item-to-item subclass edges:
  - subject item = subclass candidate
  - property = `P279`
  - object item = parent class
- build a local subclass graph or closure support structure
- ensure the graph is queryable during entity classification
- optionally materialize ancestor membership for the chosen roots only

### Recommended practical implementation
You do not need to compute the full universal transitive closure for every Q-item if that is too heavy.

A practical approach is:
- build direct `P279` edges
- compute closure only for the root families you care about:
  - film roots
  - series roots
  - person root
- maintain lookup sets such as:
  - all classes under film roots
  - all classes under series roots
  - all classes under human root if needed

### Output
A local classification support dataset allowing fast checks such as:
- does direct `P31 = X` belong to movie roots?
- does direct `P31 = X` belong to series roots?
- does direct `P31 = X` belong to person roots?

---

## Stage 3 – Identify core business entities with P31/P279*

### Objective
Identify the item entities that belong to the core business scope:
- movies
- series
- persons

### Input
All item entities (`Q...`) from the dump plus the subclass support structure from Stage 2.

### Classification rule
For each entity:

1. collect its direct `instance of (P31)` values
2. for each direct `P31` value, test root membership through the `P279` subclass graph
3. classify the entity according to root-class membership

### Recommended classification logic

#### Movie classification
Classify as movie if any direct `P31` value:
- is `Q11424` (film)
- is `Q506240` (television film)
- or is a subclass descendant of the movie roots through `P279*`

#### Series classification
Classify as series if any direct `P31` value:
- is `Q5398426` (television series)
- is `Q1259759` (miniseries)
- is `Q526877` (web series)
- or is a subclass descendant of the series roots through `P279*`

#### Person classification
Classify as person if any direct `P31` value:
- is `Q5` (human)
- or belongs to the person root family if you choose to compute root closure for humans as well

### Conflict handling
If an entity matches more than one bucket:
- apply a deterministic precedence rule
- or allow multi-bucket tagging internally and resolve at load time

Recommended precedence for your current use case:
1. person
2. movie
3. series

However, in most cases these scopes should not overlap in normal Wikidata modeling.

### Output
Three candidate streams:
- movie entities
- series entities
- person entities

### Important note
At this stage, do not insert all other item entities into `T_WC_WIKIDATA_ITEM`.

That table is not the residual bucket for everything else.

---

## Stage 4 – Populate core entity tables

### Objective
Populate the main entity tables with business-scope entities only.

### Target tables
- `T_WC_WIKIDATA_MOVIE`
- `T_WC_WIKIDATA_SERIE`
- `T_WC_WIKIDATA_PERSON`

### Tasks
For each classified entity:
- extract core entity identity
- extract labels and local presentation metadata as needed
- extract business-level fields needed by the application
- upsert the row into the appropriate entity table

### Output
The three main entity tables are refreshed from the current weekly dump.

### Notes
These tables remain the business/entity layer.
They do not replace the generic statement/value architecture.

---

## Stage 5 – Extract statements for core entities

### Objective
Populate `T_WC_WIKIDATA_STATEMENT` for all claims belonging to entities in movie / series / person scope.

### Input
The item JSON for all entities already accepted into:
- `T_WC_WIKIDATA_MOVIE`
- `T_WC_WIKIDATA_SERIE`
- `T_WC_WIKIDATA_PERSON`

### Tasks
For each claim on an in-scope entity:
- create one logical statement
- assign the subject entity id
- assign the property id
- determine datatype from `T_WC_WIKIDATA_PROPERTY_METADATA`
- assign local `VALUE_TYPE`
- extract rank
- extract statement GUID if desired
- assign lifecycle/provenance metadata
- create one row in `T_WC_WIKIDATA_STATEMENT`

### Output
A statement-header row for every supported claim on every in-scope movie / series / person entity.

### Important rule
Each statement row must later map to exactly one typed child value row.

---

## Stage 6 – Populate typed value tables

### Objective
Route each statement to exactly one typed value table.

### Input
The claim payload of each in-scope statement plus the property metadata mapping.

### Routing logic
Use `T_WC_WIKIDATA_PROPERTY_METADATA.LOCAL_VALUE_TYPE` to determine which child table receives the value.

### 6A – Populate `T_WC_WIKIDATA_ITEM_VALUE`

#### Use for
Claims whose value is another Wikidata item.

#### Tasks
For each item-valued statement:
- create a row with:
  - `ID_STATEMENT`
  - `ID_ITEM`

#### Notes
This table becomes the authoritative edge store for item-valued claims.

The referenced `ID_ITEM` may exist or not exist yet in `T_WC_WIKIDATA_ITEM`.

No FK should enforce local cache presence.

---

### 6B – Populate `T_WC_WIKIDATA_STRING_VALUE`

#### Use for
Plain textual values.

#### Tasks
For each supported string-valued statement:
- store the raw string value
- store normalized value if needed
- store optional local language metadata if applicable

---

### 6C – Populate `T_WC_WIKIDATA_EXTERNAL_ID_VALUE`

#### Use for
External identifiers.

#### Examples
- Internet Archive ID
- YouTube video ID
- IMDb ID
- TMDb ID

#### Tasks
For each external-id-valued statement:
- store the raw external id
- store the normalized external id
- optionally cache formatter-derived URL patterns if useful

#### Important note
This table stores the raw identifier only, not the resolved URL set.

---

### 6D – Populate `T_WC_WIKIDATA_MEDIA_VALUE`

#### Use for
Commons-style media/file values.

#### Examples
- `P10` video file
- other supported Commons-media properties

#### Tasks
For each media-valued statement:
- store the raw file/media value
- store repository information if desired
- optionally cache basic file-page information

#### Important note
This is still a raw claim table, not the operational media-resource layer.

---

### 6E – Populate `T_WC_WIKIDATA_TIME_VALUE`

#### Use for
Time-valued claims.

#### Tasks
For each time-valued statement:
- preserve raw Wikidata time representation
- preserve precision
- preserve calendar model where relevant
- derive normalized searchable fields such as year / month / day

#### Important note
Do not flatten time semantics excessively.

---

### 6F – Populate `T_WC_WIKIDATA_QUANTITY_VALUE`

#### Use for
Quantity-valued claims.

#### Tasks
For each quantity-valued statement:
- store amount
- preserve unit when available
- preserve lower/upper bounds when available
- derive normalized display/search values if useful

---

## Stage 7 – Build the referenced-item cache candidate set

### Objective
Build the list of Q-items that are eligible for insertion into `T_WC_WIKIDATA_ITEM`.

### Principle
Use Strategy 1: referenced-item cache.

This means:
- collect the distinct `ID_ITEM` values from `T_WC_WIKIDATA_ITEM_VALUE`
- these become the candidate support items
- only these referenced items are eligible for caching in `T_WC_WIKIDATA_ITEM`

### Input
The freshly populated `T_WC_WIKIDATA_ITEM_VALUE`.

### Tasks
- collect distinct `ID_ITEM` values
- exclude any ids already represented as movie / series / person if your logic requires separation
- build the item-cache candidate set

### Output
A compact list of referenced support items.

### Important note
This keeps `T_WC_WIKIDATA_ITEM` movie/series/person-centric and avoids inserting all remaining Wikidata items.

---

## Stage 8 – Populate `T_WC_WIKIDATA_ITEM` from the referenced-item cache

### Objective
Populate the local support-item cache only for referenced items.

### Input
The candidate `ID_ITEM` list built from Stage 7.

### Tasks
For each candidate item id:
- locate its entity record in the dump or in an indexed lookup layer built from the dump
- extract local display metadata such as:
  - label
  - description
  - maybe selected classification/support metadata
- upsert the item into `T_WC_WIKIDATA_ITEM`

### Output
A compact support-item table containing only items actually referenced by movie / series / person claims.

### Important note
This table is a selective denormalized cache, not a universal registry of all Q-items.

---

## Stage 9 – Detect media-resolution candidates

### Objective
Identify statements that require downstream media resolution.

### Relevant claim sources
Media-resolution candidates usually come from:
- `T_WC_WIKIDATA_MEDIA_VALUE`
  - especially `P10`
- `T_WC_WIKIDATA_EXTERNAL_ID_VALUE`
  - especially `P724`
  - especially `P1651`

### Tasks
Find statements for in-scope entities whose property is media-resolution relevant.

Typical candidate families:
- Commons media/file values
- Internet Archive identifiers
- YouTube video identifiers

### Output
A resolution work queue or candidate dataset.

### Important note
This stage should not yet try to resolve the URLs during raw dump parsing.
It should only identify the candidates.

---

## Stage 10 – Populate `T_WC_WIKIDATA_MEDIA_RESOURCE`

### Objective
Create one resolved media-resource row per statement/platform/identifier.

### Input
The media-resolution candidate dataset from Stage 9 plus the downstream platform resolvers.

### Tasks
For each candidate statement:
- identify the source platform:
  - `commons`
  - `internet_archive`
  - `youtube`
- extract the source identifier:
  - Commons filename
  - IA identifier
  - YouTube video id
- create or update one row in `T_WC_WIKIDATA_MEDIA_RESOURCE`
- assign:
  - `ID_STATEMENT`
  - `ID_WIKIDATA`
  - `ID_PROPERTY`
  - source platform
  - source identifier
  - resource classification
  - status fields
  - resolved metadata snapshot when available

### Output
One resource-master row per resolved media source.

---

## Stage 11 – Populate `T_WC_WIKIDATA_MEDIA_RESOURCE_URL`

### Objective
Store the concrete URL variants discovered for each resolved media resource.

### Tasks
For each resource:
- create one or more URL rows such as:
  - page URL
  - watch URL
  - embed URL
  - file URL
  - thumbnail URL
  - metadata URL
- store URL type
- store normalized URL
- store technical metadata when available
- store current validation state

### Platform examples

#### Commons
Typical URL rows:
- file page URL
- direct file URL
- thumbnail URL

#### Internet Archive
Typical URL rows:
- item page URL
- metadata URL
- one or more file URLs
- thumbnail URL

#### YouTube
Typical URL rows:
- watch URL
- embed URL
- thumbnail URL

### Output
A one-to-many URL table under each resolved media resource.

---

## Stage 12 – Populate `T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK`

### Objective
Store historical validation and resolution checks.

### Tasks
For each resource-level or URL-level check:
- create one check-history row
- assign:
  - check scope
  - check type
  - check status
  - http status when relevant
  - timing
  - diagnostic fields

### Typical checks
- metadata lookup
- API resolution
- HTTP HEAD validation
- HTTP GET validation
- redirect tracing
- embed validation
- playback validation

### Output
A historical monitoring table for resource availability and diagnostics.

---

# Update strategy for the weekly refresh

The weekly refresh should be treated as an entity-centric rebuild/upsert workflow.

## Recommended update logic

### For properties
- refresh `T_WC_WIKIDATA_PROPERTY_METADATA` from the property entities in the new dump

### For subclass support data
- rebuild or refresh the local `P279` support structure used for classification

### For movies / series / persons
For each in-scope entity in the new dump:
- classify it through the current `P31/P279*` strategy
- refresh the corresponding business-entity row
- rebuild or upsert all its statement rows
- rebuild or upsert all its typed value rows

### For referenced item cache
After rebuilding item-valued claims:
- recompute the distinct referenced `ID_ITEM` set
- refresh `T_WC_WIKIDATA_ITEM` accordingly

### For media resources
For statements tied to media-related properties:
- detect new / changed / removed candidates
- refresh the corresponding media-resource rows
- refresh URL rows
- append new check-history rows for the latest validations

---

# Recommended deletion and stale-data handling

## For statements and typed values
The safest weekly approach is entity-centric replacement.

For each refreshed movie / series / person entity:
- mark prior statement/value rows for that entity as deleted or obsolete
- rebuild from the current dump representation

This is often safer than attempting very granular nested diffs.

## For referenced items
If an item is no longer referenced by any current movie / series / person item-valued claim:
- it may be removed from `T_WC_WIKIDATA_ITEM`
- or marked inactive / deleted
- depending on your retention policy

## For media resources
If the underlying raw claim disappears or changes:
- mark the old media resource as obsolete or deleted
- refresh the downstream resolved rows accordingly

---

# Recommended staging model

For performance and operational safety, the ETL should use a staging approach.

## Recommended pattern
1. parse the dump sequentially
2. generate staged entity/claim/property/subclass datasets
3. bulk-load staging data
4. transform staging data into final MariaDB tables
5. run media-resolution tasks separately from raw dump parsing

## Why this is better
This avoids:
- excessive row-by-row transactional overhead
- repeated random access during raw parse
- direct coupling between heavy dump parsing and slow external media resolution

---

# Recommended dependency order

The stages should respect this order:

1. input preparation
2. property metadata refresh
3. subclass graph support build
4. core entity detection with `P31/P279*`
5. core entity table refresh
6. statement extraction
7. typed value extraction
8. referenced-item candidate build
9. `T_WC_WIKIDATA_ITEM` refresh
10. media-candidate detection
11. `T_WC_WIKIDATA_MEDIA_RESOURCE` refresh
12. `T_WC_WIKIDATA_MEDIA_RESOURCE_URL` refresh
13. `T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK` append/update

This order keeps the whole architecture consistent.

---

# Validation rules for the ETL

## Classification validation
The movie / series / person classifier should:
- rely on direct `P31` values on the entity
- evaluate root membership through the local `P279` support graph
- use the chosen root classes consistently
- avoid using `Q15416` television program as the main series classifier

## Statement-level validation
Each `T_WC_WIKIDATA_STATEMENT` row must:
- have a valid `ID_WIKIDATA`
- have a valid `ID_PROPERTY`
- have a valid `VALUE_TYPE`
- map to exactly one child value row

## Value-table validation
Each child row must:
- reference an existing statement row
- match the `VALUE_TYPE` declared by the parent statement
- contain a valid payload for its datatype family

## Item-cache validation
Each row in `T_WC_WIKIDATA_ITEM` should correspond to at least one referenced `ID_ITEM` in `T_WC_WIKIDATA_ITEM_VALUE`, unless explicitly retained for business reasons.

## Media-resolution validation
Each `T_WC_WIKIDATA_MEDIA_RESOURCE` row should correspond to:
- one statement
- one platform
- one source identifier

Each `MEDIA_RESOURCE_URL` row should:
- belong to one media resource
- contain one normalized URL
- declare one URL type

Each `MEDIA_RESOURCE_CHECK` row should:
- record one real resolution or validation event

---

# Recommended operational outputs of the pipeline

At the end of a weekly run, the ETL should produce:
- refreshed property metadata
- refreshed subclass support data for classification
- refreshed movie / series / person entity tables
- refreshed statement table
- refreshed typed value tables
- refreshed referenced-item cache
- refreshed media-resource master table
- refreshed media-resource URL table
- appended media check history
- run-level metrics and diagnostics

Useful run metrics include:
- number of properties processed
- number of subclass edges processed
- number of movie roots and descendants recognized
- number of series roots and descendants recognized
- number of movies processed
- number of series processed
- number of persons processed
- number of statements loaded
- number of rows per typed value table
- number of distinct referenced items cached
- number of media candidates detected
- number of resources resolved
- number of URL rows created
- number of failed checks

---

# Final summary

The weekly-dump ETL should work like this:

1. parse the dump
2. refresh property metadata
3. build local `P279` support data
4. classify entities with `P31/P279*` using these roots:
   - movies: `Q11424` film, plus explicit support for `Q506240` television film
   - series: `Q5398426` television series, `Q1259759` miniseries, `Q526877` web series
   - persons: `Q5` human
5. populate the business entity tables
6. populate the statement table
7. populate the six typed value tables
8. build the referenced-item candidate set from `T_WC_WIKIDATA_ITEM_VALUE`
9. refresh `T_WC_WIKIDATA_ITEM` as a referenced-item cache only
10. detect media-related claims
11. populate `T_WC_WIKIDATA_MEDIA_RESOURCE`
12. populate `T_WC_WIKIDATA_MEDIA_RESOURCE_URL`
13. append `T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK`

This preserves the key architectural decisions:
- movie/series/person-centric ingestion
- selective support-item caching
- no FK from `ITEM_VALUE.ID_ITEM` to `T_WC_WIKIDATA_ITEM`
- clean statement/value separation
- clean claim/resource separation
- production-grade batch processing from the weekly JSON dump
- more robust classification through a local `P31/P279*` strategy
