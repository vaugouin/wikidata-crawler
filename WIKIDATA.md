
# WIKIDATA.md

## Purpose

This document describes the recommended architecture for storing and processing Wikidata information in a MariaDB database used for movie, series, and person data.

The design evolves from a simple relation table toward a **statement‑centric architecture** inspired by Wikidata's internal data model while remaining practical for relational databases.

Goals:

- Support multiple Wikidata datatypes cleanly
- Maintain strong data integrity
- Separate **raw Wikidata claims** from **resolved operational resources**
- Support media workflows for:
  - Wikimedia Commons
  - Internet Archive
  - YouTube
- Keep the system scalable and production‑grade

This document contains **no SQL** and is not the operational ETL runbook. It focuses on schema design, table responsibilities, and the separation between raw Wikidata claims and downstream derived resources.

For operational steps, use:

- `wikidata_dump_etl_README.md` for ETL execution, pass sequencing, staging-load workflow, and Docker commands
- `README.md` for the top-level documentation map

---

# Existing Entity Tables

These tables store the core Wikidata entities used by the application.

## T_WC_WIKIDATA_MOVIE

Stores movie entities imported or synchronized from Wikidata.

Typical contents:

- Feature films
- Short films
- Documentaries
- Animated films
- Public‑domain films

Role:

- Movie‑centric queries
- Business filtering
- Enrichment of movie metadata

Each row represents one movie entity.

---

## T_WC_WIKIDATA_SERIE

Stores television series or serial audiovisual works.

Typical examples:

- TV series
- Animated series
- Mini‑series
- Serialized audiovisual works

Role:

- Series‑centric querying
- Television‑specific metadata
- Episode and production context

Each row represents one series entity.

---

## T_WC_WIKIDATA_PERSON

Stores person entities.

Typical examples:

- Actors
- Directors
- Writers
- Producers
- Cinematographers
- Composers

Role:

- Cast and crew relationships
- Person‑centric queries
- Biographical information

Each row represents one person entity.

---

## T_WC_WIKIDATA_ITEM

Stores generic Wikidata items used as reference entities across the system.

Typical examples:

- Genres
- Countries
- Languages
- Awards
- Companies
- Places
- Movements
- Narrative locations

Role:

- Semantic reference layer
- Shared lookup table for relations between entities

---

# Current Implementation: T_WC_WIKIDATA_ITEM_PROPERTY

## Current Role

The existing table stores simple triplets:

- `ID_WIKIDATA` – subject entity
- `ID_PROPERTY` – Wikidata property
- `ID_ITEM` – target Wikidata item

Example:

movie → genre → comedy

## Advantages

- Simple
- Efficient for item‑to‑item relations
- Easy joins between entities

## Limitations

The model works only for **item‑valued claims**.

It does not properly support:

- String values
- External identifiers
- Media/file values
- Time values
- Quantity values

It also mixes:

- Statement identity
- Statement value

And cannot store:

- Statement rank
- Datatype metadata
- Statement qualifiers
- Media resolution metadata
- URL variants
- Validation status

This leads to poor extensibility.

---

# New Architecture: Statement + Typed Values + Qualifiers

The recommended architecture introduces three layers.

## Level 1 – Statement Layer

Table:

```
T_WC_WIKIDATA_STATEMENT
```

Each row represents a **single Wikidata statement**.

It contains metadata common to all statements.

## Level 2 – Main Value Layer

Typed child tables store the actual value depending on datatype.

Tables:

```
T_WC_WIKIDATA_ITEM_VALUE
T_WC_WIKIDATA_STRING_VALUE
T_WC_WIKIDATA_EXTERNAL_ID_VALUE
T_WC_WIKIDATA_MEDIA_VALUE
T_WC_WIKIDATA_TIME_VALUE
T_WC_WIKIDATA_QUANTITY_VALUE
```

Each statement must have exactly one row in one of these tables.

## Level 3 – Qualifier Layer

Qualifiers attached to a statement are stored in a parallel parent/typed-value structure.

Tables:

```
T_WC_WIKIDATA_STATEMENT_QUALIFIER
T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE
T_WC_WIKIDATA_QUALIFIER_STRING_VALUE
T_WC_WIKIDATA_QUALIFIER_EXTERNAL_ID_VALUE
T_WC_WIKIDATA_QUALIFIER_MEDIA_VALUE
T_WC_WIKIDATA_QUALIFIER_TIME_VALUE
T_WC_WIKIDATA_QUALIFIER_QUANTITY_VALUE
```

Each qualifier belongs to exactly one statement and must have exactly one row in one qualifier typed value table.

This separation improves:

- data integrity
- datatype validation
- extensibility
- clarity of the schema
- support for award/work/date style modeling

---

# Practical Design Rules

## Rule 1 — One statement → one value table

Each statement must have exactly one value row in exactly one typed value table.

## Rule 1B — One qualifier → one qualifier value table

Each qualifier must have exactly one value row in exactly one qualifier typed value table.

## Rule 2 — Parent declares datatype

The statement table contains:

```
VALUE_TYPE
```

Allowed values:

```
item
string
external_id
media
time
quantity
```

## Rule 3 — Raw claims vs resolved resources

Typed value tables and qualifier typed value tables store **raw Wikidata claim values only**.

Resolved assets such as:

- playable URLs
- embed URLs
- thumbnails
- validation data

belong in the **resolution layer**.

## Rule 4 — Wikidata‑centric identifiers

All claims are anchored on:

```
ID_WIKIDATA
```

Local business tables remain useful but do not replace the generic claims layer.

## Rule 4B — Qualifiers are anchored on statements

Qualifier rows are anchored on:

```
ID_STATEMENT
```

This preserves the original Wikidata structure:

- subject entity
- property
- main value
- qualifier property
- qualifier value

## Rule 5 — Use controlled vocabularies

Important fields should use controlled values:

- VALUE_TYPE
- SOURCE_PLATFORM
- RESOURCE_KIND
- CONTENT_ROLE
- URL_TYPE
- CHECK_TYPE

## Rule 6 — Separate current state and history

Current resource state:

```
MEDIA_RESOURCE
MEDIA_RESOURCE_URL
```

History:

```
MEDIA_RESOURCE_CHECK
```

---

# Statement Table

## T_WC_WIKIDATA_STATEMENT

Represents a Wikidata claim.

Important conceptual fields:

### Identity

- ID_STATEMENT
- ID_WIKIDATA
- ID_PROPERTY

### Typing

- VALUE_TYPE
- WIKIDATA_DATATYPE

### Statement semantics

- RANK
- DISPLAY_ORDER

### Lifecycle

- DELETED
- DAT_CREAT
- TIM_UPDATED
- IMPORT_BATCH_ID

---

# Qualifier Table

## T_WC_WIKIDATA_STATEMENT_QUALIFIER

Represents a Wikidata qualifier snak attached to a parent statement.

Important conceptual fields:

### Identity

- ID_STATEMENT_QUALIFIER
- ID_STATEMENT
- ID_QUALIFIER_PROPERTY

### Typing

- VALUE_TYPE
- WIKIDATA_DATATYPE

### Qualifier semantics

- DISPLAY_ORDER

### Lifecycle

- DELETED
- DAT_CREAT
- TIM_UPDATED
- IMPORT_BATCH_ID

The qualifier table mirrors the statement table, but its parent is a statement rather than an entity.

---

# Typed Value Tables

## T_WC_WIKIDATA_ITEM_VALUE

Stores item‑valued claims.

Examples:

- genre
- occupation
- country of origin
- cast member
- director

Payload:

- ID_STATEMENT
- ID_ITEM

---

## T_WC_WIKIDATA_STRING_VALUE

Stores plain string values.

Payload:

- ID_STATEMENT
- VALUE_STRING
- VALUE_STRING_NORMALIZED

---

## T_WC_WIKIDATA_EXTERNAL_ID_VALUE

Stores external identifiers.

Examples:

- Internet Archive ID
- YouTube video ID
- IMDb ID

Payload:

- ID_STATEMENT
- VALUE_EXTERNAL_ID
- VALUE_EXTERNAL_ID_NORMALIZED

---

## T_WC_WIKIDATA_MEDIA_VALUE

Stores media file values (usually Commons).

Examples:

- P10 video file
- image file

Payload:

- ID_STATEMENT
- FILE_NAME
- MEDIA_REPOSITORY
- FILE_PAGE_URL

---

## T_WC_WIKIDATA_TIME_VALUE

Stores time values.

Examples:

- birth date
- publication date
- release date

Payload:

- ID_STATEMENT
- RAW_TIME_VALUE
- TIME_PRECISION
- YEAR_VALUE
- MONTH_VALUE
- DAY_VALUE

---

## T_WC_WIKIDATA_QUANTITY_VALUE

Stores numeric quantities.

Examples:

- budget
- box office
- runtime

Payload:

- ID_STATEMENT
- AMOUNT
- UNIT_ID_WIKIDATA
- LOWER_BOUND
- UPPER_BOUND

---

# Property Metadata Cache

Table:

```
T_WC_WIKIDATA_PROPERTY_METADATA
```

Purpose:

Local cached copy of Wikidata property metadata.

This table defines:

- property datatype
- mapping to VALUE_TYPE
- formatting rules

Example mapping:

| Wikidata datatype | Local VALUE_TYPE |
|------------------|------------------|
| wikibase-item | item |
| string | string |
| external-id | external_id |
| commonsMedia | media |
| time | time |
| quantity | quantity |

This table allows automatic routing of claims to the correct value table.

---

# Qualifier Typed Value Tables

The qualifier typed value tables mirror the main typed value tables, but use `ID_STATEMENT_QUALIFIER` as their parent key.

Examples:

- `T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE`
- `T_WC_WIKIDATA_QUALIFIER_TIME_VALUE`

These are especially important for award modeling because Wikidata often stores:

- the award itself as the main value of `P166`
- the award date as qualifier `P585`
- the related work as qualifier `P1686`

---

# Award Example

To derive an award table with:

- award
- year
- work of art
- person

the relevant modeling pattern is:

## Main statement

- `T_WC_WIKIDATA_STATEMENT.ID_PROPERTY = 'P166'`
- `T_WC_WIKIDATA_ITEM_VALUE.ID_ITEM = <award QID>`

## Qualifiers

- `P585` → date/year in `T_WC_WIKIDATA_QUALIFIER_TIME_VALUE`
- `P1686` → work in `T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE`

## Subject

- `T_WC_WIKIDATA_STATEMENT.ID_WIKIDATA`
  - usually the person when the award is attributed to a person
  - sometimes a movie or other work when the award is attached directly to the work

This means the V2 model now supports a derived award table without flattening qualifier columns into the statement table itself.

---

# Resolution Layer

The resolution layer stores **derived media resources**.

Raw claims may contain:

- Commons filename
- Internet Archive ID
- YouTube video ID

The system resolves them to:

- playable URLs
- downloadable files
- embed links
- thumbnails
- technical metadata

This data is stored separately.

---

# Resolution Tables

## T_WC_WIKIDATA_MEDIA_RESOURCE

Represents one resolved media resource.

Examples:

- one Commons video file
- one Internet Archive item
- one YouTube video

Key fields:

Identity

- ID_MEDIA_RESOURCE
- ID_STATEMENT
- ID_WIKIDATA
- ID_PROPERTY

Source identity

- SOURCE_PLATFORM
- SOURCE_IDENTIFIER

Classification

- RESOURCE_KIND
- CONTENT_ROLE

Metadata

- RESOURCE_TITLE
- CHANNEL_OR_COLLECTION
- DURATION_SECONDS

Availability

- IS_PLAYABLE
- IS_DOWNLOADABLE
- IS_EMBEDDABLE

Lifecycle

- RESOLUTION_STATUS
- LAST_RESOLVED_AT
- LAST_CHECKED_AT

---

## T_WC_WIKIDATA_MEDIA_RESOURCE_URL

Stores URL variants for a resource.

Examples:

Commons

- page URL
- direct file URL
- thumbnail

Internet Archive

- item page
- file download URLs

YouTube

- watch URL
- embed URL

Key fields:

Identity

- ID_MEDIA_RESOURCE_URL
- ID_MEDIA_RESOURCE

URL metadata

- URL_TYPE
- URL
- URL_NORMALIZED

Technical data

- MIME_TYPE
- FILE_SIZE
- WIDTH
- HEIGHT
- VIDEO_CODEC

Validation

- HTTP_STATUS
- LAST_CHECKED_AT
- ERROR_MESSAGE

---

## T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK

Stores validation history.

Purpose:

- detect dead links
- monitor availability
- diagnose failures

Key fields:

Identity

- ID_MEDIA_RESOURCE_CHECK
- ID_MEDIA_RESOURCE
- ID_MEDIA_RESOURCE_URL

Check metadata

- CHECK_TYPE
- CHECK_SCOPE
- CHECK_STATUS

Results

- HTTP_STATUS
- RESPONSE_TIME_MS
- RESULT_SUMMARY

Timing

- CHECKED_AT

---

# How Layers Work Together

Example workflow for YouTube:

1. Statement created in `T_WC_WIKIDATA_STATEMENT`
2. YouTube ID stored in `T_WC_WIKIDATA_EXTERNAL_ID_VALUE`
3. Resolver creates row in `T_WC_WIKIDATA_MEDIA_RESOURCE`
4. URLs stored in `T_WC_WIKIDATA_MEDIA_RESOURCE_URL`
5. Validation logged in `T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK`

Same principle applies to:

- Commons files
- Internet Archive resources

---

# Final Architectural Principle

Two layers must remain separated.

### Raw Claims Layer

Represents what Wikidata states:

- entity
- property
- value
- datatype
- rank

### Operational Resolution Layer

Represents what the system derives:

- playable media resources
- URLs
- metadata
- availability checks

Keeping these layers separate ensures:

- schema clarity
- scalability
- easier maintenance
- production‑grade reliability
