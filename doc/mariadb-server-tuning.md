# MariaDB Server Tuning (for the Wikidata crawler load)

This belongs in the **server installation documentation**: it describes the MariaDB instance
settings that make the Wikidata crawler's bulk load (step 110) and the rest of the database
workload fast. The single most important one — the InnoDB buffer pool — benefits **every** database
on the instance, not just the crawler.

Target instance: the `mariadb` service in the `damp` Docker stack (container
`damp-vaugouin-com-mariadb-1`), MariaDB 11.2.x.

---

## 1. InnoDB buffer pool — the primary knob (permanent)

`innodb_buffer_pool_size` is the in-memory cache for InnoDB data and indexes. The MariaDB default
(128 MB) is far too small for a multi-hundred-GB database: the crawler's `T_WC_WIKIDATA_STATEMENT`
load (hundreds of millions of rows + index maintenance) thrashes disk when the working set doesn't
fit in the pool.

**It is a global, instance-wide, persistent setting and it helps every workload** (TMDB, LBA, the
Wikidata tables, …) — a bigger shared cache speeds up reads and writes for all of them. The only cost
is RAM. There is no per-script downside.

### Sizing

Set it to roughly **60–70 % of the RAM available to the database container**, leaving headroom for
the OS, other containers, and MariaDB's own per-connection buffers. Check what the container has:

```bash
docker exec damp-vaugouin-com-mariadb-1 sh -c 'cat /proc/meminfo | head -1'   # MemTotal
# or, if the container is memory-limited by Docker:
docker inspect damp-vaugouin-com-mariadb-1 --format '{{.HostConfig.Memory}}'   # bytes, 0 = unlimited
```

Examples: 16 GB box → ~10G; 32 GB → ~20G; 64 GB → ~45G.

### Apply online (takes effect immediately, NOT persistent across restart)

MariaDB 10.2.2+ resizes the buffer pool online — no restart, no downtime:

```sql
-- run via: docker exec -i damp-vaugouin-com-mariadb-1 mariadb -uroot -p
SET GLOBAL innodb_buffer_pool_size = 10 * 1024 * 1024 * 1024;   -- e.g. 10 GiB; adjust to your RAM

-- watch the resize complete (should report "Completed resizing buffer pool"):
SHOW STATUS LIKE 'Innodb_buffer_pool_resize_status';
-- confirm the new size:
SELECT @@innodb_buffer_pool_size / 1024 / 1024 / 1024 AS gib;
```

### Make it persistent (survives container/MariaDB restart)

The online `SET GLOBAL` is lost on restart. Persist it one of two ways:

**a) Docker config file (recommended).** Mount a file at `/etc/mysql/conf.d/zz-wikidata-tuning.cnf`
into the MariaDB container (via the stack's `docker-compose.yml` volumes), containing:

```ini
[mysqld]
# Primary cache — size to ~60-70% of the container's RAM
innodb_buffer_pool_size      = 10G

# Helps large loads; safe defaults for a mixed workload
innodb_buffer_pool_instances = 8
innodb_log_file_size         = 2G
innodb_flush_method          = O_DIRECT
```

**b) Compose command flags.** Add to the mariadb service's `command:` in `docker-compose.yml`:

```yaml
command: >-
  --innodb-buffer-pool-size=10G
  --innodb-log-file-size=2G
  --innodb-flush-method=O_DIRECT
```

Either way, restart the service to pick it up, then re-run the verification query above.

---

## 2. Bulk-load session settings — safe, isolated, NOT global

These speed up the crawler's bulk load and are **session-scoped**: they apply only to the loader's
own connection and have **zero effect on other scripts/connections** writing to the database
concurrently. **The crawler already sets these automatically** at the start of step 110
(`step_bulk_load` in `wikidata_crawler.py`); they reset when the loader connection closes. For a
manual `LOAD DATA`/import run the same three apply:

```sql
SET SESSION sql_log_bin = 0;          -- skip the binary log for loader writes (fine unless replicating)
SET SESSION unique_checks = 0;        -- the data is unique by construction
SET SESSION foreign_key_checks = 0;   -- and FK-clean by construction
```

Durability/availability impact on other databases: **none** — they are local to the loader connection.

---

## 3. Durability knob — global, window it

```sql
SET GLOBAL innodb_flush_log_at_trx_commit = 2;   -- big write speedup, but GLOBAL
```

This is **instance-wide**: with `2`, the redo log is flushed to disk ~once per second instead of on
every commit, so on an OS/server crash up to ~1 second of *committed* transactions can be lost — for
**all** writers, not just the crawler. It is a real speedup for the bulk load but affects every
database on the instance.

- Use it **only during a maintenance window** for the bulk load, then restore:
  ```sql
  SET GLOBAL innodb_flush_log_at_trx_commit = 1;   -- back to full durability
  ```
- If other applications are doing durability-sensitive writes at the same time, **leave it at `1`** —
  the buffer-pool change (§1) is where most of the speedup comes from anyway.

---

## 4. Verify

```sql
SELECT @@innodb_buffer_pool_size/1024/1024/1024 AS pool_gib,
       @@innodb_buffer_pool_instances           AS instances,
       @@innodb_flush_log_at_trx_commit          AS flush_commit,
       @@innodb_flush_method                     AS flush_method;
```

---

## 5. Why this matters here

The last full Wikidata run took ~12 days. The dump-ETL passes dominate wall-clock, but the
staging+bulk-load stage (steps 108–113) is a large InnoDB write workload whose speed is gated almost
entirely by the buffer pool. Raising it from the 128 MB default is the highest-value, lowest-risk DB
change available, and unlike the ETL-side optimizations it improves every other workload on the
instance too. See `doc/wikidata-v1-v2-gap-analysis.md` for the end-to-end run-time breakdown.
