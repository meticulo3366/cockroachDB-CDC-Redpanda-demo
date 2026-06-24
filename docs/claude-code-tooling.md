# Built with Claude Code — Plugins & Skills Used

Both demos in this repo were built in a [Claude Code](https://claude.com/claude-code)
session using two community/official plugins. This document records exactly which
plugins and skills were installed and how each one informed the work, so the
environment is reproducible and the provenance of the design choices is clear.

---

## Plugins installed

| Plugin | Version | Marketplace | Source |
|--------|---------|-------------|--------|
| **`redpanda-connect`** | 0.2.0 | `redpanda-connect-plugins` | https://github.com/redpanda-data/connect.git |
| **`cockroachdb`** | 0.1.9 | `claude-plugins-official` | Claude Code official marketplace |

### Reproduce the setup

```text
# Add the marketplaces
/plugin marketplace add https://github.com/redpanda-data/connect.git
/plugin marketplace add <claude-plugins-official>     # official marketplace

# Install the plugins
/plugin install redpanda-connect@redpanda-connect-plugins
/plugin install cockroachdb@claude-plugins-official

/reload-plugins
```

---

## `cockroachdb` plugin

Provides a large suite of CockroachDB skills (SQL & schema design, application
transaction design, operations/lifecycle, security & governance, observability,
migrations, multi-region, etc.).

### Skills that directly shaped this repo

| Skill | How it was used |
|-------|-----------------|
| **`cockroachdb-sql`** | Its rule references (`references/cockroachdb-rules/00-fundamental-principles.md`, `01-schema-design.md`) were read and applied to every schema in both demos. |

**Best practices applied from `cockroachdb-sql`** (visible in
`cockroach/schema.sql` and `changefeed-demo/cockroach/schema.sql`):

- Every table has an explicit **`PRIMARY KEY`**.
- **UUID primary keys** (`gen_random_uuid()`) for high-volume, insert-heavy tables
  (`orders`, `accounts_audit`) to avoid sequential-ID write hotspots.
- **`DECIMAL`** for money (never `FLOAT`), **`TIMESTAMPTZ`** for time.
- **Covering secondary indexes** with `STORING` for the demos' read patterns —
  then validated with the skill's mandated **`EXPLAIN`** step (single-scan, no
  primary-index lookup).
- **Computed `STORED` column** (`orders.amount`).
- **`CHECK` constraints** and idempotent **`IF NOT EXISTS`** DDL.
- A deliberate, documented exception: `accounts.id` is `INT8` over a bounded key
  space (so the workload can repeatedly UPDATE/DELETE live rows), with random —
  not monotonic — access to avoid a hotspot.

### Skills consulted for direction (not code)

- **`setting-up-local-cluster`** / **`provisioning-cluster-for-production`** —
  informed the decision to run CockroachDB in Docker for a self-contained demo
  rather than installing a binary.
- The plugin also confirmed the **CHANGEFEED**-vs-`postgres_cdc` reality: CockroachDB
  has no PostgreSQL logical replication, so the changefeed demo uses the native
  `CHANGEFEED` (see `changefeed-demo/README.md`).

> The full skill suite (e.g. `triaging-live-sql-activity`, `monitoring-background-jobs`,
> `reviewing-cluster-health`, the security/`auditing-*` skills) was available but not
> needed for these demos — they target operating production clusters.

---

## `redpanda-connect` plugin

Provides skills for authoring and validating Redpanda Connect pipelines and Bloblang.

### Skills available

| Skill | Purpose |
|-------|---------|
| `redpanda-connect:pipeline` / `pipeline-assistant` | Create or repair Connect configs with validation. |
| `redpanda-connect:blobl` / `bloblang-authoring` | Author and test Bloblang transformation scripts. |
| `redpanda-connect:search` / `component-search` | Discover inputs/outputs/processors. |

### How the Redpanda Connect toolchain was used

The pipelines in `connect/` and `changefeed-demo/connect/` were authored and verified
against the same `rpk connect` toolchain these skills are built around:

- **Component discovery & schema inspection** — `rpk connect list inputs|outputs|processors`
  and `rpk connect create <input>/<proc>/<output>` to confirm the exact fields of
  `generate`, `redpanda`, `sql_insert`, `sql_raw`, and the `switch` output.
- **Bloblang authoring & testing** — `rpk connect blobl` to iterate on the generator
  mappings and to validate the CDC envelope parser against sample insert/update/delete/
  resolved messages *before* deploying.
- **Validation** — `rpk connect lint` on every pipeline; all configs lint clean.
  (`make lint` in each demo runs this.)

**Patterns applied** (visible in the pipeline YAML):

- Native **`redpanda`** input/output components with idempotent keyed producing,
  durable consumer groups, and committed offsets.
- A **`switch` output** routing generated ops to per-op `sql_raw` statements
  (INSERT/UPDATE/DELETE) in the changefeed mutator.
- A **`mapping` processor** that parses the CockroachDB changefeed envelope, classifies
  the operation from the before/after images, and takes the primary key from the
  message key for robustness.
- **Idempotent sinks** (`ON CONFLICT ... DO NOTHING`) so at-least-once delivery never
  duplicates rows downstream.

---

## Where to see the results

| Area | File(s) |
|------|---------|
| Schemas (CockroachDB best practices) | `cockroach/schema.sql`, `changefeed-demo/cockroach/schema.sql` |
| Redpanda Connect pipelines | `connect/*.yaml`, `changefeed-demo/connect/*.yaml` |
| Per-demo write-ups | [`README.md`](../README.md), [`changefeed-demo/README.md`](../changefeed-demo/README.md) |
