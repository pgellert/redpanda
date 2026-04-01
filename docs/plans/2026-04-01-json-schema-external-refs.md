# JSON Schema External References — Decision Log

## Goal

Enable JSON schemas in the schema registry to reference other registered schemas
via the `references[]` field, matching existing Avro and Protobuf support.

## Current State

- `make_json_schema_definition(store, schema)` accepts a `schema_getter&` but **never
  uses it** — external references are ignored
- Only inline bundled schemas (objects with `$id` within the document) are resolved
- Compatibility checking fails if a `$ref` points to an unresolved external schema
- `make_canonical_json_schema` calls `check_references` to verify referenced
  subjects/versions exist, but the actual schema content is never fetched

## Approach: Bundle at the jsoncons level

**Decision:** Inject external schemas into the jsoncons document as bundled schemas
(under `$defs`) before the existing `collect_bundled_schema_and_fix_refs` runs.

**Why this approach over alternatives:**

| Alternative | Problem |
|---|---|
| Modify rapidjson doc after `parse_json` | Would bypass the `collect_bundled_schema_and_fix_refs` fix-up; need manual pointer setup and `$ref` absolutification |
| Extend `schema_context` to hold multiple documents | Invasive change to `resolve_reference` and all callers; every pointer dereference needs to know which document to navigate |
| Re-serialize jsoncons → iobuf → re-parse | Wasteful round-trip; the existing `parse_json` can be cleanly split |

**Chosen approach** reuses all existing machinery:
1. Split `parse_json` into `parse_json_doc` (parse + validate) and `finalize_json_doc` (collect bundled + serialize to rapidjson)
2. Between the two phases, fetch external schemas and inject them with `$id` = reference name
3. `collect_bundled_schema_and_fix_refs` naturally discovers injected schemas via `$id`
4. `$ref` fix-up naturally makes refs absolute, matching the injected `$id` after resolution

## Key Decisions

### 1. Injection location: `$defs` always

The `collect_bundled_schemas_and_fix_refs` function scans ALL nested objects
recursively — it discovers bundled schemas by `$id` presence, not by being under
a specific key. Using `$defs` is standard and harmless even for older drafts
(draft4/6/7 use `definitions` but additional properties are allowed at root).

### 2. The injected `$id` uses the reference `.name` field

The reference `.name` matches the `$ref` value in the schema. After URI
resolution against the same base, both produce the same `json_id_uri` key for
the `bundled_schemas` lookup. This works for:
- Relative refs: `"person.json"` → resolved against root base → same key
- Absolute refs: `"https://example.com/person.json"` → stays the same

### 3. Recursive injection for transitive references

If schema A refs B and B refs C, we recursively inject C into B's document
before injecting B into A's document. The final document has all transitive
dependencies bundled.

### 4. Test style: GTest with store_fixture (matching avro_schema_references.cc)

Following the existing avro test pattern for consistency. Tests verify:
- Basic external reference resolution
- Transitive references (A → B → C)
- Compatibility checking with external references
- Diamond dependencies

### 5. No `$schema` needed on injected schemas

Per the JSON Schema spec and the existing `process_work_item` implementation
(line 2300-2306), bundled schemas without `$schema` inherit the parent's dialect.
So we don't need to add `$schema` to injected schemas.

### 6. `id_keyword(dialect)` determines the identity keyword

Draft4 uses `"id"`, all others use `"$id"`. The injected schema uses the
**parent schema's** dialect to set the id keyword, since the collection function
uses the parent's dialect to find the keyword.

## Risk Assessment

- **Low risk:** The `parse_json` refactoring preserves exact behavior — `parse_json`
  becomes a trivial composition of the two new functions
- **Medium risk:** Schemas with root `$id` that conflicts with an injected schema's
  `$id` after resolution could have unexpected behavior. This is an edge case
  that would also be problematic in Confluent's implementation.
- **Not addressed:** Cross-dialect references (e.g., draft7 schema referencing a
  draft2020-12 schema). The existing dialect consistency check in
  `resolve_reference` (line 835-838) would reject this. This matches existing
  behavior for inline bundled schemas.

## Post-review additions

- **Depth limit:** Added `max_reference_depth = 50` to prevent infinite recursion
  on cyclic store data. Neither Avro nor Protobuf have this guard.
- **Error context:** Parse failures on referenced schemas now include the
  reference name in the error message for debuggability.
- **$defs key naming:** Changed from `__bundled_ref_{index}` to
  `__bundled_ref_{ref.name}` to avoid collisions with user-defined `$defs` entries.

### Reviewed and intentionally not changed

- **Sequential RPCs in the reference loop:** Matches Avro/Protobuf behavior.
  Schema registration is a cold path; parallelizing adds complexity for minimal gain.
- **Diamond re-fetch:** Shared schemas fetched+parsed twice in the diamond case.
  Matches Avro/Protobuf. A dedup cache would add parameter sprawl.
- **Metaschema re-validation of stored schemas:** True overhead but cheap relative
  to the cross-shard RPC. Adding a skip-validation flag is parameter sprawl.
- **Cross-dialect `$id` keyword mismatch:** Documented in code comment. The
  existing `resolve_reference` dialect consistency check catches this at
  compatibility-check time.
