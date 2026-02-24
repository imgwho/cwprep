# cwprep Best Practices & Common Pitfalls

## Critical: Tableau Prep Syntax != SQL

| Wrong | Correct | Note |
|-------|---------|------|
| `[f] IN ('A','B')` | `[f] = 'A' OR [f] = 'B'` | No IN — use OR or value_filter node |
| `[f] != 'x'` | `[f] <> 'x'` | No != operator |
| `[f] BETWEEN 1 AND 5` | `[f] >= 1 AND [f] <= 5` | No BETWEEN |
| `[f] = "text"` | `[f] = 'text'` | Strings must use single quotes |
| `[f] = NULL` | `ISNULL([f])` | Use ISNULL() or IFNULL() for nulls |

## Flow Design Rules

1. **Node ordering** — Nodes must be defined in dependency order;
   a node cannot reference a parent that hasn't been defined yet.
2. **Prefer value_filter for exact matches** — Simpler and less
   error-prone than filter with OR chains.
3. **Aggregate keeps only specified columns** — After an aggregate
   node, only group_by + aggregation output columns survive.
4. **Always validate first** — Call `validate_flow_definition`
   before `generate_tfl` to catch errors early.
5. **Double-check join columns** — Verify column names against
   the database schema before joining.
