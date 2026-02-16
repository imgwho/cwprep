# Tableau Prep Calculation Syntax Rules

> ⚠️ **IMPORTANT**: Tableau Prep calculation syntax differs from standard SQL. Please strictly follow these rules.

## Unsupported Syntax & Alternatives

| Unsupported SQL Syntax | Alternative |
|-------------------|----------|
| `IN (1, 2, 3)` | Use `OR`: `[id] = 1 OR [id] = 2 OR [id] = 3` |
| `NOT IN (...)` | Use `NOT (...)`: `NOT ([id] = 1 OR [id] = 2)` |
| `BETWEEN a AND b` | Use comparison operators: `[val] >= a AND [val] <= b` |
| `!=` | Use `<>` |
| Double quoted strings `"text"` | **Must** use single quotes `'text'` |

## Detailed Syntax Guide

### 1. String Comparison
Must use **single quotes**. Double quotes in Tableau Prep are typically used for internal referencing or specific field names, but recommended field referencing is `[Field Name]`.

- ❌ Incorrect: `[Region] == "North"`
- ✅ Correct: `[Region] = 'North'`

### 2. Logical Expressions

**Multi-value check (Alternative to IN)**:
```
[Region] = 'North' OR [Region] = 'South' OR [Region] = 'East'
```

**Excluding multiple values (Alternative to NOT IN)**:
```
NOT ([Category] = 'Office Supplies' OR [Category] = 'Furniture')
```

**Regex Match (Advanced Filtering)**:
```
REGEXP_MATCH(STR([Order ID]), '^[A-Z]{2}-\d{4}$')
```

### 3. Null Handling
Use `ISNULL()` or `IFNULL()` functions. Do not use `= NULL`.

- ✅ `ISNULL([Discount])`
- ✅ `IFNULL([Discount], 0)`

## Supported Functions List

### Numeric Functions
`ABS`, `ROUND`, `CEILING`, `FLOOR`, `POWER`, `SQRT`, `ZN` (Zero if Null)

### String Functions
`CONTAINS`, `LEFT`, `RIGHT`, `LEN`, `TRIM`, `UPPER`, `LOWER`, `SPLIT`, `REPLACE`, `MID`, `STARTSWITH`, `ENDSWITH`

### Date Functions
`DATEADD`, `DATEDIFF`, `DATEPART`, `DATETRUNC`, `YEAR`, `MONTH`, `DAY`, `NOW`, `TODAY`, `MAKEDATE`, `MAKEDATETIME`

### Logical Functions
`IF`, `THEN`, `ELSE`, `ELSEIF`, `END`, `CASE`, `WHEN`, `IIF`, `IFNULL`, `ISNULL`, `AND`, `OR`, `NOT`

### Type Conversion
`INT`, `FLOAT`, `STR`, `DATE`, `DATETIME`, `BOOL`
