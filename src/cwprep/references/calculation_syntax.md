# Tableau Prep Calculation Syntax Reference

## Important: Syntax Differences from SQL

| Unsupported | Alternative |
|-------------|------------|
| `IN (1, 2, 3)` | Use `OR` to chain: `[x] = 1 OR [x] = 2 OR [x] = 3` |
| `BETWEEN a AND b` | Use `[x] >= a AND [x] <= b` |
| `!=` | Use `<>` |

## String Rules
- Strings must use **single quotes**: `[Field] = 'Value'`
- Field references use **square brackets**: `[Field Name]`
- Incorrect: `[name] == Headquarter` ❌
- Correct: `[name] = 'Headquarter'` ✅

## Logical Expressions
```
# Multiple value check (alternative to IN)
[status] = 2 OR [status] = 3 OR [status] = 4

# Exclude multiple values
NOT ([branch] = 'Main' OR [branch] = 'Sales')

# Regex match
REGEXP_MATCH(STR([status]), '^[2-8]$')
```

## Function Categories

### Numeric
ABS, ROUND, CEILING, FLOOR, POWER, SQRT, LN, LOG, EXP, SIGN, SQUARE, PI, ACOS, ASIN, ATAN, COS, SIN, TAN

### String
CONTAINS, STARTSWITH, ENDSWITH, FIND, FINDNTH, LEFT, RIGHT, MID, LEN, TRIM, LTRIM, RTRIM,
UPPER, LOWER, PROPER, REPLACE, SPLIT, SPACE, CHAR, ASCII, REGEXP_MATCH, REGEXP_REPLACE, REGEXP_EXTRACT

### Date
DATEADD, DATEDIFF, DATEPART, DATETRUNC, DATENAME, DATEPARSE,
YEAR, MONTH, DAY, WEEK, QUARTER, MAKEDATE, MAKEDATETIME, NOW, TODAY, ISDATE

### Logic
IF / THEN / ELSEIF / ELSE / END
CASE [field] WHEN value THEN result ... ELSE default END
IIF(condition, then, else)
IFNULL(expr, alternate)
ISNULL(expr)
ZN(expr)  — returns 0 if null
ISBLANK(expr)

### Type Conversion
INT(expr), FLOAT(expr), STR(expr), DATE(expr), DATETIME(expr)

### Aggregate (in calculated fields)
SUM, AVG, COUNT, COUNTD, MIN, MAX, MEDIAN, STDEV, VAR, ATTR
