# Median aggregate

The task of this code is to implement an aggregate
function that calculates the *median* over an input set (typically
values from a table). Documentation on how to create aggregates can be
found in the official PostgreSQL documentation under [User-defined
Aggregates](https://www.postgresql.org/docs/current/static/xaggr.html)


A typical median query is:

```sql
SELECT median(temp) FROM conditions;
```

## Compiling and installing

To compile and install the extension:

```bash
> make
> make install
```

Note, that depending on installation location, installing the
extension might require super-user permissions.

## Testing

Tests can be run with

```bash
> make installcheck
```

