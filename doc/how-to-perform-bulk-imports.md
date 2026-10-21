# How to perform bulk imports

To use the bulk imports feature in go-mssqldb, you need to import the sql and go-mssqldb packages.

```
import (
    "database/sql"
    "github.com/microsoft/go-mssqldb"
)
```

The `mssql.CopyIn` function creates a string which can be prepared by passing it to `Prepare`. The string returned contains information such as the name of the table and columns to bulk import data into, and bulk options.

```
bulkImportStr := mssql.CopyIn("tablename", mssql.BulkOptions{}, "column1", "column2", "column3")
stmt, err := db.Prepare(bulkImportStr)
```

Bulk options can be specified using the `mssql.BulkOptions` type. The following is how the `BulkOptions` type is defined:

```
type BulkOptions struct {
    CheckConstraints  bool
    FireTriggers      bool
    KeepNulls         bool
    KilobytesPerBatch int
    RowsPerBatch      int
    Order             []string
    Tablock           bool
}
```

The statement can be executed many times to copy data into the table specified.

```
for i := 0; i < 10; i++ {
	_, err = stmt.Exec(col1Data[i], col2Data[i], col3Data[i])
}
```

After all the data is processed, call `Exec` once with no arguments to flush all the buffered data.

```
_, err = stmt.Exec()
```

## Example
[Bulk import example](../bulkimport_example_test.go)

## Character encoding

Bulk imports into non-UTF-8 `varchar` columns (including `varchar(max)`)
use Unicode transport. SQL Server converts the text to the destination
column's code page, independent of the database default collation.
Characters not representable in that code page follow SQL Server's conversion
rules. The transport also accommodates characters whose UTF-8 encoding is
longer than their destination encoding.

For these columns, both Go `string` and `[]byte` text inputs are assumed to
contain valid UTF-8. This changes the interpretation
of `[]byte` values previously sent verbatim: callers supplying text in the
destination code page must convert it to UTF-8 first.

UTF-8 `varchar` columns retain their existing transport. Native `nvarchar`
columns retain their existing behavior, including UTF-16LE `[]byte` inputs.
