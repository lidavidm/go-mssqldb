package mssql_test

import (
	"context"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"
)

func TestRawReader(t *testing.T) {
	driver := &mssql.Driver{}
	db, err := driver.Open("sqlserver://sa:Password1!@localhost:1433?trustServerCertificate=true")
	require.NoError(t, err)
	defer db.Close()

	conn := db.(*mssql.Conn)

	// TODO: prepopulate with different types, numbers of columns,
	// etc. Ensure we read the expected number of rows.
	// TODO: also test errors, prepared statements, cancellation, etc.
	stmt, err := conn.Prepare("SELECT * FROM flights")
	require.NoError(t, err)
	defer stmt.Close()

	rdr, ok := stmt.(mssql.RawReader)
	require.True(t, ok)

	ch := rdr.QueryRaw(context.Background(), nil)
	for v := range ch {
		require.NoError(t, v.Err)
	}
}
