package mssql_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"
)

func TestFoo(t *testing.T) {
	driver := &mssql.Driver{}
	db, err := driver.Open("sqlserver://sa:Password1!@localhost:1433?trustServerCertificate=true")
	require.NoError(t, err)
	defer db.Close()

	conn := db.(*mssql.Conn)

	stmt, err := conn.Prepare("SELECT * FROM flights")
	require.NoError(t, err)
	defer stmt.Close()

	rdr, ok := stmt.(mssql.RawReader)
	require.True(t, ok)

	for range 5 {
		start := time.Now()
		ch := rdr.QueryRaw(context.Background(), nil)
		for v := range ch {
			require.NoError(t, v.Err)
		}
		fmt.Printf("QueryRaw took %v\n", time.Since(start))
	}
}

func TestBar(t *testing.T) {
	d := &mssql.Driver{}
	db, err := d.Open("sqlserver://sa:Password1!@localhost:1433?trustServerCertificate=true")
	require.NoError(t, err)
	defer db.Close()

	conn := db.(*mssql.Conn)

	stmt, err := conn.Prepare("SELECT * FROM flights")
	require.NoError(t, err)
	defer stmt.Close()

	rows, err := stmt.(driver.StmtQueryContext).QueryContext(context.Background(), nil)
	require.NoError(t, err)
	defer rows.Close()

	vals := make([]driver.Value, 27)
	rows.Next(vals)

	fmt.Println(vals)
}
