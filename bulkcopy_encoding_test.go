// Copyright (c) 2026 Columnar Technologies Inc.  All rights reserved.

package mssql

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/microsoft/go-mssqldb/internal/cp"
	"github.com/microsoft/go-mssqldb/msdsn"
	"github.com/stretchr/testify/require"
)

type bulkEncodingValuer struct {
	value driver.Value
	err   error
}

func (v bulkEncodingValuer) Value() (driver.Value, error) {
	return v.value, v.err
}

func TestBulkVarcharTransportMetadata(t *testing.T) {
	for _, tc := range []struct {
		name          string
		typeID        byte
		size          int
		utf8          bool
		declaration   string
		transportSize int
	}{
		{"narrow", typeBigVarChar, 1, false, "nvarchar(1)", 2},
		{"bounded", typeBigVarChar, 4000, false, "nvarchar(4000)", 8000},
		{"large", typeBigVarChar, 4001, false, "nvarchar(max)", 0xffff},
		{"max", typeBigVarChar, 0xffff, false, "nvarchar(max)", 0xffff},
		{"zero", typeBigVarChar, 0, false, "nvarchar(max)", 0xffff},
		{"utf8", typeBigVarChar, 100, true, "varchar(100)", 100},
		{"nvarchar", typeNVarChar, 100, false, "nvarchar(50)", 100},
		{"char", typeBigChar, 10, false, "char(10)", 10},
		{"text", typeText, 100, false, "text", 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := columnStruct{ti: typeInfo{TypeId: tc.typeID, Size: tc.size}}
			if tc.utf8 {
				original.ti.Collation = cp.Collation{LcidAndFlags: 0x04000000}
			}
			col := makeBulkColumn(original)
			require.Equal(t, original, col.columnStruct)
			require.Equal(t, tc.declaration, makeDecl(col.transport))
			require.Equal(t, tc.transportSize, col.transport.Size)
			var metadata bytes.Buffer
			require.NoError(t, writeTypeInfo(&metadata, &col.transport, false, msdsn.EncodeParameters{}))
			require.Equal(t, col.transport.TypeId, metadata.Bytes()[0])
			require.NotNil(t, col.transport.Writer)
		})
	}
}

func TestBulkVarcharTransportValues(t *testing.T) {
	col := makeBulkColumn(columnStruct{ColName: "value", ti: typeInfo{TypeId: typeBigVarChar, Size: 10}})
	var metadata bytes.Buffer
	require.NoError(t, writeTypeInfo(&metadata, &col.transport, false, msdsn.EncodeParameters{}))
	b := Bulk{}
	for _, tc := range []struct {
		name    string
		input   any
		payload []byte
	}{
		{"string", "é", []byte{0xe9, 0}},
		{"bytes", []byte{0xe9, 0}, []byte{0xe9, 0}},
		{"raw_bytes", []byte{0xe9, 0x80, 0xc3, 0xa9}, []byte{0xe9, 0x80, 0xc3, 0xa9}},
		{"byte_valuer", bulkEncodingValuer{value: []byte{0xe9, 0}}, []byte{0xe9, 0}},
		{"null_byte_valuer", bulkEncodingValuer{value: []byte(nil)}, nil},
		{"empty_byte_valuer", bulkEncodingValuer{value: []byte{}}, []byte{}},
		{"CJK", "測", []byte{0x2c, 0x6e}},
		{"emoji", "😀", []byte{0x3d, 0xd8, 0x00, 0xde}},
		{"emoji_bytes", []byte{0x3d, 0xd8, 0xa2, 0xde}, []byte{0x3d, 0xd8, 0xa2, 0xde}},
		{"supplementary_CJK", "𠀀", []byte{0x40, 0xd8, 0x00, 0xdc}},
		{"musical_symbol", "𝄞", []byte{0x34, 0xd8, 0x1e, 0xdd}},
		{"combining", "e\u0301", []byte{0x65, 0x00, 0x01, 0x03}},
		{"integer", int64(12), []byte{'1', 0, '2', 0}},
		{"valuer", sql.NullString{String: "é", Valid: true}, []byte{0xe9, 0}},
		{"null", nil, nil},
		{"null_valuer", sql.NullString{}, nil},
		{"null_bytes", []byte(nil), nil},
		{"empty", "", []byte{}},
		{"empty_bytes", []byte{}, []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := b.makeBulkParam(tc.input, col)
			require.NoError(t, err)
			require.Equal(t, byte(typeNVarChar), p.ti.TypeId)
			require.Equal(t, tc.payload, p.buffer)
			require.Equal(t, len(tc.payload), p.ti.Size)
			var row bytes.Buffer
			require.NoError(t, col.transport.Writer(&row, p.ti, p.buffer, msdsn.EncodeParameters{}))
			if tc.payload == nil {
				require.Equal(t, []byte{0xff, 0xff}, row.Bytes())
			} else {
				require.Equal(t, append([]byte{byte(len(tc.payload)), 0}, tc.payload...), row.Bytes())
			}
		})
	}
	_, err := b.makeBulkParam(strings.Repeat("é", 40000), col)
	require.ErrorContains(t, err, "exceeds Unicode bulk transport length")
	_, err = b.makeBulkParam(bytes.Repeat([]byte{0xe9, 0}, 11), col)
	require.ErrorContains(t, err, "exceeds Unicode bulk transport length")
	valuerErr := errors.New("valuer failed")
	_, err = b.makeBulkParam(bulkEncodingValuer{err: valuerErr}, col)
	require.ErrorIs(t, err, valuerErr)

	for _, tc := range []struct {
		typeID    byte
		collation cp.Collation
		input     []byte
	}{
		{typeNVarChar, cp.Collation{}, []byte{0xe9, 0}},
		{typeBigVarChar, cp.Collation{LcidAndFlags: 0x04000000}, []byte("é")},
	} {
		col := makeBulkColumn(columnStruct{ti: typeInfo{TypeId: tc.typeID, Size: 100, Collation: tc.collation}})
		p, err := b.makeBulkParam(tc.input, col)
		require.NoError(t, err)
		require.Equal(t, tc.typeID, p.ti.TypeId)
		require.Equal(t, tc.input, p.buffer)
	}
}

func TestBulkVarcharEncoding(t *testing.T) {
	pool, logger := open(t)
	defer pool.Close()
	defer logger.StopLogging()
	for _, tc := range []struct {
		name        string
		declaration string
		input       any
		expected    string
		stored      []byte
		fails       bool
	}{
		{"narrow", "varchar(1) COLLATE SQL_Latin1_General_CP1_CI_AS", "é", "é", []byte{0xe9}, false},
		{"bytes", "varchar(1) COLLATE SQL_Latin1_General_CP1_CI_AS", []byte{0xe9, 0}, "é", []byte{0xe9}, false},
		{"boundary", "varchar(4000) COLLATE SQL_Latin1_General_CP1_CI_AS", strings.Repeat("é", 4000), strings.Repeat("é", 4000), bytes.Repeat([]byte{0xe9}, 4000), false},
		{"PLP", "varchar(4001) COLLATE SQL_Latin1_General_CP1_CI_AS", strings.Repeat("é", 4001), strings.Repeat("é", 4001), bytes.Repeat([]byte{0xe9}, 4001), false},
		{"PLP_bytes", "varchar(4001) COLLATE SQL_Latin1_General_CP1_CI_AS", bytes.Repeat([]byte{0xe9, 0}, 4001), strings.Repeat("é", 4001), bytes.Repeat([]byte{0xe9}, 4001), false},
		{"max", "varchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS", strings.Repeat("é", 9000), strings.Repeat("é", 9000), bytes.Repeat([]byte{0xe9}, 9000), false},
		{"CJK", "varchar(2) COLLATE Chinese_Taiwan_Stroke_CI_AS", "測", "測", []byte{0xb4, 0xfa}, false},
		{"CJK_overflow", "varchar(2) COLLATE Chinese_Taiwan_Stroke_CI_AS", "測測", "", nil, true},
		{"overflow", "varchar(1) COLLATE SQL_Latin1_General_CP1_CI_AS", "éé", "", nil, true},
		{"PLP_overflow", "varchar(4001) COLLATE SQL_Latin1_General_CP1_CI_AS", strings.Repeat("é", 4002), "", nil, true},
		{"utf8", "varchar(6) COLLATE Latin1_General_100_CI_AS_SC_UTF8", "🚢é", "🚢é", []byte("🚢é"), false},
		{"utf8_pair_boundary", "varchar(4) COLLATE Latin1_General_100_CI_AS_SC_UTF8", "𠀀", "𠀀", []byte("𠀀"), false},
		{"utf8_pair_overflow", "varchar(3) COLLATE Latin1_General_100_CI_AS_SC_UTF8", "𠀀", "", nil, true},
		{"utf16_pair_boundary", "nvarchar(2) COLLATE Latin1_General_100_CI_AS_SC", "𝄞", "𝄞", []byte{0x34, 0xd8, 0x1e, 0xdd}, false},
		{"utf16_pair_overflow", "nvarchar(1) COLLATE Latin1_General_100_CI_AS_SC", "𝄞", "", nil, true},
		{"nvarchar_bytes", "nvarchar(1)", []byte{0xe9, 0}, "é", []byte{0xe9, 0}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			conn, err := pool.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE #bulk_encoding (value %s)", tc.declaration))
			require.NoError(t, err)
			defer func() {
				_, err := conn.ExecContext(ctx, "DROP TABLE #bulk_encoding")
				require.NoError(t, err)
			}()
			stmt, err := conn.PrepareContext(ctx, CopyIn("#bulk_encoding", BulkOptions{}, "value"))
			require.NoError(t, err)
			defer stmt.Close()
			_, rowErr := stmt.ExecContext(ctx, tc.input)
			_, doneErr := stmt.ExecContext(ctx)
			if tc.fails {
				require.True(t, rowErr != nil || doneErr != nil, "oversized value must be rejected")
				return
			}
			require.NoError(t, rowErr)
			require.NoError(t, doneErr)
			var value string
			var stored []byte
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT CAST(value AS nvarchar(max)), CONVERT(varbinary(max), value) FROM #bulk_encoding").Scan(&value, &stored))
			require.Equal(t, tc.expected, value)
			require.Equal(t, tc.stored, stored)
		})
	}
}
