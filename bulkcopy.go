package mssql

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/microsoft/go-mssqldb/internal/decimal"
	"github.com/microsoft/go-mssqldb/msdsn"
	shopspring "github.com/shopspring/decimal"
)

type Bulk struct {
	// ctx is used only for AddRow and Done methods.
	// This could be removed if AddRow and Done accepted
	// a ctx field as well, which is available with the
	// database/sql call.
	ctx context.Context

	cn          *Conn
	metadata    []columnStruct
	bulkColumns []columnStruct
	columnsName []string
	tablename   string
	numRows     int

	headerSent bool
	Options    BulkOptions
	Debug      bool
}
type BulkOptions struct {
	CheckConstraints  bool
	FireTriggers      bool
	KeepNulls         bool
	KilobytesPerBatch int
	RowsPerBatch      int
	Order             []string
	Tablock           bool
}

type DataValue interface{}

const (
	sqlDateFormat     = "2006-01-02"
	sqlDateTimeFormat = "2006-01-02 15:04:05.999999999Z07:00"
	sqlTimeFormat     = "15:04:05.9999999"
)

func (c *Conn) CreateBulk(table string, columns []string) (_ *Bulk) {
	b := Bulk{ctx: context.Background(), cn: c, tablename: table, headerSent: false, columnsName: columns}
	b.Debug = false
	return &b
}

func (c *Conn) CreateBulkContext(ctx context.Context, table string, columns []string) (_ *Bulk) {
	b := Bulk{ctx: ctx, cn: c, tablename: table, headerSent: false, columnsName: columns}
	b.Debug = false
	return &b
}

func (b *Bulk) sendBulkCommand(ctx context.Context) (err error) {
	//get table columns info
	err = b.getMetadata(ctx)
	if err != nil {
		return err
	}

	//match the columns
	for _, colname := range b.columnsName {
		var bulkCol *columnStruct

		for _, m := range b.metadata {
			if m.ColName == colname {
				bulkCol = &m
				break
			}
		}
		if bulkCol != nil {
			// Note that for INSERT BULK operations, XMLTYPE is to be sent as NVARCHAR(N) or NVARCHAR(MAX) data type.
			// An error is produced if XMLTYPE is specified.
			//
			// https://learn.microsoft.com/openspecs/windows_protocols/ms-tds/ab4a7d62-cd1f-4db1-b67d-ecae58f493e3
			if bulkCol.ti.TypeId == typeXml {
				bulkCol.ti.TypeId = typeNVarChar
			}

			if bulkCol.ti.TypeId == typeUdt {
				//send udt as binary
				bulkCol.ti.TypeId = typeBigVarBin
			}
			b.bulkColumns = append(b.bulkColumns, *bulkCol)
			b.dlogf(ctx, "Adding column %s %s %#x", colname, bulkCol.ColName, bulkCol.ti.TypeId)
		} else {
			return fmt.Errorf("column %s does not exist in destination table %s", colname, b.tablename)
		}
	}

	//create the bulk command

	//columns definitions
	var col_defs bytes.Buffer
	q := TSQLQuoter{}
	for i, col := range b.bulkColumns {
		if i != 0 {
			col_defs.WriteString(", ")
		}
		col_defs.WriteString(q.ID(col.ColName) + " " + makeDecl(col.ti))
	}

	//options
	var with_opts []string

	if b.Options.CheckConstraints {
		with_opts = append(with_opts, "CHECK_CONSTRAINTS")
	}
	if b.Options.FireTriggers {
		with_opts = append(with_opts, "FIRE_TRIGGERS")
	}
	if b.Options.KeepNulls {
		with_opts = append(with_opts, "KEEP_NULLS")
	}
	if b.Options.KilobytesPerBatch > 0 {
		with_opts = append(with_opts, fmt.Sprintf("KILOBYTES_PER_BATCH = %d", b.Options.KilobytesPerBatch))
	}
	if b.Options.RowsPerBatch > 0 {
		with_opts = append(with_opts, fmt.Sprintf("ROWS_PER_BATCH = %d", b.Options.RowsPerBatch))
	}
	if len(b.Options.Order) > 0 {
		with_opts = append(with_opts, fmt.Sprintf("ORDER(%s)", strings.Join(b.Options.Order, ",")))
	}
	if b.Options.Tablock {
		with_opts = append(with_opts, "TABLOCK")
	}
	var with_part string
	if len(with_opts) > 0 {
		with_part = fmt.Sprintf("WITH (%s)", strings.Join(with_opts, ","))
	}

	query := fmt.Sprintf("INSERT BULK %s (%s) %s", b.tablename, col_defs.String(), with_part)

	stmt, err := b.cn.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("Prepare failed: %s", err.Error())
	}
	b.dlogf(ctx, "%s", query)

	_, err = stmt.(*Stmt).ExecContext(ctx, nil)
	if err != nil {
		return err
	}

	b.headerSent = true

	var buf = b.cn.sess.buf
	buf.BeginPacket(packBulkLoadBCP, false)

	// Send the columns metadata.
	columnMetadata := b.createColMetadata()
	_, err = buf.Write(columnMetadata)

	return
}

// AddRow immediately writes the row to the destination table.
// The arguments are the row values in the order they were specified.
func (b *Bulk) AddRow(row []interface{}) (err error) {
	if !b.headerSent {
		err = b.sendBulkCommand(b.ctx)
		if err != nil {
			return
		}
	}

	if len(row) != len(b.bulkColumns) {
		return fmt.Errorf("row does not have the same number of columns than the destination table %d %d",
			len(row), len(b.bulkColumns))
	}

	bytes, err := b.makeRowData(row)
	if err != nil {
		return
	}

	_, err = b.cn.sess.buf.Write(bytes)
	if err != nil {
		return
	}

	b.numRows = b.numRows + 1
	return
}

func (b *Bulk) makeRowData(row []interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(tokenRow))

	var logcol bytes.Buffer
	for i, col := range b.bulkColumns {

		if b.Debug {
			logcol.WriteString(fmt.Sprintf(" col[%d]='%v' ", i, row[i]))
		}
		param, err := b.makeParam(row[i], col)
		if err != nil {
			return nil, fmt.Errorf("bulkcopy: %s", err.Error())
		}

		if col.ti.Writer == nil {
			return nil, fmt.Errorf("no writer for column: %s, TypeId: %#x",
				col.ColName, col.ti.TypeId)
		}
		err = col.ti.Writer(buf, param.ti, param.buffer, b.cn.sess.encoding)
		if err != nil {
			return nil, fmt.Errorf("bulkcopy: %s", err.Error())
		}
	}

	b.dlogf(b.ctx, "row[%d] %s", b.numRows, logcol.String())

	return buf.Bytes(), nil
}

func (b *Bulk) Done() (rowcount int64, err error) {
	if !b.headerSent {
		//no rows had been sent
		return 0, nil
	}
	var buf = b.cn.sess.buf
	buf.WriteByte(byte(tokenDone))

	binary.Write(buf, binary.LittleEndian, uint16(doneFinal))
	binary.Write(buf, binary.LittleEndian, uint16(0)) //     curcmd

	if b.cn.sess.loginAck.TDSVersion >= verTDS72 {
		binary.Write(buf, binary.LittleEndian, uint64(0)) //rowcount 0
	} else {
		binary.Write(buf, binary.LittleEndian, uint32(0)) //rowcount 0
	}

	buf.FinishPacket()

	reader := startReading(b.cn.sess, b.ctx, outputs{})
	err = reader.iterateResponse()
	if err != nil {
		return 0, b.cn.checkBadConn(b.ctx, err, false)
	}

	return reader.rowCount, nil
}

func (b *Bulk) createColMetadata() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(tokenColMetadata))                              // token
	binary.Write(buf, binary.LittleEndian, uint16(len(b.bulkColumns))) // column count

	// TODO: Write a valid CEK table if any parameters have cekTableEntry values
	if b.cn.sess.alwaysEncrypted {
		binary.Write(buf, binary.LittleEndian, uint16(0))
	}
	for i, col := range b.bulkColumns {

		if b.cn.sess.loginAck.TDSVersion >= verTDS72 {
			binary.Write(buf, binary.LittleEndian, uint32(col.UserType)) //  usertype, always 0?
		} else {
			binary.Write(buf, binary.LittleEndian, uint16(col.UserType))
		}
		binary.Write(buf, binary.LittleEndian, uint16(col.Flags))

		writeTypeInfo(buf, &b.bulkColumns[i].ti, false, b.cn.sess.encoding)

		if col.ti.TypeId == typeNText ||
			col.ti.TypeId == typeText ||
			col.ti.TypeId == typeImage {

			tablename_ucs2 := str2ucs2(b.tablename)
			binary.Write(buf, binary.LittleEndian, uint16(len(tablename_ucs2)/2))
			buf.Write(tablename_ucs2)
		}
		colname_ucs2 := str2ucs2(col.ColName)
		buf.WriteByte(uint8(len(colname_ucs2) / 2))
		buf.Write(colname_ucs2)
	}

	return buf.Bytes()
}

func (b *Bulk) getMetadata(ctx context.Context) (err error) {
	stmt, err := b.cn.prepareContext(ctx, "SET FMTONLY ON")
	if err != nil {
		return
	}

	_, err = stmt.ExecContext(ctx, nil)
	if err != nil {
		return
	}

	// Ensure we always SET FMTONLY OFF even if the next statement fails
	resetFmtonly := true
	defer func() {
		if !resetFmtonly {
			return
		}

		// Don't let resetErr shadow the "real" error, since this should
		// generally only happen if one of the calls below failed
		stmt, resetErr := b.cn.prepareContext(ctx, "SET FMTONLY OFF")
		if resetErr != nil {
			// This _should_ be infallible as prepareContext doesn't
			// actually contact the server
			b.cn.sess.logger.Log(ctx, msdsn.LogErrors, fmt.Sprintf("Could not reset FMTONLY: %v", resetErr))
			return
		}
		// stmt.Close is a no-op so ignore it
		_, resetErr = stmt.ExecContext(ctx, nil)
		if resetErr != nil {
			b.cn.sess.logger.Log(ctx, msdsn.LogErrors, fmt.Sprintf("Could not reset FMTONLY: %v", resetErr))
			return
		}
	}()

	// Get columns info.
	stmt, err = b.cn.prepareContext(ctx, fmt.Sprintf("select * from %s SET FMTONLY OFF", b.tablename))
	if err != nil {
		return
	}
	rows, err := stmt.QueryContext(ctx, nil)
	if err != nil {
		return fmt.Errorf("get columns info failed: %v", err)
	}
	resetFmtonly = false
	b.metadata = rows.(*Rows).cols

	if b.Debug {
		for _, col := range b.metadata {
			b.dlogf(ctx, "col: %s typeId: %#x size: %d scale: %d prec: %d flags: %d lcid: %#x",
				col.ColName, col.ti.TypeId, col.ti.Size, col.ti.Scale, col.ti.Prec,
				col.Flags, col.ti.Collation.LcidAndFlags)
		}
	}

	return rows.Close()
}

func (b *Bulk) makeParam(val DataValue, col columnStruct) (res param, err error) {
	res.ti.Size = col.ti.Size
	res.ti.TypeId = col.ti.TypeId
	loc := getTimezone(b.cn)

	if val == nil {
		res.ti.Size = 0
		return
	}

	if col.ti.TypeId == typeVariant {
		return b.makeVariantParam(val, col)
	}

	switch valuer := val.(type) {
	case Money[shopspring.Decimal]:
		return b.makeParam(valuer.Decimal, col)
	case Money[shopspring.NullDecimal]:
		return b.makeParam(valuer.Decimal, col)
	case driver.Valuer:
		var e error
		val, e = driver.DefaultParameterConverter.ConvertValue(valuer)
		if e != nil {
			err = e
			return
		}
		if val != nil {
			return b.makeParam(val, col)
		}
	}

	if val == nil {
		res.ti.Size = 0
		return
	}

	switch col.ti.TypeId {

	case typeInt1, typeInt2, typeInt4, typeInt8, typeIntN:
		var intvalue int64

		switch val := val.(type) {
		case int:
			intvalue = int64(val)
		case int32:
			intvalue = int64(val)
		case int64:
			intvalue = val
		case float32:
			intvalue = int64(val)
		case float64:
			intvalue = int64(val)
		default:
			err = fmt.Errorf("mssql: invalid type for int column: %T", val)
			return
		}

		res.buffer = make([]byte, res.ti.Size)
		if col.ti.Size == 1 {
			res.buffer[0] = byte(intvalue)
		} else if col.ti.Size == 2 {
			binary.LittleEndian.PutUint16(res.buffer, uint16(intvalue))
		} else if col.ti.Size == 4 {
			binary.LittleEndian.PutUint32(res.buffer, uint32(intvalue))
		} else if col.ti.Size == 8 {
			binary.LittleEndian.PutUint64(res.buffer, uint64(intvalue))
		}
	case typeFlt4, typeFlt8, typeFltN:
		var floatvalue float64

		switch val := val.(type) {
		case float32:
			floatvalue = float64(val)
		case float64:
			floatvalue = val
		case int:
			floatvalue = float64(val)
		case int64:
			floatvalue = float64(val)
		default:
			err = fmt.Errorf("mssql: invalid type for float column: %T %s", val, val)
			return
		}

		if col.ti.Size == 4 {
			res.buffer = make([]byte, 4)
			binary.LittleEndian.PutUint32(res.buffer, math.Float32bits(float32(floatvalue)))
		} else if col.ti.Size == 8 {
			res.buffer = make([]byte, 8)
			binary.LittleEndian.PutUint64(res.buffer, math.Float64bits(floatvalue))
		}
	case typeNVarChar, typeNText, typeNChar:

		switch val := val.(type) {
		case string:
			res.buffer = str2ucs2(val)
		case int64:
			res.buffer = []byte(strconv.FormatInt(val, 10))
		case int:
			res.buffer = []byte(strconv.FormatInt(int64(val), 10))
		case int8:
			res.buffer = []byte(strconv.FormatInt(int64(val), 10))
		case int32:
			res.buffer = []byte(strconv.FormatInt(int64(val), 10))
		case int16:
			res.buffer = []byte(strconv.FormatInt(int64(val), 10))
		case []byte:
			res.buffer = val
		default:
			err = fmt.Errorf("mssql: invalid type for nvarchar column: %T %s", val, val)
			return
		}
		res.ti.Size = len(res.buffer)

	case typeVarChar, typeBigVarChar, typeText, typeChar, typeBigChar:
		switch val := val.(type) {
		case string:
			res.buffer = []byte(val)
		case []byte:
			res.buffer = val
		case int:
			res.buffer = []byte(strconv.FormatInt(int64(val), 10))
		case int8:
			res.buffer = []byte(strconv.FormatInt(int64(val), 10))
		case int16:
			res.buffer = []byte(strconv.FormatInt(int64(val), 10))
		case int32:
			res.buffer = []byte(strconv.FormatInt(int64(val), 10))
		case int64:
			res.buffer = []byte(strconv.FormatInt(val, 10))
		default:
			err = fmt.Errorf("mssql: invalid type for varchar column: %T %s", val, val)
			return
		}
		res.ti.Size = len(res.buffer)

	case typeBit, typeBitN:
		if reflect.TypeOf(val).Kind() != reflect.Bool {
			err = fmt.Errorf("mssql: invalid type for bit column: %T %s", val, val)
			return
		}
		res.ti.TypeId = typeBitN
		res.ti.Size = 1
		res.buffer = make([]byte, 1)
		if val.(bool) {
			res.buffer[0] = 1
		}
	case typeDateTime2N:
		switch val := val.(type) {
		case time.Time:
			res.buffer = encodeDateTime2(val, int(col.ti.Scale))
			res.ti.Size = len(res.buffer)
		case string:
			var t time.Time
			if t, err = time.Parse(sqlDateTimeFormat, val); err != nil {
				return res, fmt.Errorf("bulk: unable to convert string to date: %v", err)
			}
			res.buffer = encodeDateTime2(t, int(col.ti.Scale))
			res.ti.Size = len(res.buffer)
		default:
			err = fmt.Errorf("mssql: invalid type for datetime2 column: %T %s", val, val)
			return
		}
	case typeDateTimeOffsetN:
		switch val := val.(type) {
		case time.Time:
			res.buffer = encodeDateTimeOffset(val, int(col.ti.Scale))
			res.ti.Size = len(res.buffer)
		case string:
			var t time.Time
			if t, err = time.Parse(sqlDateTimeFormat, val); err != nil {
				return res, fmt.Errorf("bulk: unable to convert string to date: %v", err)
			}
			res.buffer = encodeDateTimeOffset(t, int(col.ti.Scale))
			res.ti.Size = len(res.buffer)
		default:
			err = fmt.Errorf("mssql: invalid type for datetimeoffset column: %T %s", val, val)
			return
		}
	case typeDateN:
		switch val := val.(type) {
		case time.Time:
			res.buffer = encodeDate(val)
			res.ti.Size = len(res.buffer)
		case string:
			var t time.Time
			if t, err = time.ParseInLocation(sqlDateFormat, val, loc); err != nil {
				return res, fmt.Errorf("bulk: unable to convert string to date: %v", err)
			}
			res.buffer = encodeDate(t)
			res.ti.Size = len(res.buffer)
		default:
			err = fmt.Errorf("mssql: invalid type for date column: %T %s", val, val)
			return
		}
	case typeDateTime, typeDateTimeN, typeDateTim4:
		var t time.Time
		switch val := val.(type) {
		case time.Time:
			t = val
		case string:
			if t, err = time.Parse(sqlDateTimeFormat, val); err != nil {
				return res, fmt.Errorf("bulk: unable to convert string to date: %v", err)
			}
		default:
			err = fmt.Errorf("mssql: invalid type for datetime column: %T %s", val, val)
			return
		}

		if col.ti.Size == 4 {
			res.buffer = encodeDateTim4(t, loc)
			res.ti.Size = len(res.buffer)
		} else if col.ti.Size == 8 {
			res.buffer = encodeDateTime(t)
			res.ti.Size = len(res.buffer)
		} else {
			err = fmt.Errorf("mssql: invalid size of column %d", col.ti.Size)
		}
	case typeTimeN:
		var t time.Time
		switch val := val.(type) {
		case time.Time:
			res.buffer = encodeTime(val.Hour(), val.Minute(), val.Second(), val.Nanosecond(), int(col.ti.Scale))
			res.ti.Size = len(res.buffer)
		case string:
			if t, err = time.Parse(sqlTimeFormat, val); err != nil {
				return res, fmt.Errorf("bulk: unable to convert string to time: %v", err)
			}
			res.buffer = encodeTime(t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), int(col.ti.Scale))
			res.ti.Size = len(res.buffer)
		default:
			err = fmt.Errorf("mssql: invalid type for time column: %T %s", val, val)
			return
		}
	case typeMoney, typeMoney4, typeMoneyN:
		switch v := val.(type) {
		case string:
			money, err := decimal.StringToDecimalScale(v, 4)
			if err != nil {
				return res, err
			}

			buf := make([]byte, col.ti.Size)

			integer0 := money.GetInteger(0)
			if col.ti.Size == 4 {
				if money.IsPositive() {
					binary.LittleEndian.PutUint32(buf, integer0)
				} else {
					binary.LittleEndian.PutUint32(buf, ^integer0+1)
				}
			} else {
				integer := (uint64(money.GetInteger(1)) << 32) | uint64(integer0)
				if !money.IsPositive() {
					integer = ^integer + 1
				}

				binary.LittleEndian.PutUint32(buf, uint32(integer>>32))
				binary.LittleEndian.PutUint32(buf[4:], uint32(integer))
			}

			res.buffer = buf
		default:
			return res, fmt.Errorf("unknown value for money: %T %#v", v, v)
		}
	case typeDecimal, typeDecimalN, typeNumeric, typeNumericN:
		prec := col.ti.Prec
		scale := col.ti.Scale
		var dec decimal.Decimal
		switch v := val.(type) {
		case int:
			dec = decimal.Int64ToDecimalScale(int64(v), 0)
		case int8:
			dec = decimal.Int64ToDecimalScale(int64(v), 0)
		case int16:
			dec = decimal.Int64ToDecimalScale(int64(v), 0)
		case int32:
			dec = decimal.Int64ToDecimalScale(int64(v), 0)
		case int64:
			dec = decimal.Int64ToDecimalScale(int64(v), 0)
		case float32:
			dec, err = decimal.Float64ToDecimalScale(float64(v), scale)
		case float64:
			dec, err = decimal.Float64ToDecimalScale(float64(v), scale)
		case string:
			dec, err = decimal.StringToDecimalScale(v, scale)
		default:
			return res, fmt.Errorf("unknown value for decimal: %T %#v", v, v)
		}

		if err != nil {
			return res, err
		}
		dec.SetPrec(prec)

		var length byte
		switch {
		case prec <= 9:
			length = 4
		case prec <= 19:
			length = 8
		case prec <= 28:
			length = 12
		default:
			length = 16
		}

		buf := make([]byte, length+1)
		// first byte length written by typeInfo.writer
		res.ti.Size = int(length) + 1
		// second byte sign
		if !dec.IsPositive() {
			buf[0] = 0
		} else {
			buf[0] = 1
		}

		ub := dec.UnscaledBytes()
		l := len(ub)
		if l > int(length) {
			err = fmt.Errorf("decimal out of range: %s", dec)
			return res, err
		}
		// reverse the bytes
		for i, j := 1, l-1; j >= 0; i, j = i+1, j-1 {
			buf[i] = ub[j]
		}
		res.buffer = buf
	case typeBigVarBin, typeBigBinary, typeImage:
		switch val := val.(type) {
		case []byte:
			res.ti.Size = len(val)
			res.buffer = val
		default:
			err = fmt.Errorf("mssql: invalid type for Binary column: %T %s", val, val)
			return
		}
	case typeGuid:
		switch val := val.(type) {
		case []byte:
			res.ti.Size = len(val)
			res.buffer = val
		default:
			err = fmt.Errorf("mssql: invalid type for Guid column: %T %s", val, val)
			return
		}
	default:
		err = fmt.Errorf("mssql: type %x not implemented", col.ti.TypeId)
	}
	return

}

func (b *Bulk) makeVariantParam(val DataValue, col columnStruct) (res param, err error) {
	res.ti.TypeId = typeVariant
	res.ti.Size = col.ti.Size

	if val == nil {
		res.ti.Size = 0
		return
	}

	switch valuer := val.(type) {
	case SQLVariant:
		res.buffer, err = b.encodeVariantPayload(valuer, true)
	case Money[shopspring.Decimal]:
		res.buffer, err = b.encodeVariantPayload(SQLVariant{
			BaseTypeID: typeMoney,
			Value:      valuer.Decimal.String(),
			Scale:      4,
		}, true)
	case Money[shopspring.NullDecimal]:
		if !valuer.Decimal.Valid {
			res.ti.Size = 0
			return
		}
		res.buffer, err = b.encodeVariantPayload(SQLVariant{
			BaseTypeID: typeMoney,
			Value:      valuer.Decimal.Decimal.String(),
			Scale:      4,
		}, true)
	case shopspring.Decimal:
		scale, scaleErr := variantDecimalScaleFromShopspring(valuer)
		if scaleErr != nil {
			err = scaleErr
			return
		}
		res.buffer, err = b.encodeVariantPayload(SQLVariant{
			BaseTypeID: typeDecimalN,
			Value:      valuer.String(),
			Scale:      scale,
		}, true)
	case shopspring.NullDecimal:
		if !valuer.Valid {
			res.ti.Size = 0
			return
		}
		scale, scaleErr := variantDecimalScaleFromShopspring(valuer.Decimal)
		if scaleErr != nil {
			err = scaleErr
			return
		}
		res.buffer, err = b.encodeVariantPayload(SQLVariant{
			BaseTypeID: typeDecimalN,
			Value:      valuer.Decimal.String(),
			Scale:      scale,
		}, true)
	case driver.Valuer:
		converted, convertErr := driver.DefaultParameterConverter.ConvertValue(valuer)
		if convertErr != nil {
			err = convertErr
			return
		}
		if converted == nil {
			res.ti.Size = 0
			return
		}
		return b.makeVariantParam(converted, col)
	default:
		var variant SQLVariant
		variant, err = inferVariantValue(val)
		if err != nil {
			return
		}
		res.buffer, err = b.encodeVariantPayload(variant, false)
	}
	if err != nil {
		return
	}
	res.ti.Size = len(res.buffer)
	return
}

func (b *Bulk) encodeVariantPayload(variant SQLVariant, explicit bool) ([]byte, error) {
	if variant.Value == nil {
		return nil, nil
	}

	baseType, value, err := variantBaseTypeInfo(variant, explicit)
	if err != nil {
		return nil, err
	}
	baseParam, err := b.makeParam(value, columnStruct{ti: baseType})
	if err != nil {
		return nil, err
	}
	baseParam.ti.TypeId = baseType.TypeId
	baseParam.ti.Prec = baseType.Prec
	baseParam.ti.Scale = baseType.Scale
	baseParam.ti.Collation = baseType.Collation

	props, err := variantProperties(baseParam.ti)
	if err != nil {
		return nil, err
	}
	if len(props) > 0xff {
		return nil, fmt.Errorf("mssql: sql_variant base type %#x has too many property bytes: %d", baseType.TypeId, len(props))
	}

	var payload bytes.Buffer
	payload.WriteByte(baseType.TypeId)
	payload.WriteByte(byte(len(props)))
	payload.Write(props)
	payload.Write(baseParam.buffer)
	return payload.Bytes(), nil
}

func inferVariantValue(val DataValue) (SQLVariant, error) {
	switch v := val.(type) {
	case bool:
		return SQLVariant{BaseTypeID: typeBit, Value: v}, nil
	case byte:
		return SQLVariant{BaseTypeID: typeInt1, Value: int64(v)}, nil
	case int8:
		return SQLVariant{BaseTypeID: typeInt2, Value: int64(v)}, nil
	case int16:
		return SQLVariant{BaseTypeID: typeInt2, Value: int64(v)}, nil
	case int32:
		return SQLVariant{BaseTypeID: typeInt4, Value: int64(v)}, nil
	case int:
		if v >= -1<<31 && v <= 1<<31-1 {
			return SQLVariant{BaseTypeID: typeInt4, Value: int64(v)}, nil
		}
		return SQLVariant{BaseTypeID: typeInt8, Value: int64(v)}, nil
	case int64:
		return SQLVariant{BaseTypeID: typeInt8, Value: v}, nil
	case float32:
		return SQLVariant{BaseTypeID: typeFlt4, Value: float64(v)}, nil
	case float64:
		return SQLVariant{BaseTypeID: typeFlt8, Value: v}, nil
	case string:
		return SQLVariant{BaseTypeID: typeNVarChar, Value: v}, nil
	case []byte:
		return SQLVariant{BaseTypeID: typeBigVarBin, Value: v}, nil
	case time.Time:
		return SQLVariant{BaseTypeID: typeDateTimeOffsetN, Value: v, Scale: 7}, nil
	default:
		return SQLVariant{}, fmt.Errorf("mssql: unsupported sql_variant value type %T", val)
	}
}

func variantBaseTypeInfo(variant SQLVariant, explicit bool) (typeInfo, any, error) {
	ti := typeInfo{TypeId: variant.BaseTypeID}
	value := variant.Value
	var err error

	switch variant.BaseTypeID {
	case typeGuid:
		guid, ok := value.([]byte)
		if !ok {
			return ti, nil, fmt.Errorf("mssql: invalid value type for sql_variant uniqueidentifier: %T", value)
		}
		if len(guid) != 16 {
			return ti, nil, fmt.Errorf("mssql: invalid sql_variant uniqueidentifier length: %d", len(guid))
		}
		ti.Size = 16
	case typeBit:
		ti.Size = 1
	case typeInt1:
		ti.Size = 1
		value, err = variantInt64Value(value)
		if err != nil {
			return ti, nil, err
		}
	case typeInt2:
		ti.Size = 2
		value, err = variantInt64Value(value)
		if err != nil {
			return ti, nil, err
		}
	case typeInt4:
		ti.Size = 4
		value, err = variantInt64Value(value)
		if err != nil {
			return ti, nil, err
		}
	case typeInt8:
		ti.Size = 8
		value, err = variantInt64Value(value)
		if err != nil {
			return ti, nil, err
		}
	case typeDateTime:
		ti.Size = 8
	case typeDateTim4:
		ti.Size = 4
	case typeFlt4:
		ti.Size = 4
	case typeFlt8:
		ti.Size = 8
	case typeMoney4:
		ti.Size = 4
		value = variantStringValue(value)
	case typeMoney:
		ti.Size = 8
		value = variantStringValue(value)
	case typeDateN:
		ti.Size = 3
	case typeTimeN:
		ti.Scale = variantScale(variant.Scale, explicit)
		ti.Size = calcTimeSize(int(ti.Scale))
	case typeDateTime2N:
		ti.Scale = variantScale(variant.Scale, explicit)
		ti.Size = calcTimeSize(int(ti.Scale)) + 3
	case typeDateTimeOffsetN:
		ti.Scale = variantScale(variant.Scale, explicit)
		ti.Size = calcTimeSize(int(ti.Scale)) + 5
	case typeBigVarBin, typeBigBinary:
		bytesValue, ok := value.([]byte)
		if !ok {
			return ti, nil, fmt.Errorf("mssql: invalid value type for sql_variant binary: %T", value)
		}
		ti.Size = len(bytesValue)
	case typeDecimalN, typeNumericN:
		strValue, ok := variantStringValue(value).(string)
		if !ok {
			return ti, nil, fmt.Errorf("mssql: invalid value type for sql_variant decimal: %T", value)
		}
		scale := variant.Scale
		if scale == 0 {
			var err error
			scale, err = variantDecimalScaleFromString(strValue)
			if err != nil {
				return ti, nil, err
			}
		}
		prec, err := variantDecimalPrecision(strValue, scale)
		if err != nil {
			return ti, nil, err
		}
		ti.Prec = prec
		ti.Scale = scale
		value = strValue
	case typeBigVarChar, typeBigChar:
		strValue, ok := variantStringValue(value).(string)
		if !ok {
			return ti, nil, fmt.Errorf("mssql: invalid value type for sql_variant varchar: %T", value)
		}
		bytesValue := []byte(strValue)
		ti.Collation = utf8VariantCollation
		ti.Size = len(bytesValue)
		value = bytesValue
	case typeNVarChar, typeNChar:
		strValue, ok := variantStringValue(value).(string)
		if !ok {
			return ti, nil, fmt.Errorf("mssql: invalid value type for sql_variant nvarchar: %T", value)
		}
		bytesValue := str2ucs2(strValue)
		ti.Collation = utf8VariantCollation
		ti.Size = len(bytesValue)
		value = bytesValue
	default:
		return ti, nil, fmt.Errorf("mssql: unsupported sql_variant base type %#x", variant.BaseTypeID)
	}

	return ti, value, nil
}

func variantScale(scale uint8, explicit bool) uint8 {
	if explicit || scale != 0 {
		return scale
	}
	return 7
}

func variantInt64Value(value any) (int64, error) {
	switch v := value.(type) {
	case byte:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	default:
		return 0, fmt.Errorf("mssql: invalid value type for sql_variant integer: %T", value)
	}
}

func variantStringValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return value
	}
}

func variantDecimalScaleFromShopspring(value shopspring.Decimal) (uint8, error) {
	exponent := value.Exponent()
	if exponent >= 0 {
		return 0, nil
	}
	scale := -exponent
	if scale > 38 {
		return 0, fmt.Errorf("mssql: sql_variant decimal scale out of range: %d", scale)
	}
	return uint8(scale), nil
}

func variantDecimalScaleFromString(value string) (uint8, error) {
	value = strings.TrimSpace(value)
	dot := strings.IndexByte(value, '.')
	if dot < 0 {
		return 0, nil
	}
	scale := len(value) - dot - 1
	if scale > 38 {
		return 0, fmt.Errorf("mssql: sql_variant decimal scale out of range: %d", scale)
	}
	return uint8(scale), nil
}

func variantDecimalPrecision(value string, scale uint8) (uint8, error) {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "+")
	value = strings.TrimPrefix(value, "-")
	value = strings.ReplaceAll(value, ".", "")
	value = strings.TrimLeft(value, "0")

	precision := len(value)
	if precision == 0 {
		precision = 1
	}
	if precision < int(scale) {
		precision = int(scale)
	}
	if precision > 38 {
		return 0, fmt.Errorf("mssql: sql_variant decimal precision out of range: %d", precision)
	}
	return uint8(precision), nil
}

func variantProperties(ti typeInfo) ([]byte, error) {
	var props bytes.Buffer
	switch ti.TypeId {
	case typeDecimalN, typeNumericN:
		props.WriteByte(ti.Prec)
		props.WriteByte(ti.Scale)
	case typeTimeN, typeDateTime2N, typeDateTimeOffsetN:
		props.WriteByte(ti.Scale)
	case typeBigVarBin, typeBigBinary:
		if ti.Size > 0xffff {
			return nil, fmt.Errorf("mssql: sql_variant binary metadata too large: %d", ti.Size)
		}
		if err := binary.Write(&props, binary.LittleEndian, uint16(ti.Size)); err != nil {
			return nil, err
		}
	case typeBigVarChar, typeBigChar, typeNVarChar, typeNChar:
		if ti.Size > 0xffff {
			return nil, fmt.Errorf("mssql: sql_variant character metadata too large: %d", ti.Size)
		}
		if err := writeCollation(&props, ti.Collation); err != nil {
			return nil, err
		}
		if err := binary.Write(&props, binary.LittleEndian, uint16(ti.Size)); err != nil {
			return nil, err
		}
	}
	return props.Bytes(), nil
}

func (b *Bulk) dlogf(ctx context.Context, format string, v ...interface{}) {
	if b.Debug {
		b.cn.sess.LogF(ctx, msdsn.LogDebug, format, v...)
	}
}
