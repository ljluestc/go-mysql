package mysql

import (
	"strconv"

	"github.com/go-mysql-org/go-mysql/utils"
	"github.com/pingcap/errors"
)

type RowData []byte

func (p RowData) Parse(f []*Field, binary bool, dst []FieldValue) ([]FieldValue, error) {
	if binary {
		return p.ParseBinary(f, dst)
	} else {
		return p.ParseText(f, dst)
	}
}

func (p RowData) ParseText(f []*Field, dst []FieldValue) ([]FieldValue, error) {
	for len(dst) < len(f) {
		dst = append(dst, FieldValue{})
	}
	data := dst[:len(f)]

	var err error
	var v []byte
	var isNull bool
	var pos, n int

	for i := range f {
		v, isNull, n, err = LengthEncodedString(p[pos:])
		if err != nil {
			return nil, errors.Trace(err)
		}

		pos += n

		if isNull {
			data[i].Type = FieldValueTypeNull
		} else {
			isUnsigned := f[i].Flag&UNSIGNED_FLAG != 0

			switch f[i].Type {
			case MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_INT24,
				MYSQL_TYPE_LONGLONG, MYSQL_TYPE_LONG, MYSQL_TYPE_YEAR:
				if isUnsigned {
					var val uint64
					data[i].Type = FieldValueTypeUnsigned
					val, err = strconv.ParseUint(utils.ByteSliceToString(v), 10, 64)
					data[i].value = val
				} else {
					var val int64
					data[i].Type = FieldValueTypeSigned
					val, err = strconv.ParseInt(utils.ByteSliceToString(v), 10, 64)
					data[i].value = utils.Int64ToUint64(val)
				}
			case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:
				var val float64
				data[i].Type = FieldValueTypeFloat
				val, err = strconv.ParseFloat(utils.ByteSliceToString(v), 64)
				data[i].value = utils.Float64ToUint64(val)
			default:
				data[i].Type = FieldValueTypeString
				data[i].str = append(data[i].str[:0], v...)
			}

			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	return data, nil
}

// ParseBinary parses the binary format of data
// see https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
func (p RowData) ParseBinary(f []*Field, dst []FieldValue) ([]FieldValue, error) {
	for len(dst) < len(f) {
		dst = append(dst, FieldValue{})
	}
	data := dst[:len(f)]

	if p[0] != OK_HEADER {
		return nil, ErrMalformPacket
	}

	pos := 1 + ((len(f) + 7 + 2) >> 3)

	nullBitmap := p[1:pos]

	var isNull bool
	var n int
	var err error
	var v []byte
	for i := range data {
		if nullBitmap[(i+2)/8]&(1<<(uint(i+2)%8)) > 0 {
			data[i].Type = FieldValueTypeNull
			continue
		}

		isUnsigned := f[i].Flag&UNSIGNED_FLAG != 0

		switch f[i].Type {
		case MYSQL_TYPE_NULL:
			data[i].Type = FieldValueTypeNull
			continue

		case MYSQL_TYPE_TINY:
			if isUnsigned {
				v := ParseBinaryUint8(p[pos : pos+1])
				data[i].Type = FieldValueTypeUnsigned
				data[i].value = uint64(v)
			} else {
				v := ParseBinaryInt8(p[pos : pos+1])
				data[i].Type = FieldValueTypeSigned
				data[i].value = utils.Int64ToUint64(int64(v))
			}
			pos++
			continue

		case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
			if isUnsigned {
				v := ParseBinaryUint16(p[pos : pos+2])
				data[i].Type = FieldValueTypeUnsigned
				data[i].value = uint64(v)
			} else {
				v := ParseBinaryInt16(p[pos : pos+2])
				data[i].Type = FieldValueTypeSigned
				data[i].value = utils.Int64ToUint64(int64(v))
			}
			pos += 2
			continue

		case MYSQL_TYPE_INT24, MYSQL_TYPE_LONG:
			if isUnsigned {
				v := ParseBinaryUint32(p[pos : pos+4])
				data[i].Type = FieldValueTypeUnsigned
				data[i].value = uint64(v)
			} else {
				v := ParseBinaryInt32(p[pos : pos+4])
				data[i].Type = FieldValueTypeSigned
				data[i].value = utils.Int64ToUint64(int64(v))
			}
			pos += 4
			continue

		case MYSQL_TYPE_LONGLONG:
			if isUnsigned {
				v := ParseBinaryUint64(p[pos : pos+8])
				data[i].Type = FieldValueTypeUnsigned
				data[i].value = v
			} else {
				v := ParseBinaryInt64(p[pos : pos+8])
				data[i].Type = FieldValueTypeSigned
				data[i].value = utils.Int64ToUint64(v)
			}
			pos += 8
			continue

		case MYSQL_TYPE_FLOAT:
			v := ParseBinaryFloat32(p[pos : pos+4])
			data[i].Type = FieldValueTypeFloat
			data[i].value = utils.Float64ToUint64(float64(v))
			pos += 4
			continue

		case MYSQL_TYPE_DOUBLE:
			v := ParseBinaryFloat64(p[pos : pos+8])
			data[i].Type = FieldValueTypeFloat
			data[i].value = utils.Float64ToUint64(v)
			pos += 8
			continue

		case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB,
			MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING, MYSQL_TYPE_GEOMETRY, MYSQL_TYPE_JSON:
			v, isNull, n, err = LengthEncodedString(p[pos:])
			pos += n
			if err != nil {
				return nil, errors.Trace(err)
			}

			if !isNull {
				data[i].Type = FieldValueTypeString
				data[i].str = append(data[i].str[:0], v...)
				continue
			} else {
				data[i].Type = FieldValueTypeNull
				continue
			}

		case MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE:
			var num uint64
			num, isNull, n = LengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				data[i].Type = FieldValueTypeNull
				continue
			}

			data[i].Type = FieldValueTypeString
			data[i].str, err = FormatBinaryDate(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, errors.Trace(err)
			}

		case MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATETIME:
			var num uint64
			num, isNull, n = LengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				data[i].Type = FieldValueTypeNull
				continue
			}

			data[i].Type = FieldValueTypeString
			data[i].str, err = FormatBinaryDateTime(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, errors.Trace(err)
			}

		case MYSQL_TYPE_TIME:
			var num uint64
			num, isNull, n = LengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				data[i].Type = FieldValueTypeNull
				continue
			}

			data[i].Type = FieldValueTypeString
			data[i].str, err = FormatBinaryTime(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, errors.Trace(err)
			}

		default:
			return nil, errors.Errorf("Stmt Unknown FieldType %d %s", f[i].Type, f[i].Name)
		}
	}

	return data, nil
}

// WKB constants
const (
    WKBPoint   = 0x00000001
    WKBPolygon = 0x00000003
)

// ParseWKBPoint parses a POINT from WKB format
func ParseWKBPoint(data []byte) (x, y float64, err error) {
    if len(data) < 25 || binary.LittleEndian.Uint32(data[1:5]) != WKBPoint {
        return 0, 0, fmt.Errorf("invalid WKB POINT data: %x", data)
    }
    x = binary.LittleEndian.Float64(data[9:17])  // X coordinate
    y = binary.LittleEndian.Float64(data[17:25]) // Y coordinate
    return x, y, nil
}

// ParseWKBPolygon parses a POLYGON from WKB format
func ParseWKBPolygon(data []byte) ([][][2]float64, error) {
    if len(data) < 13 || binary.LittleEndian.Uint32(data[1:5]) != WKBPolygon {
        return nil, fmt.Errorf("invalid WKB POLYGON data: %x", data)
    }
    numRings := binary.LittleEndian.Uint32(data[9:13])
    if len(data) < int(13+numRings*4) {
        return nil, fmt.Errorf("incomplete WKB POLYGON data")
    }

    rings := make([][][2]float64, numRings)
    offset := 13
    for i := uint32(0); i < numRings; i++ {
        numPoints := binary.LittleEndian.Uint32(data[offset : offset+4])
        offset += 4
        if len(data) < int(offset+numPoints*16) {
            return nil, fmt.Errorf("incomplete POLYGON ring data")
        }
        points := make([][2]float64, numPoints)
        for j := uint32(0); j < numPoints; j++ {
            x := binary.LittleEndian.Float64(data[offset : offset+8])
            y := binary.LittleEndian.Float64(data[offset+8 : offset+16])
            points[j] = [2]float64{x, y}
            offset += 16
        }
        rings[i] = points
    }
    return rings, nil
}

// readRowData parses binary row data from a binlog event
func (r *RowsEvent) readRowData(table *schema.Table, data []byte) ([]interface{}, error) {
    values := make([]interface{}, len(table.Columns))
    nullBitmap := data[:((len(table.Columns)+7)/8)]
    data = data[((len(table.Columns)+7)/8):]

    pos := 0
    for i, col := range table.Columns {
        if nullBitmap[i/8]&(1<<(uint(i)%8)) != 0 {
            values[i] = nil
            continue
        }

        switch col.Type {
        case MYSQL_TYPE_TINY:
            values[i] = int8(data[pos])
            pos++
        case MYSQL_TYPE_SHORT:
            values[i] = int16(binary.LittleEndian.Uint16(data[pos : pos+2]))
            pos += 2
        case MYSQL_TYPE_INT24:
            values[i] = int32(ParseBinaryInt24(data[pos : pos+3]))
            pos += 3
        case MYSQL_TYPE_LONG:
            values[i] = int32(binary.LittleEndian.Uint32(data[pos : pos+4]))
            pos += 4
        case MYSQL_TYPE_LONGLONG:
            values[i] = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
            pos += 8
        case MYSQL_TYPE_FLOAT:
            values[i] = float32(binary.LittleEndian.Uint32(data[pos : pos+4]))
            pos += 4
        case MYSQL_TYPE_DOUBLE:
            values[i] = float64(binary.LittleEndian.Uint64(data[pos : pos+8]))
            pos += 8
        case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_STRING, MYSQL_TYPE_VAR_STRING:
            length := int(data[pos])
            if length >= 255 {
                length = int(binary.LittleEndian.Uint16(data[pos : pos+2]))
                pos += 2
            } else {
                pos++
            }
            values[i] = string(data[pos : pos+length])
            pos += length
        case MYSQL_TYPE_NEWDECIMAL:
            // Simplified decimal handling (adjust as per actual implementation)
            length := int(data[pos])
            pos++
            values[i] = string(data[pos : pos+length])
            pos += length
        case MYSQL_TYPE_GEOMETRY:
            // Handle geospatial data
            length := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
            pos += 2
            geoData := data[pos : pos+length]
            pos += length
            switch col.RawType {
            case "point":
                x, y, err := ParseWKBPoint(geoData)
                if err != nil {
                    return nil, fmt.Errorf("failed to parse POINT at column %d: %v", i, err)
                }
                values[i] = fmt.Sprintf("POINT(%f %f)", x, y) // WKT format
            case "polygon":
                rings, err := ParseWKBPolygon(geoData)
                if err != nil {
                    return nil, fmt.Errorf("failed to parse POLYGON at column %d: %v", i, err)
                }
                // Convert to WKT format for simplicity
                var wkt strings.Builder
                wkt.WriteString("POLYGON(")
                for j, ring := range rings {
                    if j > 0 {
                        wkt.WriteString(",")
                    }
                    wkt.WriteString("(")
                    for k, point := range ring {
                        if k > 0 {
                            wkt.WriteString(",")
                        }
                        fmt.Fprintf(&wkt, "%f %f", point[0], point[1])
                    }
                    wkt.WriteString(")")
                }
                wkt.WriteString(")")
                values[i] = wkt.String()
            default:
                return nil, fmt.Errorf("unsupported geometry type: %s", col.RawType)
            }
        default:
            return nil, fmt.Errorf("unsupported column type %d at column %d", col.Type, i)
        }
    }
    return values, nil
}