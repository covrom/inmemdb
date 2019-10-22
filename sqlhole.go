package inmemdb

import (
	"database/sql/driver"
	"reflect"
)

type GreyHole struct {
	T     reflect.Type
	V     interface{}
	Valid bool
}

func (b *GreyHole) Scan(value interface{}) error {
	var err error
	if b.T != nil {
		b.V, err = ConvertToType(value, b.T)
		b.Valid = value != nil
	} else {
		b.V = nil
		b.Valid = false
	}
	return err
}

func (b GreyHole) Value() (driver.Value, error) {
	if !b.Valid {
		return nil, nil
	}
	return b.V, nil
}
