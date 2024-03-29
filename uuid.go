package inmemdb

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	"github.com/google/uuid"
)

type UUIDv4 struct {
	uuid.UUID
}

func init() {
	gob.Register(&UUIDv4{})
}

func (u UUIDv4) Value() (driver.Value, error) {
	b, err := u.MarshalText()
	return b, err
}

func (u *UUIDv4) Scan(src interface{}) error {
	switch src := src.(type) {
	case nil:
		return nil

	case string:
		// if an empty UUID comes from a table, we return a null UUID
		if src == "" {
			return nil
		}

		// see Parse for required string format
		uu, err := uuid.Parse(src)
		if err != nil {
			return fmt.Errorf("Scan: %v", err)
		}
		u.UUID = uu

	case []byte:
		// if an empty UUID comes from a table, we return a null UUID
		if len(src) == 0 {
			return nil
		}

		// assumes a simple slice of bytes if 16 bytes
		// otherwise attempts to parse
		if len(src) != 16 {
			// see Parse for required string format
			uu, err := uuid.ParseBytes(src)
			if err != nil {
				return fmt.Errorf("Scan: %v", err)
			}
			u.UUID = uu
		} else {
			copy(u.UUID[:], src)
		}

	default:
		return fmt.Errorf("Scan: unable to scan type %T into UUID", src)
	}

	return nil
}

func (u UUIDv4) IsZero() bool {
	return binary.BigEndian.Uint64(u.UUID[0:8]) == 0 && binary.BigEndian.Uint64(u.UUID[8:16]) == 0
}

func (u UUIDv4) String() string {
	return u.UUID.String()
}

func (u UUIDv4) GobEncode() ([]byte, error) {
	return u.MarshalBinary()
}

func (u *UUIDv4) GobDecode(data []byte) error {
	return (&(u.UUID)).UnmarshalBinary(data)
}

func NewV4() UUIDv4 {
	return UUIDv4{uuid.New()}
}

func FromString(s string) (UUIDv4, error) {
	id, err := uuid.Parse(s)
	return UUIDv4{id}, err
}

// store.Converter interface, u must contain zero value before call
func (u *UUIDv4) ConvertFrom(v interface{}) error {
	if v == nil {
		return nil
	}
	switch vv := v.(type) {
	case UUIDv4:
		*u = vv
		return nil
	case *UUIDv4:
		*u = *vv
		return nil
	}
	if err := u.Scan(v); err != nil {
		return err
	}
	return nil
}

// ModelSortable interface
func (u UUIDv4) ModelLess(ms ModelSortable) bool {
	mv := ms.(UUIDv4).UUID
	return bytes.Compare(u.UUID[:16], mv[:16]) < 0
}
func (u UUIDv4) ModelEqual(ms ModelSortable) bool {
	mv := ms.(UUIDv4).UUID
	return bytes.Compare(u.UUID[:16], mv[:16]) == 0
}
