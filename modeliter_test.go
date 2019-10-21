package inmemdb

import (
	"reflect"
	"testing"
)

type TestMO struct {
	ID   UUIDv4
	Name String
}

func (t TestMO) StoreName() string { return "testmo" }

func TestColumnIterator(t *testing.T) {
	tt := TestMO{}
	md, _ := NewModelDescription(reflect.TypeOf(t), tt.StoreName())
	namefd, _ := md.GetColumnByFieldName("Name")

	mt := NewModelTable(md, 10)

	mo := NewModelObject(md)
	mo.SetIDField(NewV4())
	mo.SetField(namefd, "test3")
	if err := mt.Upsert(mo); err != nil {
		t.Fatal(err)
	}

	mo = NewModelObject(md)
	mo.SetIDField(NewV4())
	mo.SetField(namefd, "test2")
	if err := mt.Upsert(mo); err != nil {
		t.Fatal(err)
	}

	mo = NewModelObject(md)
	mo.SetIDField(NewV4())
	mo.SetField(namefd, "test1")
	if err := mt.Upsert(mo); err != nil {
		t.Fatal(err)
	}

	mi := mt.CreateIndex(namefd)
	coliter := NewColumnIterator(mi, nil)
	for coliter.HasNext() {
		t.Log(coliter.NextID())
	}
}
