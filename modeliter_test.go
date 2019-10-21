package inmemdb

import (
	"reflect"
	"testing"
)

type TestMO struct {
	ID   UUIDv4
	Name String
}

func TestColumnIterator(t *testing.T) {
	md, _ := NewModelDescription(reflect.TypeOf(TestMO{}), "testmo")
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
