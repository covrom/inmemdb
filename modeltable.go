package inmemdb

import (
	"fmt"
)

type ModelSortable interface {
	ModelLess(ModelSortable) bool
	ModelEqual(ModelSortable) bool
}

type KV struct {
	K ModelSortable
	V ModelSortable // IdField in ModelTable.md
}

type ModelIndex struct {
	kvs []KV
}

func NewModelIndex(capacity int) *ModelIndex {
	return &ModelIndex{
		kvs: make([]KV, 0, capacity),
	}
}

func searchKV(a []KV, x KV, i, n uint32) uint32 {
	if n > 0 {
		j := n
		for i < j {
			h := (i + j) >> 1
			if a[h].K.ModelLess(x.K) || (a[h].K.ModelEqual(x.K) && a[h].V.ModelLess(x.V)) {
				i = h + 1
			} else {
				j = h
			}
		}
	}
	return i
}

func searchK(a []KV, x ModelSortable, i, n uint32) uint32 {
	if n > 0 {
		j := n
		for i < j {
			h := (i + j) >> 1
			if a[h].K.ModelLess(x) {
				i = h + 1
			} else {
				j = h
			}
		}
	}
	return i
}

func (mi *ModelIndex) Insert(kv KV) {
	ln := uint32(len(mi.kvs))
	idx := searchKV(mi.kvs, kv, 0, ln)
	mi.kvs = append(mi.kvs, kv)
	if idx < ln {
		copy(mi.kvs[idx+1:], mi.kvs[idx:])
		mi.kvs[idx] = kv
	}
}

func (mi *ModelIndex) Delete(kv KV) {
	ln := uint32(len(mi.kvs))
	idx := searchKV(mi.kvs, kv, 0, ln)
	if idx < ln && mi.kvs[idx].K.ModelEqual(kv.K) && mi.kvs[idx].V.ModelEqual(kv.V) {
		copy(mi.kvs[idx:], mi.kvs[idx+1:])
		mi.kvs = mi.kvs[:len(mi.kvs)-1]
	}
}

func (mi *ModelIndex) DeleteAllForKey(kk ModelSortable) {
	ln := uint32(len(mi.kvs))
	idxl := searchK(mi.kvs, kk, 0, ln)
	if idxl < ln && mi.kvs[idxl].K.ModelEqual(kk) {
		lndel := uint32(1)
		for idxl+lndel < ln && mi.kvs[idxl+lndel].K.ModelEqual(kk) {
			lndel++
		}
		if idxl+lndel <= ln {
			copy(mi.kvs[idxl:], mi.kvs[idxl+lndel:])
			mi.kvs = mi.kvs[:len(mi.kvs)-int(lndel)]
		}
	}
}

// IterColumner interface
func (mi *ModelIndex) Key(i int) ModelSortable { return mi.kvs[i].K }
func (mi *ModelIndex) Len() int                { return len(mi.kvs) }

type ModelTable struct {
	md   *ModelDescription
	t    []ModelObject // sorted by IdField ascending, that must implements ModelSortable
	idxs []*ModelIndex // index in slice is index of field in md.ColumnPtrs, that values must implements ModelSortable
}

func NewModelTable(md *ModelDescription, capacity int) *ModelTable {
	mt := &ModelTable{
		md:   md,
		t:    make([]ModelObject, 0, capacity),
		idxs: make([]*ModelIndex, len(md.ColumnPtrs)),
	}
	return mt
}

func (mt *ModelTable) searchMO(x ModelSortable, fIdx, i, n uint32) uint32 {
	if n > 0 {
		j := n
		for i < j {
			h := (i + j) >> 1
			if mt.t[h].v[fIdx].(ModelSortable).ModelLess(x) {
				i = h + 1
			} else {
				j = h
			}
		}
	}
	return i
}

func (mt *ModelTable) Upsert(mo ModelObject) error {
	if mo.md != mt.md {
		return fmt.Errorf("model description for model object is not equal to model table model object")
	}
	idIdx := uint32(mt.md.IdField.Idx)
	smo, ok := mo.v[idIdx].(ModelSortable)
	if !ok {
		return fmt.Errorf("model object not implements sortable interface")
	}
	ln := uint32(len(mt.t))
	idx := mt.searchMO(smo, idIdx, 0, ln)
	if idx == ln || !smo.ModelEqual(mt.t[idx].v[idIdx].(ModelSortable)) {
		mt.t = append(mt.t, mo)
		if idx < ln {
			copy(mt.t[idx+1:], mt.t[idx:])
			mt.t[idx] = mo
		}
	} else {
		for imi, mi := range mt.idxs {
			if mi == nil {
				continue
			}
			mi.Delete(KV{
				K: mt.t[idx].v[mt.md.ColumnPtrs[imi].Idx].(ModelSortable),
				V: smo,
			})
		}
		mt.t[idx] = mo
	}
	for imi, mi := range mt.idxs {
		if mi == nil {
			continue
		}
		mi.Insert(KV{
			K: mo.v[mt.md.ColumnPtrs[imi].Idx].(ModelSortable),
			V: smo,
		})
	}
	return nil
}

func (mt *ModelTable) CreateIndex(fd *FieldDescription) *ModelIndex {
	mi := NewModelIndex(cap(mt.t))
	for _, mo := range mt.t {
		mi.Insert(KV{
			K: mo.v[fd.Idx].(ModelSortable),
			V: mo.v[mo.md.IdField.Idx].(ModelSortable),
		})
	}
	mt.idxs[fd.Idx] = mi
	return mi
}

func (mt *ModelTable) DeleteIndex(fd *FieldDescription) {
	mt.idxs[fd.Idx] = nil
}

func (mt *ModelTable) HasIndex(fd *FieldDescription) bool {
	return mt.idxs[fd.Idx] != nil
}

// IterColumner interface
func (mt *ModelTable) Key(i int) ModelSortable { return mt.t[i].IDField().(ModelSortable) }
func (mt *ModelTable) Len() int                { return len(mt.t) }
