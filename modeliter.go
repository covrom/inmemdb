package inmemdb

// IterColumner must be sorted by key in ascending order
type IterColumner interface {
	Key(i int) ModelSortable
	Len() int
}

type IDIterator interface {
	HasNext() bool
	NextID() ModelSortable
	JumpTo(ModelSortable) bool
	Range() (ModelSortable, ModelSortable)
	Cardinality() int
	Clone() IDIterator
}

func NewColumnIterator(c IterColumner, filterSkip func(idx ModelSortable) bool) *ColumnIterator {
	return &ColumnIterator{
		pos:        -1,
		col:        c,
		maxpos:     c.Len() - 1,
		minpos:     0,
		filterSkip: filterSkip,
	}
}

type ColumnIterator struct {
	pos        int
	minpos     int
	maxpos     int
	col        IterColumner
	filterSkip func(idx ModelSortable) bool
	lastJumpTo ModelSortable
	lastJumpOk bool
}

func (iter *ColumnIterator) Clone() IDIterator {
	rv := &ColumnIterator{}
	*rv = *iter
	return rv
}

func (iter *ColumnIterator) Cardinality() int {
	return iter.maxpos - iter.minpos + 1
}

func (iter *ColumnIterator) Range() (ModelSortable, ModelSortable) {
	a, b := iter.col.Key(iter.minpos), iter.col.Key(iter.maxpos)
	if b.ModelLess(a) {
		a, b = b, a
	}
	return a, b
}

func (iter *ColumnIterator) JumpTo(id ModelSortable) bool {
	if iter.lastJumpTo.ModelEqual(id) {
		return iter.lastJumpOk
	}
	iter.lastJumpTo = id
	newpos := id
	if newpos.ModelLess(iter.col.Key(iter.minpos)) || iter.col.Key(iter.maxpos).ModelLess(newpos) {
		iter.lastJumpOk = false
		return false
	}
	if iter.col.Key(iter.pos).ModelEqual(newpos) {
		iter.lastJumpOk = true
		return true
	}

	n := iter.col.Len()
	i, j := 0, n

	for i < j {
		h := (i + j) >> 1
		if iter.col.Key(h).ModelLess(id) {
			i = h + 1
		} else {
			j = h
		}
	}

	iter.pos = i - 1
	iter.lastJumpOk = iter.HasNext()
	return iter.lastJumpOk
}

func (iter *ColumnIterator) HasNext() bool {
	ipos, imin, imax := iter.pos, iter.minpos, iter.maxpos
	ipos++
	var ikey ModelSortable
	if ipos >= imin && ipos <= imax {
		ikey = iter.col.Key(ipos)
		for iter.filterSkip != nil && iter.filterSkip(ikey) {
			ipos++
			if ipos < imin || ipos > imax {
				break
			}
			ikey = iter.col.Key(ipos)
		}
	}
	res := ipos >= imin && ipos <= imax
	if res {
		iter.pos = ipos
		iter.lastJumpTo = ikey
		iter.lastJumpOk = true
	}
	return res
}

func (iter *ColumnIterator) NextID() ModelSortable {
	return iter.col.Key(iter.pos)
}
