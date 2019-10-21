package inmemdb

import "sort"

type IntersectIterator struct {
	iterators    []IDIterator
	iterdiffs    []IDIterator
	currid       ModelSortable
	lastJumpTo   ModelSortable
	lastJumpOk   bool
	notIntersect bool
}

func NewIteratorIntersect() *IntersectIterator {
	return &IntersectIterator{
		iterators: make([]IDIterator, 0, 10),
		iterdiffs: make([]IDIterator, 0, 2),
	}
}

func (iter *IntersectIterator) Clone() IDIterator {
	rv := &IntersectIterator{}
	*rv = *iter
	return rv
}

func (iter *IntersectIterator) Append(iterator IDIterator) {
	if iterator == nil {
		return
	}
	ln := len(iter.iterators)
	idx := sort.Search(ln, func(i int) bool {
		return iter.iterators[i].Cardinality() >= iterator.Cardinality()
	})
	iter.iterators = append(iter.iterators, iterator)
	if idx < ln {
		copy(iter.iterators[idx+1:], iter.iterators[idx:])
		iter.iterators[idx] = iterator
	}

check:
	for i, it := range iter.iterators {
		imin, imax := it.Range()
		if imin == nil && imax == nil {
			iter.notIntersect = true
			break check
		}
		for j := i + 1; j < len(iter.iterators); j++ {
			jmin, jmax := iter.iterators[j].Range()
			if jmin == nil && jmax == nil {
				iter.notIntersect = true
				break check
			}
			// imin imax < jmin jmax
			// jmin jmax < imin imax
			if jmax.ModelLess(imin) || imax.ModelLess(jmin) {
				iter.notIntersect = true
				break check
			}
		}
	}
}

// at least one iterator needed for successful difference
func (iter *IntersectIterator) AppendDiff(iterator IDIterator) {
	if iterator == nil {
		return
	}
	ln := len(iter.iterdiffs)
	idx := sort.Search(ln, func(i int) bool {
		return iter.iterdiffs[i].Cardinality() <= iterator.Cardinality()
	})
	iter.iterdiffs = append(iter.iterdiffs, iterator)
	if idx < ln {
		copy(iter.iterdiffs[idx+1:], iter.iterdiffs[idx:])
		iter.iterdiffs[idx] = iterator
	}
}

func (iter *IntersectIterator) Size() int {
	return len(iter.iterators)
}

func (iter *IntersectIterator) SizeDiffs() int {
	return len(iter.iterdiffs)
}

func (iter *IntersectIterator) Iter(n int) IDIterator {
	return iter.iterators[n]
}

func (iter *IntersectIterator) IterDiff(n int) IDIterator {
	return iter.iterdiffs[n]
}

func (iter *IntersectIterator) JumpTo(id ModelSortable) bool {
	if iter.lastJumpTo == id {
		return iter.lastJumpOk
	}
	iter.lastJumpTo = id

	neq := false
	eqid := id

	for i, it := range iter.iterators {
		ok := it.JumpTo(id)
		if !ok {
			iter.lastJumpOk = false
			return false
		}
		if i == 0 {
			eqid = it.NextID()
		} else if !neq {
			v := it.NextID()
			if !v.ModelEqual(eqid) {
				neq = true
			}
		}
	}

	for _, it := range iter.iterdiffs {
		ok := it.JumpTo(id)
		if ok && !neq {
			v := it.NextID()
			if v.ModelEqual(eqid) {
				neq = true
			}
		}
	}

	if neq {
		ok := iter.HasNext()
		iter.lastJumpOk = ok
		return ok
	}

	iter.currid = eqid
	iter.lastJumpOk = true
	return true
}

func (iter *IntersectIterator) Cardinality() int {
	return iter.iterators[0].Cardinality()
}

func (iter *IntersectIterator) Range() (ModelSortable, ModelSortable) {
	return iter.iterators[0].Range()
}

func (iter *IntersectIterator) HasNext() bool {

	if iter.notIntersect {
		return false
	}

retry:
	for _, it := range iter.iterators {
		if !it.HasNext() {
			return false
		}
	}

	it0 := iter.iterators[0]
	cmpID := it0.NextID()
	iidx := 1
	for {
		if iidx >= len(iter.iterators) {
			for _, it := range iter.iterdiffs {
				ok := it.JumpTo(cmpID)
				if ok {
					if it.NextID().ModelEqual(cmpID) {
						goto retry
					}
				}
			}
			break
		}
		it := iter.iterators[iidx]
		v := it.NextID()
		if v.ModelEqual(cmpID) {
			iidx++
		} else {
			if cmpID.ModelLess(v) {
				if !it0.JumpTo(v) {
					return false
				}
				cmpID = it0.NextID()
				iidx = 1
			} else {
				// v < cmpID
				if !it.JumpTo(cmpID) {
					return false
				}
			}
		}
	}
	iter.currid = cmpID
	return iidx >= len(iter.iterators)
}

func (iter *IntersectIterator) NextID() ModelSortable {
	return iter.currid
}
