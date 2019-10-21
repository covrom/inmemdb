package inmemdb

type MergeIterator struct {
	iterators   []IDIterator
	currid      ModelSortable
	minheap     *IDAccHeap
	cardinality int
	min, max    ModelSortable
	lastJumpTo  ModelSortable
	lastJumpOk  bool
}

func NewMergeIterator(iterators ...IDIterator) *MergeIterator {
	if len(iterators) == 0 {
		panic("iterators not defined")
	}

	h := NewIDAccHeap(len(iterators))
	InitIDAccHeap(h)
	maxSz := 0
	var l, r ModelSortable

	for i, it := range iterators {
		if it == nil {
			continue
		}
		il, ir := it.Range()
		if i == 0 || il.ModelLess(l) {
			l = il
		}
		if i == 0 || r.ModelLess(ir) {
			r = ir
		}
		lenList := it.Cardinality()
		if lenList > maxSz {
			maxSz = lenList
		}
		if it.HasNext() {
			PushIDAccHeap(h, ElemHeapIDAcc{
				ID:       it.NextID(),
				Iterator: it,
			})
		}
	}

	return &MergeIterator{
		iterators:   iterators,
		minheap:     h,
		min:         l,
		max:         r,
		cardinality: maxSz,
	}
}

func (iter *MergeIterator) Clone() IDIterator {
	rv := &MergeIterator{}
	*rv = *iter
	rv.minheap = iter.minheap.Clone()
	rv.iterators = make([]IDIterator, len(iter.iterators))
	for i := range rv.minheap.Elems {
		rv.iterators[i] = rv.minheap.Elems[i].Iterator
	}
	return rv
}

func (iter *MergeIterator) JumpTo(id ModelSortable) bool {
	if iter.lastJumpTo == id {
		return iter.lastJumpOk
	}
	iter.lastJumpTo = id

	iter.minheap.Elems = iter.minheap.Elems[:0]

	ok := false

	for _, it := range iter.iterators {
		if it == nil {
			continue
		}
		cok := it.JumpTo(id)
		ok = ok || cok
		if cok {
			PushIDAccHeap(iter.minheap, ElemHeapIDAcc{
				ID:       it.NextID(),
				Iterator: it,
			})
		}
	}
	if ok {
		iter.currid = iter.minheap.Elems[0].ID
	} else {
		iter.currid = nil
	}
	iter.lastJumpOk = ok
	return ok
}

func (iter *MergeIterator) Cardinality() int {
	return iter.cardinality
}

func (iter *MergeIterator) Range() (l ModelSortable, r ModelSortable) {
	return iter.min, iter.max
}

func (iter *MergeIterator) HasNext() bool {
	for iter.minheap.Len() > 0 {
		me := iter.minheap.Elems[0] // Peek at the top element in heap.
		if iter.currid == nil || !me.ID.ModelEqual(iter.currid) {
			iter.currid = me.ID // Add if unique.
			return true
		}
		if !me.Iterator.HasNext() {
			PopIDAccHeap(iter.minheap)
		} else {
			val := me.Iterator.NextID()
			iter.minheap.Elems[0].ID = val
			FixIDAccHeap(iter.minheap, 0) // Faster than Pop() followed by Push().
		}
	}
	return false
}

func (iter *MergeIterator) NextID() ModelSortable {
	return iter.currid
}
