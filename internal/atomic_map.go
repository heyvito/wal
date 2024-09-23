package internal

import "sync"

type AtomicMap[K comparable, V any] struct {
	m sync.Map
}

func (a *AtomicMap[K, V]) Load(key K) (value V, ok bool) {
	v, ok := a.m.Load(key)
	if !ok {
		return
	}
	value = v.(V)
	return
}

func (a *AtomicMap[K, V]) Store(key K, value V) {
	a.m.Store(key, value)
}

func (a *AtomicMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	v, l := a.m.LoadOrStore(key, value)
	if l {
		loaded = true
		actual = v.(V)
	}
	return
}

func (a *AtomicMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, l := a.m.LoadAndDelete(key)
	if l {
		loaded = true
		value = v.(V)
	}
	return
}

func (a *AtomicMap[K, V]) Delete(key K) {
	a.m.Delete(key)
}

func (a *AtomicMap[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	p, l := a.m.Swap(key, value)
	if l {
		loaded = true
		previous = p.(V)
	}
	return
}

func (a *AtomicMap[K, V]) CompareAndSwap(key K, old, new V) bool {
	return a.m.CompareAndSwap(key, old, new)
}

func (a *AtomicMap[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	return a.m.CompareAndDelete(key, old)
}

func (a *AtomicMap[K, V]) Range() func(func(key K, value V) bool) {
	return func(yield func(key K, value V) bool) {
		a.m.Range(func(key, value any) bool {
			return yield(key.(K), value.(V))
		})
	}
}
