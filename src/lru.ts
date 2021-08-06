/** Implements LRU map, but no automatically removing */
export class LRUMap<K, T> {
  private map = new Map<K, Entry<K, T>>();
  private newest: Entry<K, T> | null = null;
  private oldest: Entry<K, T> | null = null;

  get count() {
    return this.map.size;
  }

  add(key: K, val: T) {
    const entry: Entry<K, T> = {
      key,
      value: val,
      older: this.newest,
      newer: null,
    };
    this.map.set(key, entry);
    if (this.newest !== null) this.newest.newer = entry;
    this.newest = entry;
    if (this.oldest === null) this.oldest = entry;
  }

  get(key: K): T | undefined {
    const entry = this.map.get(key);
    if (entry === undefined) return undefined;
    if (entry !== this.newest) {
      if (entry === this.oldest) {
        this.oldest = entry.newer;
        this.oldest!.older = null;
      } else {
        entry.newer!.older = entry.older;
        entry.older!.newer = entry.newer;
      }
      this.newest!.newer = entry;
      entry.older = this.newest;
      entry.newer = null;
      this.newest = entry;
    }
    return entry.value;
  }

  remove(key: K) {
    const entry = this.map.get(key);
    if (entry === undefined) return false;
    this.map.delete(key);
    if (entry === this.newest) {
      this.newest = entry.older;
      if (this.newest) this.newest.newer = null;
      else this.oldest = null;
    } else if (entry === this.oldest) {
      this.oldest = entry.newer;
      this.oldest!.older = null;
    } else {
      entry.newer!.older = entry.older;
      entry.older!.newer = entry.newer;
    }
    return true;
  }

  *valuesFromOldest() {
    for (let node = this.oldest; node !== null; node = node.newer) {
      yield node.value;
    }
  }
}

type Entry<K, T> = {
  key: K;
  value: T;
  older: Entry<K, T> | null;
  newer: Entry<K, T> | null;
};
