const resolved = Promise.resolve();

export type Deferred<T> = Promise<T> & {
  resolve(val: T): void;
  reject(err: any): void;
};

export function deferred<T>(): Deferred<T> {
  let resolve, reject;
  var prom = new Promise((r, rej) => {
    resolve = r, reject = rej;
  }) as any;
  prom.resolve = resolve;
  prom.reject = reject;
  return prom;
}

export class OneWriterLock {
  readers = 0;
  writers = 0;
  pendingReaders = 0;
  wakeAllReaders: Deferred<void> | null = null;
  wakeWriters: Deferred<void>[] = [];

  enterReader() {
    if (!this.writers) {
      this.readers++;
      return resolved;
    } else {
      if (!this.wakeAllReaders) this.wakeAllReaders = deferred();
      this.pendingReaders++;
      return this.wakeAllReaders;
    }
  }

  exitReader() {
    if (this.writers != 0 || this.readers <= 0) throw new Error("BUG");
    this.readers--;
    if (this.wakeWriters.length && this.readers == 0 && this.writers == 0) {
      this.wakeWriters.pop()!.resolve();
      this.writers++;
    }
  }

  enterWriterFromReader() {
    if (this.writers != 0 || this.readers <= 0) throw new Error("BUG");
    this.readers--;
    return this.enterWriter(true);
  }

  enterWriter(asap = false) {
    if (!this.writers && !this.readers) {
      this.writers++;
      return resolved;
    } else {
      const wait = deferred<void>();
      if (asap) this.wakeWriters.unshift(wait);
      else this.wakeWriters.push(wait);
      return wait;
    }
  }

  exitWriter() {
    if (this.writers != 1 || this.readers != 0) {
      throw new Error("BUG, " + this.writers + ", " + this.readers);
    }
    this.writers--;
    if (Math.random() < 0.5) {
      // Prefer to wake reader rather than writer
      if (this.wakeAllReaders) {
        this.wakeAllReaders.resolve();
        this.wakeAllReaders = null;
        this.readers = this.pendingReaders;
        this.pendingReaders = 0;
      } else if (this.wakeWriters.length) {
        this.wakeWriters.pop()!.resolve();
        this.writers++;
      }
    } else {
      // Prefer to wake writer rather than reader
      if (this.wakeWriters.length) {
        this.wakeWriters.pop()!.resolve();
        this.writers++;
      } else if (this.wakeAllReaders) {
        this.wakeAllReaders.resolve();
        this.wakeAllReaders = null;
        this.readers = this.pendingReaders;
        this.pendingReaders = 0;
      }
    }
  }
}
