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
  private _prefer = false;

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
    this.exitReader();
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
    this._prefer = !this._prefer;
    if (this._prefer) {
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

export interface Task {
  run(): Promise<void>;
}

export class TaskQueue<T extends Task> {
  tasks: T[] = [];
  running: Promise<void> | null = null;

  enqueue(task: T) {
    this.tasks.push(task);
    if (this.tasks.length == 1) {
      this._run();
    }
  }

  waitCurrentLastTask() {
    if (!this.tasks.length) return Promise.resolve();
    const toWait = this.tasks[this.tasks.length - 1];
    return this.waitTask(toWait);
  }

  async waitTask(toWait: T) {
    do {
      await this.running;
    } while (this.tasks.indexOf(toWait) > 0);
    return await this.running!;
  }

  private async _run() {
    while (this.tasks.length) {
      this.running = this.tasks[0]!.run();
      // Assuming no errors
      await this.running;
      this.tasks.shift();
    }
    this.running = null;
  }
}
