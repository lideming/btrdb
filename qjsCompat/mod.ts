import {
  Runtime as orignalRuntime,
  RuntimeFile,
  setRuntimeImplementaion,
} from "../src/runtime.ts";

export * from "../mod.ts";

import { doit } from "./EncoderDecoderTogether.min.js";

doit();

import * as std from "std";
import * as os from "os";

const global = globalThis as any;
if (!global["Deno"]) {
  const Runtime: typeof orignalRuntime = {} as any;
  setRuntimeImplementaion(Runtime);

  console.info = console.warn = console.error = console.log;

  console.time = console.timeEnd = () => {};

  globalThis.URL = class URL {};
  globalThis.WeakRef = class URL {};

  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  Runtime.encode = (str: string) => encoder.encode(str),
    Runtime.decode = (buffer: any) => decoder.decode(buffer),
    Runtime.inspect = (obj) => {
      return "(inspect not implemented)";
    };

  enum SeekMode {
    Start = 0,
    Current = 1,
    End = 2,
  }

  const checkErrno = (errno: number) => {
    if (errno) {
      throw new Error("errno: " + std.strerror(errno));
    }
  };

  Runtime.SeekMode = SeekMode;

  Runtime.mkdir = (path: string | URL, options?: Deno.MkdirOptions) => {
    os.mkdir(path);
    return Promise.resolve();
  };

  Runtime.test = function () {};

  Runtime.remove = (path: string | URL) => {
    checkErrno(os.remove(path));
    return Promise.resolve();
  };

  Runtime.rename = (oldPath: string | URL, newPath: string | URL) => {
    checkErrno(os.rename(oldPath, newPath));
    return Promise.resolve();
  };

  class File {
    constructor(readonly fd: number, readonly path: string) {
    }
    write(p: Uint8Array): Promise<number> {
      const r = os.write(this.fd, p.buffer, p.byteOffset, p.byteLength);
      if (r < 0) throw new Error("write() error: " + std.strerror(-r));
      return Promise.resolve(r);
    }
    truncate(len?: number): Promise<void> {
      throw new Error("Not implemented.");
    }
    read(p: Uint8Array): Promise<number | null> {
      const r = os.read(this.fd, p.buffer, p.byteOffset, p.byteLength);
      if (r < 0) throw new Error("read() error: " + std.strerror(-r));
      return Promise.resolve(r);
    }
    async seek(offset: number, whence: SeekMode): Promise<number> {
      return Promise.resolve(os.seek(this.fd, offset, whence));
    }
    stat(): Promise<any> {
      const r = os.stat(this.path);
      return Promise.resolve(r[0]);
    }
    close(): void {
      os.close(this.fd);
    }
  }

  Runtime.open = async function (
    path: string | URL,
    options?: Deno.OpenOptions,
  ): Promise<RuntimeFile> {
    const fd = os.open(path, os.O_CREAT | os.O_RDWR);
    return new File(fd, path as string) as any;
  };

  Runtime.fdatasync = function (fd: number) {
    // Not implemented
    return Promise.resolve();
  };
}
