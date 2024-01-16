import {
  Runtime as orignalRuntime,
  RuntimeFile,
  setRuntimeImplementaion,
} from "../src/utils/runtime.ts";
import * as btrdb from "../mod.ts";

// @ts-expect-error
module.exports = btrdb;

const global = globalThis as any;
if (!global["Deno"]) {
  // @ts-expect-error
  const util = require("util");
  // @ts-expect-error
  const fs = require("fs");
  // @ts-expect-error
  const fsPromises = require("fs/promises");
  // @ts-expect-error
  const crypto = require("crypto");
  // @ts-expect-error
  const process = require("process");

  const Runtime: typeof orignalRuntime = {} as any;
  setRuntimeImplementaion(Runtime);

  Runtime.inspect = util.inspect;

  enum SeekMode {
    Start = 0,
    Current = 1,
    End = 2,
  }

  Runtime.SeekMode = SeekMode;

  Runtime.mkdir = fsPromises.mkdir;

  Runtime.test = function () {} as any;

  Runtime.remove = (path: string | URL) => fsPromises.rm(path);

  Runtime.rename = (oldPath: string | URL, newPath: string | URL) =>
    fsPromises.rename(oldPath, newPath);

  Runtime.writeTextFile = (path, text, _options) =>
    fsPromises.writeFile(path, text);

  Runtime.readTextFile = (path: string | URL) =>
    fsPromises.readFile(path, "utf-8");

  class File {
    pos = 0;
    get rid() {
      return this.fh.fd;
    }
    constructor(readonly fh: any) {
    }
    async write(p: Uint8Array): Promise<number> {
      const { bytesWritten } = await this.fh.write(
        p,
        0,
        p.byteLength,
        this.pos,
      );
      this.pos += bytesWritten;
      return bytesWritten;
    }
    truncate(len?: number): Promise<void> {
      return this.fh.truncate(len);
    }
    async read(p: Uint8Array): Promise<number | null> {
      const { bytesRead } = await this.fh.read(p, 0, p.byteLength, this.pos);
      this.pos += bytesRead;
      return bytesRead;
    }
    async seek(offset: number, whence: SeekMode): Promise<number> {
      if (whence == SeekMode.Start) {
        this.pos = offset;
      } else if (whence == SeekMode.Current) {
        this.pos += offset;
      } else {
        this.pos = (await this.stat()).size + offset;
      }
      return this.pos;
    }
    stat(): Promise<any> {
      return this.fh.stat();
    }
    close(): void {
      this.fh.close();
    }
  }

  const { O_RDWR, O_CREAT } = fs.constants;

  Runtime.open = async function (
    path: string | URL,
    options?: Deno.OpenOptions,
  ): Promise<RuntimeFile> {
    return new File(await fsPromises.open(path, O_RDWR | O_CREAT)) as any;
  };

  Runtime.fdatasync = function (fd: number) {
    return new Promise<void>((resolve, reject) => {
      fs.fdatasync(fd, (err: any) => {
        if (!err) resolve();
        else reject(err);
      });
    });
  };

  Runtime.getRandomValues = crypto.randomFillSync;

  Runtime.memoryUsage = process.memoryUsage;

  Runtime.stat = fsPromises.stat;
}
