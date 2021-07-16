import {
  Runtime as orignalRuntime,
  RuntimeFile,
  setRuntimeImplementaion,
} from "../src/runtime.ts";
export * from "../mod.ts";

const global = globalThis as any;
if (!global["Deno"]) {
  const Runtime: typeof orignalRuntime = {} as any;
  setRuntimeImplementaion(Runtime);

  const util: any = await import("util");
  const fs: any = await import("fs");
  const fsPromises: any = await import("fs/promises");

  Runtime.inspect = util.inspect;

  enum SeekMode {
    Start = 0,
    Current = 1,
    End = 2,
  }

  Runtime.SeekMode = SeekMode;

  Runtime.mkdir = fsPromises.mkdir;

  Runtime.test = function () {};

  Runtime.remove = (path: string | URL) => fsPromises.rm(path);

  class File {
    pos = 0;
    get rid() {
      return this.fh.fd;
    }
    constructor(readonly fh: any) {
    }
    write(p: Uint8Array): Promise<number> {
      return this.fh.write(p, 0, p.byteLength, this.pos);
    }
    truncate(len?: number): Promise<void> {
      return this.fh.truncate(len);
    }
    read(p: Uint8Array): Promise<number | null> {
      return this.fh.read(p, 0, p.byteLength, this.pos);
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

  Runtime.open = async function (
    path: string | URL,
    options?: Deno.OpenOptions,
  ): Promise<RuntimeFile> {
    return new File((await fsPromises.open(path, "a+"))) as any;
  };

  Runtime.fdatasync = function (fd: number) {
    return new Promise<void>((resolve, reject) => {
      fs.fdatasync(fd, (err: any) => {
        if (!err) resolve();
        else reject(err);
      });
    });
  };
}
