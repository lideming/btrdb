const global = globalThis as any;
if (!global["Deno"]) {
  const Deno: any = {};
  global.Deno = Deno;

  const util: any = await import("util");
  const fsPromises: any = await import("fs/promises");

  Deno.inspect = util.inspect;

  enum SeekMode {
    Start = 0,
    Current = 1,
    End = 2,
  }

  Deno.SeekMode = SeekMode;

  Object.assign(Deno, fsPromises);

  Deno.test = function () {};

  Deno.remove = (path: string) => fsPromises.rm(path);

  class File {
    pos = 0;
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

  Deno.open = async function (
    path: string | URL,
    options?: Deno.OpenOptions,
  ): Promise<File> {
    return new File((await fsPromises.open(path, "a+")));
  };
}

export * from "../mod.ts";
