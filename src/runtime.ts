// Wraps all non-Web Runtime API used by btrdb

const _Deno = globalThis["Deno"];

// Just use Deno Runtime API when it's running on Deno
export let Runtime = !globalThis["Deno"] ? null! : {
  mkdir: _Deno.mkdir,
  remove: _Deno.remove,
  rename: _Deno.rename,
  writeTextFile: _Deno.writeTextFile,
  readTextFile: _Deno.readTextFile,
  test: _Deno.test,
  open: _Deno.open,
  inspect: _Deno.inspect,
  fdatasync: _Deno.fdatasync,
  customInspect: Symbol.for("Deno.customInspect"),
  env: _Deno.env,
  SeekMode: _Deno.SeekMode,
  File: _Deno.File,
  getRandomValues: crypto.getRandomValues,
};

if (!Runtime) {
  Runtime = {
    customInspect: Symbol.for("Deno.customInspect"),
  } as any;
}

export type RuntimeFile = Deno.File;
export type RuntimeInspectOptions = Deno.InspectOptions;

// When running on Node.js, change the runtime API implementation.
export function setRuntimeImplementaion(runtime: typeof Runtime) {
  Runtime = runtime;
}
