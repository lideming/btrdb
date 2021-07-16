// Wraps all non-Web Runtime API used by btrdb

// Just use Deno Runtime API when it's running on Deno
export let Runtime = !globalThis["Deno"] ? null! : {
  mkdir: Deno.mkdir,
  remove: Deno.remove,
  writeTextFile: Deno.writeTextFile,
  test: Deno.test,
  open: Deno.open,
  inspect: Deno.inspect,
  fdatasync: Deno.fdatasync,
  customInspect: Symbol.for("Deno.customInspect"),
  env: Deno.env,
  SeekMode: Deno.SeekMode,
  File: Deno.File,
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
