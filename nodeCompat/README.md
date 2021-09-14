# Compatibility layer for Node.js

In [mod.ts](mod.ts), we have implemented Deno APIs that are used by btrdb (see
[runtime.ts](../src/runtime.ts)).

And we use `rollup` to make UMD and ESM bundles that can run on Node.js (see
build script in [package.json](../package.json)).
