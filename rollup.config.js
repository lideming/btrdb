import { terser } from "rollup-plugin-terser";
import typescript from "@rollup/plugin-typescript";

function options(input, output, outputName) {
  return {
    input: input,
    output: [
      {
        file: output + ".js",
        format: "umd",
        name: outputName,
      },
      {
        file: output + ".mjs",
        format: "es",
        name: outputName,
      },
      {
        file: output + ".min.js",
        format: "umd",
        name: outputName,
        sourcemap: true,
        plugins: [terser()],
      },
    ],
    external: ["fs", "process", "fs/promises", "util"],
    plugins: [
      {
        /** @param code {string} */
        transform(code, id) {
          return code.replaceAll('.ts"', '"');
        },
      },
      typescript(),
      {
        resolveId(id) {
          if (id == "https://deno.land/std@0.100.0/testing/asserts") {
            return "./dist/asserts.mjs";
          }
        },
      },
    ],
  };
}

/** @type {import('rollup').RollupOptions} */
export default [
  options("nodeCompat/mod.ts", "dist/btrdb", "btrdb"),
  options("nodeCompat/nodeTest.js", "dist/nodeTest", "nodeTest"),
];
