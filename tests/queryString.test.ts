import { query } from "../mod.ts";
import { assertThrows } from "./test.dep.ts";

Deno.test("should throw on invalid query", () => {
  assertThrows(() => {
    query`
      id > ${1} SKIP ${1} SKIP ${2}
    `;
  });

  assertThrows(() => {
    query`
      id > ${1} LIMIT ${1} LIMIT ${2}
    `;
  });

  assertThrows(() => {
    query`
      id > ${1} LIMIT not_a_value
    `;
  });
});
