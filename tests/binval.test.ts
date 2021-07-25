import { decodeValue, encodeValue } from "../src/binval.ts";
import { Buffer } from "../src/buffer.ts";
import { assertEquals } from "./test.dep.ts";

function genNumbers(len: number) {
  return new Array(len).fill(0).map((x, i) => i);
}

const values = [
  null,
  undefined,
  false,
  true,
  "",
  "abc",
  "abcdefghijklmnopqrstuvwxyz12345",
  "abcdefghijklmnopqrstuvwxyz123456",
  "abcdefghijklmnopqrstuvwxyz1234567",
  [],
  genNumbers(4),
  genNumbers(8),
  genNumbers(9),
  genNumbers(128),
  genNumbers(128),
  new Uint8Array(genNumbers(0)),
  new Uint8Array(genNumbers(4)),
  new Uint8Array(genNumbers(32)),
  new Uint8Array(genNumbers(33)),
  new Uint8Array(genNumbers(128)),
  [1, 253, 254, 255, 256, 65535, 65536, 2 ** 16, 2 ** 32, 2 ** 52 - 1],
  [NaN, 0, 1.1],
  {},
  { a: 1, b: 2, c: 3, d: 4 },
  { a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, h: 8, i: 9, j: 10 },
];

Deno.test("binval", () => {
  const buffer = new Buffer(new Uint8Array(1024), 0);
  for (const val of values) {
    // console.info(val);
    buffer.pos = 0;
    encodeValue(val, buffer);
    // console.info(buffer.buffer.slice(0, buffer.pos));
    buffer.pos = 0;
    assertEquals(decodeValue(buffer), val);
  }
});
