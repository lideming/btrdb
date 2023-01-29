import { Runtime } from "../utils/runtime.ts";

let bytes: Uint8Array | null = null;

export function nanoid(size = 21) {
  let id = "";
  if (!bytes || bytes.length != size) {
    bytes = new Uint8Array(size);
  }
  Runtime.getRandomValues(bytes);
  while (size--) {
    let byte = bytes[size] & 63;
    if (byte < 36) {
      id += byte.toString(36);
    } else if (byte < 62) {
      id += (byte - 26).toString(36).toUpperCase();
    } else if (byte < 63) {
      id += "_";
    } else {
      id += "-";
    }
  }
  return id;
}
