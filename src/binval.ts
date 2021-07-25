// "binval" - Yet another BSON
// See `docs/dev_binval.md`

import { Buffer, decoder, DynamicBuffer, encoder } from "./buffer.ts";

export function encodeValue(val: any) {
  const buf = new DynamicBuffer();
  writeValue(val, buf);
  return buf.buffer.subarray(0, buf.pos);
}

export function decodeValue(buf: Uint8Array) {
  return readValue(new Buffer(buf, 0));
}

export function writeValue(obj: any, buf: Buffer) {
  if (obj === null) {
    buf.writeU8(0);
  } else if (obj === undefined) {
    buf.writeU8(1);
  } else if (obj === false) {
    buf.writeU8(2);
  } else if (obj === true) {
    buf.writeU8(3);
  } else if (typeof obj === "number") {
    if (Number.isInteger(obj)) {
      if (obj >= -7 && obj <= 127) {
        if (obj > 0) {
          buf.writeU8(128 + obj);
        } else if (obj < 0) {
          buf.writeU8(127 + obj);
        } else {
          if (Object.is(obj, -0)) {
            buf.writeU8(127);
          } else { // +0
            buf.writeU8(128);
          }
        }
      } else {
        let negative = 0;
        if (obj < 0) {
          obj = -obj;
          negative = 3;
        }
        if (obj < 256) {
          buf.writeU8(4 + negative);
          buf.writeU8(obj);
        } else if (obj < 65536) {
          buf.writeU8(5 + negative);
          buf.writeU16(obj);
        } else if (obj < 2 ** 32) {
          buf.writeU8(6 + negative);
          buf.writeU32(obj);
        } else {
          // float64
          buf.writeU8(10);
          buf.writeF64(negative ? -obj : obj);
        }
      }
    } else {
      buf.writeU8(10);
      buf.writeF64(obj);
    }
  } else if (typeof obj === "string") {
    const encoded = encoder.encode(obj);
    if (encoded.length <= 32) {
      buf.writeU8(87 + encoded.length);
    } else {
      buf.writeU8(11);
      buf.writeEncodedUint(encoded.length);
    }
    buf.writeBuffer(encoded);
  } else if (typeof obj === "object") {
    if (obj instanceof Array) {
      if (obj.length <= 8) {
        buf.writeU8(36 + obj.length);
      } else {
        buf.writeU8(14);
        buf.writeEncodedUint(obj.length);
      }
      for (const val of obj) {
        writeValue(val, buf);
      }
    } else if (obj instanceof Uint8Array) {
      if (obj.length <= 32) {
        buf.writeU8(54 + obj.byteLength);
      } else {
        buf.writeU8(12);
        buf.writeEncodedUint(obj.byteLength);
      }
      buf.writeBuffer(obj);
    } else {
      const keys = Object.keys(obj);
      if (keys.length <= 8) {
        buf.writeU8(45 + keys.length);
      } else {
        buf.writeU8(13);
        buf.writeEncodedUint(keys.length);
      }
      for (const key of keys) {
        buf.writeString(key);
        writeValue(obj[key], buf);
      }
    }
  } else {
    throw new Error("Unsupported value " + obj);
  }
}

export function readValue(buf: Buffer) {
  const type = buf.readU8();
  return decodeMap[type](buf, type);
}

const decodeMap: Array<(buf: Buffer, type: number) => any> = [];

// 0 ~ 3
decodeMap.push(() => null);
decodeMap.push(() => undefined);
decodeMap.push(() => false);
decodeMap.push(() => true);

// big number
// 4 ~ 6
decodeMap.push((buf) => buf.readU8());
decodeMap.push((buf) => buf.readU16());
decodeMap.push((buf) => buf.readU32());
// 7 ~ 9
decodeMap.push((buf) => -buf.readU8());
decodeMap.push((buf) => -buf.readU16());
decodeMap.push((buf) => -buf.readU32());
// 10
decodeMap.push((buf) => buf.readF64());

// 11 big string
decodeMap.push((buf) => buf.readString());

// 12 big blob
decodeMap.push((buf) => {
  const len = buf.readEncodedUint();
  return buf.readBuffer(len);
});

// 13 big object
decodeMap.push((buf) => decodeObject(buf, buf.readEncodedUint()));

// 14 big array
decodeMap.push((buf) => decodeArray(buf, buf.readEncodedUint()));

// 15 ~ 35 (unused)
for (let i = 15; i <= 35; i++) {
  decodeMap.push(decodeUnused);
}

// 36 ~ 44 small array
for (let i = 0; i <= 8; i++) {
  decodeMap.push(decodeSmallArray);
}

// 45 ~ 53 small object
for (let i = 0; i <= 8; i++) {
  decodeMap.push(decodeSmallObject);
}

// 54 ~ 86 small blob
for (let i = 0; i <= 32; i++) {
  decodeMap.push(decodeSmallBlob);
}

// 87 ~ 119 small string
decodeMap.push(() => "");
for (let i = 1; i <= 32; i++) {
  decodeMap.push(decodeSmallString);
}

// 120 ~ 255 small number
for (let i = 120; i <= 126; i++) {
  decodeMap.push(decodeSmallNegativeNumber);
}
// 127 "-0"
decodeMap.push(() => -0);
for (let i = 128; i <= 255; i++) {
  decodeMap.push(decodeSmallPositiveNumber);
}

function decodeSmallArray(buf: Buffer, type: number) {
  return decodeArray(buf, type - 36);
}

function decodeSmallObject(buf: Buffer, type: number) {
  return decodeObject(buf, type - 45);
}

function decodeSmallBlob(buf: Buffer, type: number) {
  return buf.readBuffer(type - 54);
}

function decodeSmallString(buf: Buffer, type: number) {
  const buffer = buf.readBufferReadonly(type - 87);
  return decoder.decode(buffer);
}

function decodeSmallNegativeNumber(buf: Buffer, type: number) {
  return type - 127;
}

function decodeSmallPositiveNumber(buf: Buffer, type: number) {
  return type - 128;
}

function decodeUnused(buf: Buffer, type: number) {
  throw new Error("Unsupported value type " + type);
}

function decodeObject(buf: Buffer, propCount: number) {
  let obj: any = {};
  for (let i = 0; i < propCount; i++) {
    const key = buf.readString();
    obj[key] = readValue(buf);
  }
  return obj;
}

function decodeArray(buf: Buffer, itemCount: number) {
  let arr: any[] = [];
  for (let i = 0; i < itemCount; i++) {
    arr.push(readValue(buf));
  }
  return arr;
}

[].forEach((x) => {
  console.info();
});
