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
    buf.writeU8(Type.Null);
  } else if (obj === undefined) {
    buf.writeU8(Type.Undefined);
  } else if (obj === false) {
    buf.writeU8(Type.False);
  } else if (obj === true) {
    buf.writeU8(Type.True);
  } else if (typeof obj === "number") {
    if (Number.isInteger(obj)) {
      if (obj >= -7 && obj <= 127) {
        if (obj > 0) {
          buf.writeU8(Type.Number0 + obj);
        } else if (obj < 0) {
          buf.writeU8(Type.NumberNeg0 + obj);
        } else {
          if (Object.is(obj, -0)) {
            buf.writeU8(Type.NumberNeg0);
          } else { // +0
            buf.writeU8(Type.Number0);
          }
        }
      } else {
        let negative = 0;
        if (obj < 0) {
          obj = -obj;
          negative = 3;
        }
        if (obj < 256) {
          buf.writeU8(Type.Uint8 + negative);
          buf.writeU8(obj);
        } else if (obj < 65536) {
          buf.writeU8(Type.Uint16 + negative);
          buf.writeU16(obj);
        } else if (obj < 2 ** 32) {
          buf.writeU8(Type.Uint32 + negative);
          buf.writeU32(obj);
        } else {
          // float64
          buf.writeU8(Type.Float64);
          buf.writeF64(negative ? -obj : obj);
        }
      }
    } else {
      buf.writeU8(Type.Float64);
      buf.writeF64(obj);
    }
  } else if (typeof obj === "string") {
    const len = Buffer.calcStringSize(obj);
    if (len <= 32) {
      buf.writeU8(Type.String0 + len);
    } else {
      buf.writeU8(Type.String);
      buf.writeEncodedUint(len);
    }
    buf.beforeWriting(len);
    const r = encoder.encodeInto(obj, buf.buffer.subarray(buf.pos));
    if (r.written !== len) {
      throw new Error("Expect " + r.written + " == " + len);
    }
    buf.pos += len;
  } else if (typeof obj === "object") {
    if (obj instanceof Array) {
      if (obj.length <= 8) {
        buf.writeU8(Type.Array0 + obj.length);
      } else {
        buf.writeU8(Type.Array);
        buf.writeEncodedUint(obj.length);
      }
      for (const val of obj) {
        writeValue(val, buf);
      }
    } else if (obj instanceof Uint8Array) {
      if (obj.length <= 32) {
        buf.writeU8(Type.Binary0 + obj.byteLength);
      } else {
        buf.writeU8(Type.Binary);
        buf.writeEncodedUint(obj.byteLength);
      }
      buf.writeBuffer(obj);
    } else {
      const keys = Object.keys(obj);
      if (keys.length <= 8) {
        buf.writeU8(Type.Object0 + keys.length);
      } else {
        buf.writeU8(Type.Object);
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

export function calcEncodedLength(obj: any): number {
  if (obj === null) {
    return 1;
  } else if (obj === undefined) {
    return 1;
  } else if (obj === false) {
    return 1;
  } else if (obj === true) {
    return 1;
  } else if (typeof obj === "number") {
    if (Number.isInteger(obj)) {
      if (obj >= -7 && obj <= 127) {
        return 1;
      } else {
        if (obj < 0) obj = -obj;
        if (obj < 256) {
          return 2;
        } else if (obj < 65536) {
          return 3;
        } else if (obj < 2 ** 32) {
          return 5;
        } else {
          // float64
          return 9;
        }
      }
    } else {
      return 9;
    }
  } else if (typeof obj === "string") {
    const len = Buffer.calcStringSize(obj);
    if (len <= 32) {
      return 1 + len;
    } else {
      return 1 + Buffer.calcEncodedUintSize(len) + len;
    }
  } else if (typeof obj === "object") {
    if (obj instanceof Array) {
      let len = 0;
      if (obj.length <= 8) {
        len += 1;
      } else {
        len += 1 + Buffer.calcEncodedUintSize(obj.length);
      }
      for (const val of obj) {
        len += calcEncodedLength(val);
      }
      return len;
    } else if (obj instanceof Uint8Array) {
      let len = 0;
      if (obj.length <= 32) {
        len += 1;
      } else {
        len += 1 + Buffer.calcEncodedUintSize(obj.byteLength);
      }
      len += obj.byteLength;
      return len;
    } else {
      let len = 0;
      const keys = Object.keys(obj);
      if (keys.length <= 8) {
        len += 1;
      } else {
        len += 1 + Buffer.calcEncodedUintSize(keys.length);
      }
      for (const key of keys) {
        len += Buffer.calcLenEncodedStringSize(key);
        len += calcEncodedLength(obj[key]);
      }
      return len;
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
  return decodeArray(buf, type - Type.Array0);
}

function decodeSmallObject(buf: Buffer, type: number) {
  return decodeObject(buf, type - Type.Object0);
}

function decodeSmallBlob(buf: Buffer, type: number) {
  return buf.readBuffer(type - Type.Binary0);
}

function decodeSmallString(buf: Buffer, type: number) {
  const buffer = buf.readBufferReadonly(type - Type.String0);
  return decoder.decode(buffer);
}

function decodeSmallNegativeNumber(buf: Buffer, type: number) {
  return type - Type.NumberNeg0;
}

function decodeSmallPositiveNumber(buf: Buffer, type: number) {
  return type - Type.Number0;
}

function decodeUnused(buf: Buffer, type: number) {
  throw new Error("Unsupported value type " + type);
}

function BinvalObject() {}
(BinvalObject as any).prototype = Object.create(null);
declare class BinvalObject {}

function decodeObject(buf: Buffer, propCount: number) {
  let obj: any = new BinvalObject();
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

const enum Type {
  Null = 0,
  Undefined,
  False,
  True,
  Uint8,
  Uint16,
  Uint32,
  NegUint8,
  NegUint16,
  NegUint32,
  Float64,
  String,
  Binary,
  Object,
  Array,
  // 15 ~ 35 not used
  Array0 = 36,
  Object0 = 45,
  Binary0 = 54,
  String0 = 87,
  NumberNeg0 = 127,
  Number0 = 128,
}
