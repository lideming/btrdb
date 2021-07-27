export const encoder = new TextEncoder();
export const decoder = new TextDecoder();

const tmpbuf = new ArrayBuffer(8);
const f64arr = new Float64Array(tmpbuf);
const u8arr = new Uint8Array(tmpbuf);

export class Buffer {
  constructor(
    public buffer: Uint8Array,
    public pos: number,
  ) {}

  writeF64(num: number) {
    this.beforeWriting(8);
    f64arr[0] = num;
    this.writeBuffer(u8arr);
  }
  readF64() {
    for (let i = 0; i < 8; i++) {
      u8arr[i] = this.buffer[this.pos + i];
    }
    this.pos += 8;
    return f64arr[0];
  }
  writeU32(num: number) {
    this.beforeWriting(4);
    this.buffer[this.pos++] = (num >>> 24) & 0xff;
    this.buffer[this.pos++] = (num >>> 16) & 0xff;
    this.buffer[this.pos++] = (num >>> 8) & 0xff;
    this.buffer[this.pos++] = num & 0xff;
  }
  readU32() {
    return (
      this.buffer[this.pos++] << 24 |
      this.buffer[this.pos++] << 16 |
      this.buffer[this.pos++] << 8 |
      this.buffer[this.pos++]
    ) >>> 0;
  }
  writeU16(num: number) {
    this.beforeWriting(2);
    this.buffer[this.pos++] = (num >> 8) & 0xff;
    this.buffer[this.pos++] = num & 0xff;
  }
  readU16() {
    return this.buffer[this.pos++] << 8 |
      this.buffer[this.pos++];
  }
  writeU8(num: number) {
    this.beforeWriting(1);
    this.buffer[this.pos++] = num & 0xff;
  }
  readU8() {
    return this.buffer[this.pos++];
  }
  writeBuffer(buf: Uint8Array) {
    this.beforeWriting(buf.byteLength);
    try {
      this.buffer.set(buf, this.pos);
    } catch {
      debugger;
    }
    this.pos += buf.byteLength;
  }
  readBuffer(len: number) {
    var buf = this.buffer.slice(this.pos, this.pos + len);
    this.pos += len;
    return buf;
  }
  readBufferReadonly(len: number) {
    var buf = this.buffer.subarray(this.pos, this.pos + len);
    this.pos += len;
    return buf;
  }
  writeString(str: string) {
    const len = Buffer.calcStringSize(str);
    this.writeEncodedUint(len);
    this.beforeWriting(len);
    const r = encoder.encodeInto(str, this.buffer.subarray(this.pos));
    this.pos += r.written;
  }
  writeLenEncodedBuffer(buf: Uint8Array) {
    this.writeEncodedUint(buf.length);
    this.writeBuffer(buf);
  }
  writeEncodedUint(val: number) {
    if (val < 254) {
      this.writeU8(val);
    } else if (val < 65536) {
      this.writeU8(254);
      this.writeU16(val);
    } else {
      this.writeU8(255);
      this.writeU32(val);
    }
  }
  readEncodedUint() {
    var val = this.readU8();
    if (val < 254) {
      return val;
    } else if (val == 254) {
      return this.readU16();
    } else {
      return this.readU32();
    }
  }
  readString() {
    const len = this.readEncodedUint();
    const str = decoder.decode(this.buffer.subarray(this.pos, this.pos + len));
    this.pos += len;
    return str;
  }
  static calcStringSize(str: string) {
    if (nativeStringSize !== undefined) return nativeStringSize(str);
    let bytes = 0;
    const len = str.length;
    for (let i = 0; i < len; i++) {
      const codePoint = str.charCodeAt(i);
      if (codePoint < 0x80) {
        bytes += 1;
      } else if (codePoint < 0x800) {
        bytes += 2;
      } else if (codePoint >= 0xD800 && codePoint < 0xE000) {
        if (codePoint < 0xDC00 && i + 1 < len) {
          const next = str.charCodeAt(i + 1);
          if (next >= 0xDC00 && next < 0xE000) {
            bytes += 4;
            i++;
          } else {
            bytes += 3;
          }
        } else {
          bytes += 3;
        }
      } else {
        bytes += 3;
      }
    }
    return bytes;
  }
  static calcLenEncodedStringSize(str: string) {
    const len = Buffer.calcStringSize(str);
    return Buffer.calcEncodedUintSize(len) + len;
  }
  static calcLenEncodedBufferSize(buf: Uint8Array) {
    return Buffer.calcEncodedUintSize(buf.length) + buf.length;
  }
  static calcEncodedUintSize(len: number) {
    return (len < 254) ? 1 : (len < 65536) ? 3 : 5;
  }

  beforeWriting(size: number) {}
}

const _globalThis = globalThis as any;

const nativeStringSize: undefined | ((str: string) => number) = (
  _globalThis?.Buffer?.byteLength ?? undefined
);

export class DynamicBuffer extends Buffer {
  constructor(initSize = 32) {
    super(new Uint8Array(initSize), 0);
  }
  beforeWriting(size: number) {
    const minsize = this.pos + size;
    if (minsize > this.buffer.byteLength) {
      let newsize = this.buffer.byteLength * 4;
      while (minsize > newsize) {
        newsize *= 4;
      }
      const newBuffer = new Uint8Array(newsize);
      newBuffer.set(this.buffer);
      this.buffer = newBuffer;
    }
  }
}
