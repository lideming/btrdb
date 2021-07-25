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
    this.buffer[this.pos++] = (num >> 8) & 0xff;
    this.buffer[this.pos++] = num & 0xff;
  }
  readU16() {
    return this.buffer[this.pos++] << 8 |
      this.buffer[this.pos++];
  }
  writeU8(num: number) {
    this.buffer[this.pos++] = num & 0xff;
  }
  readU8() {
    return this.buffer[this.pos++];
  }
  writeBuffer(buf: Uint8Array) {
    this.buffer.set(buf, this.pos);
    this.pos += buf.length;
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
    var buf = encoder.encode(str);
    this.writeLenEncodedBuffer(buf);
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
    return Buffer.calcLenEncodedBufferSize(encoder.encode(str));
  }
  static calcLenEncodedBufferSize(buf: Uint8Array) {
    return Buffer.calcEncodedUintSize(buf.length) + buf.length;
  }
  static calcEncodedUintSize(len: number) {
    return (len < 254) ? 1 : (len < 65536) ? 3 : 5;
  }
}
