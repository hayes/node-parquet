import * as varint from 'varint';
import { TFramedTransport, TCompactProtocol, TProtocol } from 'thrift';

export default class ByteReader {
  buffer: Buffer;
  offset: number = 0;
  end: number;

  constructor(buffer: Buffer, offset = 0, end = buffer.length) {
    this.buffer = buffer;
    this.offset = offset;
    this.end = end;
  }

  remaining() {
    return Math.max(this.end - this.offset, 0);
  }

  child(size: number, consume = false) {
    const child = new ByteReader(this.buffer, this.offset, this.offset + size);

    if (consume) {
      this.offset += size;
    }

    return child;
  }

  rest() {
    const buf = this.buffer.slice(this.offset, this.end);

    this.offset = this.end;

    return buf;
  }

  readByte() {
    return this.buffer[this.offset++];
  }

  consume(count: number) {
    this.offset += count;
  }

  readVarint() {
    const int = varint.decode(this.buffer, this.offset);

    this.offset += varint.encodingLength(int);

    return int;
  }

  readInt32() {
    const int = this.buffer.readInt32LE(this.offset);
    this.offset += 4;

    return int;
  }

  // TODO: fallback for older versions of node
  readInt64() {
    const int = this.buffer.readBigInt64LE(this.offset);
    this.offset += 8;

    return int;
  }

  readInt96() {
    const low = this.buffer.readBigUInt64LE(this.offset);
    const high = this.buffer.readInt32LE(this.offset + 8);
    this.offset += 12;

    return low + (BigInt(high) << 64n);
  }

  readFloat() {
    const float = this.buffer.readFloatLE(this.offset);
    this.offset += 4;

    return float;
  }

  readDouble() {
    const double = this.buffer.readDoubleLE(this.offset);
    this.offset += 8;

    return double;
  }

  readByteArray() {
    const length = this.readInt32();

    this.offset += length;

    return this.buffer.slice(this.offset - length, this.offset);
  }

  readFixedLengthByteArray(length: number) {
    return this.buffer.slice(this.offset - length, this.offset);
  }

  readBooleans(count: number) {
    const values = [];

    for (let i = 0; values.length < count; i += 1) {
      const byte = this.readByte();

      for (let j = 0; j < 8 && values.length < count; j += 1) {
        values.push((byte & (1 << j % 8)) > 0);
      }
    }

    return values;
  }

  readVarints(count: number) {
    const values = [];

    for (let i = 0; i < count; ++i) {
      values.push(this.readVarint());
    }

    return values;
  }

  readInt32s(count: number) {
    const values = [];

    for (let i = 0; i < count; ++i) {
      values.push(this.readInt32());
    }

    return values;
  }

  readInt64s(count: number) {
    const values = [];

    for (let i = 0; i < count; ++i) {
      values.push(this.readInt64());
    }

    return values;
  }

  readInt96s(count: number) {
    const values = [];

    for (let i = 0; i < count; ++i) {
      values.push(this.readInt96());
    }

    return values;
  }

  readFloats(count: number) {
    const values = [];

    for (let i = 0; i < count; ++i) {
      values.push(this.readFloat());
    }

    return values;
  }

  readDoubles(count: number) {
    const values = [];

    for (let i = 0; i < count; ++i) {
      values.push(this.readDouble());
    }

    return values;
  }

  readByteArrays(count: number) {
    const values = [];

    for (let i = 0; i < count; ++i) {
      values.push(this.readByteArray());
    }

    return values;
  }

  readFixedLengthByteArrays(count: number, length: number) {
    const values = [];

    for (let i = 0; i < count; ++i) {
      values.push(this.readFixedLengthByteArray(length));
    }

    return values;
  }

  decodeThriftObject<T>(Class: { new (): T }) {
    const obj = new Class();

    const transport = new TFramedTransport(this.buffer) as TFramedTransport & { readPos: number };
    const protocol = new TCompactProtocol(transport);

    transport.readPos = this.offset;

    (obj as T & { read: (protocol: TProtocol) => void }).read(protocol);

    this.offset += transport.readPos - this.offset;

    return obj;
  }
}
