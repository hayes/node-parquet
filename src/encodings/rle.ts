import BaseEncoding from './base';
import ByteReader from '../reader/byte';
import { readBitPackedInts } from '../byte-utils';

export default class RunLengthEncoding extends BaseEncoding {
  name: 'RLE' = 'RLE';
  bitWidth: number;
  disableEnvelope: boolean;

  constructor(reader: ByteReader, bitWidth: number, disableEnvelope: boolean = false) {
    super(reader);

    this.bitWidth = bitWidth;
    this.disableEnvelope = disableEnvelope;
  }

  decodeInt32s(count: number) {
    return this.decodeValues(count).map((int) => Number(int));
  }

  decodeInt64s(count: number) {
    return this.decodeValues(count);
  }

  decodeInt96s(count: number) {
    return this.decodeValues(count);
  }

  decodeBooleans(count: number) {
    return this.decodeValues(count).map(Boolean);
  }

  private decodeValues(count: number) {
    if (!this.disableEnvelope) {
      this.reader.consume(4);
    }

    const values: bigint[] = [];

    while (values.length < count) {
      const header = BigInt(this.reader.readVarint());

      const size = header >> 1n;

      if (header & 1n) {
        values.push(...readBitPackedInts(this.reader, size * 8n, this.bitWidth));
      } else {
        const bytes = Math.ceil(this.bitWidth / 8);
        let value = 0n;

        for (let i = 0n; i < bytes; i += 1n) {
          value += (value << 8n) + BigInt(this.reader.readByte());
        }

        values.push(...(new Array(Number(size)).fill(value) as bigint[]));
      }
    }

    return values.slice(0, count);
  }
}
