import { readUnsignedVarInt, readZigZagVarInt, readBitPackedInts } from '@parquet/util';
import BaseEncoding from './base';

interface DeltaHeader {
  blockSize: number;
  miniBlockPerBlock: number;
  miniSize: number;
  totalValues: number;
  firstValue: bigint;
}

export default class DeltaEncoding extends BaseEncoding {
  name: 'DELTA' = 'DELTA';

  decodeInt32s(count: number) {
    return this.decodeValues(count).map((int) => Number(int));
  }

  decodeInt64s(count: number) {
    return this.decodeValues(count);
  }

  decodeInt96s(count: number) {
    return this.decodeValues(count);
  }

  private decodeValues(count: number) {
    const header = this.readHeader();

    const values = [];

    while (values.length < header.totalValues) {
      const blockValues = this.readBlock(header);

      for (const value of blockValues) {
        if (values.length < count) {
          values.push(value);
        }
      }
    }

    return values;
  }

  private readHeader(): DeltaHeader {
    const blockSize = Number(readUnsignedVarInt(this.reader));
    const miniBlockPerBlock = Number(readUnsignedVarInt(this.reader));
    const miniSize = blockSize / miniBlockPerBlock;
    const totalValues = Number(readUnsignedVarInt(this.reader));
    const firstValue = readUnsignedVarInt(this.reader);

    if (miniSize % 8 !== 0) {
      throw new Error(`miniBlockSize must be multiple of 8, but it's ${miniSize}`);
    }

    return {
      blockSize,
      miniBlockPerBlock,
      miniSize,
      totalValues,
      firstValue,
    };
  }

  private readBlock(header: DeltaHeader) {
    const minDelta = readZigZagVarInt(this.reader);
    const bitWidths = new Array(header.miniBlockPerBlock);
    // TODO this should be not use header for subsequent blocks
    const values = [header.firstValue];

    let previous = header.firstValue;

    for (let i = 0; i < header.miniBlockPerBlock; i += 1) {
      bitWidths[i] = this.reader.readByte();
    }

    for (const bitWidth of bitWidths) {
      const deltas = readBitPackedInts(this.reader, header.miniSize, bitWidth);

      for (const delta of deltas) {
        previous = previous + minDelta + delta;

        values.push(previous);
      }
    }

    return values;
  }
}
