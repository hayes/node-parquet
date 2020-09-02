import { ByteReader } from '@parquet/util';
import BaseEncoding from './base';
import RunLengthEncoding from './rle';

export default class DictionaryEncoding extends BaseEncoding {
  name: 'DICTIONARY' = 'DICTIONARY';

  private dictionary: unknown[];

  constructor(reader: ByteReader, dictionary: unknown[]) {
    super(reader);

    this.dictionary = dictionary;
  }

  decodeBooleans(count: number) {
    return this.decodeValues<boolean>(count);
  }

  decodeInt32s(count: number) {
    return this.decodeValues<number>(count);
  }

  decodeInt64s(count: number) {
    return this.decodeValues<bigint>(count);
  }

  decodeInt96s(count: number) {
    return this.decodeValues<bigint>(count);
  }

  decodeFloats(count: number) {
    return this.decodeValues<number>(count);
  }

  decodeDoubles(count: number) {
    return this.decodeValues<number>(count);
  }

  decodeByteArrays(count: number) {
    return this.decodeValues<Buffer>(count);
  }

  decodeFixedLengthByteArrays(count: number) {
    return this.decodeValues<Buffer>(count);
  }

  private decodeValues<T>(count: number) {
    const bitWidth = this.reader.readByte();

    const rle = new RunLengthEncoding(this.reader, bitWidth, true);
    const values = rle.decodeInt32s(count);

    return values.map((idx) => {
      if (Number(idx) >= this.dictionary.length) {
        throw new RangeError('Value not found in dictionary');
      }

      return this.dictionary[Number(idx)] as T;
    });
  }
}
