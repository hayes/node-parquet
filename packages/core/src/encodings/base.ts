import { ByteReader } from '@parquet/util';

export default abstract class BaseEncoding {
  protected reader: ByteReader;
  protected typeLength?: number;
  abstract name: string;

  constructor(reader: ByteReader, typeLength?: number | undefined) {
    this.reader = reader;
    this.typeLength = typeLength;
  }

  decodeBooleans(count: number): boolean[] {
    throw new Error(`${this.name} encoder does not support Boolean`);
  }

  decodeInt32s(count: number): number[] {
    throw new Error(`${this.name} encoder does not support Int32`);
  }

  decodeInt64s(count: number): BigInt[] {
    throw new Error(`${this.name} encoder does not support Int64`);
  }

  decodeInt96s(count: number): BigInt[] {
    throw new Error(`${this.name} encoder does not support Int96`);
  }

  decodeFloats(count: number): number[] {
    throw new Error(`${this.name} encoder does not support Float`);
  }

  decodeDoubles(count: number): number[] {
    throw new Error(`${this.name} encoder does not support Double`);
  }

  decodeByteArrays(count: number): Buffer[] {
    throw new Error(`${this.name} encoder does not support ByteArray`);
  }

  decodeFixedLengthByteArrays(count: number): Buffer[] {
    throw new Error(`${this.name} encoder does not support FixedLengthByteArray`);
  }
}
