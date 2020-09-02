import BaseEncoding from './base';

export default class PlainEncoding extends BaseEncoding {
  name: 'PLAIN' = 'PLAIN';

  decodeBooleans(count: number) {
    return this.reader.readBooleans(count);
  }

  decodeInt32s(count: number) {
    return this.reader.readInt32s(count);
  }

  decodeInt64s(count: number) {
    return this.reader.readInt64s(count);
  }

  decodeInt96s(count: number) {
    return this.reader.readInt96s(count);
  }

  decodeFloats(count: number) {
    return this.reader.readFloats(count);
  }

  decodeDoubles(count: number) {
    return this.reader.readDoubles(count);
  }

  decodeByteArrays(count: number) {
    return this.reader.readByteArrays(count);
  }

  decodeFixedLengthByteArrays(count: number) {
    if (this.typeLength == null) {
      throw new Error('typeLength is required for FIXED_LEN_BYTE_ARRAY');
    }

    return this.reader.readFixedLengthByteArrays(count, this.typeLength);
  }
}
