import { PageHeader, Type, PageType } from '@parquet/thrift';
import ColumnChunkReader from './column-chunk';
import BaseEncoding from '../encodings/base';
import ByteReader from '../../../util/src/byt-reader';

export interface PageData<T = unknown> {
  dLevels: number[];
  rLevels: number[];
  values: T[];
  count: number;
}

export default abstract class BasePageReader<T = unknown> {
  pageHeader: PageHeader;
  columnChunkReader: ColumnChunkReader<T>;
  type: Type;
  pageType: PageType;
  reader: ByteReader;
  pageSize: number;

  constructor(pageHeader: PageHeader, columnChunkReader: ColumnChunkReader<T>, reader: ByteReader) {
    this.pageHeader = pageHeader;
    this.columnChunkReader = columnChunkReader;
    this.type = this.columnChunkReader.type;
    this.pageType = this.pageHeader.type;
    this.pageSize = this.pageHeader.compressed_page_size;
    this.reader = reader;
  }

  async createValueReader() {
    const reader = new ByteReader(
      await this.columnChunkReader.compression.inflate(this.reader.rest()),
    );

    return reader;
  }

  decodeValues<U = unknown>(type: Type, encoding: BaseEncoding, count: number): U[] {
    switch (type) {
      case Type.BOOLEAN:
        return (encoding.decodeBooleans(count) as unknown[]) as U[];
      case Type.INT32:
        return (encoding.decodeInt32s(count) as unknown[]) as U[];
      case Type.INT64:
        return (encoding.decodeInt64s(count) as unknown[]) as U[];
      case Type.INT96:
        return (encoding.decodeInt96s(count) as unknown[]) as U[];
      case Type.FLOAT:
        return (encoding.decodeFloats(count) as unknown[]) as U[];
      case Type.DOUBLE:
        return (encoding.decodeDoubles(count) as unknown[]) as U[];
      case Type.BYTE_ARRAY:
        return (encoding.decodeByteArrays(count) as unknown[]) as U[];
      case Type.FIXED_LEN_BYTE_ARRAY:
        return (encoding.decodeFixedLengthByteArrays(count) as unknown[]) as U[];
      default:
        throw new Error(`Unsupported value type ${type}`);
    }
  }

  abstract decodePageData(): Promise<PageData<T>>;
}
