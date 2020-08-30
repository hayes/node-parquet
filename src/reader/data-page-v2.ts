import {
  PageHeader,
  PageType,
  DataPageHeaderV2,
  Encoding,
  Type,
} from '../../gen-nodejs/parquet_types';
import ColumnChunkReader from './column-chunk';
import BasePageReader, { PageData } from './page';
import ByteReader from './byte';
import { createEncoding } from '../encodings';
import { minBitWidth } from '../byte-utils';

export default class DataPageV2Reader<T = unknown> extends BasePageReader<T> {
  header: DataPageHeaderV2;
  encoding: Encoding;
  numValues: number;
  numNonNullValues: number;

  constructor(pageHeader: PageHeader, columnChunkReader: ColumnChunkReader<T>, reader: ByteReader) {
    super(pageHeader, columnChunkReader, reader);

    if (pageHeader.type !== PageType.DATA_PAGE_V2 || !pageHeader.data_page_header_v2) {
      throw new TypeError('Expected DATA_PAGE_V2');
    }

    this.header = pageHeader.data_page_header_v2;
    this.encoding = pageHeader.data_page_header_v2.encoding;
    this.numValues = this.header.num_values;
    this.numNonNullValues = this.numValues - this.header.num_nulls;
  }

  async decodePageData(): Promise<PageData<T>> {
    const { maxRLevel = 0, maxDLevel = 0, logicalType } = this.columnChunkReader.field;

    let rLevels: number[] = new Array(this.numValues) as number[];

    if (maxRLevel > 0) {
      const rLevelEncoding = createEncoding(Type.INT32, this.reader, Encoding.RLE, {
        bitWidth: minBitWidth(maxRLevel),
        disableEnvelope: true,
      });

      rLevels = this.decodeValues(Type.INT32, rLevelEncoding, this.numValues);
    } else {
      rLevels.fill(0);
    }

    let dLevels = new Array<number>(this.numValues);
    if (maxDLevel > 0) {
      const dLevelEncoding = createEncoding(Type.INT32, this.reader, Encoding.RLE, {
        bitWidth: minBitWidth(maxDLevel),
        disableEnvelope: true,
      });

      dLevels = this.decodeValues(Type.INT32, dLevelEncoding, this.numValues);
    } else {
      dLevels.fill(0);
    }

    const valueReader = this.header.is_compressed ? await this.createValueReader() : this.reader;

    const encoding = createEncoding(this.type, valueReader, this.encoding, {
      typeLength: logicalType!.typeLength,
      dictionary: this.columnChunkReader.dictionary,
    });

    const values = this.decodeValues<T>(
      this.columnChunkReader.type,
      encoding,
      this.numNonNullValues,
    );

    return {
      dLevels,
      rLevels,
      values,
      count: this.numValues,
    };
  }
}
