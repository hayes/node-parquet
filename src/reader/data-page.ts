import {
  PageHeader,
  PageType,
  DataPageHeader,
  Encoding,
  Type,
} from '../../gen-nodejs/parquet_types';
import ColumnChunkReader from './column-chunk';
import BasePageReader, { PageData } from './page';
import ByteReader from './byte';
import { createEncoding } from '../encodings';
import { minBitWidth } from '../byte-utils';

export default class DataPageReader<T = unknown> extends BasePageReader<T> {
  header: DataPageHeader;
  encoding: Encoding;
  rLevelEncoding: Encoding;
  dLevelEncoding: Encoding;
  numValues: number;

  constructor(pageHeader: PageHeader, columnChunkReader: ColumnChunkReader<T>, reader: ByteReader) {
    super(pageHeader, columnChunkReader, reader);

    if (pageHeader.type !== PageType.DATA_PAGE || !pageHeader.data_page_header) {
      throw new TypeError('Expected DATA_PAGE');
    }

    this.header = pageHeader.data_page_header;
    this.numValues = this.header.num_values;
    this.encoding = this.header.encoding;
    this.rLevelEncoding = this.header.repetition_level_encoding;
    this.dLevelEncoding = this.header.definition_level_encoding;
  }

  async decodePageData(): Promise<PageData<T>> {
    const reader = await this.createValueReader();
    const { maxRLevel, maxDLevel, logicalType } = this.columnChunkReader.field;

    let rLevels = new Array<number>(this.numValues);

    if (maxRLevel! > 0) {
      const rLevelEncoding = createEncoding(Type.INT32, reader, this.rLevelEncoding, {
        bitWidth: minBitWidth(maxRLevel),
        disableEnvelope: false,
      });

      rLevels = this.decodeValues<number>(Type.INT32, rLevelEncoding, this.numValues);
    } else {
      rLevels.fill(0);
    }

    let dLevels = new Array<number>(this.numValues);
    if (maxDLevel! > 0) {
      const dLevelEncoding = createEncoding(Type.INT32, reader, this.dLevelEncoding, {
        bitWidth: minBitWidth(maxDLevel),
        disableEnvelope: false,
      });

      dLevels = this.decodeValues<number>(Type.INT32, dLevelEncoding, this.numValues);
    } else {
      dLevels.fill(0);
    }

    let valueCountNonNull = 0;
    for (const dlvl of dLevels) {
      if (dlvl === maxDLevel) {
        ++valueCountNonNull;
      }
    }

    const encoding = createEncoding(this.type, reader, this.encoding, {
      typeLength: logicalType!.typeLength,
      bitWidth: logicalType!.typeLength,
      dictionary: this.columnChunkReader.dictionary,
    });

    const values = this.decodeValues<T>(this.columnChunkReader.type, encoding, valueCountNonNull);

    return {
      dLevels,
      rLevels,
      values,
      count: this.numValues,
    };
  }
}
