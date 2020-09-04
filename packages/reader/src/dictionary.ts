import { PageHeader, PageType, DictionaryPageHeader, Encoding } from '@parquet/thrift';
import { ByteReader } from '@parquet/util';
import ColumnChunkReader from './column-chunk';
import BasePageReader, { PageData } from './page';
import { createEncoding } from '../encodings';

export default class DictionaryPageReader<T = unknown> extends BasePageReader<T> {
  header: DictionaryPageHeader;
  encoding: Encoding;
  numValues: number;

  constructor(pageHeader: PageHeader, columnChunkReader: ColumnChunkReader<T>, reader: ByteReader) {
    super(pageHeader, columnChunkReader, reader);

    if (pageHeader.type !== PageType.DICTIONARY_PAGE || !pageHeader.dictionary_page_header) {
      throw new TypeError('Expected DICTIONARY_PAGE');
    }

    this.header = pageHeader.dictionary_page_header;
    this.encoding = this.header.encoding;
    this.numValues = this.header.num_values;
  }

  decodePageData(): Promise<PageData<T>> {
    throw new Error('Not supported for Dictionary pages');
  }

  async decodeDictionary() {
    const values = this.decodeValues<T>(
      this.columnChunkReader.type,
      createEncoding(this.type, await this.createValueReader(), Encoding.PLAIN, {}),
      this.numValues,
    );

    return values;
  }
}
