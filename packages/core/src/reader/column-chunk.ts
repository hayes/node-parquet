import { FileHandle } from 'fs/promises';
import { ColumnChunk, Type, ColumnMetaData, PageHeader, PageType } from '@parquet/thrift';
import { ParquetField } from '@parquet/schema';
import DictionaryPageReader from './dictionary';
import BasePageReader, { PageData } from './page';
import DataPageReader from './data-page';
import DataPageV2Reader from './data-page-v2';
import ByteReader from '../../../util/src/byt-reader';
import ParquetSchema from '../../../schema/src/schema';
import compressionFromThrift, { BaseCompression } from '../compression';

type PageTypeToReader<Type extends PageType, T> = Type extends PageType.DATA_PAGE
  ? DataPageReader<T>
  : Type extends PageType.DATA_PAGE_V2
  ? DataPageV2Reader<T>
  : Type extends PageType.DICTIONARY_PAGE
  ? DictionaryPageReader<T>
  : BasePageReader<T>;

export default class ColumnChunkReader<T = unknown> {
  schema: ParquetSchema;
  chunk: ColumnChunk;
  field: ParquetField;
  type: Type;
  compression: BaseCompression;
  fileHandle: FileHandle;
  metaData: ColumnMetaData;
  dictionary: null | T[] = null;
  buffer: null | Buffer = null;
  bufferStart: number = 0;

  constructor(schema: ParquetSchema, chunk: ColumnChunk, fileHandle: FileHandle) {
    if (!chunk.meta_data) {
      throw new Error(`Missing column metadata for ${chunk.meta_data!.path_in_schema.join('.')}`);
    }

    this.schema = schema;
    this.chunk = chunk;
    this.fileHandle = fileHandle;
    this.metaData = chunk.meta_data;

    this.field = this.schema.getField(this.metaData.path_in_schema);
    this.type = this.metaData.type;
    this.compression = compressionFromThrift(this.metaData.codec);
  }

  release() {
    this.dictionary = null;
    this.buffer = null;
  }

  async getBuffer() {
    if (this.buffer) {
      return this.buffer;
    }

    const start = [
      this.metaData.data_page_offset,
      this.metaData.dictionary_page_offset,
      this.metaData.index_page_offset,
    ]
      .filter((val) => val != null && val.toNumber() !== 0)
      .map((val) => val.toNumber())
      .sort((a, b) => (a > b ? 1 : -1))[0];

    const end = start + this.metaData.total_compressed_size.toNumber();
    const size = end - start;

    this.bufferStart = start;
    this.buffer = Buffer.alloc(size);

    await this.fileHandle.read(this.buffer, 0, size, start);

    return this.buffer;
  }

  async createReader(offset: number) {
    const buffer = await this.getBuffer();

    return new ByteReader(buffer, offset - this.bufferStart);
  }

  getPage<Type extends PageType>(parentReader: ByteReader, type?: Type): PageTypeToReader<Type, T> {
    const pageHeader = parentReader.decodeThriftObject(PageHeader);

    if (type && pageHeader.type !== type) {
      throw new TypeError(`Expected ${type}, got ${pageHeader.type}`);
    }

    const reader = parentReader.child(pageHeader.compressed_page_size, true);
    let page;
    switch (pageHeader.type) {
      case PageType.DICTIONARY_PAGE:
        page = new DictionaryPageReader(pageHeader, this, reader) as PageTypeToReader<Type, T>;
        break;
      case PageType.DATA_PAGE:
        page = new DataPageReader(pageHeader, this, reader) as PageTypeToReader<Type, T>;
        break;
      case PageType.DATA_PAGE_V2:
        page = new DataPageV2Reader(pageHeader, this, reader) as PageTypeToReader<Type, T>;
        break;
      default:
        throw new TypeError(`Unsupported page type ${pageHeader.type}`);
    }

    return page;
  }

  async initializeDictionary() {
    if (this.dictionary) {
      return this.dictionary;
    }

    const offset = this.chunk.meta_data.dictionary_page_offset;
    if (!offset || offset.toNumber() === 0) {
      return null;
    }

    const reader = await this.createReader(offset.toNumber());
    const pageReader = await this.getPage(reader, PageType.DICTIONARY_PAGE);

    this.dictionary = await pageReader.decodeDictionary();

    return this.dictionary;
  }

  async decodeDataPages() {
    const data: PageData<T> = {
      rLevels: [],
      dLevels: [],
      values: [],
      count: 0,
    };

    const reader = await this.createReader(this.metaData.data_page_offset.toNumber());

    await this.initializeDictionary();

    while (reader.remaining() !== 0) {
      const page = this.getPage(reader);

      if (!this.dictionary && page instanceof DictionaryPageReader) {
        // eslint-disable-next-line no-await-in-loop
        this.dictionary = await page.decodeDictionary();

        // eslint-disable-next-line no-continue
        continue;
      }

      if (
        page.pageHeader.type !== PageType.DATA_PAGE &&
        page.pageHeader.type !== PageType.DATA_PAGE_V2
      ) {
        throw new TypeError(`Expected data page, but got ${page.pageHeader.type}`);
      }

      // eslint-disable-next-line no-await-in-loop
      const pageData = await page.decodePageData();

      data.rLevels.push(...pageData.rLevels);
      data.dLevels.push(...pageData.dLevels);
      data.values.push(...pageData.values);
      data.count += pageData.count;
    }

    return data;
  }
}
