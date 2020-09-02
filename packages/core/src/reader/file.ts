// eslint-disable-next-line node/no-unsupported-features/node-builtins
import { promises as fs } from 'fs';
import { FileMetaData, RowGroup } from '@parquet/thrift';
import { RecordGroup, dremelDecode, ParquetSchema } from '@parquet/schema';
import ColumnChunkReader from './column-chunk';
import { decodeThriftObject } from '../byte-utils';

const PARQUET_ID = 'PAR1';
const HEADER_SIZE = PARQUET_ID.length;
const FOOTER_SIZE = PARQUET_ID.length + 4;

interface ParquetFileMetadata {
  version: number;
  numRows: number;
  rowGroups: RowGroup[];
  metadata: Record<string, string>;
  createdBy: string;
}

export default class ParquetFileReader {
  fileHandle: fs.FileHandle;
  schema: ParquetSchema;
  fileMetaData: ParquetFileMetadata;

  constructor(fileHandle: fs.FileHandle, schema: ParquetSchema, fileMetaData: ParquetFileMetadata) {
    this.fileHandle = fileHandle;
    this.schema = schema;
    this.fileMetaData = fileMetaData;
  }

  static async openFile(path: string) {
    const fileHandle = await fs.open(path, 'r');
    const { schema, metadata } = await this.readMetadata(fileHandle);

    return new ParquetFileReader(fileHandle, schema, metadata);
  }

  static async readMetadata(
    fileHandle: fs.FileHandle,
  ): Promise<{ schema: ParquetSchema; metadata: ParquetFileMetadata }> {
    const stat = await fileHandle.stat();
    const headerStr = (
      await fileHandle.read(Buffer.alloc(HEADER_SIZE), 0, HEADER_SIZE, 0)
    ).buffer.toString('ascii');

    if (headerStr !== PARQUET_ID) {
      throw new Error('File is not a valid parquet file (invalid header)');
    }

    const footer = await fileHandle.read(
      Buffer.alloc(FOOTER_SIZE),
      0,
      FOOTER_SIZE,
      stat.size - FOOTER_SIZE,
    );
    const metadataSize = footer.buffer.readUInt32LE(0);
    const metadataOffset = stat.size - FOOTER_SIZE - metadataSize;

    if (
      footer.bytesRead !== FOOTER_SIZE ||
      footer.buffer.slice(PARQUET_ID.length).toString('ascii') !== PARQUET_ID ||
      metadataOffset < HEADER_SIZE
    ) {
      throw new Error('File is not a valid parquet file (invalid footer)');
    }

    const metadataBuffer = (
      await fileHandle.read(Buffer.alloc(metadataSize), 0, metadataSize, metadataOffset)
    ).buffer;

    const metadata = decodeThriftObject(FileMetaData, metadataBuffer, 0).object;

    const schema = ParquetSchema.fromMetadata(metadata);

    const customMetadata: Record<string, string> = {};

    for (const { key, value } of metadata.key_value_metadata || []) {
      customMetadata[key] = value;
    }

    return {
      schema,
      metadata: {
        version: metadata.version,
        numRows: metadata.num_rows.toNumber(),
        rowGroups: metadata.row_groups,
        metadata: customMetadata,
        createdBy: metadata.created_by,
      },
    };
  }

  close() {
    return this.fileHandle.close();
  }

  async *readRecords() {
    for (const group of this.fileMetaData.rowGroups) {
      yield* this.readRowGroup(group);
    }
  }

  async *readRowGroup(rowGroup: RowGroup) {
    const recordGroup: RecordGroup = {
      numRows: Number(rowGroup.num_rows),
      columns: {},
    };

    for (const column of rowGroup.columns) {
      const columnReader = new ColumnChunkReader(this.schema, column, this.fileHandle);
      const columnPath = column.meta_data.path_in_schema.join('.');

      // eslint-disable-next-line no-await-in-loop
      const data = await columnReader.decodeDataPages();

      if (!recordGroup.columns[columnPath]) {
        recordGroup.columns[columnPath] = {
          count: 0,
          values: [],
          rLevels: [],
          dLevels: [],
        };
      }

      recordGroup.columns[columnPath].count += data.count;
      recordGroup.columns[columnPath].values.push(...data.values);
      recordGroup.columns[columnPath].rLevels.push(...data.rLevels);
      recordGroup.columns[columnPath].dLevels.push(...data.dLevels);
    }

    const rows = dremelDecode(recordGroup, this.schema);

    for (const row of rows) {
      yield row;
    }
  }
}
