// eslint-disable-next-line node/no-unsupported-features/node-builtins
import { promises as fs } from 'fs';
import ParquetSchema from '../../../schema/src/schema';

const PARQUET_ID = 'PAR1';

export default class ParquetFileReader {
  fileHandle: fs.FileHandle;
  schema: ParquetSchema;
  offset: number;

  constructor(fileHandle: fs.FileHandle, schema: ParquetSchema, offset: number) {
    this.fileHandle = fileHandle;
    this.schema = schema;
    this.offset = offset;
  }

  static async createFile(path: string, schema: ParquetSchema) {
    const fileHandle = await fs.open(path, 'ax');

    await fileHandle.write(PARQUET_ID, 0, 'ascii');

    return new ParquetFileReader(fileHandle, schema, PARQUET_ID.length);
  }
}
