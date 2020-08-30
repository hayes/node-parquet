import { FileMetaData } from '../gen-nodejs/parquet_types';
import ParquetField, { SchemaFields } from './field';

export default class ParquetSchema {
  fields: SchemaFields;

  constructor(fields: SchemaFields) {
    this.fields = fields;
  }

  static fromMetadata(metadata: FileMetaData) {
    if (metadata.encryption_algorithm) {
      throw new Error('Encryption is not supported yet');
    }

    const fields = ParquetField.fromThrift(metadata.schema);

    return new ParquetSchema(fields);
  }

  getField(path: string | string[]) {
    const strPath = Array.isArray(path) ? path.join('.') : path;
    const field = this.fields.map[strPath];

    if (!field) {
      throw new Error(`Path not found in schema: ${strPath}`);
    }

    return field;
  }
}
