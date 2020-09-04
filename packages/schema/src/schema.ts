import { ParquetField, ParquetNestedField } from './field';
import { ParquetSchemaDefinition, JSTypeFromDefinition } from './types';
import { ColumnData } from '.';

export default class ParquetSchema<T extends Record<string, unknown> = Record<string, unknown>> {
  fields: Record<string, ParquetField | ParquetNestedField> = {};
  leafFields: ParquetField[] = [];

  static fromDefinition<T extends ParquetSchemaDefinition>(
    definition: T,
  ): ParquetSchema<JSTypeFromDefinition<T>> {
    const schema = new ParquetSchema<JSTypeFromDefinition<T>>();

    Object.keys(definition.fields).forEach((fieldName) => {
      const fieldDefinition = definition.fields[fieldName];

      schema.addField(
        fieldDefinition.fields
          ? new ParquetNestedField(fieldName, fieldDefinition)
          : new ParquetField(fieldName, fieldDefinition),
      );
    });

    return schema;
  }

  addField(field: ParquetField | ParquetNestedField) {
    this.fields[field.pathName] = field;

    if (field instanceof ParquetField) {
      this.leafFields.push(field);
    } else {
      field.fieldList.forEach((child) => {
        this.addField(child);
      });
    }
  }

  disassemble(rows: T[]) {
    const disassembled: Record<string, ColumnData> = {};

    for (const field of this.leafFields) {
      disassembled[field.pathName] = field.disassemble(rows);
    }

    return disassembled;
  }

  assemble(data: Record<string, ColumnData>) {
    const rows: Record<string, {}>[] = [];

    for (const field of this.leafFields) {
      field.assemble(data[field.pathName], rows);
    }

    return rows as T[];
  }

  // static fromMetadata(metadata: FileMetaData) {
  //   if (metadata.encryption_algorithm) {
  //     throw new Error('Encryption is not supported yet');
  //   }

  //   const fields = ParquetField.fromThrift(metadata.schema);

  //   return new ParquetSchema(fields);
  // }

  getField(path: string | string[]) {
    const strPath = Array.isArray(path) ? path.join('.') : path;
    const field = this.fields[strPath];

    if (!field) {
      throw new Error(`Path not found in schema: ${strPath}`);
    }

    return field;
  }
}
