/* eslint-disable max-classes-per-file */
import { Type } from '@parquet/thrift';
import LogicalType from './logical-types';
import {
  ParquetNestedFieldDefinition,
  FieldRepetitionTypeName,
  ParquetLeafFieldDefinition,
} from './types';
import { ColumnData, dremelDisassemble, dremelAssemble } from '.';

export class ParquetField {
  type: Type;
  name: string;
  repetitionType: FieldRepetitionTypeName;
  pathName: string;
  rLevel: number;
  dLevel: number;
  logicalType: LogicalType;
  fieldId: number | null;
  parents: ParquetNestedField[];

  constructor(
    name: string,
    fieldDefinition: ParquetLeafFieldDefinition,
    parent?: ParquetNestedField,
  ) {
    this.name = name;
    this.repetitionType = fieldDefinition.repetitionType;
    this.pathName = parent ? `${parent.pathName}.${name}` : name;
    this.rLevel = (parent?.rLevel || 0) + (this.repetitionType === 'REPEATED' ? 1 : 0);
    this.dLevel = (parent?.dLevel || 0) + (this.repetitionType === 'REQUIRED' ? 0 : 1);
    this.logicalType = LogicalType.fromFieldDefinition(fieldDefinition);
    this.type = this.logicalType.type;
    this.fieldId = fieldDefinition.fieldId ?? null;
    this.parents = parent ? [...parent.parents, parent] : [];
  }

  disassemble(rows: Record<string, unknown>[]): ColumnData {
    return dremelDisassemble(this, rows);
  }

  assemble<T extends Record<string, unknown>>(data: ColumnData, rows: T[]): T[] {
    dremelAssemble(this, data, rows);

    return rows;
  }
}

export class ParquetNestedField {
  name: string;
  repetitionType: FieldRepetitionTypeName;
  pathName: string;
  rLevel: number;
  dLevel: number;
  fieldId: number | null;
  parents: ParquetNestedField[];
  fieldMap: Record<string, ParquetNestedField | ParquetField> = {};
  fieldList: (ParquetNestedField | ParquetField)[] = [];

  constructor(
    name: string,
    fieldDefinition: ParquetNestedFieldDefinition,
    parent?: ParquetNestedField,
  ) {
    this.name = name;
    this.repetitionType = fieldDefinition.repetitionType;
    this.pathName = parent ? `${parent.pathName}.${name}` : name;
    this.rLevel = (parent?.rLevel || 0) + (this.repetitionType === 'REPEATED' ? 1 : 0);
    this.dLevel = (parent?.dLevel || 0) + (this.repetitionType === 'REQUIRED' ? 0 : 1);
    this.fieldId = fieldDefinition.fieldId ?? null;
    this.parents = parent ? [...parent.parents, parent] : [];

    Object.keys(fieldDefinition.fields).forEach((childName) => {
      const childDefinition = fieldDefinition.fields[childName];

      if (childDefinition.fields) {
        this.addChild(new ParquetNestedField(childName, childDefinition, this));
      } else {
        this.addChild(new ParquetField(childName, childDefinition, this));
      }
    });
  }

  addChild(field: ParquetNestedField | ParquetField) {
    if (this.fieldMap[field.name]) {
      throw new Error(`Nested child with name ${field.name} already exists`);
    }

    this.fieldMap[field.name] = field;
    this.fieldList.push(field);
  }
}
