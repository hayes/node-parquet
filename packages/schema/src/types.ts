import { LogicalType } from '.';

export interface TypeNameToJSType {
  STRING: string;
  BOOLEAN: boolean;
  INT32: number;
  INT64: bigint;
  FLOAT: number;
  DOUBLE: number;
  BYTE_ARRAY: Buffer;
  ENUM: string;
  DATE: Date;
  INTERVAL: number | bigint;
  UNKNOWN: null;
  JSON: unknown;
  BSON: unknown;
  UUID: string;
  FIXED_LEN_BYTE_ARRAY: Buffer;
  DECIMAL: number;
  TIME: unknown;
  TIMESTAMP: unknown;
  INTEGER: number | bigint;
}

export type SimpleTypeName =
  | 'STRING'
  | 'BOOLEAN'
  | 'INT32'
  | 'INT64'
  | 'FLOAT'
  | 'DOUBLE'
  | 'BYTE_ARRAY'
  | 'ENUM'
  | 'DATE'
  | 'INTERVAL'
  | 'UNKNOWN'
  | 'JSON'
  | 'BSON'
  | 'UUID';

export type FieldRepetitionTypeName = 'REQUIRED' | 'OPTIONAL' | 'REPEATED';

export type SimpleTypeDefinition =
  | SimpleTypeName
  | {
      name: SimpleTypeName;
    };

export interface FixedLengthByteArrayTypeDefinition {
  name: 'FIXED_LEN_BYTE_ARRAY';
  typeLength: number;
}

export interface DecimalTypeDefinition {
  name: 'DECIMAL';
  scale: number;
  precision: number;
}

export interface TimeTypeDefinition {
  name: 'TIME';
  isAdjustedToUTC: boolean;
  precision: 'MILLIS' | 'MICROS' | 'NANOS';
}

export interface TimestampTypeDefinition {
  name: 'TIMESTAMP';
  isAdjustedToUTC: boolean;
  precision: 'MILLIS' | 'MICROS' | 'NANOS';
}

export interface IntegerTypeDefinition {
  name: 'INTEGER';
  bitWidth: 8 | 16 | 32 | 64;
  isSigned: boolean;
}

export interface ParquetLeafFieldDefinition {
  repetitionType: FieldRepetitionTypeName;
  type:
    | SimpleTypeDefinition
    | DecimalTypeDefinition
    | TimeTypeDefinition
    | TimestampTypeDefinition
    | IntegerTypeDefinition
    | LogicalType;
  fields?: undefined;
  fieldId?: number;
}

export interface ParquetNestedFieldDefinition {
  repetitionType: FieldRepetitionTypeName;
  fields: Record<string, ParquetFieldDefinition>;
  fieldId?: number;
}

export type ParquetFieldDefinition = ParquetLeafFieldDefinition | ParquetNestedFieldDefinition;

export interface ParquetSchemaDefinition {
  fields: Record<string, ParquetFieldDefinition>;
}

export type JSTypeFromRepetition<T, R extends FieldRepetitionTypeName> = R extends 'REPEATED'
  ? T[]
  : R extends 'REQUIRED'
  ? T
  : T | null | undefined;

export type JSTypeFromLeafFieldDefinition<
  T extends ParquetLeafFieldDefinition
> = T['type'] extends {
  name: keyof TypeNameToJSType;
}
  ? JSTypeFromRepetition<TypeNameToJSType[T['type']['name']], T['repetitionType']>
  : T['type'] extends keyof TypeNameToJSType
  ? JSTypeFromRepetition<TypeNameToJSType[T['type']], T['repetitionType']>
  : never;

export type JSTypeFromDefinition<
  T extends ParquetFieldDefinition | ParquetSchemaDefinition
> = T extends ParquetLeafFieldDefinition
  ? JSTypeFromLeafFieldDefinition<T>
  : T extends ParquetNestedFieldDefinition
  ? JSTypeFromRepetition<
      {
        [K in keyof T['fields']]: JSTypeFromDefinition<T['fields'][K]>;
      },
      T['repetitionType']
    >
  : T extends ParquetSchemaDefinition
  ? {
      [K in keyof T['fields']]: JSTypeFromDefinition<T['fields'][K]>;
    }
  : never;
