/* eslint-disable @typescript-eslint/no-use-before-define, complexity, max-classes-per-file */

import * as uuid from 'uuid';
import { Type, SchemaElement, ConvertedType, TimeUnit } from '@parquet/thrift';

export default class LogicalType<T = unknown> {
  type: Type;
  typeLength?: number;
  bitWidth?: number;

  constructor(
    type: Type,
    options: {
      typeLength?: number;
      scale?: number;
      precision?: number;
      bitWidth?: number;
    } = {},
  ) {
    this.type = type;
    this.typeLength = options.typeLength;
    this.bitWidth = options.bitWidth;
  }

  static fromConvertedType(element: SchemaElement) {
    switch (element.converted_type) {
      case ConvertedType.UTF8:
        return new StringType(element.type);
      case ConvertedType.MAP:
        throw new Error('Map types not implemented yet');
      case ConvertedType.MAP_KEY_VALUE:
        throw new Error('Map types not implemented yet');
      case ConvertedType.LIST:
        throw new Error('Map types not implemented yet');
      case ConvertedType.ENUM:
        return new EnumType(element.type);
      case ConvertedType.DECIMAL:
        return new DecimalType(element.type, element.scale, element.precision);
      case ConvertedType.DATE:
        return new DateType(element.type);
      case ConvertedType.TIME_MILLIS:
        return new TimeType(element.type, true, 'MILLIS');
      case ConvertedType.TIME_MICROS:
        return new TimeType(element.type, true, 'MICROS');
      case ConvertedType.TIMESTAMP_MILLIS:
        return new TimestampType(element.type, true, 'MILLIS');
      case ConvertedType.TIMESTAMP_MICROS:
        return new TimestampType(element.type, true, 'MICROS');
      case ConvertedType.UINT_8:
        return new IntType(element.type, 8, false);
      case ConvertedType.UINT_16:
        return new IntType(element.type, 16, false);
      case ConvertedType.UINT_32:
        return new IntType(element.type, 32, false);
      case ConvertedType.UINT_64:
        return new IntType(element.type, 64, false);
      case ConvertedType.INT_8:
        return new IntType(element.type, 8, true);
      case ConvertedType.INT_16:
        return new IntType(element.type, 16, true);
      case ConvertedType.INT_32:
        return new IntType(element.type, 32, true);
      case ConvertedType.INT_64:
        return new IntType(element.type, 64, true);
      case ConvertedType.JSON:
        return new JSONType(element.type);
      case ConvertedType.BSON:
        return new BSONType(element.type);
      case ConvertedType.INTERVAL:
        return new IntervalType(element.type);
      default:
        return new LogicalType(element.type, {
          typeLength: element.type_length,
        });
    }
  }

  static fromThrift(element: SchemaElement) {
    if (!element.logicalType) {
      if (element.converted_type) {
        return this.fromConvertedType(element);
      }

      return new LogicalType(element.type, {
        typeLength: element.type_length,
      });
    }

    switch (true) {
      case !!element.logicalType.STRING:
        return new StringType(element.type);
      case !!element.logicalType.MAP:
        throw new Error('Map types not implemented yet');
      case !!element.logicalType.LIST:
        throw new Error('List types not implemented yet');
      case !!element.logicalType.ENUM:
        return new EnumType(element.type);
      case !!element.logicalType.DECIMAL:
        return new DecimalType(
          element.logicalType.DECIMAL.scale,
          element.logicalType.DECIMAL.precision,
          element.type,
        );
      case !!element.logicalType.DATE:
        return new DateType(element.type);
      case !!element.logicalType.TIME:
        return new TimeType(
          element.type,
          element.logicalType.TIME.isAdjustedToUTC,
          parseTimeUnit(element.logicalType.TIME.unit),
        );
      case !!element.logicalType.TIMESTAMP:
        return new TimestampType(
          element.type,
          element.logicalType.TIMESTAMP.isAdjustedToUTC,
          parseTimeUnit(element.logicalType.TIMESTAMP.unit),
        );
      case !!element.logicalType.INTEGER:
        return new IntType(
          element.type,
          element.logicalType.INTEGER.bitWidth,
          element.logicalType.INTEGER.isSigned,
        );
      case !!element.logicalType.UNKNOWN:
        return new NullType(element.type);
      case !!element.logicalType.JSON:
        return new JSONType(element.type);
      case !!element.logicalType.BSON:
        return new BSONType(element.type);
      case !!element.logicalType.UUID:
        return new UUIDType(element.type);
      default:
        return new LogicalType(element.type, {
          typeLength: element.type_length,
        });
    }
  }

  serialize(value: T): unknown {
    return value;
  }

  deserialize(value: unknown): T {
    return value as T;
  }
}

export class StringType extends LogicalType<string> {
  constructor(type: Type) {
    if (type !== Type.BYTE_ARRAY) {
      throw new Error('Strings type can only be represented by a BYTE_ARRAY column');
    }

    super(Type.BYTE_ARRAY);
  }

  serialize(value: string) {
    return Buffer.from(value, 'utf8');
  }

  deserialize(value: unknown) {
    return (value as Buffer).toString('utf8');
  }
}

export class UUIDType extends LogicalType<string> {
  constructor(type: Type) {
    if (type !== Type.FIXED_LEN_BYTE_ARRAY) {
      throw new Error('UUIDS type can only be represented by a FIXED_LEN_BYTE_ARRAY column');
    }
    super(Type.FIXED_LEN_BYTE_ARRAY, {
      typeLength: 16,
    });
  }

  serialize(value: string) {
    return Buffer.from(uuid.parse(value));
  }

  deserialize(value: unknown) {
    return uuid.stringify(value as Buffer);
  }
}

export class EnumType extends LogicalType<string> {
  constructor(type: Type) {
    if (type !== Type.BYTE_ARRAY) {
      throw new Error('ENUMS type can only be represented by a BYTE_ARRAY column');
    }
    super(Type.BYTE_ARRAY);
  }

  serialize(value: string) {
    return Buffer.from(value, 'utf8');
  }

  deserialize(value: unknown) {
    return (value as Buffer).toString('utf8');
  }
}

export class IntType extends LogicalType<number | bigint> {
  isSigned: boolean;
  bitWidth: number;

  constructor(type: Type, bitWidth: number, isSigned: boolean) {
    switch (bitWidth) {
      case 8:
        if (type !== Type.INT32) {
          throw new Error('8 bit INTs must be represented with the INT32 physical type');
        }
        super(Type.INT32, {
          bitWidth,
        });
        break;
      case 16:
        if (type !== Type.INT32) {
          throw new Error('16 bit INTs must be represented with the INT32 physical type');
        }
        super(Type.INT32, {
          bitWidth,
        });
        break;
      case 32:
        if (type !== Type.INT32) {
          throw new Error('32 bit INTs must be represented with the INT32 physical type');
        }
        super(Type.INT32, {
          bitWidth,
        });
        break;
      case 64:
        if (type !== Type.INT64) {
          throw new Error('64 bit INTs must be represented with the INT64 physical type');
        }
        super(Type.INT64, {
          bitWidth,
        });
        break;
      case 96:
        if (type !== Type.INT96) {
          throw new Error('96 bit INTs must be represented with the INT96 physical type');
        }
        super(Type.INT96, {
          bitWidth,
        });
        break;
      default:
        throw new Error(`Unsupported bitWidth: ${bitWidth}`);
    }

    this.isSigned = isSigned;
    this.bitWidth = bitWidth;
  }
}

export class DecimalType extends LogicalType<number> {
  scale: number;
  precision: number;
  constructor(type: Type, scale: number, precision: number) {
    if (
      type !== Type.INT32 &&
      type !== Type.INT64 &&
      type !== Type.BYTE_ARRAY &&
      type !== Type.FIXED_LEN_BYTE_ARRAY
    ) {
      throw new Error(
        'Decimal columns must be represented by an integer or byte array physical type',
      );
    }

    super(type);

    this.scale = scale;
    this.precision = precision;
  }

  serialize(value: number) {
    throw new Error('Decimal serialization not implemented yet');
  }

  deserialize(value: unknown): number {
    throw new Error('Decimal deserialization not implemented yet');
  }
}

export class DateType extends LogicalType<unknown> {
  constructor(type: Type) {
    if (type !== Type.INT32) {
      throw new Error('Date columns must be represented with the INT32 physical type');
    }

    super(Type.INT32);
  }

  serialize(value: number) {
    throw new Error('Date serialization not implemented yet');
  }

  deserialize(value: unknown): number {
    throw new Error('Date deserialization not implemented yet');
  }
}

export class TimeType extends LogicalType<unknown> {
  isAdjustedToUTC: boolean;
  precision: 'MILLIS' | 'MICROS' | 'NANOS';

  constructor(type: Type, isAdjustedToUTC: boolean, precision: 'MILLIS' | 'MICROS' | 'NANOS') {
    if (precision === 'MILLIS') {
      if (type !== Type.INT32) {
        throw new Error(
          'Time columns using MILLIS precision must be represented with the INT32 physical type',
        );
      }
      super(type);
    } else {
      if (type !== Type.INT64) {
        throw new Error(
          'Time columns using MICROS or NANOS precision must be represented with the INT64 physical type',
        );
      }
      super(type);
    }

    this.isAdjustedToUTC = isAdjustedToUTC;
    this.precision = precision;
  }

  serialize(value: number) {
    throw new Error('Time serialization not implemented yet');
  }

  deserialize(value: unknown): number {
    throw new Error('Time deserialization not implemented yet');
  }
}

export class TimestampType extends LogicalType<unknown> {
  isAdjustedToUTC: boolean;
  precision: 'MILLIS' | 'MICROS' | 'NANOS';

  constructor(type: Type, isAdjustedToUTC: boolean, precision: 'MILLIS' | 'MICROS' | 'NANOS') {
    if (type !== Type.INT64) {
      throw new Error('Timestamp columns must be represented with the INT64 physical type');
    }
    super(Type.INT64);

    this.isAdjustedToUTC = isAdjustedToUTC;
    this.precision = precision;
  }

  serialize(value: number) {
    throw new Error('Timestamp serialization not implemented yet');
  }

  deserialize(value: unknown): number {
    throw new Error('Timestamp deserialization not implemented yet');
  }
}

export class IntervalType extends LogicalType<unknown> {
  constructor(type: Type) {
    if (type !== Type.FIXED_LEN_BYTE_ARRAY) {
      throw new Error(
        'Interval columns must be represented with the FIXED_LEN_BYTE_ARRAY physical type',
      );
    }

    super(Type.FIXED_LEN_BYTE_ARRAY, {
      typeLength: 12,
    });
  }

  serialize(value: unknown) {
    throw new Error('Interval serialization not implemented yet');
  }

  deserialize(value: unknown): unknown {
    throw new Error('Interval deserialization not implemented yet');
  }
}

export class JSONType extends LogicalType<unknown> {
  constructor(type: Type) {
    if (type !== Type.BYTE_ARRAY) {
      throw new Error('JSON columns must be represented with the BYTE_ARRAY physical type');
    }
    super(Type.BYTE_ARRAY);
  }

  serialize(value: unknown) {
    return Buffer.from(JSON.stringify(value));
  }

  deserialize(value: unknown): unknown {
    return JSON.parse((value as Buffer).toString('utf8'));
  }
}

export class BSONType extends LogicalType<unknown> {
  constructor(type: Type) {
    if (type !== Type.BYTE_ARRAY) {
      throw new Error('BSON columns must be represented with the BYTE_ARRAY physical type');
    }
    super(Type.BYTE_ARRAY);
  }

  serialize(value: unknown) {
    throw new Error('BSON serialization not implemented yet');
  }

  deserialize(value: unknown): unknown {
    throw new Error('BSON deserialization not implemented yet');
  }
}

export class MapType extends LogicalType<unknown> {
  serialize(value: unknown) {
    throw new Error('Map serialization not implemented yet');
  }

  deserialize(value: unknown): unknown {
    throw new Error('Map deserialization not implemented yet');
  }
}

export class ListType extends LogicalType<unknown> {
  serialize(value: unknown) {
    throw new Error('List serialization not implemented yet');
  }

  deserialize(value: unknown): unknown {
    throw new Error('List deserialization not implemented yet');
  }
}

export class NullType extends LogicalType<null> {
  serialize(value: unknown) {
    throw new Error('Should not attempt to serialize null values');
  }

  deserialize(value: unknown): null {
    throw new Error('Should not attempt to deserialize null values');
  }
}

function parseTimeUnit(unit: TimeUnit) {
  switch (true) {
    case !!unit.MICROS:
      return 'MICROS' as const;
    case !!unit.MILLIS:
      return 'MILLIS' as const;
    case !!unit.NANOS:
      return 'NANOS' as const;
    default:
      throw new Error(`Unsupported TimeUnit ${unit}`);
  }
}
