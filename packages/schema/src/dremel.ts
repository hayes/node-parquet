/* eslint-disable complexity */
import { FieldRepetitionType } from '@parquet/thrift';
import ParquetSchema from './schema';
import ParquetField from './field';

export interface ColumnData {
  count: number;
  values: unknown[];
  rLevels: number[];
  dLevels: number[];
}

export interface RecordGroup {
  numRows: number;
  columns: Record<string, ColumnData>;
}

export function dremelEncode(rows: Record<string, unknown>[], schema: ParquetSchema) {
  const rowGroup: RecordGroup = {
    numRows: rows.length,
    columns: {},
  };

  schema.fields.list.forEach((field) => {
    rowGroup.columns[field.pathName] = encodeColumn(field, rows);
  });

  return rowGroup;
}

function encodeColumn(column: ParquetField, rows: Record<string, unknown>[]): ColumnData {
  const data: ColumnData = {
    count: 0,
    values: [],
    rLevels: [],
    dLevels: [],
  };

  const path = [...column.parents, column];
  let rLevel = 0;

  encodeLevel(path, { [path[0].name]: rows }, true, 0);

  return data;

  function encodeLevel(
    subPath: ParquetField[],
    obj: Record<string, unknown>,
    repeated: boolean,
    parentDLevel: number,
  ) {
    const field = subPath[0];
    const value = obj[field.name];
    const dLevel = parentDLevel + field.repetitionType === FieldRepetitionType.REQUIRED ? 1 : 0;

    if (value === null || value === undefined) {
      data.count += 1;
      data.dLevels.push(dLevel);
      data.rLevels.push(rLevel);
    } else if (subPath.length === 1) {
      data.count += 1;
      data.dLevels.push(dLevel);
      data.rLevels.push(rLevel);
      data.values.push(value);
    }

    if (repeated) {
      if (!Array.isArray(value)) {
        throw new TypeError(`Expected array for ${field.pathName} but got ${value}`);
      }

      for (const item of value) {
        rLevel += 1;
        encodeLevel(subPath.slice(1), item, false, dLevel);
        rLevel -= 1;
      }
    } else {
      encodeLevel(subPath.slice(1), value as Record<string, unknown>, false, dLevel);
    }
  }
}

export function dremelDecode(recordGroup: RecordGroup, schema: ParquetSchema) {
  const columns = Object.keys(recordGroup.columns).map((col) => schema.getField(col));
  const rows: Record<string, unknown>[] = [];

  for (const column of columns) {
    decodeColumn(column, recordGroup.columns[column.pathName], rows);
  }

  return rows;
}

function decodeColumn(
  column: ParquetField,
  { rLevels, dLevels, values }: ColumnData,
  rows: Record<string, unknown>[],
) {
  const indexes: number[] = new Array(column.rLevel + 1).fill(0) as number[];
  const path = [...column.parents, column];
  let valueOffset = 0;

  for (const [i, dLevel] of dLevels.entries()) {
    const rLevel = rLevels[i];

    if (i !== 0) {
      indexes[rLevel] += 1;
      indexes.fill(0, rLevel + 1);
    }

    let current: Record<string, unknown> = {
      [path[0].name]: rows,
    };

    const value =
      dLevel === column.dLevel ? column.logicalType!.deserialize(values[valueOffset++]) : null;

    let currentDLevel = 0;

    for (let depth = -1; depth < path.length - 1; depth += 1) {
      const field = path[depth + 1];
      const repeated = field.repetitionType === FieldRepetitionType.REPEATED || depth < 0;
      const nullable = field.repetitionType !== FieldRepetitionType.REQUIRED;

      if (nullable && depth >= 0 && dLevel > currentDLevel) {
        currentDLevel += 1;
      }

      const finalValue =
        depth >= 0 &&
        currentDLevel === dLevel &&
        (!path[depth + 2] || path[depth + 2].repetitionType !== FieldRepetitionType.REQUIRED);

      if (repeated) {
        if (!current[field.name]) {
          current[field.name] = [];
        }

        const list = current[field.name] as unknown[];
        const idx = indexes[field.rLevel];

        if (finalValue) {
          list[idx] = value;
          break;
        } else {
          list[idx] = list[idx] || {};
          current = list[idx] as Record<string, unknown>;
        }
      } else if (finalValue) {
        current[field.name] = value;
        break;
      } else {
        current[field.name] = current[field.name] || {};
        current = current[field.name] as Record<string, unknown>;
      }
    }
  }
}
