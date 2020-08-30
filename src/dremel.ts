/* eslint-disable complexity */
import ParquetSchema from './schema';
import { FieldRepetitionType } from '../gen-nodejs/parquet_types';
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

export function dremelEncode() {
  throw new Error('Not implemented');
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
  const indexes: number[] = new Array(column.maxRLevel + 1).fill(0) as number[];
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
      dLevel === column.maxDLevel ? column.logicalType!.deserialize(values[valueOffset++]) : null;

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
        const idx = indexes[field.maxRLevel];

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
