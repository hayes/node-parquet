/* eslint-disable no-param-reassign */
/* eslint-disable complexity */
import { ParquetField } from './field';

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

export function dremelDisassemble(column: ParquetField, rows: Record<string, unknown>[]) {
  const data: ColumnData = {
    count: 0,
    values: [],
    rLevels: [],
    dLevels: [],
  };

  let nextRLevel = 0;

  const path = [...column.parents, column];

  for (const row of rows) {
    disassembleObject(row, 0);
    nextRLevel = 0;
  }

  return data;

  function disassembleObject(obj: Record<string, unknown>, pathIdx: number) {
    const field = path[pathIdx];
    if (field instanceof ParquetField) {
      if (field.repetitionType === 'REPEATED') {
        const list = obj[field.name];

        if (!Array.isArray(list)) {
          throw new TypeError(`Expected ${field.pathName} to be an array, but got ${list}`);
        }

        if (list.length === 0) {
          data.dLevels.push(field.dLevel - 1);
          data.rLevels.push(nextRLevel);
          data.count++;
        } else {
          for (const item of list) {
            data.values.push(field.logicalType.serialize(item));
            data.dLevels.push(field.dLevel);
            data.rLevels.push(nextRLevel);
            data.count += 1;
            nextRLevel = field.rLevel;
          }
        }
      } else {
        const value = obj[field.name];
        const isNull = value === null || value === undefined;
        if (isNull && field.repetitionType !== 'OPTIONAL') {
          throw new TypeError(`Missing required value ${field.pathName}`);
        }

        if (!isNull) {
          data.values.push(field.logicalType.serialize(value));
        }
        data.dLevels.push(field.dLevel - (isNull ? 1 : 0));
        data.rLevels.push(nextRLevel);
        data.count += 1;
      }
    } else if (field.repetitionType === 'REPEATED') {
      const list = obj[field.name];

      if (!Array.isArray(list)) {
        throw new TypeError(`Expected ${field.pathName} to be an array, but got ${list}`);
      }

      if (list.length === 0) {
        data.dLevels.push(field.dLevel - 1);
        data.rLevels.push(nextRLevel);
        data.count++;
      } else {
        for (const item of list) {
          if (!item || typeof item !== 'object') {
            throw new Error(`List items are required ${field.pathName}`);
          }
          disassembleObject(item as Record<string, unknown>, pathIdx + 1);
          nextRLevel = field.rLevel;
        }
      }
    } else {
      const value = obj[field.name];
      const isNull = value === null || value === undefined;
      if (isNull && field.repetitionType !== 'OPTIONAL') {
        throw new TypeError(`Missing required value ${field.pathName}`);
      }

      if (!isNull && typeof value !== 'object') {
        throw new Error(`Expected an object for nested field ${field.pathName}`);
      }

      if (isNull) {
        data.dLevels.push(field.dLevel - 1);
        data.rLevels.push(nextRLevel);
        data.count += 1;
      } else {
        disassembleObject(value as Record<string, unknown>, pathIdx + 1);
      }
    }
  }
}

export function dremelAssemble(
  column: ParquetField,
  data: ColumnData,
  existingRows?: Record<string, unknown>[],
) {
  const rows = existingRows || [];
  const path = [...column.parents, column];

  let index = 0;
  let valueIndex = 0;
  let rowIndex = 0;

  while (index < data.dLevels.length) {
    if (!rows[rowIndex]) {
      rows[rowIndex] = {};
    }

    assembleObject(rows[rowIndex], 0);
    rowIndex += 1;
  }

  return rows;

  function assembleObject(obj: Record<string, unknown>, pathIdx: number) {
    const field = path[pathIdx];
    if (field instanceof ParquetField) {
      if (field.repetitionType === 'REPEATED') {
        if (!obj[field.name]) {
          obj[field.name] = [];
        }

        const list = obj[field.name] as unknown[];

        if (data.dLevels[index] === field.dLevel) {
          do {
            list.push(field.logicalType.deserialize(data.values[valueIndex++]));
            index += 1;
          } while (data.rLevels[index] === field.rLevel);
        } else {
          index += 1;
        }
      } else {
        if (field.dLevel === data.dLevels[index]) {
          obj[field.name] = field.logicalType.deserialize(data.values[valueIndex++]);
        } else {
          obj[field.name] = null;
        }

        index++;
      }
    } else if (field.repetitionType === 'REPEATED') {
      if (!obj[field.name]) {
        obj[field.name] = [];
      }

      const list = obj[field.name] as Record<string, unknown>[];
      let listIndex = 0;

      if (data.dLevels[index] >= field.dLevel) {
        do {
          if (!list[listIndex]) {
            list[listIndex] = {};
          }
          const item = list[listIndex++];
          assembleObject(item, pathIdx + 1);
        } while (data.rLevels[index] === field.rLevel);
      } else {
        index += 1;
      }
    } else if (field.repetitionType === 'OPTIONAL' && data.dLevels[index] < field.dLevel) {
      obj[field.name] = null;
      index++;
    } else {
      if (!obj[field.name]) {
        obj[field.name] = {};
      }

      assembleObject(obj[field.name] as Record<string, unknown>, pathIdx + 1);
    }
  }
}
