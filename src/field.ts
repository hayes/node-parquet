import { Type, FieldRepetitionType, SchemaElement } from '../gen-nodejs/parquet_types';
import LogicalType from './logical-types';

export interface SchemaFields {
  root: ParquetField;
  list: ParquetField[];
  map: Record<string, ParquetField>;
}

export default class ParquetField {
  type: Type;
  fieldId: number;
  repetitionType: FieldRepetitionType;
  name: string;
  pathName: string;
  logicalType: LogicalType | null;
  children: Record<string, ParquetField> | null;
  maxRLevel: number;
  maxDLevel: number;
  parents: ParquetField[];
  isLeaf: boolean;

  constructor(
    thriftElement: SchemaElement,
    {
      maxDLevel,
      maxRLevel,
      parents,
      children,
      isLeaf,
    }: {
      maxRLevel: number;
      maxDLevel: number;
      parents: ParquetField[];
      children: Record<string, ParquetField> | null;
      isLeaf: boolean;
    },
  ) {
    this.name = thriftElement.name;
    this.type = thriftElement.type;
    this.isLeaf = isLeaf;
    this.repetitionType = thriftElement.repetition_type;
    this.fieldId = thriftElement.field_id;
    this.parents = parents;
    this.children = children;
    this.pathName = [...parents.slice(1), this].map((el) => el.name).join('.');
    this.logicalType = this.isLeaf ? LogicalType.fromThrift(thriftElement) : null;
    this.maxRLevel = maxRLevel;
    this.maxDLevel = maxDLevel;
  }

  static fromThrift(elements: SchemaElement[]): SchemaFields {
    let i = 0;
    const list: ParquetField[] = [];
    const map: Record<string, ParquetField> = {};

    for (; i < elements.length; i += 1) {
      createElement([], 0, -1);
    }

    function createElement(parents: ParquetField[], rLevel: number, dLevel: number) {
      const thriftElement = elements[i++];

      const children: Record<string, ParquetField> = {};
      const maxRLevel =
        rLevel + (thriftElement.repetition_type === FieldRepetitionType.REPEATED ? 1 : 0);
      const maxDLevel =
        dLevel + (thriftElement.repetition_type === FieldRepetitionType.REQUIRED ? 0 : 1);

      const element = new ParquetField(thriftElement, {
        maxRLevel,
        maxDLevel,
        parents,
        children: null,
        isLeaf: !thriftElement.num_children,
      });

      list.push(element);

      for (let j = 0; j < thriftElement.num_children || 0; j += 1) {
        const child = createElement([...parents, element], maxRLevel, maxDLevel);

        children[child.name] = child;
        map[child.pathName] = child;
      }

      element.children = children;

      return element;
    }

    return {
      root: list[0],
      list,
      map,
    };
  }
}
