import { Type, FieldRepetitionType, SchemaElement } from '@parquet/thrift';
import LogicalType from './logical-types';

export interface SchemaFields {
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
  rLevel: number;
  dLevel: number;
  parents: ParquetField[];
  isLeaf: boolean;

  constructor(
    thriftElement: SchemaElement,
    {
      dLevel,
      rLevel,
      parents,
      isLeaf,
    }: {
      rLevel: number;
      dLevel: number;
      parents: ParquetField[];
      isLeaf: boolean;
    },
  ) {
    this.name = thriftElement.name;
    this.type = thriftElement.type;
    this.isLeaf = isLeaf;
    this.repetitionType = thriftElement.repetition_type;
    this.fieldId = thriftElement.field_id;
    this.parents = parents;
    this.pathName = [...parents.slice(1), this].map((el) => el.name).join('.');
    this.logicalType = this.isLeaf ? LogicalType.fromThrift(thriftElement) : null;
    this.rLevel = rLevel;
    this.dLevel = dLevel;
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
      const currentRLevel =
        rLevel + (thriftElement.repetition_type === FieldRepetitionType.REPEATED ? 1 : 0);
      const currentDLevel =
        dLevel + (thriftElement.repetition_type === FieldRepetitionType.REQUIRED ? 0 : 1);

      const element = new ParquetField(thriftElement, {
        rLevel: currentRLevel,
        dLevel: currentDLevel,
        parents,
        isLeaf: !thriftElement.num_children,
      });

      if (element.isLeaf) {
        list.push(element);
      }

      for (let j = 0; j < thriftElement.num_children || 0; j += 1) {
        const child = createElement([...parents, element], currentRLevel, currentDLevel);

        children[child.name] = child;
        map[child.pathName] = child;
      }

      return element;
    }

    return {
      list,
      map,
    };
  }
}
