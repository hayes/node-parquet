import { FieldRepetitionType, SchemaElement } from '@parquet/thrift';
import { ParquetField } from '.';

export interface SchemaFields {
  list: ParquetField[];
  map: Record<string, ParquetField>;
}

export function schemaFromThrift(elements: SchemaElement[]): SchemaFields {
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
