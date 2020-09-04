import { ParquetSchema } from '../src';

describe('dremel encoding', () => {
  const schema = ParquetSchema.fromDefinition({
    fields: {
      a: {
        type: 'STRING',
        repetitionType: 'REQUIRED',
      },
      b: {
        type: 'STRING',
        repetitionType: 'OPTIONAL',
      },
      c: {
        type: 'STRING',
        repetitionType: 'REPEATED',
      },
      d: {
        repetitionType: 'REQUIRED',
        fields: {
          e: {
            repetitionType: 'OPTIONAL',
            fields: {
              f: {
                type: 'STRING',
                repetitionType: 'REPEATED',
              },
              g: {
                type: 'STRING',
                repetitionType: 'OPTIONAL',
              },
              h: {
                type: 'STRING',
                repetitionType: 'REQUIRED',
              },
              i: {
                repetitionType: 'REPEATED',
                fields: {
                  j: {
                    type: 'STRING',
                    repetitionType: 'REPEATED',
                  },
                  k: {
                    type: 'STRING',
                    repetitionType: 'OPTIONAL',
                  },
                  l: {
                    type: 'STRING',
                    repetitionType: 'REQUIRED',
                  },
                },
              },
            },
          },
        },
      },
    },
  });

  const sampleData = [
    {
      a: 'a',
      b: 'b',
      c: ['c', 'c'],
      d: {
        e: {
          f: ['f', 'f'],
          g: 'g',
          h: 'h',
          i: [
            {
              j: ['j', 'j'],
              k: 'k',
              l: 'l',
            },
          ],
        },
      },
    },
    {
      a: 'a',
      b: null,
      c: [],
      d: {
        e: null,
      },
    },
    {
      a: 'a',
      b: null,
      c: ['c', 'c'],
      d: {
        e: {
          f: [],
          g: null,
          h: 'h',
          i: [
            {
              j: [],
              k: null,
              l: 'l',
            },
          ],
        },
      },
    },
    {
      a: 'a',
      b: 'b',
      c: ['c', 'c'],
      d: {
        e: {
          f: ['f', 'f'],
          g: 'g',
          h: 'h',
          i: [
            {
              j: ['j', 'j'],
              k: 'k',
              l: 'l',
            },
          ],
        },
      },
    },
  ];

  test('disassemble some data', () => {
    const data = schema.disassemble(sampleData);

    expect(data).toEqual({
      a: {
        count: 4,
        values: [Buffer.from('a'), Buffer.from('a'), Buffer.from('a'), Buffer.from('a')],
        rLevels: [0, 0, 0, 0],
        dLevels: [0, 0, 0, 0],
      },
      b: {
        count: 4,
        values: [Buffer.from('b'), Buffer.from('b')],
        rLevels: [0, 0, 0, 0],
        dLevels: [1, 0, 0, 1],
      },
      c: {
        count: 7,
        values: [
          Buffer.from('c'),
          Buffer.from('c'),
          Buffer.from('c'),
          Buffer.from('c'),
          Buffer.from('c'),
          Buffer.from('c'),
        ],
        rLevels: [0, 1, 0, 0, 1, 0, 1],
        dLevels: [1, 1, 0, 1, 1, 1, 1],
      },
      'd.e.f': {
        count: 6,
        values: [Buffer.from('f'), Buffer.from('f'), Buffer.from('f'), Buffer.from('f')],
        rLevels: [0, 1, 0, 0, 0, 1],
        dLevels: [2, 2, 0, 1, 2, 2],
      },
      'd.e.g': {
        count: 4,
        values: [Buffer.from('g'), Buffer.from('g')],
        rLevels: [0, 0, 0, 0],
        dLevels: [2, 0, 1, 2],
      },
      'd.e.h': {
        count: 4,
        values: [Buffer.from('h'), Buffer.from('h'), Buffer.from('h')],
        rLevels: [0, 0, 0, 0],
        dLevels: [1, 0, 1, 1],
      },
      'd.e.i.j': {
        count: 6,
        values: [Buffer.from('j'), Buffer.from('j'), Buffer.from('j'), Buffer.from('j')],
        rLevels: [0, 2, 0, 0, 0, 2],
        dLevels: [3, 3, 0, 2, 3, 3],
      },
      'd.e.i.k': {
        count: 4,
        values: [Buffer.from('k'), Buffer.from('k')],
        rLevels: [0, 0, 0, 0],
        dLevels: [3, 0, 2, 3],
      },
      'd.e.i.l': {
        count: 4,
        values: [Buffer.from('l'), Buffer.from('l'), Buffer.from('l')],
        rLevels: [0, 0, 0, 0],
        dLevels: [2, 0, 2, 2],
      },
    });
  });

  test('reassemble some data', () => {
    const data = schema.disassemble(sampleData);
    const assembled = schema.assemble(data);

    expect(assembled).toEqual(sampleData);
  });
});
