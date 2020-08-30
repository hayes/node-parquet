/* eslint-disable no-cond-assign */
import * as path from 'path';
import { ParquetFileReader } from '../src';

describe('compatibility', () => {
  test('alltypes_plane', async () => {
    const reader = await ParquetFileReader.openFile(
      path.join(__dirname, './data/alltypes_plain.parquet'),
    );

    const records = reader.readRecords();

    const all = [];

    for await (const record of records) {
      all.push(record);
    }

    expect(
      all.map((row) => ({
        ...row,
        string_col: (row.string_col as Buffer).toString('utf8'),
        date_string_col: (row.date_string_col as Buffer).toString('utf8'),
      })),
    ).toMatchSnapshot();

    await reader.close();
  });

  test('alltypes_plane.snappy', async () => {
    const reader = await ParquetFileReader.openFile(
      path.join(__dirname, './data/alltypes_plain.snappy.parquet'),
    );

    const records = reader.readRecords();

    const all = [];

    for await (const record of records) {
      all.push(record);
    }

    expect(
      all.map((row) => ({
        ...row,
        string_col: (row.string_col as Buffer).toString('utf8'),
        date_string_col: (row.date_string_col as Buffer).toString('utf8'),
      })),
    ).toMatchSnapshot();

    await reader.close();
  });

  test('alltypes_dictionary', async () => {
    const reader = await ParquetFileReader.openFile(
      path.join(__dirname, './data/alltypes_dictionary.parquet'),
    );

    const records = reader.readRecords();

    const all = [];

    for await (const record of records) {
      all.push(record);
    }

    expect(
      all.map((row) => ({
        ...row,
        string_col: (row.string_col as Buffer).toString('utf8'),
        date_string_col: (row.date_string_col as Buffer).toString('utf8'),
      })),
    ).toMatchSnapshot();

    await reader.close();
  });

  test('binary', async () => {
    const reader = await ParquetFileReader.openFile(path.join(__dirname, './data/binary.parquet'));

    const records = reader.readRecords();

    const all = [];

    for await (const record of records) {
      all.push(record);
    }

    expect(all).toMatchSnapshot();

    await reader.close();
  });

  // // test('bloom_filter', async () => {
  // //   const reader = await ParquetReader.openFile(path.join(__dirname, './data/bloom_filter.bin'));

  // //   const cursor = reader.getCursor();

  // //   const all = [];
  // //   let record: any;
  // //   while ((record = await cursor.next())) {
  // //     all.push(record);
  // //   }

  // //   expect(all).toMatchSnapshot();

  // //   await reader.close();
  // // });

  // // test('byte_array_decimal', async () => {
  // //   const reader = await ParquetReader.openFile(
  // //     path.join(__dirname, './data/byte_array_decimal.parquet'),
  // //   );

  // //   const cursor = reader.getCursor();

  // //   const all = [];
  // //   let record: any;
  // //   while ((record = await cursor.next())) {
  // //     all.push(record);
  // //   }

  // //   expect(all).toMatchSnapshot();

  // //   await reader.close();
  // // });

  test('datapage_v2', async () => {
    const reader = await ParquetFileReader.openFile(
      path.join(__dirname, '/data/datapage_v2.snappy.parquet'),
    );

    const records = reader.readRecords();

    const all = [];

    for await (const record of records) {
      all.push(record);
    }

    expect(all).toMatchSnapshot();

    await reader.close();
  });

  test('dict-page-offset-zero', async () => {
    const reader = await ParquetFileReader.openFile(
      path.join(__dirname, './data/dict-page-offset-zero.parquet'),
    );

    const records = reader.readRecords();

    const all = [];

    for await (const record of records) {
      all.push(record);
    }

    expect(all).toMatchSnapshot();

    await reader.close();
  });

  // test('single_nan', async () => {
  //   const reader = await ParquetFileReader.openFile(
  //     path.join(__dirname, './data/single_nan.parquet'),
  //   );

  //   const records = reader.readRecords();

  //   const all = [];

  //   for await (const record of records) {
  //     all.push(record);
  //   }

  //   expect(all).toMatchSnapshot();

  //   await reader.close();
  // });
});
