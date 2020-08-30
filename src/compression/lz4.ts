import BaseCompression from './base';

interface LZ4 {
  compress: (buffer: number[] | Buffer, maxSize?: number) => number[];
  decompress: (buffer: number[] | Buffer, maxSize?: number) => number[];
}

function lazyLoadLZ4(): LZ4 {
  // eslint-disable-next-line global-require, import/no-extraneous-dependencies
  return require('lz4js');
}

export default class LZ4Compression extends BaseCompression {
  inflate(buffer: Buffer) {
    return Buffer.from(lazyLoadLZ4().decompress(buffer));
  }

  deflate(buffer: Buffer) {
    return Buffer.from(lazyLoadLZ4().compress(buffer));
  }
}
