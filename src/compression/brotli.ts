import BaseCompression from './base';

interface Brotli {
  compress: (buffer: Buffer | string, isText: boolean) => Buffer;
  decompress: (buffer: Buffer | string) => Buffer;
}

function lazyLoadLBrotli(): Brotli {
  // eslint-disable-next-line global-require, import/no-extraneous-dependencies
  return require('brotli');
}

export default class BrotliCompression extends BaseCompression {
  inflate(buffer: Buffer) {
    return lazyLoadLBrotli().decompress(buffer);
  }

  deflate(buffer: Buffer) {
    return lazyLoadLBrotli().compress(buffer, false);
  }
}
