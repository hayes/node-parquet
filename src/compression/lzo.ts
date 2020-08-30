import BaseCompression from './base';

interface LZO {
  compress: (buffer: Buffer) => Buffer;
  decompress: (buffer: Buffer) => Buffer;
}

function lazyLoadLzo(): LZO {
  // eslint-disable-next-line global-require, import/no-extraneous-dependencies
  return require('lzo');
}

export default class LZOCompression extends BaseCompression {
  lzo: LZO;

  constructor() {
    super();

    this.lzo = lazyLoadLzo();
  }

  inflate(buffer: Buffer) {
    return lazyLoadLzo().decompress(buffer);
  }

  deflate(buffer: Buffer) {
    return lazyLoadLzo().compress(buffer);
  }
}
