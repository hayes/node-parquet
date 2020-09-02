import BaseCompression from './base';

interface ZSTD {
  compress: (buffer: Buffer, cb: (error: Error | null, output: Buffer) => void) => void;
  decompress: (buffer: Buffer, cb: (error: Error | null, output: Buffer) => void) => void;
}

function lazyLoadZSTD(): ZSTD {
  // eslint-disable-next-line global-require, import/no-extraneous-dependencies
  return require('cppzst');
}

export default class ZSTDCompression extends BaseCompression {
  inflate(buffer: Buffer) {
    return new Promise<Buffer>((resolve, reject) => {
      lazyLoadZSTD().decompress(buffer, (error, result) => {
        if (error) {
          reject(error);
        } else {
          resolve(result);
        }
      });
    });
  }

  deflate(buffer: Buffer) {
    return new Promise<Buffer>((resolve, reject) => {
      lazyLoadZSTD().decompress(buffer, (error, result) => {
        if (error) {
          reject(error);
        } else {
          resolve(result);
        }
      });
    });
  }
}
