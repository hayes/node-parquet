import type * as zlib from 'zlib';
import BaseCompression from './base';

function lazyLoadZlib(): typeof zlib {
  // eslint-disable-next-line global-require
  return require('zlib');
}

export default class GzipCompression extends BaseCompression {
  inflate(buffer: Buffer) {
    return new Promise<Buffer>((resolve, reject) => {
      lazyLoadZlib().inflate(buffer, (error, result) => {
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
      lazyLoadZlib().deflate(buffer, (error, result) => {
        if (error) {
          reject(error);
        } else {
          resolve(result);
        }
      });
    });
  }
}
