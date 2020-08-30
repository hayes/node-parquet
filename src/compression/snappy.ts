import type snappy from 'snappy';
import BaseCompression from './base';

function lazyLoadSnappy(): typeof snappy {
  // eslint-disable-next-line global-require, import/no-extraneous-dependencies
  return require('snappy');
}

export default class SnappyCompression extends BaseCompression {
  async inflate(buffer: Buffer) {
    return new Promise<Buffer>((resolve, reject) => {
      lazyLoadSnappy().uncompress(
        buffer,
        {
          asBuffer: true,
        },
        (error, result) => {
          if (error) {
            reject(error instanceof Error ? error : new Error(error));
          } else {
            resolve(result as Buffer);
          }
        },
      );
    });
  }

  async deflate(buffer: Buffer) {
    return new Promise<Buffer>((resolve, reject) => {
      lazyLoadSnappy().compress(buffer, (error, result) => {
        if (error) {
          reject(error instanceof Error ? error : new Error(error));
        } else {
          resolve(result);
        }
      });
    });
  }
}
