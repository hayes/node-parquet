import BaseCompression from './base';

export default class UncompressedCompression extends BaseCompression {
  inflate(buffer: Buffer) {
    return buffer;
  }

  deflate(buffer: Buffer) {
    return buffer;
  }
}
