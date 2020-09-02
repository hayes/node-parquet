export default abstract class BaseCompression {
  abstract inflate(buffer: Buffer): Buffer | Promise<Buffer>;
  abstract deflate(buffer: Buffer): Buffer | Promise<Buffer>;
}
