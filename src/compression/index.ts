import { CompressionCodec } from '../../gen-nodejs/parquet_types';
import BrotliCompression from './brotli';
import GzipCompression from './gzip';
import LZ4Compression from './lz4';
import LZOCompression from './lzo';
import SnappyCompression from './snappy';
import UncompressedCompression from './uncompressed';
import ZSTDCompression from './zstd';
import BaseCompression from './base';

export {
  BaseCompression,
  BrotliCompression,
  GzipCompression,
  LZ4Compression,
  LZOCompression,
  SnappyCompression,
  UncompressedCompression,
  ZSTDCompression,
};

export default function compressionFromThrift(codec: CompressionCodec) {
  switch (codec) {
    case CompressionCodec.BROTLI:
      return new BrotliCompression();
    case CompressionCodec.GZIP:
      return new GzipCompression();
    case CompressionCodec.LZ4:
      return new LZ4Compression();
    case CompressionCodec.LZO:
      return new LZOCompression();
    case CompressionCodec.SNAPPY:
      return new SnappyCompression();
    case CompressionCodec.UNCOMPRESSED:
      return new UncompressedCompression();
    case CompressionCodec.ZSTD:
      return new ZSTDCompression();
    default:
      throw new Error(`Unknown compression codec ${codec}`);
  }
}
