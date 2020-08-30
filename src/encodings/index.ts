/* eslint-disable complexity */
import ByteReader from '../reader/byte';
import { Encoding, Type } from '../../gen-nodejs/parquet_types';
import PlainEncoding from './plain';
import DictionaryEncoding from './dictionary';
import RunLengthEncoding from './rle';
import DeltaEncoding from './delta';

export { PlainEncoding, DictionaryEncoding, RunLengthEncoding, DeltaEncoding };

export function createEncoding(
  type: Type,
  reader: ByteReader,
  encoding: Encoding,
  options: {
    bitWidth?: number;
    typeLength?: number;
    dictionary?: unknown[] | null;
    disableEnvelope?: boolean;
  },
) {
  switch (encoding) {
    case Encoding.PLAIN:
      return new PlainEncoding(reader, options.typeLength);
    case Encoding.PLAIN_DICTIONARY:
    case Encoding.RLE_DICTIONARY:
      if (!options.dictionary) {
        throw new Error('dictionary is required when using Dictionary encoding');
      }

      return new DictionaryEncoding(reader, options.dictionary);
    case Encoding.RLE:
      // eslint-disable-next-line no-case-declarations
      const bitWidth = type === Type.BOOLEAN ? 1 : options.bitWidth;

      if (bitWidth == null) {
        throw new Error('bitWidth is required when using RLE encoding');
      }

      return new RunLengthEncoding(reader, bitWidth, options.disableEnvelope);
    case Encoding.DELTA_BINARY_PACKED:
      return new DeltaEncoding(reader, options.typeLength);
    case Encoding.BIT_PACKED:
    case Encoding.DELTA_LENGTH_BYTE_ARRAY:
    case Encoding.DELTA_BYTE_ARRAY:
    case Encoding.BYTE_STREAM_SPLIT:
    default:
      throw new Error(`Encoding type ${encoding} is not implemented yet`);
  }
}
