import { TFramedTransport, TCompactProtocol, TProtocol } from 'thrift';
import ByteReader from './reader/byte';

export function readUnsignedVarInt(reader: ByteReader) {
  let value = 0n;
  let i = 0n;
  let byte = BigInt(reader.readByte());

  while ((byte & 0x80n) !== 0n) {
    value |= (byte & 0x7fn) << i;
    i += 7n;

    byte = BigInt(reader.readByte());
  }

  return value | (byte << i);
}

export function readZigZagVarInt(reader: ByteReader) {
  const raw = readUnsignedVarInt(reader);

  return raw % 2n ? -(raw / 2n) - 1n : raw / 2n;
}

export function readBitPackedInts(
  reader: ByteReader,
  count: number | bigint,
  bitWidth: number | bigint,
) {
  // eslint-disable-next-line no-param-reassign
  bitWidth = BigInt(bitWidth);
  // eslint-disable-next-line no-param-reassign
  count = BigInt(count);

  if (count % 8n !== 0n) {
    throw new RangeError('count must be a multiple of 8');
  }

  const values: bigint[] = [];

  for (let i = 0; i < count; i += 1) {
    values.push(0n);
  }

  let byte!: bigint;
  let value = 0n;
  for (let bit = 0n; bit < bitWidth * count; bit += 1n) {
    if (bit % 8n === 0n) {
      byte = BigInt(reader.readByte());
    }

    if (byte & (1n << bit % 8n)) {
      value |= 1n << bit % bitWidth;
    }

    if ((bit + 1n) % bitWidth === 0n) {
      values[Number(bit / bitWidth)] = value;
      value = 0n;
    }
  }

  return values;
}

export function minBitWidth(num: number) {
  if (num === 0) {
    return 0;
  }

  return Math.ceil(Math.log2(num + 1));
}

export function decodeThriftObject<T>(Class: { new (): T }, buffer: Buffer, offset: number) {
  const object = new Class();

  const transport = new TFramedTransport(buffer) as TFramedTransport & { readPos: number };
  const protocol = new TCompactProtocol(transport);

  transport.readPos = offset;

  (object as T & { read: (protocol: TProtocol) => void }).read(protocol);

  return {
    object,
    bytesRead: transport.readPos - offset,
  };
}
