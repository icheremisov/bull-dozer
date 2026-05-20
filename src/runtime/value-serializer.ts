import { SerializationError } from '../errors/serialization.error';

const ENVELOPE_MARKER = '__dozer_serialized__';

type SerializedEnvelopeType =
  | 'undefined'
  | 'date'
  | 'array-buffer'
  | 'buffer'
  | 'typed-array'
  | 'data-view'
  | 'blob';

interface SerializedEnvelope {
  [ENVELOPE_MARKER]: SerializedEnvelopeType;
  v?: string;
  c?: string;
  m?: string;
}

const typedArrayNames = new Set<string>([
  'Int8Array',
  'Uint8Array',
  'Uint8ClampedArray',
  'Int16Array',
  'Uint16Array',
  'Int32Array',
  'Uint32Array',
  'Float32Array',
  'Float64Array',
  'BigInt64Array',
  'BigUint64Array',
]);

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return typeof value === 'object' && value !== null;
};

const isPlainObject = (value: unknown): value is Record<string, unknown> => {
  if (!isRecord(value)) {
    return false;
  }

  const prototype = Object.getPrototypeOf(value) as object | null;
  return prototype === Object.prototype || prototype === null;
};

const isSerializedEnvelope = (value: unknown): value is SerializedEnvelope => {
  if (!isRecord(value)) {
    return false;
  }

  return typeof value[ENVELOPE_MARKER] === 'string';
};

const bytesToBase64 = (bytes: Uint8Array): string => {
  return Buffer.from(bytes).toString('base64');
};

const base64ToBytes = (encoded: string): Uint8Array => {
  return Buffer.from(encoded, 'base64');
};

const bytesToArrayBuffer = (bytes: Uint8Array): ArrayBuffer => {
  const buffer = new ArrayBuffer(bytes.byteLength);
  const view = new Uint8Array(buffer);
  view.set(bytes);
  return buffer;
};

const toStepPath = (path: string, next: string): string => {
  if (!path) {
    return next;
  }
  return `${path}.${next}`;
};

const decodeTypedArray = (typeName: string, encoded: string): unknown => {
  const bytes = base64ToBytes(encoded);
  if (typeName === 'Uint8Array') {
    return new Uint8Array(bytes);
  }
  if (typeName === 'Int8Array') {
    return new Int8Array(bytesToArrayBuffer(bytes));
  }
  if (typeName === 'Uint8ClampedArray') {
    return new Uint8ClampedArray(bytesToArrayBuffer(bytes));
  }
  if (typeName === 'Int16Array') {
    return new Int16Array(bytesToArrayBuffer(bytes));
  }
  if (typeName === 'Uint16Array') {
    return new Uint16Array(bytesToArrayBuffer(bytes));
  }
  if (typeName === 'Int32Array') {
    return new Int32Array(bytesToArrayBuffer(bytes));
  }
  if (typeName === 'Uint32Array') {
    return new Uint32Array(bytesToArrayBuffer(bytes));
  }
  if (typeName === 'Float32Array') {
    return new Float32Array(bytesToArrayBuffer(bytes));
  }
  if (typeName === 'Float64Array') {
    return new Float64Array(bytesToArrayBuffer(bytes));
  }
  if (typeName === 'BigInt64Array') {
    return new BigInt64Array(bytesToArrayBuffer(bytes));
  }
  if (typeName === 'BigUint64Array') {
    return new BigUint64Array(bytesToArrayBuffer(bytes));
  }

  throw new SerializationError(
    `Unknown typed array constructor "${typeName}".`,
  );
};

const deserializeEnvelope = (value: SerializedEnvelope): unknown => {
  const type = value[ENVELOPE_MARKER];
  if (type === 'undefined') {
    return undefined;
  }
  if (type === 'date') {
    const serializedDate = value.v ?? '';
    const date = new Date(serializedDate);
    if (Number.isNaN(date.getTime())) {
      throw new SerializationError(
        `Invalid serialized date value "${serializedDate}".`,
      );
    }
    return date;
  }
  if (type === 'array-buffer') {
    return bytesToArrayBuffer(base64ToBytes(value.v ?? ''));
  }
  if (type === 'buffer') {
    return Buffer.from(value.v ?? '', 'base64');
  }
  if (type === 'typed-array') {
    const constructorName = value.c;
    if (!constructorName) {
      throw new SerializationError('Missing typed array constructor metadata.');
    }
    return decodeTypedArray(constructorName, value.v ?? '');
  }
  if (type === 'data-view') {
    const bytes = base64ToBytes(value.v ?? '');
    return new DataView(bytesToArrayBuffer(bytes));
  }
  if (type === 'blob') {
    const bytes = base64ToBytes(value.v ?? '');
    if (typeof Blob === 'undefined') {
      return bytes;
    }

    return new Blob([bytesToArrayBuffer(bytes)], {
      type: value.m ?? '',
    });
  }

  throw new SerializationError(
    `Unknown serialized envelope type "${String(type)}".`,
  );
};

const serializeInternal = async (
  value: unknown,
  path: string,
  seen: WeakSet<object>,
): Promise<unknown> => {
  if (value === undefined) {
    const envelope: SerializedEnvelope = {
      [ENVELOPE_MARKER]: 'undefined',
    };
    return envelope;
  }
  if (value === null) {
    return null;
  }

  const valueType = typeof value;
  if (
    valueType === 'string' ||
    valueType === 'number' ||
    valueType === 'boolean'
  ) {
    return value;
  }
  if (valueType === 'bigint') {
    throw new SerializationError(
      `Unsupported bigint value at "${path}". Use string/number representation.`,
    );
  }
  if (valueType === 'function' || valueType === 'symbol') {
    throw new SerializationError(
      `Unsupported ${valueType} value at "${path}".`,
    );
  }

  if (!isRecord(value)) {
    return value;
  }

  if (seen.has(value)) {
    throw new SerializationError(`Circular reference detected at "${path}".`);
  }

  if (Buffer.isBuffer(value)) {
    const envelope: SerializedEnvelope = {
      [ENVELOPE_MARKER]: 'buffer',
      v: value.toString('base64'),
    };
    return envelope;
  }

  if (value instanceof Date) {
    const timestamp = value.getTime();
    if (Number.isNaN(timestamp)) {
      throw new SerializationError(`Invalid Date value at "${path}".`);
    }

    const envelope: SerializedEnvelope = {
      [ENVELOPE_MARKER]: 'date',
      v: value.toISOString(),
    };
    return envelope;
  }

  if (typeof Blob !== 'undefined' && value instanceof Blob) {
    const bytes = new Uint8Array(await value.arrayBuffer());
    const envelope: SerializedEnvelope = {
      [ENVELOPE_MARKER]: 'blob',
      v: bytesToBase64(bytes),
      m: value.type,
    };
    return envelope;
  }

  if (value instanceof ArrayBuffer) {
    const envelope: SerializedEnvelope = {
      [ENVELOPE_MARKER]: 'array-buffer',
      v: bytesToBase64(new Uint8Array(value)),
    };
    return envelope;
  }

  if (ArrayBuffer.isView(value)) {
    if (value instanceof DataView) {
      const bytes = new Uint8Array(
        value.buffer,
        value.byteOffset,
        value.byteLength,
      );
      const envelope: SerializedEnvelope = {
        [ENVELOPE_MARKER]: 'data-view',
        v: bytesToBase64(bytes),
      };
      return envelope;
    }

    const constructorName = (value as { constructor?: { name?: string } })
      .constructor?.name;
    if (!constructorName || !typedArrayNames.has(constructorName)) {
      throw new SerializationError(
        `Unsupported typed array constructor at "${path}".`,
      );
    }

    const bytes = new Uint8Array(
      value.buffer,
      value.byteOffset,
      value.byteLength,
    );
    const envelope: SerializedEnvelope = {
      [ENVELOPE_MARKER]: 'typed-array',
      c: constructorName,
      v: bytesToBase64(bytes),
    };
    return envelope;
  }

  if (Array.isArray(value)) {
    seen.add(value);
    try {
      const serialized: unknown[] = [];
      for (let index = 0; index < value.length; index += 1) {
        serialized.push(
          await serializeInternal(value[index], `${path}[${index}]`, seen),
        );
      }

      return serialized;
    } finally {
      seen.delete(value);
    }
  }

  if (!isPlainObject(value)) {
    throw new SerializationError(`Unsupported object type at "${path}".`);
  }

  seen.add(value);
  try {
    const serialized: Record<string, unknown> = {};
    for (const key of Object.keys(value)) {
      serialized[key] = await serializeInternal(
        value[key],
        toStepPath(path, key),
        seen,
      );
    }

    return serialized;
  } finally {
    seen.delete(value);
  }
};

const deserializeInternal = (value: unknown): unknown => {
  if (!isRecord(value)) {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map((item) => deserializeInternal(item));
  }

  if (isSerializedEnvelope(value)) {
    return deserializeEnvelope(value);
  }

  const deserialized: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value)) {
    deserialized[key] = deserializeInternal(entry);
  }
  return deserialized;
};

export const serializeForStorage = async (
  value: unknown,
  path = 'value',
): Promise<unknown> => {
  return serializeInternal(value, path, new WeakSet<object>());
};

export const deserializeFromStorage = (value: unknown): unknown => {
  return deserializeInternal(value);
};
