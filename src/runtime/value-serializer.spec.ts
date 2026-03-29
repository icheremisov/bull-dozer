import { SerializationError } from '../errors/serialization.error';
import {
  deserializeFromStorage,
  serializeForStorage,
} from './value-serializer';

const roundTrip = async (value: unknown): Promise<unknown> => {
  const serialized = await serializeForStorage(value);
  return deserializeFromStorage(serialized);
};

describe('serializeForStorage + deserializeFromStorage (round-trip)', () => {
  it('passes null through unchanged', async () => {
    expect(await roundTrip(null)).toBeNull();
  });

  it('passes string through unchanged', async () => {
    expect(await roundTrip('hello')).toBe('hello');
  });

  it('passes number through unchanged', async () => {
    expect(await roundTrip(42)).toBe(42);
  });

  it('passes boolean through unchanged', async () => {
    expect(await roundTrip(true)).toBe(true);
  });

  it('restores undefined from envelope', async () => {
    expect(await roundTrip(undefined)).toBeUndefined();
  });

  it('restores nested undefined inside object', async () => {
    const result = (await roundTrip({ a: undefined, b: 1 })) as Record<string, unknown>;
    expect(result.a).toBeUndefined();
    expect(result.b).toBe(1);
  });

  it('restores Date correctly', async () => {
    const date = new Date('2026-01-15T10:30:00.000Z');
    const result = await roundTrip(date);
    expect(result).toBeInstanceOf(Date);
    expect((result as Date).toISOString()).toBe(date.toISOString());
  });

  it('restores Uint8Array correctly', async () => {
    const bytes = new Uint8Array([1, 2, 3, 255]);
    const result = await roundTrip(bytes);
    expect(result).toBeInstanceOf(Uint8Array);
    expect(Array.from(result as Uint8Array)).toEqual([1, 2, 3, 255]);
  });

  it('restores Buffer correctly', async () => {
    const buf = Buffer.from([10, 20, 30]);
    const result = await roundTrip(buf);
    expect(Buffer.isBuffer(result)).toBe(true);
    expect(Array.from(result as Buffer)).toEqual([10, 20, 30]);
  });

  it('restores ArrayBuffer correctly', async () => {
    const ab = new Uint8Array([5, 6, 7]).buffer;
    const result = await roundTrip(ab);
    expect(result).toBeInstanceOf(ArrayBuffer);
    expect(Array.from(new Uint8Array(result as ArrayBuffer))).toEqual([5, 6, 7]);
  });

  it('restores DataView correctly', async () => {
    const ab = new Uint8Array([1, 2, 3, 4]).buffer;
    const dv = new DataView(ab);
    const result = await roundTrip(dv);
    expect(result).toBeInstanceOf(DataView);
    expect((result as DataView).getUint8(0)).toBe(1);
    expect((result as DataView).getUint8(3)).toBe(4);
  });

  it('restores all supported typed arrays', async () => {
    const cases: Array<[string, ArrayBufferView]> = [
      ['Int8Array', new Int8Array([-1, 0, 1])],
      ['Uint8ClampedArray', new Uint8ClampedArray([0, 128, 255])],
      ['Int16Array', new Int16Array([-100, 0, 100])],
      ['Uint16Array', new Uint16Array([0, 1000, 65535])],
      ['Int32Array', new Int32Array([-1000, 0, 1000])],
      ['Uint32Array', new Uint32Array([0, 1, 4294967295])],
      ['Float32Array', new Float32Array([1.5, -2.5])],
      ['Float64Array', new Float64Array([1.1, -2.2])],
    ];

    for (const [name, typed] of cases) {
      const result = await roundTrip(typed);
      expect(result?.constructor?.name).toBe(name);
      expect(Array.from(result as Int8Array)).toEqual(Array.from(typed as Int8Array));
    }
  });

  it('restores nested object with mixed types', async () => {
    const input = {
      count: 3,
      name: 'test',
      at: new Date('2026-01-01T00:00:00.000Z'),
      bytes: new Uint8Array([9, 8, 7]),
      flag: true,
      empty: null,
      missing: undefined,
    };
    const result = (await roundTrip(input)) as typeof input;
    expect(result.count).toBe(3);
    expect(result.name).toBe('test');
    expect(result.at).toBeInstanceOf(Date);
    expect(result.bytes).toBeInstanceOf(Uint8Array);
    expect(result.flag).toBe(true);
    expect(result.empty).toBeNull();
    expect(result.missing).toBeUndefined();
  });

  it('restores array with mixed types', async () => {
    const input = [1, 'two', null, undefined, new Date('2026-01-01T00:00:00.000Z')];
    const result = (await roundTrip(input)) as unknown[];
    expect(result[0]).toBe(1);
    expect(result[1]).toBe('two');
    expect(result[2]).toBeNull();
    expect(result[3]).toBeUndefined();
    expect(result[4]).toBeInstanceOf(Date);
  });
});

describe('serializeForStorage — error cases', () => {
  it('throws SerializationError for bigint', async () => {
    await expect(serializeForStorage(BigInt(42))).rejects.toBeInstanceOf(
      SerializationError,
    );
  });

  it('throws SerializationError for function', async () => {
    await expect(serializeForStorage(() => {})).rejects.toBeInstanceOf(
      SerializationError,
    );
  });

  it('throws SerializationError for symbol', async () => {
    await expect(serializeForStorage(Symbol('x'))).rejects.toBeInstanceOf(
      SerializationError,
    );
  });

  it('throws SerializationError for class instance (non-plain object)', async () => {
    class Foo { value = 1; }
    await expect(serializeForStorage(new Foo())).rejects.toBeInstanceOf(
      SerializationError,
    );
  });

  it('throws SerializationError for circular reference', async () => {
    const obj: Record<string, unknown> = {};
    obj.self = obj;
    await expect(serializeForStorage(obj)).rejects.toBeInstanceOf(
      SerializationError,
    );
  });

  it('throws SerializationError for invalid Date', async () => {
    await expect(serializeForStorage(new Date('invalid'))).rejects.toBeInstanceOf(
      SerializationError,
    );
  });

  it('includes path in error message for nested bigint', async () => {
    let error!: SerializationError;
    try {
      await serializeForStorage({ nested: { value: BigInt(1) } }, 'step-result');
    } catch (e) {
      error = e as SerializationError;
    }
    expect(error).toBeInstanceOf(SerializationError);
    expect(error.message).toContain('nested.value');
  });
});

describe('deserializeFromStorage — passthrough cases', () => {
  it('passes through primitive string', () => {
    expect(deserializeFromStorage('hello')).toBe('hello');
  });

  it('passes through number', () => {
    expect(deserializeFromStorage(42)).toBe(42);
  });

  it('passes through null', () => {
    expect(deserializeFromStorage(null)).toBeNull();
  });

  it('passes through undefined', () => {
    expect(deserializeFromStorage(undefined)).toBeUndefined();
  });

  it('passes through boolean', () => {
    expect(deserializeFromStorage(false)).toBe(false);
  });
});
