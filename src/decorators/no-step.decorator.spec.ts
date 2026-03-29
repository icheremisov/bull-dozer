import 'reflect-metadata';
import { NOSTEP_METADATA } from '../constants';
import { NoStep } from './no-step.decorator';

class TestClass {
  @NoStep()
  myMethod(): void {}

  plainMethod(): void {}
}

describe('@NoStep', () => {
  it('sets NOSTEP_METADATA on decorated method', () => {
    const descriptor = Object.getOwnPropertyDescriptor(
      TestClass.prototype,
      'myMethod',
    );
    expect(Reflect.getMetadata(NOSTEP_METADATA, descriptor!.value)).toBe(true);
  });

  it('does not set NOSTEP_METADATA on plain method', () => {
    const descriptor = Object.getOwnPropertyDescriptor(
      TestClass.prototype,
      'plainMethod',
    );
    expect(
      Reflect.getMetadata(NOSTEP_METADATA, descriptor!.value),
    ).toBeUndefined();
  });
});
