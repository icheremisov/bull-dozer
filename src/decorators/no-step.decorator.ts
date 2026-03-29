import 'reflect-metadata';
import { NOSTEP_METADATA } from '../constants';

export function NoStep(): MethodDecorator {
  return (
    _target: object,
    _key: string | symbol,
    descriptor: PropertyDescriptor,
  ) => {
    Reflect.defineMetadata(NOSTEP_METADATA, true, descriptor.value as object);
    return descriptor;
  };
}
