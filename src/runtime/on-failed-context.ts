import { AsyncLocalStorage } from 'node:async_hooks';

const storage = new AsyncLocalStorage<true>();

export const OnFailedContextStorage = {
  run<T>(callback: () => T): T {
    return storage.run(true, callback);
  },
  isActive(): boolean {
    return storage.getStore() === true;
  },
};
