import { Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'date-serialization' })
export class DateSerializationWorkflow {
  constructor(private readonly failureMemory: FailureMemoryService) {}

  @Step({ name: 'read-date' })
  readDate(input: {
    id: string;
    at: Date;
  }): Promise<{ iso: string; at: Date; plusMs: Date; isDate: boolean }> {
    this.failureMemory.markAndShouldFail(
      `date-serialization:read:${input.id}`,
      0,
    );

    return Promise.resolve({
      iso: input.at.toISOString(),
      at: input.at,
      plusMs: new Date(input.at.getTime() + 1234),
      isDate: input.at instanceof Date,
    });
  }

  @Step({ name: 'fail-once' })
  failOnce(id: string): Promise<void> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `date-serialization:fail:${id}`,
      1,
    );
    if (shouldFail) {
      return Promise.reject(new Error('date-serialization-fail-once'));
    }

    return Promise.resolve();
  }

  async run(input: {
    id: string;
    at: Date;
  }): Promise<{ iso: string; at: Date; plusMs: Date; isDate: boolean }> {
    const result = await this.readDate(input);
    await this.failOnce(input.id);
    return result;
  }
}
