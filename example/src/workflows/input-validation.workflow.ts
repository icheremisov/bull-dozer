import { Step, Workflow } from 'dozer';

@Workflow({ name: 'input-validation' })
export class InputValidationWorkflow {
  @Step({ name: 'validate' })
  validate(input: { orderId?: unknown }): Promise<{ orderId: number }> {
    const raw = input.orderId;
    if (
      typeof raw !== 'number' ||
      !Number.isInteger(raw) ||
      raw <= 0 ||
      !Number.isFinite(raw)
    ) {
      return Promise.reject(new Error('invalid-input: orderId'));
    }

    return Promise.resolve({ orderId: raw });
  }

  run(input: { orderId?: unknown }): Promise<{ orderId: number }> {
    return this.validate(input);
  }
}
