import { Injectable } from '@nestjs/common';

@Injectable()
export class BranchSelectorService {
  private readonly branchByKey = new Map<string, 'left' | 'right'>();

  setBranch(key: string, branch: 'left' | 'right'): void {
    this.branchByKey.set(key, branch);
  }

  pick(key: string): 'left' | 'right' {
    return this.branchByKey.get(key) ?? 'left';
  }

  reset(): void {
    this.branchByKey.clear();
  }
}
