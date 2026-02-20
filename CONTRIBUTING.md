# Contributing to dozer

Thanks for contributing.

## Development setup

1. Install dependencies:

```bash
npm install
cd example && npm install && cd ..
```

2. Start Redis for integration/e2e/perf:

```bash
cd example
npm run redis:up
```

3. Run checks:

```bash
npm run lint
npm run test:unit
npm run test:integration
npm run test:e2e
```

## Contribution scope

- Bug fixes and reliability improvements are preferred.
- New features should include tests in `src/*.spec.ts` and/or `example/test/*`.
- Keep workflow replay semantics deterministic.

## Pull request checklist

- [ ] tests pass locally
- [ ] public API changes are reflected in `README.md`
- [ ] behavior changes are reflected in `TEST_MATRIX.md`
- [ ] no unrelated generated files are committed

## Commit style

Use clear, imperative commit messages, for example:
- `fix: preserve trace on determinism probe failure`
- `test: add replay coverage for inherited @Step override`
