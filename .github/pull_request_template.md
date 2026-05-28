<!--
PR title MUST follow Conventional Commits — it becomes the squashed commit on `main`
and drives automated versioning. See CONTRIBUTING.md for the full spec.

Format:  <type>(<scope>)!?: <subject>
Examples:
  feat(etl-app): add bronze partition pruning
  fix(data-generator): respect rows_per_day cap
  chore(deps): bump pyiceberg to 0.11.2
  feat!: switch catalog from Nessie to Polaris
-->

## Summary

<!-- What does this PR change and why? -->

## Scope of impact

- [ ] `data-generator`
- [ ] `etl-app`
- [ ] `airflow` DAGs
- [ ] Compose / infra
- [ ] Docs only

## Verification

<!-- How did you test this? Commands, screenshots, dashboards, etc. -->

## Breaking change?

<!-- If yes, describe the migration path. Append `!` to your PR title or add a `BREAKING CHANGE:` footer below. -->
