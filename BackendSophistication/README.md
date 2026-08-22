# Backend Sophistication Recovery

This folder records the cross-package recovery program without coupling package
release cycles. The CSV is the machine-readable rolling ledger; each qualified
package change updates its method row, test status, exact commit, and next
priority.

## Operating rules

- Capability and authority are separate. Unsupported conclusions fail closed;
  sophisticated methods are not removed merely to simplify governance.
- Existing engine power is restored before commodity methods are added.
- Every package qualifies and publishes independently.
- Dirty package worktrees are not used for recovery changes; isolated branches
  or worktrees protect unrelated work.
- Workstation is outside this program until backend integration is explicitly
  requested.
- Performance claims require evidence in this repository. Method-count claims
  do not qualify an upgrade.

## Status values

Use `INVENTORY_ONLY`, `BASELINE_AUDITED`, `IN_PROGRESS`, `NOT_STARTED`,
`REVIEW_REQUIRED`, `QUALIFIED`, or `BLOCKED` in `TEST_STATUS`.
