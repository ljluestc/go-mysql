# PR Description: Fix canal `RunFrom` impossible binlog position handling (#642)

## Summary
This change addresses canal startup failures in `RunFrom` when replication is requested from an impossible binlog position:

> Client requested master to start replication from impossible position; the first event 'binlog.024078' at 1247892405, the last event read from 'binlog.024078' at 4, the last byte read from 'binlog.024078' at 4.

The fix improves startup behavior around invalid/out-of-range replication offsets so canal can recover deterministically instead of failing hard on this class of position mismatch.

## Problem
When `RunFrom` is called with a stale or impossible position (for example, file exists but offset is outside valid event boundaries for current server state), the server rejects the replication request and canal exits with the error above.

In practical deployments this can happen after:
- binlog rotation or purge,
- source failover/topology change,
- checkpoint drift between recorded position and actual available event stream.

The observed result is startup failure instead of a controlled recovery path.

## Root cause
`RunFrom` startup path assumes the provided position is valid for current upstream binlog contents. For impossible positions, the mysql replication stream returns an unrecoverable error at connect/start time, and the call path does not sufficiently normalize/recover that position before starting the dump/sync loop.

## Proposed fix
- Detect the specific impossible-position startup failure condition in the `RunFrom` initialization path.
- Fall back to a valid starting point strategy (server-acceptable binlog position) rather than failing immediately.
- Preserve existing behavior for valid positions.
- Keep error propagation for genuinely non-recoverable startup failures.

## Behavior after fix
- Valid `RunFrom` positions behave exactly as before.
- Impossible/out-of-range startup positions no longer hard-fail this path; canal recovers to a valid start point and proceeds.
- Non-related replication errors are still surfaced normally.

## Scope and compatibility
- Scope is limited to canal startup position handling for `RunFrom`.
- No API shape changes.
- Backward-compatible behavior for normal/valid startup cases.

## Testing
Validation should cover:
- startup from valid position (no regression),
- startup from impossible/out-of-range position (now recovers),
- unchanged handling for unrelated replication failures.

Manual verification should include reproducing the original issue scenario and confirming successful startup instead of immediate failure.

## Risk assessment
Potential risk is incorrect fallback selection in edge cases. Mitigations:
- fallback only on recognized impossible-position startup error class,
- do not broaden suppression of unrelated errors,
- preserve existing flow for valid positions.

## Rollout and operations notes
- Safe to roll out as a bug fix.
- No migration steps required.
- If operators rely on strict fail-fast for invalid checkpoints, they should review startup logs to confirm fallback behavior on affected instances.

## Issue linkage
- Closes: #642
