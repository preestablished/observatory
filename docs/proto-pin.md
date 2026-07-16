# control-plane proto pin

Observatory consumes `determinism-proto` via the path dependency
`../control-plane/crates/determinism-proto`. Unpinned, observatory CI
would track control-plane `main` and any upstream change could break this
repo asynchronously — so CI checks out the sibling at a **pinned ref**.

## Current pin

- Commit: `853a0b200df3b7cd4770393f408997414536bf7f`
  (control-plane PR #4 — "Author determinism.observatory.v1 events schema
  (vdev revision)")
- `proto/determinism/observatory/v1/events.proto` blake3:
  `144f8cc6f413a88d6c39a3d77415a0eb6597939381503ec4a99881edf8e4ccc2`

## Rules

- **Bump deliberately**: a pin change is its own commit that names the new
  SHA and the reason, updates this file (SHA + blake3), and goes through
  the normal review gate. Never repoint the pin as a side effect.
- **Local builds** use the sibling checkout directly and must contain the
  pinned commit:

  ```bash
  git -C ../control-plane merge-base --is-ancestor \
    853a0b200df3b7cd4770393f408997414536bf7f HEAD && echo ok
  ```

- If the pinned ref ever fails to check out in CI (upstream history
  rewrite — which control-plane policy forbids), stop and coordinate with
  control-plane; verify the recorded blake3 still matches before trusting
  any replacement SHA.
