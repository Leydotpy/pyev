# Architecture decision records

These records capture the responsibility boundaries that keep pyev portable. They supplement the
build specification and explain decisions that should remain stable even as implementations evolve.

- [0001: Use one broker façade](0001-single-broker-facade.md)
- [0002: Put envelopes and bytes at the transport boundary](0002-envelope-bytes-transport-boundary.md)
- [0003: Model engines through capabilities](0003-capability-based-engines.md)
- [0004: Centralize reliability and lifecycle policy](0004-central-reliability-and-lifecycle.md)
- [0005: Keep imports side-effect free and registries injectable](0005-side-effect-free-imports.md)

New records use the same Context / Decision / Consequences structure. Supersede an accepted record
with a new one rather than rewriting its historical decision.
