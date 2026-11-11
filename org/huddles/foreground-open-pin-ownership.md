# Huddle decision: foreground-open-pin-ownership

- Adjudicator: Storage root; owner applying decision: responsiveness lead.
- based_on: `0631a73bff98b7649b9d65c181b5fe04dcf72ff9`.
- applied_at: design-source checkpoint descending from `a95f6d6c34413f042aa4cd02febc524132ba9b12`.
- Trigger: third formal convergence exposed two byte-freshness races in
  opener-private pin transfer and unlocked final cleanup.
- Decision: retain the warm-open demand admission guarantee; do not classify
  every open as ordinary. Represent an observed changed member as a shared,
  path-scoped, generation-checked freshness obligation. Its full boundary is
  `contracts/path-freshness-obligation.md`.
- Authority: contracts, design and deterministic two-member regression source
  only. Root reviews the boundary before authorizing application-code repair.
  Runtime tests, all gates, daemon probes and deployment remain held.
- Open product question: `oq-foreground-freshness-stat-slot` records whether
  cap 3's sole protected demand slot may be occupied by a cached-open member
  stat until timeout. Current ruling keeps the limit explicit; any further
  product choice belongs to Ted/root.
