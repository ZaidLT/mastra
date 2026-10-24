---
'@mastra/core': patch
---

Reduced repeated working-memory and observation reads during durable agent execution. Shared reads are scoped to one model step and remain outside persisted snapshots. Addresses the redundant memory reads reported in #28366.
