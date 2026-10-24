---
'@mastra/inngest': patch
---

Fixed duplicate tool approvals and settled active agent streams after native Inngest cancellation or exhausted retries. Approval claims are released through terminal delivery, while stale execution notifications cannot end a later approval. Fixes #27485.
