---
"@mastra/inngest": patch
---

Fixed duplicate approvals of an in-flight tool call starting another Inngest resume. Claims are released after setup or dispatch failure, completion, abort, and suspension (#27485).
