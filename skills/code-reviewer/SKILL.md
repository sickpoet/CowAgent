---
name: code-reviewer
description: 代码审查 skill。用户让你 review PR/代码、找 bug/安全问题/可维护性问题、或需要给出修改建议与可执行清单时使用。
---

# Code Reviewer

## Review Focus

- Correctness: edge cases, null/None, concurrency, timeouts, retries
- Security: injection, authz/authn, secrets handling, unsafe deserialization
- Reliability: error handling, logging quality (no sensitive data), idempotency
- Maintainability: naming, cohesion, duplication, layering, test coverage
- Performance: obvious hot paths, N+1 queries, unnecessary network calls

## Output Format

- High priority: must-fix issues (with file/line references)
- Medium priority: improvements
- Low priority: style/nits (only if they help consistency)

## When Suggesting Fixes

- Prefer minimal diffs
- Match existing conventions and libraries
- Include a clear verification step (test/lint/typecheck) when applicable
