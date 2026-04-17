---
name: fullstack-developer
description: 全栈开发专家 skill。用户要做端到端功能（前端+后端+数据库/存储+鉴权+部署）、集成第三方 API、性能优化、或需要“从需求到可运行实现”的全链路交付时使用。
---

# Fullstack Developer

## Default Approach

1. Understand the feature boundary: inputs, outputs, permissions, edge cases
2. Inspect existing architecture: API layer, DB access patterns, UI patterns
3. Implement vertically:
   - data model / schema (if needed)
   - backend API
   - frontend integration
   - validation + error handling + observability
4. Add tests and run lint/typecheck if available in this repo

## Engineering Constraints

- Do not introduce new dependencies unless the repo already uses them
- Keep changes incremental and reversible
- Prefer secure-by-default: avoid leaking secrets, validate inputs, least privilege

## Deliverables

- Working feature with clear verification steps
- Any required migration/config changes documented in the change summary
