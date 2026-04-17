---
name: web-artifacts-builder
description: 多组件页面构建 skill。用户要搭建一个页面/后台模块/多组件布局（列表、表单、弹窗、表格、筛选、分页、详情等）或要“从零拼一个可用页面骨架”时使用。优先复用现有组件库与工程约定。
---

# Web Artifacts Builder

## What To Build

- Page skeleton: layout, routing entry, data loading boundaries
- Components: list/table, filters, form, modal/drawer, detail panel
- State handling: loading/empty/error, optimistic updates where suitable

## Rules

- Follow existing stack and conventions; do not introduce new UI libs blindly
- Prefer composing existing components over creating new ones
- Keep components small and testable: container (data) vs presentational (UI)

## Steps

1. Inspect existing pages/components for patterns (routing, data fetching, styling)
2. Create a minimal end-to-end path:
   - route → page → data fetch → render → basic interactions
3. Add UI states and UX polish after the baseline works
4. Add tests if the repo has a testing setup; otherwise keep changes isolated

## Deliverables

- Clean file tree changes (page + components + shared utilities if needed)
- Clear component boundaries and props contracts
- Basic state handling for loading/empty/error
