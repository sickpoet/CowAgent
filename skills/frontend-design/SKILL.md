---
name: frontend-design
description: Frontend visual design skill. Use when the user asks for 前端视觉设计、页面美化、配色/排版/字体/间距/组件视觉规范、响应式布局与可访问性视觉优化，或需要把“功能页面”改成“更好看、更统一”的设计。
---

# Frontend Visual Design

## Goals

- Improve visual hierarchy, spacing, typography, and color consistency
- Align components to a reusable design system
- Keep accessibility and responsive behavior intact

## Working Method

1. Identify the target surface: page(s), component(s), or a system-wide refresh
2. Read existing UI conventions in the codebase (design tokens, theme, component library)
3. Propose a small set of visual rules (spacing scale, typography scale, color roles)
4. Apply changes in a minimal, consistent way across related components
5. Verify responsiveness (mobile/tablet/desktop) and contrast (WCAG)

## Deliverables

- Updated UI code following existing patterns
- A short, concrete “visual diff” summary: what changed and why
- If a design system exists: update tokens/variables instead of hardcoding styles

## Checks

- Layout: alignment, rhythm, consistent padding/margins
- Typography: line-height, font weights, heading scale
- Color: semantic roles (bg/surface/text/primary/danger), contrast
- Interaction: hover/active/focus-visible, disabled states
- Density: consistent component height, icon sizing, separators
