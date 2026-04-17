---
name: ui-ux-pro-max-skill
description: UI/UX 专业设计 skill。用户提到 UI/UX 设计、交互流程、信息架构、用户旅程、线框图/原型、可用性问题、转化优化、表单体验、空状态/错误提示等时使用。
---

# UI/UX Professional Design

## Scope

- Information architecture: navigation, content grouping, labels
- Interaction design: flows, states, micro-interactions, error recovery
- Usability: reduce steps, reduce cognitive load, sensible defaults
- Accessibility: keyboard, screen reader considerations, focus management

## Process

1. Clarify user goal and success criteria (what “done” looks like)
2. Map the current flow: entry → key actions → completion → recovery paths
3. Identify friction: unclear labels, missing states, too many steps, weak feedback
4. Propose an improved flow with explicit states:
   - loading, empty, error, success, partial success, offline, permission denied
5. Translate into implementable UI changes following the codebase patterns

## Output Format

- Flow summary (before/after) in bullets
- State matrix for critical screens/components
- Concrete UI changes: copy text, component structure, validation rules

## UX Heuristics Checklist

- Visibility of system status
- Match between system and real world language
- User control and undo
- Consistency and standards
- Error prevention and helpful recovery
