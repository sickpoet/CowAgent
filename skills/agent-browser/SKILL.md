---
name: agent-browser
description: 浏览器自动化 skill。用户要自动打开网页、登录、填写表单、抓取网页信息、跑一段可复现的浏览器操作流程，或需要“像人一样用浏览器做事”时使用。
---

# Agent Browser Automation

## Preferred Path

1. Use the built-in browser automation capability if available
2. Keep steps deterministic and explicit: URL, selectors, expected page states
3. Avoid unsafe operations (downloading unknown executables, exposing secrets)

## Typical Flows

- Navigate + search + click
- Fill forms with validation handling
- Extract structured data from tables/lists
- Take screenshots for verification

## Verification

- Provide a step-by-step run log: actions taken + key extracted results
- If automation is flaky, fallback to a robust extraction approach (API, RSS, static HTML)
