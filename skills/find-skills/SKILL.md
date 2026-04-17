---
name: find-skills
description: 技能发现与安装 skill。用户想找/装 skill、浏览 Skill Hub、从 GitHub/URL 安装技能、或管理启用/禁用/卸载技能时使用。适用于 cow skill / /skill 命令的指导与排错。
---

# Find & Install Skills

## Discover

- Browse remote Skill Hub: `cow skill list --remote`
- Search by keyword: `cow skill search <query>`
- Check installed skills: `cow skill list`

## Install

- From Skill Hub name: `cow skill install <name>`
- From GitHub shorthand: `cow skill install owner/repo`
- From GitHub URL: `cow skill install https://github.com/owner/repo`
- From local directory: `cow skill install .\path\to\skill`

## Manage

- Details: `cow skill info <name>`
- Enable/disable: `cow skill enable <name>` / `cow skill disable <name>`
- Uninstall: `cow skill uninstall <name>`

## Troubleshooting

- If install fails, retry and check network reachability to `https://skills.cowagent.ai/`
- If skill not found, verify the exact name from `search` results
