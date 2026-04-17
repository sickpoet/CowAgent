---
name: image-analysis
description: 图片分析/OCR skill。用户要识别图片文字、解析截图内容、表格/票据/证件 OCR、看图描述、找 UI 问题、提取关键信息或做简单视觉判断时使用。
---

# Image Analysis / OCR

## What To Do

- Read the image carefully and extract structured facts
- If OCR is needed, return text with line breaks preserved when useful
- For UI screenshots: point out layout/alignment/contrast issues and improvements

## Output Formats

- Plain text extraction (best-effort OCR)
- Structured JSON (fields + confidence notes) when the image is a form/receipt
- UI review bullets (issues → suggested fixes)

## Guardrails

- Do not invent unreadable text; mark uncertain parts explicitly
- Avoid leaking sensitive content; redact if user asks for sharing
