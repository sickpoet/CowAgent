---
name: document-processing
description: 办公文档处理 skill。用户要处理/生成/转换 PDF、Word、Excel、PPT、图片转文字、批量改格式、抽取表格、合并拆分、加水印、生成报告等办公文档相关任务时使用。
---

# Document Processing

## Typical Tasks

- PDF: merge/split/rotate/extract text, convert to images, add watermark
- Word/Markdown: format cleanup, template filling, section reordering
- Excel/CSV: clean data, pivot-like summaries, chart-ready exports
- PPT: generate slide outlines, consistent layout, export assets

## Working Rules

- Prefer using existing project tools/integrations if available
- If code changes are needed, first check the repo for existing libraries
- Keep outputs deterministic: clear inputs, clear file paths, clear verification

## Deliverables

- Produced/updated files with predictable names
- A short manifest: inputs → operations → outputs
