# Claude Code Instructions

## 项目概述

gaokao-vault — 阳光高考全量数据抓取系统，从 gaokao.chsi.com.cn 抓取 13 类高考数据，存入 PostgreSQL。

## 技术栈

- Python 3.10+，包管理用 **uv**
- Web 抓取：scrapling（Spider 框架 + AsyncStealthySession）
- 数据库：PostgreSQL + asyncpg
- CLI：Typer
- 数据校验：Pydantic / Pydantic Settings
- 测试：pytest + hypothesis
- Lint/Format：ruff（line-length=120）
- 类型检查：ty
- 文档：mkdocs-material

## 常用命令

```bash
uv sync                          # 安装依赖
uv run pre-commit install        # 安装 pre-commit hooks
make check                       # lint + type check + deptry
make test                        # pytest with coverage
make docs                        # 本地文档服务
```

## 代码规范

- 所有模块使用 `from __future__ import annotations`
- Ruff 规则集：YTT, S, B, A, C4, T10, SIM, I, C90, E, W, F, PGH, UP, RUF, TRY
- 测试文件允许 `assert`（`S101` 已豁免）
- 配置通过 pydantic-settings 管理，环境变量前缀 `GAOKAO_DB__`、`GAOKAO_CRAWL__`、`GAOKAO_PROXY__`

## 项目结构

- `src/gaokao_vault/spiders/` — 各类数据 Spider，继承 `BaseGaokaoSpider`（基于 scrapling Spider）
- `src/gaokao_vault/pipeline/` — 数据处理：去重(content_hash SHA-256)、校验、入库
- `src/gaokao_vault/anti_detect/` — 反爬对抗：代理池、UA 池、限速器
- `src/gaokao_vault/db/` — 数据库连接、迁移、SQL 查询
- `src/gaokao_vault/scheduler/` — 三阶段任务编排
- `src/gaokao_vault/models/` — Pydantic 数据模型
- `src/gaokao_vault/storage/` — S3/MinIO 存储
- `src/gaokao_vault/vision/` — OpenAI 视觉分析
- `tests/` — 测试目录

## 注意事项

- Spider 新增需继承 `BaseGaokaoSpider`，设置 `name`、`task_type`、`start_urls`
- 数据库 Schema 定义在 `src/gaokao_vault/db/schema.sql`
- Docker Compose 支持一键启动（`docker compose up -d db`）
- 日志持久化到 `crawl_data/logs/crawl.log`（RotatingFileHandler，50MB，保留 5 个备份），可通过 `GAOKAO_CRAWL__LOG_DIR` 环境变量修改路径

## 跨会话审查基线

审查或修改行为前，先读取 `docs/review-baseline.md`、相关架构文档和测试。该文件记录了可能不同于通用最佳实践的已确认决定。

- 必须明确 `BASE..HEAD` 审查范围；没有范围先询问。
- 有证据且符合基线的非默认行为标记为 `intentional behavior`，不要当作 bug。
- finding 必须包含代码位置、证据、影响和违反的契约。
- 如果决定与测试、安全边界或当前需求冲突，先报告冲突并等待确认。
- 新的非默认行为必须更新基线或 ADR，并增加 fixture/测试。
