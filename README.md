# gaokao-vault

[![Build status](https://img.shields.io/github/actions/workflow/status/lifefloating/gaokao-vault/main.yml?branch=main)](https://github.com/lifefloating/gaokao-vault/actions/workflows/main.yml?query=branch%3Amain)
[![License](https://img.shields.io/github/license/lifefloating/gaokao-vault)](https://github.com/lifefloating/gaokao-vault/blob/main/LICENSE)

阳光高考全量数据抓取系统 — 从 [gaokao.chsi.com.cn](https://gaokao.chsi.com.cn) 抓取 13 类高考数据（院校、专业、分数线、招生计划等），存入 PostgreSQL，支持全量/增量抓取、断点续爬、反爬对抗。

- **仓库**: <https://github.com/lifefloating/gaokao-vault>
- **文档**: <https://lifefloating.github.io/gaokao-vault/>

## 数据预览

![score_lines 批次分数线数据](docs/images/score_lines_preview.png)

## 功能概览

- 13 类数据源全量覆盖（院校、专业、分数线、一分一段、招生计划、章程等）
- 三阶段任务编排，自动处理数据依赖
- content_hash (SHA-256) 增量去重 + 变更追踪
- 双 Session 反爬：HTTP 快速请求 + Stealth 浏览器自动切换
- 三层代理池：付费代理 > 免费代理 > 直连
- 断点续爬（Ctrl+C 优雅暂停，再次启动自动恢复）
- Typer CLI，操作简单

## 快速开始（Docker Compose，推荐）

无需本地安装 Python 或 PostgreSQL，一键启动：

```bash
git clone https://github.com/lifefloating/gaokao-vault.git
cd gaokao-vault

# 启动 PostgreSQL
docker compose up -d db

# 初始化数据库（建表 + 种子数据）
docker compose run --rm crawler init-db

# 全量抓取
docker compose run --rm crawler crawl --mode full

# 启动常驻定时调度（默认北京时间每天 00:00 增量抓取）
docker compose up -d scheduler

# 查看任务状态
docker compose run --rm crawler status

# 停止服务
docker compose down
```

更多 compose 命令：

```bash
# 增量抓取
docker compose run --rm crawler crawl --mode incremental

# 常驻调度容器（读取 GAOKAO_SCHEDULE__CRON）
docker compose up -d scheduler

# 指定类型
docker compose run --rm crawler crawl --types schools majors

# 单独调试某个 Spider
docker compose run --rm crawler run-spider schools -v
```

数据持久化在 Docker volume 中（`pgdata` 存数据库，`crawl_data` 存断点文件），`docker compose down` 不会丢数据。彻底清理用 `docker compose down -v`。

## 本地安装

### 环境要求

- Python 3.10+
- PostgreSQL 18+
- [uv](https://docs.astral.sh/uv/) 包管理器

### 安装

```bash
git clone https://github.com/lifefloating/gaokao-vault.git
cd gaokao-vault

uv sync
uv run scrapling install --force
```

### 配置

```bash
cp .env.example .env
# 编辑 .env，填入你的 PostgreSQL 连接信息
```

核心配置项：

```bash
# 数据库连接（与 docker-compose 默认值一致）
GAOKAO_DB__DSN=postgresql://gaokao:gaokao@localhost:5432/gaokao_vault
GAOKAO_DB__POOL_MIN=5
GAOKAO_DB__POOL_MAX=20

# 代理配置（可选）
GAOKAO_PROXY__STATIC_PROXIES=[]
GAOKAO_PROXY__USE_FREEPROXY=false

# 爬取参数
GAOKAO_CRAWL__CONCURRENCY=5
GAOKAO_CRAWL__BASE_DELAY=1.0

# 定时调度（固定按北京时间解析，默认每天 00:00）
GAOKAO_SCHEDULE__CRON="0 0 * * *"
```

### 使用

```bash
# 初始化数据库（18 张表 + 种子数据）
gaokao-vault init-db

# 全量抓取（三阶段自动编排）
gaokao-vault crawl --mode full

# 增量抓取（通过 content_hash 跳过未变数据）
gaokao-vault crawl --mode incremental

# 指定数据类型
gaokao-vault crawl --types schools majors score_lines

# 单独运行某个 Spider（调试用）
gaokao-vault run-spider schools -v

# 查看任务状态
gaokao-vault status
```

### 断点续爬

爬取过程中按 `Ctrl+C` 优雅暂停，进度自动保存。再次运行相同命令即可从断点恢复。按两次 `Ctrl+C` 强制停止。

### 辽宁考生专项导出

辽宁专项会把生源地限定为辽宁，抓取计划年份及此前四年的招生计划、专业录取、一分一段和招生章程，最后生成与核对表兼容的 65 列 Excel。首次运行需要刷新学校、专业等全国公共目录：

```bash
# 抓取 2026 计划和 2022-2025 历史数据，然后导出 Excel
gaokao-vault crawl-liaoning --plan-year 2026 --subject all \
  --output "crawl_data/辽宁-2026数据(2022-2026).xlsx"

# 公共目录已有完整数据时可跳过刷新，缩短运行时间
gaokao-vault crawl-liaoning --plan-year 2026 --reuse-catalog

# 只重新导出数据库里已有的数据，不发起网络抓取
gaokao-vault export-liaoning --plan-year 2026 --subject 物理 \
  --output "crawl_data/辽宁-2026-物理.xlsx"
```

`--subject` 支持 `all`、`物理`、`历史`，用于筛选最终工作簿；上游接口按学校、省份和年份返回整表，因此不会减少全部网络请求。院校/专业代码按文本保存，前导零不会丢失；历史数据优先按原始院校代码、专业代码和完整专业名匹配，无法唯一确认时留空，避免把相近专业误配。

导出保留附件中的院校画像和专业画像列。当前数据源能可靠提供的学校标签、办学性质、专业层次、专业实力和章程会自动填写；城市等级、保研率、第三方排名、学科评估、硕博点等没有稳定来源的字段保持空白，不自动猜测。

### 纯 Docker（不用 compose）

```bash
docker build -t gaokao-vault .
docker run --rm --env-file .env gaokao-vault init-db
docker run --rm --env-file .env gaokao-vault crawl --mode full
```

## 三阶段编排

| 阶段 | 数据 | 说明 |
|------|------|------|
| 1 维度数据 | provinces, subject_categories | 由 `init-db` 种子数据完成 |
| 2 核心实体 | schools, majors, score_lines, timelines, announcements | 并行抓取 |
| 3 关联数据 | school_majors, score_segments, enrollment_plans, charters, special, *_satisfaction, interpretations | 依赖阶段2，并行抓取 |

## 数据类型一览

| 类型 | 说明 | 预估数据量 |
|------|------|-----------|
| schools | 院校信息 | ~2,900 |
| majors | 专业知识库 | ~3,000 |
| school_majors | 院校-专业关联 | ~100,000 |
| score_lines | 历年批次线 | ~数万 |
| score_segments | 一分一段表 | ~百万 |
| enrollment_plans | 招生计划 | ~百万 |
| charters | 招生章程 | ~数万 |
| timelines | 志愿填报时间线 | ~数百 |
| special | 特殊类型招生 | ~数千 |
| school_satisfaction | 院校满意度 | ~数千 |
| major_satisfaction | 专业满意度 | ~数千 |
| interpretations | 专业解读 | ~数千 |
| announcements | 省级招办公告 | ~数千 |

## 技术栈

- [Scrapling](https://github.com/D4Vinci/Scrapling) — 爬虫框架（Spider + FetcherSession + AsyncStealthySession）
- [asyncpg](https://github.com/MagicStack/asyncpg) — PostgreSQL 异步驱动
- [Typer](https://typer.tiangolo.com/) — CLI 框架
- [Pydantic](https://docs.pydantic.dev/) — 数据校验 + 配置管理

## 开发

```bash
uv sync --group dev
uv run pytest tests/ -v
uv run ruff check src/
uv run ruff format src/
uv run pre-commit run -a
```

## License

[MIT](LICENSE)
