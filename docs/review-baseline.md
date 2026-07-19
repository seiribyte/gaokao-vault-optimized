# 跨会话代码审查基线

本文件把已经确认的抓取、数据管道和运行约束从聊天记录中固定下来。审查新会话必须先读本文件，再读 [AGENTS.md](../AGENTS.md)、[CLAUDE.md](../CLAUDE.md) 和变更涉及的架构文档。

表中的初始条目是从现有规则、架构文档、测试和代码整理出的候选基线。只有你明确确认过的条目才算 `accepted`；不确定的条目先改为 `proposed`，不能用它直接否定审查 finding。

## 审查协议

1. 先确定审查范围。优先审查用户指定的 `BASE..HEAD`；没有范围时先询问，不要把历史代码整体当成新改动。
2. 先判断是否违反本文件、项目规则、schema、测试或任务验收，再引用通用最佳实践。
3. 与 accepted 决定一致的非默认行为标记为 `intentional behavior`，不能仅因为“通常应该这样”就列为 bug。
4. 真正的 finding 必须包含位置、证据、影响和被违反的契约。没有证据只能作为 `question/risk`。
5. 决定与测试、安全边界或新需求冲突时，报告冲突并等待确认，不要自行重写抓取或入库策略。

## 当前 accepted 决定

| ID | 决定 | 审查时重点 |
| --- | --- | --- |
| GV-001 | 新 Spider 继承 `BaseGaokaoSpider`，并声明 `name`、`task_type`、`start_urls` | 不要为了局部方便绕过基类或另造生命周期 |
| GV-002 | 数据去重使用规范化内容的 SHA-256 `content_hash`，区分 new/updated/unchanged | 不要用不稳定的页面顺序、时间戳或随机值替代 |
| GV-003 | pipeline 先做解析、字段校验和安全检查，再交给数据库 sink | 解析失败不能静默入库或用默认值掩盖 |
| GV-004 | PostgreSQL 表结构以 `src/gaokao_vault/db/schema.sql` 和迁移脚本为准 | 查询、模型和导出字段必须与 schema 一起审查 |
| GV-005 | 全量抓取按三阶段 scheduler 的依赖顺序执行，不能把有前置数据的任务并行化 | 修改调度时必须说明依赖、重试和断点行为 |
| GV-006 | 反爬策略由 proxy pool、UA pool 和 rate limiter 共同控制 | 测试不得绕过限速；失败处理不能无限重试 |
| GV-007 | 单元测试和 Spider 契约测试默认使用离线 fixture，不依赖真实网站 | 不要把网络成功当成测试稳定性的证明 |
| GV-008 | `uv`、ruff/pre-commit、ty、deptry 和 pytest 是交付门槛 | 只改文档或 fixture 也要说明实际运行的检查范围 |

来源：[架构文档](architecture.md)、[模块文档](modules.md)、[AGENTS.md](../AGENTS.md) 及现有测试。若实现已经改变某项决定，先新增或更新记录，不要让代码悄悄成为新规范。

## Finding 分类

- `confirmed defect`：违反上述契约、schema、测试或安全边界。
- `regression`：相对明确基线破坏了已接受行为。
- `test gap`：实现可能正确，但没有测试锁定决定。
- `question/risk`：需要领域确认，暂时不是缺陷。
- `intentional behavior`：有记录、有证据的刻意偏离通用做法，不列为 finding。

## 新决定记录模板

```markdown
### DEC-YYYY-MM-DD-short-name

- 状态：accepted | superseded | proposed
- 范围：Spider、pipeline、schema、scheduler 或导出模块
- 决定：系统必须做什么
- 有意不做：看起来合理但本次明确不做什么
- 原因：数据来源、可靠性、性能或兼容性原因
- 证据：代码路径、fixture、测试、任务或提交
- 审查规则：未来什么情况算违反，什么情况只是重新讨论
- 替代/关联：相关任务、提交或文档
```

每次实现结束前，要求 Codex 提取本次对话中明确确认的非默认行为，把它写入本文件或独立 ADR，并添加对应的 fixture/测试。聊天记录不能作为唯一证据。

## 当前 proposed 决定

### DEC-2026-07-19-enrollment-plan-api-throttle

- 状态：proposed
- 范围：`EnrollmentPlanSpider` 和通用请求限速器
- 决定：`api.zjzw.cn` 的招生计划请求必须在实际发包前统一排队，请求间隔不小于 `max(2 秒, crawl.base_delay)` 并加入只向后延长的配置抖动；单次传输只尝试一次。每个 Spider 使用稳定且与 Chrome TLS 指纹匹配的桌面 UA。收到业务码 `1069` 或 HTTP 限流状态后，全局延长冷却、把 Spider 并发降为 1；连续 3 次限流则暂停并保存 checkpoint，成功响应后恢复正常并发。
- 有意不做：不绕过验证码，不用高并发试探限额，不为规避限流而自动轮换 IP 或免费代理。
- 原因：该接口以 HTTP 200 返回业务层限流码，底层隐式重试和已排队请求会放大请求量；统一发包闸门能覆盖首次请求、分页、重试和断点恢复。
- 证据：`src/gaokao_vault/anti_detect/rate_limiter.py`、`src/gaokao_vault/anti_detect/ua_pool.py`、`src/gaokao_vault/spiders/enrollment_plan_spider.py`、`tests/test_rate_limiter.py`、`tests/test_ua_pool.py`、`tests/test_enrollment_plan_spider.py`
- 审查规则：绕过发包闸门、恢复底层多次重试、限流后继续并发请求或无限重试属于违反；调整具体间隔、抖动和熔断阈值属于需要数据支持的重新讨论。
- 替代/关联：GV-006、GV-007

## 固定审查提示

```text
先读取 AGENTS.md、CLAUDE.md、docs/review-baseline.md 和变更涉及的架构/测试文档。
只审查指定的 BASE..HEAD。
先验证是否违反项目已记录的抓取、去重、校验、schema 和调度契约。
符合 accepted 决定的非默认行为标记为 intentional behavior，不要列为 bug。
对决定与测试、安全边界或新需求的冲突，报告证据并等待确认，不要自行改策略。
```
