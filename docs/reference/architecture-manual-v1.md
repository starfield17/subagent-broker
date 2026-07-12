# Multi-Harness Parallel Subagent Skill 架构与实现 Manual

**文档状态：** Architecture Baseline / Implementation Manual  
**版本：** V1.0  
**日期：** 2026-07-12  
**目标实现语言：** Go  
**目标读者：** 负责实现、审查、测试和维护本 Skill 的 Coding Agent / 开发者  
**支持 Harness：** Claude Code、Codex、Grok Build、OpenCode

---

## 0. 文档用途

本文档是本 Skill 的架构基线和实现约束。

实现者不得把本文档仅当作“建议列表”。其中使用以下规范词：

- **MUST / 必须**：违反即视为架构或验收失败。
- **MUST NOT / 禁止**：实现中不得出现。
- **SHOULD / 应当**：除非有明确且记录在案的理由，否则必须遵守。
- **SHOULD NOT / 不应**：原则上禁止，例外必须有明确理由。
- **MAY / 可以**：可选能力，不影响基础验收。

本文档重点定义：

1. Skill 的边界和核心目标；
2. Go 程序与 `SKILL.md` 的职责划分；
3. 多 Harness Adapter 架构；
4. 共享工作区中的并行协作模型；
5. Run、Wave、Task、WorkerSession 的生命周期；
6. 进程存活、等待和疑似卡死的判断；
7. 主 Agent 与 Subagent 的消息与报告协议；
8. 运行数据的目录和持久化方式；
9. 故障恢复、取消、验证和验收标准；
10. V1 的实现范围与明确非目标。

本文档**不提供具体 Go 代码**。实现者应先通过架构验收，再进入代码实现。

---

# 1. 核心目标

本 Skill 的核心目标是：

> 利用多个外部 Coding Agent Harness 作为并行 Subagent，在同一个项目工作区中执行经过严格拆分、逻辑上可并行的局部任务，并让使用 Skill 的主 Agent能够观察、询问、干预、收集和验证这些任务。

V1 计划支持：

1. Claude Code
2. Codex
3. Grok Build
4. OpenCode

Skill 本身不是新的 Coding Agent，也不负责替代主 Agent 的推理能力。

主 Agent 负责：

- 理解用户目标；
- 拆分任务；
- 判断任务是否可并行；
- 指定每个任务的职责与写入范围；
- 处理跨任务设计决策；
- 回答 Subagent 的问题；
- 验收和整合最终结果。

Skill / Supervisor 负责：

- 确定性地启动和管理 Harness；
- 维护运行状态；
- 规范化事件；
- 路由消息；
- 检测冲突和越界；
- 记录日志和结果；
- 执行生命周期控制；
- 向主 Agent 提供稳定、简洁的可读接口。

Subagent 负责：

- 在 Task Contract 约束内完成局部任务；
- 不擅自扩大任务边界；
- 遇到阻塞时提出明确问题；
- 提交符合协议的结果；
- 不承担全局编排职责。

---

# 2. 核心架构结论

以下决策属于不可轻易更改的架构基线。

## 2.1 单一实现语言

整个执行程序 **MUST 使用 Go**。

V1 不使用 Python、Rust、Node.js 或其他语言实现核心运行时。

允许调用各 Harness 自身的 CLI、Server 或协议，但 Skill 的：

- 调度；
- 进程管理；
- 状态机；
- Adapter；
- 事件规范化；
- 持久化；
- 报告渲染；
- CLI；

必须由 Go 实现。

## 2.2 `SKILL.md` 与 Go 程序分层

系统由两部分组成：

### `SKILL.md`

负责教主 Agent：

- 什么任务可以并行；
- 什么任务禁止并行；
- 如何定义 Task Contract；
- 如何启动 Run；
- 如何查询状态；
- 如何处理问题；
- 如何收集报告；
- 如何验证结果。

### Go 程序

负责所有确定性执行逻辑：

- 启动进程；
- 保持 Harness 会话；
- 解析事件；
- 写入状态；
- 路由消息；
- 生成报告；
- 管理取消和恢复。

`SKILL.md` **MUST NOT** 承担依赖 shell 技巧才能维持正确性的复杂运行逻辑。

## 2.3 Run-scoped Supervisor

系统采用：

> 每个 Run 一个临时 Run Supervisor。

它不是长期全局 daemon，也不是把每个 Worker 完全脱管。

Run Supervisor：

- 随 Run 启动；
- 是本 Run 内 Worker 的父级管理者；
- 在 Run 存活期间常驻；
- 管理所有 Harness 的双向 I/O；
- 维护 Run 内存状态；
- 持续将状态和事件落盘；
- 在 Run 进入终态后退出。

CLI 命令通过本地 IPC 与对应 Run Supervisor 通信。若 Supervisor 不可达，CLI 必须能够读取磁盘快照并进入降级诊断模式。

## 2.4 共享工作区为唯一默认协作模式

V1 使用同一个项目 checkout 作为所有写入型 Worker 的工作区。

V1 **MUST NOT 实现或自动使用独立 Git worktree**。

理由：

- 如果两个任务需要修改同一文件，它们不应并行；
- 如果一个任务依赖另一个任务正在修改的接口，它们不应并行；
- 如果任务属于大范围重构，它们不应在同一个 Wave 并行；
- worktree 只能延后冲突，不能解决语义冲突；
- 本 Skill 的目标是高质量协作式并行，而非竞争性分叉执行。

未来如需“多个 Agent 尝试不同方案”，必须定义为独立的 speculative/competitive 模式，不得混入默认协作模式。

## 2.5 并行使用 Wave + Barrier

任务不能无约束地任意并发。

一个 Run 由一个或多个 Wave 构成：

```text
Run
  Wave 1
    Task A
    Task B
    Task C
  Barrier
  Wave 2
    Task D
    Task E
  Final Verification
```

同一 Wave 中的任务必须逻辑独立。

Wave 完成后，必须经过 Barrier：

- 等待本 Wave 的所有写入停止；
- 收集报告；
- 检查 scope；
- 检查全局 diff；
- 执行必要的集成验证；
- 决定是否进入下一 Wave。

## 2.6 软 Scope Contract，不做文件 ACL

每个 Task 必须声明允许写入范围。

约束通过：

- Prompt / Task Contract；
- 启动前冲突检测；
- 运行时事件审计；
- Run 结束后的 Git diff 检查；

实现。

系统默认**不通过 OS 文件权限、容器挂载或代理文件系统阻止写入**。

这是一套协作协议，而不是强制沙箱。

但“软约束”不等于没有规则：

- Scope 重叠的任务不得进入同一 Wave；
- Worker 想扩大范围时必须先报告；
- 越界修改必须被显式标记；
- 主 Agent 必须决定如何处理越界。

## 2.7 JSON 是内部协议，Markdown 是 Agent 接口

Harness 原生事件经过 Adapter 后，形成内部结构化对象。

结构化对象用于：

- Schema 校验；
- 状态机；
- 持久化；
- 自动聚合；
- 一致性检查。

结构校验和最低语义校验通过后，系统原子渲染：

- `question.md`
- `report.md`
- `run-summary.md`
- `status.md`

主 Agent 默认只阅读 Markdown，不直接面对大量 JSON。

正式 Markdown 文件一旦出现，必须代表：

> 对应消息已经通过最低结构和语义标准，并已完整、原子发布。

## 2.8 Agent 声称完成不等于验证成功

必须区分：

- `reported_complete`
- `verified_success`
- `verified_partial`
- `verification_failed`

Worker 的自然语言或结构化结果不能直接把 Task 置为成功。

## 2.9 默认禁止嵌套 Subagent

本 Skill 已经是一层并行编排。

所有 Task Contract 默认必须包含：

> 不得再生成、调用或委派给其他 Subagent。

只有主 Agent 显式允许时，某个任务才可开启嵌套 Agent。V1 可以直接不提供该开关，即全局禁止嵌套。

---

# 3. 非目标

V1 明确不做以下内容：

1. 自动理解用户需求并自主拆解全部任务；
2. 内置另一个 LLM Supervisor；
3. Worker 之间自由互相聊天；
4. 自动解决代码语义冲突；
5. 自动合并冲突分支；
6. Git worktree 隔离；
7. 容器级文件权限隔离；
8. 分布式或远程 Worker；
9. Web UI 或完整 TUI；
10. 自动选择“最聪明”的 Harness；
11. 自动修改公共接口以解决阻塞；
12. 无限制递归 Subagent；
13. 通过终端 UI 文本抓取作为首选协议；
14. 把日志提交进 Git；
15. 把“长时间无输出”直接判定为卡死并自动杀死。

---

# 4. 总体系统结构

```text
┌─────────────────────────────────────────────┐
│                  Main Agent                 │
│  拆分 / 决策 / 回答问题 / 验收 / 下一 Wave │
└───────────────────┬─────────────────────────┘
                    │ Skill CLI
┌───────────────────▼─────────────────────────┐
│                  SKILL.md                   │
│  调用规则 / 并行准则 / Task Contract 指南  │
└───────────────────┬─────────────────────────┘
                    │ local CLI + IPC
┌───────────────────▼─────────────────────────┐
│              Run-scoped Supervisor          │
│                                             │
│  Run Controller                             │
│  Wave Scheduler                             │
│  Process Supervisor                         │
│  Session Registry                           │
│  Event Normalizer                           │
│  State Machine                              │
│  Message Router                             │
│  Scope Auditor                              │
│  Report Validator / Renderer                │
│  Persistence / Recovery                     │
└───────┬────────────┬───────────┬────────────┘
        │            │           │
┌───────▼─────┐ ┌────▼─────┐ ┌──▼────────┐ ┌───────────┐
│ Claude      │ │ Codex    │ │ Grok      │ │ OpenCode  │
│ Adapter     │ │ Adapter  │ │ Adapter   │ │ Adapter   │
└─────────────┘ └──────────┘ └───────────┘ └───────────┘
```

---

# 5. 领域模型

## 5.1 Project

Project 是一次或多次 Run 所属的工作项目。

Project 身份不应仅等于“当前 shell 路径字符串”。

Project 记录至少包含：

- `project_id`
- `canonical_path`
- `git_root`（如存在）
- `git_common_dir`（如存在）
- `created_at`
- `last_seen_at`
- `path_slug`
- `path_hash`

### 项目根目录解析

优先级：

1. 显式传入的项目根目录；
2. 当前目录向上查找 Git root；
3. 若不是 Git 项目，使用规范化当前目录。

路径必须：

- 转为绝对路径；
- 清理 `.` 和 `..`；
- 尽可能解析符号链接；
- 在 Windows 上处理盘符和大小写规范；
- 不假定项目一定是 Git 仓库。

## 5.2 Run

Run 是主 Agent 发起的一次完整编排。

Run 至少包含：

- `run_id`
- `project_id`
- `created_at`
- `started_at`
- `ended_at`
- `goal`
- `base_revision`
- `base_worktree_snapshot`
- `status`
- `current_wave`
- `task_ids`
- `supervisor_identity`
- `config_snapshot`
- `schema_version`

## 5.3 Wave

Wave 是一组允许同时执行的 Task。

Wave 包含：

- `wave_id`
- `ordinal`
- `task_ids`
- `status`
- `started_at`
- `barrier_started_at`
- `ended_at`
- `integration_checks`
- `barrier_result`

Wave 状态建议：

```text
planned
preflight
running
waiting
barrier
verified
blocked
failed
cancelled
```

## 5.4 Task

Task 是逻辑工作单元，而不是进程。

Task 至少包含：

- `task_id`
- `title`
- `objective`
- `completion_criteria`
- `write_scope`
- `forbidden_scope`
- `known_read_dependencies`
- `validation_commands`
- `harness_preference`
- `model_preference`
- `depends_on`
- `wave_id`
- `status`
- `result_revision`
- `allow_nested_agents`
- `allow_public_interface_change`

Task 可能因为 Harness 崩溃、限额或重试而对应多个 WorkerSession。

## 5.5 WorkerSession

WorkerSession 是一次具体 Harness 会话。

至少包含：

- `worker_id`
- `task_id`
- `harness`
- `adapter_version`
- `native_session_id`
- `native_turn_id`
- `pid`
- `process_start_identity`
- `process_group_identity`
- `started_at`
- `last_event_at`
- `last_progress_at`
- `ended_at`
- `exit_code`
- `capabilities`
- `attempt`
- `status_dimensions`

## 5.6 Event

Event 是系统可审计的事实。

所有内部事件必须包含：

- `schema_version`
- `seq`
- `event_id`
- `run_id`
- `task_id`（可选）
- `worker_id`（可选）
- `timestamp`
- `source`
- `type`
- `severity`
- `payload`

Event 必须为 append-only。

已写入事件不得在原位修改。

## 5.7 Message

Message 是主 Agent 与 Worker 之间的逻辑消息。

类型至少包括：

- `instruction`
- `question`
- `answer`
- `scope_expansion_request`
- `scope_expansion_decision`
- `permission_request`
- `permission_decision`
- `progress_note`
- `completion_report`
- `system_notice`

Message 状态至少包括：

```text
created
validated
queued
delivered
acknowledged
answered
expired
failed
```

## 5.8 Result Envelope

Result Envelope 是 Worker 完成时必须提交的内部结构化结果。

建议字段：

- `schema_version`
- `task_id`
- `worker_id`
- `status`
- `summary`
- `work_completed`
- `files_changed`
- `validation`
- `remaining_work`
- `blocking_issues`
- `scope_expansion`
- `scope_violations_self_reported`
- `risks`
- `handoff_notes`

合法 status：

```text
succeeded
partial
blocked
failed
cancelled
```

---

# 6. Skill Home 与运行数据

## 6.1 存储原则

运行数据统一存放在 Skill 自有状态根目录中，采用类似 Claude Code 的 project-first 组织方式。

项目仓库中默认不生成 `.subagent/` 等状态目录。

“Skill Home”与“源码安装目录”必须在概念上分离。

推荐默认值：

### Linux / macOS

```text
~/.subagent-broker/
```

### Windows

```text
%USERPROFILE%\.subagent-broker\
```

实现可以遵循平台标准状态目录，但对外概念统一称为 `BROKER_HOME`。

## 6.2 推荐目录布局

```text
BROKER_HOME/
├── config.toml
├── projects/
│   └── <project-slug>--<short-hash>/
│       ├── project.json
│       ├── active-run.json
│       └── runs/
│           └── <timestamp>-<uuidv7-or-ulid>/
│               ├── run.json
│               ├── state.json
│               ├── status.md
│               ├── events.jsonl
│               ├── run-summary.md
│               ├── supervisor.json
│               ├── control/
│               │   └── local IPC endpoint metadata
│               ├── waves/
│               │   └── <wave-id>/
│               │       ├── wave.json
│               │       ├── barrier.md
│               │       └── verification.json
│               └── tasks/
│                   └── <task-id>/
│                       ├── task.json
│                       ├── contract.md
│                       ├── contract.meta.json
│                       ├── events.jsonl
│                       ├── stdout.log
│                       ├── stderr.log
│                       ├── question.md
│                       ├── question.meta.json
│                       ├── report.md
│                       ├── report.meta.json
│                       └── validation/
└── index/
    ├── active-runs.json
    └── recent-runs.json
```

不要求所有文件在 V1 同时实现，但目录语义必须一致。

## 6.3 Project key

Project 目录名称必须兼顾：

- 人类可读；
- 唯一性；
- 跨平台；
- 路径长度。

格式建议：

```text
<sanitized-path-slug>--<short-hash>
```

哈希输入必须基于规范化项目身份，而不是未经处理的原始路径。

## 6.4 Run ID

Run ID 不得仅使用秒级时间戳。

推荐：

```text
<UTC-timestamp>-<UUIDv7>
```

或：

```text
<UTC-timestamp>-<ULID>
```

要求：

- 全局碰撞概率可忽略；
- 目录名自然按时间排序；
- 可人工识别大致创建时间。

## 6.5 默认 Run 定位

当主 Agent 未显式提供 Run ID：

1. 根据当前目录解析 Project；
2. 读取 `active-run.json`；
3. 若只有一个活跃 Run，选择它；
4. 若存在多个活跃 Run：
   - 只读命令可以返回聚合结果；
   - 破坏性命令必须拒绝并要求显式 Run ID；
5. 若无活跃 Run，选择最近 Run仅用于只读查询。

## 6.6 权限与隐私

`BROKER_HOME` 应：

- 默认仅当前 OS 用户可访问；
- 不进入 Git；
- 不自动上传；
- 不把密钥明文复制进报告；
- 支持保留策略和手动清理；
- 对日志中的环境变量和认证信息进行必要脱敏。

---

# 7. Run Supervisor

## 7.1 职责

Run Supervisor 必须负责：

1. 启动和回收 Worker 进程；
2. 持有 Worker 的 stdin/stdout/stderr；
3. 维护 Harness 原生 session；
4. 接收原生协议事件；
5. 生成统一事件；
6. 更新状态机；
7. 路由主 Agent 消息；
8. 处理权限与输入请求；
9. 保存状态和日志；
10. 管理 Wave；
11. 执行取消；
12. 原子发布 Markdown；
13. 在 Run 终态后退出。

## 7.2 非职责

Supervisor 禁止：

- 用 LLM 自动重写任务目标；
- 擅自扩大 Task scope；
- 自主决定公共接口；
- 通过“看起来合理”替代主 Agent 验收；
- 自动合并两个冲突实现；
- 自动创建 worktree；
- 把一个阻塞任务静默改派为不同目标。

## 7.3 IPC

Supervisor 应提供本地 IPC。

推荐：

- Unix：Unix domain socket；
- Windows：Named Pipe。

IPC 必须：

- 只允许本地用户连接；
- 与 Run ID 绑定；
- 有协议版本；
- 支持请求超时；
- 避免通过不安全的固定 TCP 端口暴露；
- 连接失败时返回明确的降级状态。

## 7.4 Supervisor 身份

仅保存 PID 不够，因为 PID 可能复用。

`supervisor.json` 至少保存：

- PID；
- 进程启动时间或 OS 可验证的 start token；
- Run ID；
- executable identity/version；
- IPC endpoint；
- heartbeat timestamp；
- shutdown reason。

---

# 8. CLI / Skill 操作面

CLI 语义应稳定，内部实现可以演进。

建议最小命令集：

## 8.1 `dispatch`

创建 Run 或启动 Wave。

职责：

- 读取任务定义；
- 执行 preflight；
- 创建 Run 目录；
- 启动 Supervisor；
- 启动首个 Wave；
- 返回 Run ID 和简洁摘要。

## 8.2 `status`

返回 Run 的简洁状态。

默认输出应适合主 Agent 阅读，不应倾倒完整日志。

## 8.3 `inspect`

查看某个 Task 或 Worker 的详细状态、最近事件和阻塞原因。

## 8.4 `events`

按事件序号游标返回增量事件。

必须支持 `since_seq`，避免每次重复读取全部历史。

## 8.5 `inbox`

列出等待主 Agent 回答的问题、scope 请求和权限请求。

## 8.6 `send`

向某个 Worker 发送补充指令或回答。

如果 Harness 支持 active-turn steer，应即时发送。

如果只支持下一 turn，应排队并明确标记。

如果无法续接，应返回能力不足，不得静默伪装为同一会话。

## 8.7 `wait`

等待：

- 任意重要事件；
- 某个 Task 终态；
- 当前 Wave 结束；
- 整个 Run 结束。

`wait` 必须真正阻塞到指定条件满足、超时或 Run 失败。

不得在 Worker 仍运行时提前返回“完成”。

## 8.8 `collect`

返回或定位：

- `run-summary.md`
- 各 Task 的 `report.md`
- 未处理的 `question.md`
- 验证结论。

## 8.9 `verify`

触发 Wave 或 Run 的显式验证步骤。

## 8.10 `cancel`

取消：

- 单个 Worker；
- 单个 Task；
- 当前 Wave；
- 整个 Run。

取消必须尽可能终止子进程树，而不仅是顶层 PID。

## 8.11 `doctor`

检查：

- Harness 是否安装；
- 版本；
- 认证状态的可检测部分；
- 协议能力；
- 所需 CLI flags；
- Server/ACP 是否可用；
- 已知兼容性问题；
- 当前 Adapter 是否支持该版本。

## 8.12 `gc`

清理过期 Run。

不得清理：

- 活跃 Run；
- 被 pin 的 Run；
- 最近失败且仍在保留期的 Run。

---

# 9. Task Contract

## 9.1 Contract 目标

Task Contract 是主 Agent 与 Worker 的正式协作约定。

它不是一段随意 Prompt。

必须同时存在：

- 内部结构化 `task.json`
- Agent 可读 `contract.md`

## 9.2 必填内容

每个 Task Contract 必须包含：

1. 任务名称；
2. 任务目标；
3. 明确完成标准；
4. 允许修改的路径/glob；
5. 禁止修改的路径或全局对象；
6. 已知读取依赖；
7. 其他并行 Task 的职责；
8. 是否允许修改公共接口；
9. 必须执行的局部验证；
10. 遇到范围不足时的行为；
11. 最终报告要求；
12. 禁止的 Git 操作；
13. 禁止生成更多 Subagent；
14. 当前项目根目录；
15. Run / Task 身份。

## 9.3 Prompt 表达原则

应优先使用正向职责：

```text
你负责 internal/auth/** 和 tests/auth/**。
其他 Worker 正在处理数据库层和 API 层。
你可以读取整个项目，但只修改你的职责范围。
```

同时明确：

```text
如果发现必须修改职责范围外的文件，请停止该修改并提交 scope expansion request。
```

禁止只注入含糊表述：

```text
尽量不要改其他文件。
```

## 9.4 禁止操作

共享工作区中，Task Contract 必须默认禁止：

- `git reset --hard`
- `git clean`
- `git stash`
- 未经允许切换分支
- 回滚不属于自己的变更
- `git restore` / `git checkout --` 其他任务文件
- 全仓库自动格式化
- 全仓库自动生成
- 自动提交所有工作区修改
- 删除未知未跟踪文件
- 把其他 Agent 的临时修改视为需要清理的问题

---

# 10. 并行资格与 Wave 预检

## 10.1 并行不是默认行为

主 Agent 不能因为有多个 Harness 就强行并行。

任务只有通过并行资格检查，才能进入同一 Wave。

## 10.2 三类冲突

### 写—写冲突

两个 Task 可能修改同一文件或同一生成产物。

必须拒绝并行。

### 写—读冲突

Task B 的实现依赖 Task A 正在改变的接口、行为或数据结构。

即使写入文件不重叠，也必须拒绝并行。

### 验证冲突

一个 Task 的半成品会导致另一个 Task 的测试、编译或生成流程产生误导结果。

必须拆 Wave 或调整验证范围。

## 10.3 同一 Wave 的必要条件

同一 Wave 中的 Task 必须满足：

- write scope 不重叠；
- 不依赖其他 Task 本轮预期产出；
- 不共享同一个待修改的公共接口；
- 不共同修改全局依赖文件；
- 可以执行局部验证；
- 失败不会破坏其他 Task 的工作；
- 不运行全局 formatter；
- 不运行影响全仓库的生成/迁移；
- 主 Agent 能明确描述每个 Task 的所有权。

## 10.4 高风险文件

下列对象在同一 Wave 中原则上只能归一个 Task：

- `go.mod`
- `go.sum`
- `package.json`
- lockfiles
- 数据库 schema
- 公共协议定义
- 全局配置
- CI 主配置
- 代码生成入口
- 全局 formatter 配置
- 公共导出接口
- migration 序列
- 统一版本文件

## 10.5 Preflight 输出

Preflight 必须输出：

- Task scope 对照；
- 重叠检测结果；
- 已知依赖；
- 高风险文件；
- 并发数；
- Harness 可用性；
- 未满足条件；
- 是否允许启动。

Preflight 失败时，Supervisor 不得“尽量启动”。

---

# 11. Scope 管理

## 11.1 Soft Lease

Supervisor 为每个 Task 维护逻辑上的 write lease：

```text
internal/auth/**  -> task-auth
internal/cache/** -> task-cache
```

Soft Lease 用于：

- preflight；
- 状态展示；
- 越界检测；
- scope 扩展冲突判断；
- 报告验证。

Soft Lease 不修改 OS 文件权限。

## 11.2 Scope 扩展

Worker 发现原 scope 不足时，必须提交：

```text
scope_expansion_request
```

至少包含：

- 请求的文件/路径；
- 修改理由；
- 不修改的后果；
- 是否已写入部分修改；
- 与其他 Task 的依赖；
- 建议处理方式。

Task 转入：

```text
blocked_scope
```

在主 Agent 批准前，Worker 不得继续修改请求范围。

## 11.3 越界审计

共享工作区中，Git 只能确定 Run 级变化，不能天然确定具体进程责任。

因此分两层审计：

### Run 级确定性检查

```text
实际变化文件
-
所有 Task 获准范围的并集
=
未获授权文件
```

### Worker 级证据归因

综合：

- Harness 文件编辑事件；
- 工具调用；
- Worker 自报文件列表；
- scope；
- 文件时间；
- 事件顺序。

无法绝对归因时必须标记：

```text
scope_violation_owner_uncertain
```

不得编造责任归属。

---

# 12. Adapter 架构

## 12.1 设计原则

每个 Harness 一个 Adapter。

上层不得直接解析四套不同输出。

Adapter 负责：

1. 能力探测；
2. 构造启动方式；
3. 创建/恢复会话；
4. 发送消息；
5. 中断/取消；
6. 解析原生事件；
7. 转换统一事件；
8. 提交结构化最终结果；
9. 处理权限请求；
10. 暴露原生 session identity。

## 12.2 Capability Model

Adapter 必须声明能力，至少包括：

- `structured_stream`
- `bidirectional_stream`
- `resume_session`
- `steer_active_turn`
- `interrupt_turn`
- `structured_final_output`
- `permission_events`
- `diff_events`
- `usage_events`
- `native_subagents`
- `native_server_mode`
- `acp`
- `hooks`
- `session_history`

Broker 必须按能力选择行为，不得假定所有 Harness 等价。

## 12.3 Adapter 接口语义

概念操作：

- `Probe`
- `StartSession`
- `ResumeSession`
- `SendMessage`
- `SteerActiveTurn`
- `InterruptTurn`
- `TerminateSession`
- `ReadHistory`
- `RespondPermission`
- `GetDiff`
- `GetUsage`
- `NormalizeEvent`
- `CollectFinalResult`

这只是语义边界，不规定 Go 方法签名。

## 12.4 Harness 优先接入方式

### Claude Code

优先使用其非交互/流式 JSON 能力和生命周期 Hooks。

Adapter 必须优先消费结构化事件，而不是解析 TUI ANSI 文本。

### Codex

精细交互优先使用 App Server。

简单一次性任务可使用 `codex exec` 的 JSON 输出和 output schema。

需要 active turn steer、interrupt 或长期会话时，不应只依赖一次性 `exec`。

### Grok Build

优先使用 ACP over stdio。

若 ACP 不可用，可回退到官方 headless/streaming 方式。

因为产品和协议可能快速变化，必须固定兼容版本范围并由 `doctor` 探测。

### OpenCode

优先连接其 Server API：

- session；
- async prompt；
- status；
- events；
- abort；
- permission；
- diff。

不应抓取其 TUI。

## 12.5 版本兼容

每个 Adapter 必须记录：

- 已测试最低版本；
- 已测试最高版本；
- 已知不兼容版本；
- 能力探测结果；
- 降级能力。

未知版本可以尝试启动，但必须在状态中标记：

```text
compatibility_unverified
```

关键协议字段不匹配时应 fail fast，而不是静默误解析。

---

# 13. 统一事件模型

## 13.1 核心事件类型

至少支持：

### Run / Wave

- `run.created`
- `run.started`
- `run.completed`
- `run.failed`
- `wave.started`
- `wave.barrier_started`
- `wave.verified`
- `wave.blocked`

### Session / Process

- `session.starting`
- `session.started`
- `session.resumed`
- `process.spawned`
- `process.exited`
- `process.orphaned`
- `session.terminated`

### Model / Turn

- `turn.started`
- `model.output_delta`
- `model.message_completed`
- `turn.completed`
- `turn.failed`
- `turn.interrupted`
- `api.retrying`

### Tool / File

- `tool.started`
- `tool.output`
- `tool.completed`
- `tool.failed`
- `file.read`
- `file.changed`
- `file.created`
- `file.deleted`

### Wait / Permission

- `user_input.requested`
- `permission.requested`
- `permission.resolved`
- `scope_expansion.requested`
- `scope_expansion.resolved`

### Result

- `result.submitted`
- `result.validation_failed`
- `report.published`
- `task.reported_complete`
- `task.verified_success`
- `task.verification_failed`

## 13.2 原始事件保留

统一事件不能取代原生输出。

必须保留：

- 原始 stdout；
- 原始 stderr；
- 原生结构化记录（可用时）；
- 统一 events.jsonl。

目的：

- 审计；
- Adapter bug 排查；
- 未来重放；
- 协议升级迁移。

## 13.3 事件序号

每个 Run 必须有单调递增 `seq`。

事件时间戳不能作为唯一排序依据。

并发事件可有相同时间戳，但不能有相同 Run sequence。

---

# 14. 状态模型

禁止只有一个扁平的 `running/stuck/done`。

状态必须拆成四个正交维度。

## 14.1 Process State

```text
queued
starting
alive
exited
orphaned
unknown
```

## 14.2 Protocol State

```text
initializing
thinking
tool_running
streaming
retrying
waiting_permission
waiting_user
waiting_scope
idle_between_turns
closing
closed
protocol_error
```

## 14.3 Progress State

```text
active
quiet
suspected_stall
stalled
unknown
```

## 14.4 Task State

```text
planned
running
blocked
reported_complete
verifying
verified_success
verified_partial
verification_failed
failed
cancelled
```

## 14.5 展示示例

```text
process: alive
protocol: waiting_permission
progress: quiet
task: blocked
```

这不是“卡死”。

另一个例子：

```text
process: alive
protocol: tool_running
progress: suspected_stall
task: running
last action: go test ./...
confidence: low
```

---

# 15. 存活、等待与卡死判断

## 15.1 原则

“进程活着”“协议有响应”“任务有进展”是三个不同问题。

禁止用单一 heartbeat 文件得出全部结论。

## 15.2 信号优先级

从高到低：

1. Harness 原生结构化生命周期事件；
2. 协议层状态；
3. 进程和进程树存活；
4. stdout/stderr 新数据；
5. 工具调用状态；
6. 文件变化；
7. Git diff；
8. CPU、网络等辅助信息。

## 15.3 Git 的角色

Git 可以用于：

- 基准快照；
- 变化文件列表；
- scope 审计；
- Wave barrier；
- 最终验证。

Git **禁止作为唯一 heartbeat 或存活真相**。

## 15.4 Progress heartbeat

只有发生真实语义活动时才更新 `last_progress_at`：

- 新模型输出；
- 工具开始/结束；
- 命令输出；
- API retry；
- 等待状态变化；
- 权限请求；
- 文件操作；
- 会话消息。

不得由一个盲目定时器无条件刷新 Worker progress。

## 15.5 动态阈值

不同最后状态使用不同 quiet 阈值：

- 正在 streaming：短阈值；
- 模型 thinking：中等阈值；
- 正在编译/测试：较长阈值；
- API retry：基于 retry 事件；
- waiting_permission/user/scope：不进入 stall；
- 已知长任务：使用 Task 或工具提供的期限。

具体默认秒数属于配置和测试问题，不应写死在架构层。

## 15.6 Stall 迁移

```text
active
  -> quiet
  -> suspected_stall
  -> stalled
```

进入 `stalled` 应至少有多项证据：

- 长时间无协议事件；
- 无 stdout/stderr；
- 无工具输出；
- 无可识别等待状态；
- 进程仍活着或进程状态异常；
- 超过配置阈值。

## 15.7 自动终止

`suspected_stall` 不得自动终止。

只有以下条件可以自动终止：

- 明确 hard timeout；
- 用户/主 Agent 发出 cancel；
- Supervisor 关闭 Run；
- Harness 协议确认不可恢复错误；
- 资源保护策略触发，且策略已显式启用。

---

# 16. 消息与“直接对话”

## 16.1 逻辑模型

主 Agent 与 Worker 的体验是直接对话。

物理路径是：

```text
Main Agent
  <-> Run Supervisor Message Router
  <-> Harness WorkerSession
```

Worker 不直接寻找主 Agent 进程。

## 16.2 回合制现实

主 Agent 未运行时，Worker 的问题必须进入持久信箱。

主 Agent 通过：

- `status`
- `inbox`
- `wait`
- `collect`

读取新问题。

## 16.3 发送语义

Adapter 需要区分：

### Immediate steer

消息可追加到正在运行的 turn。

### Next-turn queue

消息在当前 turn 完成后发送。

### Resume required

需要恢复原生 session 后发送。

### Unsupported

无法安全保持同一会话语义。

系统必须公开真实能力，不得把“重启一个新 Agent”伪装为原会话直接对话。

## 16.4 问题类别

Worker 提问必须分类：

- `decision`
- `scope`
- `permission`
- `missing_information`
- `conflict`
- `environment`
- `validation_failure`

问题应尽可能单一明确。

---

# 17. Question / Report 发布管线

## 17.1 三层模型

```text
Harness 原生输出
  -> 内部结构化 Envelope
  -> 校验
  -> Agent 可读 Markdown
```

## 17.2 原子发布

正式文件不得被渐进写入。

流程：

1. 接收完整内部对象；
2. Schema 校验；
3. 最低语义校验；
4. 一致性检查；
5. 渲染到临时文件；
6. flush / close；
7. 原子 rename；
8. 更新状态；
9. 写入 `report.published` 或 `question.published` 事件。

## 17.3 `report.md` 的存在语义

若正式 `report.md` 存在：

- 对应内部结果已完整接收；
- 已通过 Schema；
- 已通过最低语义校验；
- 文件已原子发布；
- 主 Agent 可以读取。

它**不代表代码已最终验证成功**。

## 17.4 校验失败

若 Worker 结果不合法：

- 不得生成正式 `report.md`；
- 保存校验错误；
- Task 状态为 `result_invalid` 或相应失败状态；
- 可以向同一 Worker 请求一次修复格式；
- 重试次数必须有限。

## 17.5 最低语义校验

### succeeded

必须：

- summary 非空；
- 说明完成内容；
- 说明是否修改文件；
- 修改型任务提供文件列表；
- 提供验证结果或未验证原因；
- 不包含严重未完成项。

### partial

必须：

- 说明已完成；
- 说明未完成；
- 说明停止原因；
- 说明当前修改是否可用。

### blocked

必须：

- 给出明确问题；
- 说明阻塞原因；
- 说明当前改动状态；
- 涉及 scope 时列出请求路径。

### failed

必须：

- 说明失败阶段；
- 给出错误摘要；
- 说明是否遗留修改；
- 给出接手建议。

---

# 18. Markdown 模板

## 18.1 `question.md`

```markdown
# 需要主 Agent 决策

## 问题

明确的问题。

## 原因

为什么无法继续。

## 当前任务范围

- ...

## 请求扩大范围（如适用）

- ...

## 与其他任务的关系

说明依赖或冲突。

## 当前工作区状态

说明是否已产生修改。

## 建议

Worker 的建议，但不代替主 Agent 决策。
```

## 18.2 `report.md`

```markdown
# Task 报告

## 状态

成功 / 部分完成 / 阻塞 / 失败 / 取消。

## 完成内容

- ...

## 修改文件

- `path`

## 验证

执行了什么，结果如何。

## 未完成内容

- ...

## 风险与注意事项

- ...

## 交接说明

后续主 Agent 或下一 Wave 需要知道的信息。
```

## 18.3 `run-summary.md`

```markdown
# Run 汇总

## 总体状态

N 个成功，N 个部分完成，N 个阻塞。

## Wave 状态

说明当前 Barrier 和验证状态。

## Task 摘要

### task-a

简短结果和报告路径。

## 待主 Agent 决策

- ...

## Scope 审计

- ...

## 集成验证

- ...

## 建议下一步

- ...
```

---

# 19. 完成与验证

## 19.1 两阶段完成

Worker 提交合法报告后：

```text
task.reported_complete
```

Supervisor 或主 Agent执行验证后，才能进入：

```text
verified_success
verified_partial
verification_failed
```

## 19.2 验证层次

### Task 局部验证

由 Worker 按 Contract 执行。

### Wave Barrier 验证

所有 Task 停止写入后执行：

- scope 并集检查；
- 未授权文件检查；
- 编译/测试；
- 关键接口检查；
- 聚合报告。

### Run 最终验证

全部 Wave 完成后执行：

- 全局测试；
- 静态检查；
- 必要的集成测试；
- 最终 Git diff 审阅；
- 未处理问题检查。

## 19.3 验证失败

验证失败不等于自动回滚。

Supervisor 必须：

- 保存失败证据；
- 标记失败范围；
- 不清理其他 Agent 修改；
- 交由主 Agent决定：
  - 追加修复 Task；
  - 重开下一 Wave；
  - 取消；
  - 人工接管。

---

# 20. Wave Barrier

Barrier 是共享工作区可靠性的关键。

## 20.1 进入条件

进入 Barrier 前：

- 本 Wave 不再有 Worker 写入；
- 所有 Worker 已终止、等待或明确阻塞；
- 所有可用报告已发布；
- Supervisor 已 flush 事件。

## 20.2 Barrier 操作

至少执行：

1. 收集 Task 状态；
2. 读取所有报告；
3. 检查未回答问题；
4. 生成实际变化文件集合；
5. 与 scope 并集比较；
6. 检查高风险全局文件；
7. 运行配置的集成验证；
8. 生成 barrier 结论；
9. 更新 `run-summary.md`。

## 20.3 Barrier 结果

```text
passed
passed_with_warnings
blocked
failed
cancelled
```

只有 `passed` 或主 Agent明确接受的 `passed_with_warnings` 才能进入下一 Wave。

---

# 21. 进程管理

## 21.1 进程树

Supervisor 必须管理完整进程树。

Unix 应考虑：

- process group/session；
- signal propagation；
- 子孙进程；
- pipe 生命周期。

Windows 应考虑：

- Job Object；
- 子进程关联；
- CTRL 事件或终止策略；
- PID 复用。

## 21.2 终止顺序

取消时建议：

1. 协议级 interrupt；
2. 礼貌终止；
3. 等待短暂 grace period；
4. 终止进程组/Job；
5. 记录仍存活子进程；
6. 标记 orphan 风险。

## 21.3 Exit 与 Completion

进程 exit code 0 不等于 Task 成功。

Task 成功需要：

- 合法最终结果；
- 正式 report；
- 必要验证。

进程非零退出通常表示失败，但若已收到完整结果且退出是已知 Harness 行为，Adapter 可以按兼容规则解释；必须记录该例外。

---

# 22. 持久化

## 22.1 V1 存储方案

V1 可以使用：

- Append-only JSONL；
- 原子 `state.json`；
- 原子 Markdown；
- 小型索引 JSON。

V1 不强制 SQLite。

## 22.2 状态真相

正常运行：

- Supervisor 内存状态机是当前运行时权威；
- 每个状态迁移立即落盘。

故障恢复：

- `events.jsonl` 是可重放事实；
- `state.json` 是加速快照；
- 原始日志用于诊断。

Broker 不应通过重新解析 Markdown 恢复内部状态。

## 22.3 原子写

可变快照必须：

1. 写临时文件；
2. flush；
3. 必要时 fsync；
4. 原子替换。

不得直接覆盖写关键状态文件造成半文件。

---

# 23. 故障恢复

## 23.1 Supervisor 崩溃

重新查询 Run 时：

1. 验证 Supervisor identity；
2. 检查 IPC；
3. 检查 Worker PID 与启动身份；
4. 重放事件；
5. 对照 state snapshot；
6. 标记不确定状态；
7. 尝试重新接管仅在协议和 OS 允许时进行。

无法接管时不得谎称 Worker 仍受控。

## 23.2 Reconciliation 状态

可使用：

```text
recovering
recovered
degraded
orphaned
unrecoverable
```

## 23.3 Harness session 恢复

只有 Adapter 明确支持 resume 时才尝试。

恢复必须使用原生 session ID，并验证项目目录、Harness 身份和会话关联。

## 23.4 磁盘损坏

JSONL 尾部出现不完整行时：

- 保留完整事件；
- 截断或隔离损坏尾部；
- 写入 recovery 事件；
- 不静默丢弃历史。

---

# 24. 权限与安全

## 24.1 信任模型

用户信任 Harness 在项目内工作，但系统仍应：

- 显式展示权限；
- 记录危险操作；
- 处理 Harness 原生 permission request；
- 不自动批准明显高风险操作。

## 24.2 Prompt 注入与外部内容

Task 若访问网络或外部 issue/文档：

- 应把外部内容视为不可信；
- 不允许外部文本修改系统 Task Contract；
- 不允许 Worker 因网页指令扩大 scope；
- 报告中应注明外部来源对决策的影响。

## 24.3 凭据

日志与报告不得主动输出：

- API key；
- access token；
- cookies；
- 私钥；
- 完整环境变量。

Adapter 启动时不得把无关环境变量复制进持久化元数据。

---

# 25. 嵌套 Agent 策略

V1 默认：

```text
allow_nested_agents = false
```

Task Contract 必须明确禁止：

- 使用当前 Skill 再次 dispatch；
- 调用 Harness 原生 Subagent；
- 启动另一个并行编排器。

如未来允许：

- 必须有全局最大深度；
- 必须继承 scope；
- 必须计入并发预算；
- 必须进入统一事件树；
- 父 Task 取消时子 Agent 必须取消。

V1 可完全不实现该能力。

---

# 26. 配置

## 26.1 配置层次

建议优先级：

1. CLI 本次覆盖；
2. Project-specific Broker 配置；
3. 用户 `BROKER_HOME/config.toml`；
4. 程序默认值。

但 Project-specific 配置不得要求污染项目仓库；可以存储在 Broker Home 的 project 目录。

## 26.2 配置类别

- Harness executable path；
- 版本约束；
- 默认模型；
- 并发上限；
- quiet/stall 阈值；
- hard timeout；
- 日志保留；
- 报告模板版本；
- 权限策略；
- Adapter feature flags；
- 最大报告重试；
- 最大事件文件大小。

## 26.3 配置快照

每个 Run 必须保存启动时的有效配置快照。

后续修改全局配置不得改变已运行 Run 的历史解释。

---

# 27. Go 模块边界

以下是建议包职责，不规定最终包名。

## `cmd`

- CLI 入口；
- 参数解析；
- 输出格式；
- exit code。

## `project`

- 项目根解析；
- canonical path；
- project key；
- 项目索引。

## `run`

- Run 创建；
- Run 生命周期；
- Run 查询。

## `wave`

- Wave preflight；
- 调度；
- Barrier。

## `task`

- Task Contract；
- scope；
- completion criteria。

## `supervisor`

- Run Supervisor；
- IPC；
- Worker 管理。

## `process`

- 跨平台进程树；
- signal；
- Job Object；
- PID identity。

## `adapter`

- 通用 Adapter 接口；
- capability；
- registry。

## `adapter/claude`

- Claude Code 专用逻辑。

## `adapter/codex`

- Codex 专用逻辑。

## `adapter/grok`

- Grok Build 专用逻辑。

## `adapter/opencode`

- OpenCode 专用逻辑。

## `event`

- 统一事件；
- seq；
- append-only writer；
- replay。

## `state`

- 状态机；
- 快照；
- transition validation。

## `message`

- inbox；
- routing；
- delivery semantics。

## `report`

- Envelope Schema；
- semantic validation；
- Markdown renderer；
- atomic publish。

## `storage`

- Broker Home；
- 文件布局；
- 原子 I/O；
- retention。

## `verify`

- scope audit；
- Git baseline；
- validation execution；
- Barrier result。

## `doctor`

- Harness 探测；
- 版本兼容；
- 环境诊断。

包之间必须避免循环依赖。Adapter 不应直接修改 Run 状态文件，而应通过统一事件和 Supervisor 接口。

---

# 28. 状态输出设计

## 28.1 默认 `status`

主 Agent默认只需要摘要：

```text
Run: auth-refactor
Wave: 1 / running
Overall: 2 running, 1 blocked, 1 reported complete

task-auth / Codex
  task: running
  process: alive
  protocol: tool_running
  progress: active
  last progress: 12s ago
  scope: internal/auth/**

task-api / Claude
  task: blocked
  protocol: waiting_scope
  question: requests TokenProvider interface change

task-tests / OpenCode
  task: reported_complete
  report: ready
  verification: pending
```

## 28.2 `inspect`

详细信息可包含：

- 原生 session ID；
- 最近 N 个事件；
- 最后工具；
- 进程树；
- 最近 stdout/stderr；
- scope；
- 兼容性警告；
- 消息队列；
- stall 证据。

默认 `status` 不应输出这些内容。

---

# 29. Exit Code 语义

CLI 必须定义稳定 exit code。

建议语义类别：

- 成功；
- 使用错误；
- 找不到 Run；
- preflight 拒绝；
- 部分完成；
- Run 失败；
- 超时；
- 通信失败；
- 兼容性失败；
- 内部错误。

`wait --all` 的 exit code 必须反映 Run/Wave 最终状态，而不是仅反映 CLI 自身是否成功连接。

---

# 30. 测试策略

## 30.1 Fake Harness

必须实现可控 Fake Harness 测试夹具，模拟：

- 正常流式输出；
- 长时间 thinking；
- 长命令无输出；
- waiting permission；
- waiting user；
- scope request；
- 非零退出；
- 进程卡死；
- 子进程不退出；
- 输出半截 JSON；
- session resume；
- active steer；
- 完成报告格式错误；
- Supervisor 崩溃；
- PID 复用。

不能主要依赖真实付费 Harness 做单元测试。

## 30.2 单元测试

覆盖：

- path normalization；
- project key；
- scope overlap；
- glob；
- state transition；
- event sequence；
- report validation；
- Markdown atomic publish；
- config precedence；
- retention。

## 30.3 集成测试

覆盖：

- dispatch -> status -> wait -> collect；
- 并行 Wave；
- Barrier；
- cancel；
- inbox/send；
- Supervisor restart；
- Worker orphan；
- 跨平台进程终止。

## 30.4 Adapter contract tests

每个 Adapter 必须通过统一契约测试：

- Probe；
- Start；
- Event parsing；
- Final result；
- Cancel；
- Capability truthfulness；
- Error mapping。

## 30.5 关键回归测试

必须有测试确保：

> `wait` 不会在 Task 仍是 running 时提前返回成功。

还必须覆盖：

- 最后一个 Worker 慢速完成；
- 子进程仍活着；
- report 已出现但验证尚未结束；
- Worker exit 但结果未提交；
- reported complete 与 verified success 的差异。

---

# 31. V1 验收标准

只有全部满足，V1 才可称为架构验收通过。

## 31.1 基础

- Go 单一实现；
- 四 Harness Adapter 有独立能力声明；
- `doctor` 可检测环境；
- project-first Broker Home；
- Run ID 唯一。

## 31.2 并行

- 支持一个 Wave 多 Task；
- preflight 能拒绝 scope 重叠；
- 不创建 worktree；
- Task Contract 被实际注入；
- 默认禁止嵌套 Agent。

## 31.3 状态

- 四维状态模型；
- 增量事件；
- 不把等待权限判为卡死；
- suspected stall 有原因和置信度；
- PID identity 防复用。

## 31.4 消息

- Worker 可提交 question；
- 主 Agent可 inbox；
- 可发送 answer；
- 能公开 immediate/queued/resume/unsupported 语义。

## 31.5 报告

- 内部 Envelope 校验；
- Markdown 原子发布；
- `report.md` 不等于 verified success；
- Run summary 聚合；
- JSON 不作为主 Agent默认阅读界面。

## 31.6 生命周期

- 真正阻塞的 `wait`；
- 可靠 cancel；
- Supervisor 终态退出；
- 故障后可降级读取；
- 不丢失已落盘事件。

## 31.7 Scope 与验证

- Run 级 diff 审计；
- 未授权文件标记；
- Barrier；
- reported/verified 分离；
- 验证失败保留证据。

---

# 32. 分阶段实施计划

## Phase 0：架构骨架

交付：

- 领域模型；
- 状态机；
- 文件布局；
- Adapter interface；
- Fake Harness；
- ADR。

禁止接真实 Harness 前就堆 CLI 功能。

## Phase 1：单 Harness 纵向闭环

优先选择结构化接口最稳定的一个 Harness，完成：

- dispatch；
- Supervisor；
- status；
- events；
- wait；
- Result Envelope；
- report.md；
- cancel；
- recovery。

目标是证明完整生命周期，而非同时接四个半成品 Adapter。

## Phase 2：Wave 与共享工作区

加入：

- Task Contract；
- scope preflight；
- 多 Task；
- Barrier；
- Run summary；
- scope audit。

## Phase 3：消息系统

加入：

- inbox；
- question；
- send；
- scope expansion；
- permission routing。

## Phase 4：其余 Adapter

逐个接入，所有 Adapter 必须通过相同 contract tests。

## Phase 5：硬化

加入：

- 跨平台进程树；
- PID reuse；
- 崩溃恢复；
- retention；
- compatibility matrix；
- 压力测试；
- 真实 Harness smoke tests。

---

# 33. 架构决策记录（ADR）清单

实现仓库应包含 ADR，至少记录：

1. 为什么选择 Go；
2. 为什么使用 Run-scoped Supervisor；
3. 为什么运行数据放 Broker Home；
4. 为什么 project-first；
5. 为什么 V1 不使用 worktree；
6. 为什么使用 Wave + Barrier；
7. 为什么 scope 是软契约；
8. 为什么 JSON 内部、Markdown 对 Agent；
9. 为什么完成与验证分离；
10. 为什么默认禁止嵌套 Agent；
11. 为什么不用 Git diff 判断存活；
12. 为什么 V1 不强制 SQLite。

修改这些决定必须新增 ADR，不得直接在代码中悄悄改变。

---

# 34. 实现者禁止做出的“简化”

以下看似简单，但属于错误实现：

## 34.1 只保存 PID

错误，因为 PID 可复用，且无法管理进程树。

## 34.2 `nohup` 后完全脱管

错误，因为失去双向消息、权限、可靠取消和状态控制。

## 34.3 通过日志 mtime 判断全部状态

错误，因为等待、长命令和卡死无法区分。

## 34.4 把最后一段自然语言直接当成功

错误，因为没有结构、校验和验证。

## 34.5 让主 Agent读取所有 JSONL

错误，因为会造成上下文噪声，应提供 Markdown 摘要和增量事件。

## 34.6 发现 scope 重叠后自动 worktree

错误，因为这改变了已确定的协作模型。

## 34.7 每个 Worker 自己写全局 state

错误，容易竞争。全局 Run 状态必须由 Supervisor 单写者维护。

## 34.8 Worker 完成后自动 Git commit

错误，会影响共享工作区和主 Agent控制权。

## 34.9 无输出数分钟自动 kill

错误，必须区分工具运行、等待和 retry。

## 34.10 对不支持 resume 的 Harness 假装 resume

错误，能力必须真实。

---

# 35. 架构不变量

任何实现版本都必须保持：

1. 一个 Task 的逻辑身份不等于一个进程；
2. 同一 Wave 的写入 scope 不重叠；
3. 同一 Wave 的任务不存在预期产出依赖；
4. Supervisor 是 Run 状态的唯一运行时写入者；
5. Event append-only；
6. 正式 Markdown 原子发布；
7. `report.md` 只代表报告合法，不代表验证成功；
8. 等待状态不属于 stall；
9. suspected stall 不自动等于 kill；
10. Git 用于代码变化，不用于协议；
11. Broker Home 不进入项目 Git；
12. V1 不创建 worktree；
13. V1 默认不允许嵌套 Agent；
14. 主 Agent保留最终决策和验收权；
15. Adapter 必须如实声明能力。

---

# 36. 最终架构摘要

本 Skill 的最终定位是：

> 一个使用 Go 实现的本地多 Harness 编排控制层。它通过 Run-scoped Supervisor 管理 Claude Code、Codex、Grok Build 和 OpenCode，在同一项目 checkout 中，以 Task Contract、互斥写入责任和 Wave + Barrier 模型执行真正可并行的任务。

它的关键组成不是“同时启动四个 CLI”，而是：

1. **Task Contract**
2. **Parallel Wave**
3. **Run-scoped Supervisor**
4. **Harness Capability Adapter**
5. **Normalized Event Protocol**
6. **Four-dimensional State Machine**
7. **Persistent Message Router**
8. **Validated Result Envelope**
9. **Agent-facing Markdown Reports**
10. **Barrier Verification**

系统必须坚持：

- 并行问题在任务拆分阶段解决，而不是用 worktree 推迟；
- 运行状态依靠结构化事件和进程管理，而不是 Git 日志技巧；
- 主 Agent读取自然 Markdown，机器内部使用结构化数据；
- Worker 声称完成后仍需验证；
- 所有不确定性必须显式呈现，不得伪装成确定状态。

---

# 37. 官方接口参考（实现前必须重新运行 doctor 验证）

以下链接用于 Adapter 设计参考。外部 Harness 可能更新，实现不能只依赖本文档中的静态描述。

- Claude Code CLI reference  
  https://docs.anthropic.com/en/docs/claude-code/cli-reference

- Claude Code Hooks  
  https://docs.anthropic.com/en/docs/claude-code/hooks

- Codex App Server  
  https://developers.openai.com/codex/app-server

- Codex non-interactive mode  
  https://developers.openai.com/codex/noninteractive

- Codex subagents  
  https://developers.openai.com/codex/subagents

- OpenCode Server  
  https://opencode.ai/docs/server/

- Grok Build overview  
  https://docs.x.ai/build/overview

- Grok Build headless / ACP  
  https://docs.x.ai/build/cli/headless-scripting

---

## 结束语

实现者应先确保系统在 Fake Harness 下满足完整状态机、消息、报告、取消和恢复语义，再逐个接入真实 Harness。

任何为了“尽快跑起来”而绕过 Task Contract、Wave Barrier、状态分层、原子报告或真实能力声明的实现，都不属于可接受的 V1。
