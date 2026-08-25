# 基于 cordis-rs 复刻 deepseek-harness（暂定名 `dsh-rs`）实施计划

> 状态：待评审（尚未开始执行）
> 底座：`cordis-rs`（Cordis 4.x 的 Rust 移植，正是 deepseek-harness 所基于的框架）
> 目标：用 Rust 在 cordis-rs 之上 faithful 复刻 deepseek-harness

---

## 0. 目标与假设

- 以本仓库 `cordis-rs` 为底座，在其上用 Rust 复刻 deepseek-harness。
- 交付范围：**完整 faithful 克隆**（覆盖上游核心子系统与可扩展架构，而非一次性脚本）。
- LLM 提供方：**OpenAI 兼容 HTTP**（覆盖 deepseek / openai / ollama / vLLM 等大多数服务）。
- 交付形态：**两者都要** —— 无头 SDK/CLI（编程式 `run()` + 命令行）与 Web 聊天 UI。
- 组装方式：**两者都要** —— `cordis.yml` + bundle/profile 配置驱动，以及纯 Rust 编程式 API。
- 命名暂定 `dsh-rs`，CLI 为 `dsh`（可改名）。新 crate 作为本 workspace 成员，置于 `harness/`（路径 `harness/dsh-*`）。

---

## 1. 架构映射（对齐上游 `ctx.*` 服务）

deepseek-harness 的核心设计：**一切皆插件**，每个能力（模型适配器、工具、会话日志、agent 循环）都是挂在共享 `ctx` 上的 `Service`，通过事件通信，通过可逆 effect 注册/卸载。下表把上游子系统映射到 Rust crate 与 `Service` 实现。

| 上游包 | Rust crate | 提供的服务 (`ctx` key) |
|---|---|---|
| core/session | `dsh-core` | `ctx.sessions` — 追加式 `SessionEvent` 日志 + 内存存储 + `deriveMessages()` + fork |
| core/system-prompt | `dsh-core` | `ctx.systemPrompt` — prompt 段 & 工具 schema 组装 |
| core/tools | `dsh-core` | `ctx.tools` — 作用域工具注册 + `tools/{before,call,after}-call` 守卫执行 |
| core/agent | `dsh-core` | `ctx.agents` — `Agent` 接口/注册表 + `agent/*` 事件 |
| core/agent-loop | `dsh-core` | `ctx.agentLoop` — 默认驱动（请求 → 派生消息 → 流式 LLM → 工具 → 循环） |
| core/scope | `dsh-core` | `ctx.scope` — 每 agent 作用域注册原语（封装 `Context::isolate_with`） |
| llm/llm | `dsh-llm` | `ctx.llm` — 消息/流词汇 + 适配器接缝（`LlmAdapter` trait + OpenAI 实现） |
| fs | `dsh-fs` | `ctx.fs` — 文件系统访问与策略 |
| shell / subprocess / sandbox / terminals | `dsh-shell` / `dsh-subprocess` / `dsh-sandbox` / `dsh-terminals` | `ctx.shell` / `ctx.subprocess` / `ctx.sandbox` / `ctx.terminals` |
| jobs / commands / goals / subagent / sessionTitle / telemetry | `dsh-jobs` / `dsh-commands` / `dsh-goals` / `dsh-subagent` / `dsh-session-title` / `dsh-telemetry` | 对应 `ctx.*` |
| bundle/base, headless, web-app | `dsh-base` / `dsh-headless` / `dsh-web` | 组合层（profile 模板） |
| CLI / SDK | `dsh-cli`（`dsh run|web|headless`）、`dsh-sdk`（编程式 `DeepSeekHarness.run`） | — |
| runtime 桥接 | `dsh-runtime` | `ctx.runtime` — 持有 tokio runtime |
| 配置/打包 | `dsh-config` | bundle/profile 注册表 + 基于 `cordis-loader` 的分层启动 |

**关键不变量（对齐上游）**：会话日志是模型所见上下文的唯一真相来源。"model-visible means logged"——任何进入模型请求的输入都必须可由 `SessionEvent` 日志重建；新增模型可见输入必须新增对应的 `SessionEvent` 变体并从日志渲染。

**事件域（对齐上游）**：
- 会话事件（`turn/*`、`step/*`、`user/message`、`assistant/*`、`tool/*`）是写入日志的持久事实，经 `session/event` 广播。
- Agent 事件（`agent/*`）携带活动 `Agent`（inbox/step/status/request/validation/continuation），用于观测或拦截在途工作。
- 能力事件（`fs/*`、`tools/*`、`telemetry/*`）在不引入循环依赖的前提下挂接策略与适配器。
- `agent/pre-step`、`agent/request`、`llm/stream` 及三个 `tools/*` 为 **waterfall**（监听器须调用 `next()` 委托）；`agent/turn-stopping` 为 **serial** 且无 `next()`。

---

## 2. 关键技术决策

### 2.1 异步桥接（最重要）
cordis-rs 核心是 **executor-free** 的"急切阻塞"模型：`plugin.apply`、disposer、事件监听器不能在同线程上阻塞（会死锁/破坏生命周期协作，见 `crates/cordis/src/fiber.rs:639`、`events.rs:317` 的约束）。

因此引入 `dsh-runtime` 持有 `tokio::runtime::Runtime`（多线程），并暴露 `RuntimeService`（`ctx.runtime`，持有 `tokio::runtime::Handle`）。规则：
- **长时编排**（agent 循环、LLM 流式、Web 服务器）跑在 **tokio 任务**上；`apply` 中用 `plugin_async` 派生任务，并返回 `PluginOutput::disposer(...)` 来 abort。
- cordis 事件（`emit`/`parallel`/`serial`/`bail`/`waterfall`）仍由 cordis 自带阻塞执行器驱动；我们的**异步监听器只 await 由 tokio 工作线程推进的句柄**（oneshot/watch/channel），**绝不**在监听器内调用 `runtime.block_on`，避免重入死锁。
- 核心步进主干先以直接 service 调用 + `emit` 观测事件为主；重 async waterfall 仅用于明确需要的扩展点（`agent/pre-step`、`tools/*`）。

### 2.2 配置 / 插件发现
Rust 无法像 npm 那样按字符串名动态发现插件。方案：`dsh-config` 维护**静态 bundle 注册表**（`name -> Box<dyn Plugin>`），boot 时传给 `cordis-loader` 的 `LoaderConfig::with_registry(...)`，再用 `cordis.yml` 的 `entries` 引用这些名字——这与 `cordis-loader` 现有静态注册设计一致，是 faithful 做法。

### 2.3 LLM 接缝
定义 `LlmAdapter` trait：
```rust
#[async_trait]
trait LlmAdapter: Send + Sync + 'static {
    fn name(&self) -> &str;
    async fn complete(&self, req: LlmRequest) -> Result<LlmStream>;
}
```
`LlmStream` 为 `impl Stream<Item = LlmStreamEvent>`（`LlmStreamEvent` ∈ { `Chunk`, `ToolCall`, `Done`, `Error` }）。
`OpenAiAdapter` 用 `reqwest`（stream）+ `serde_json` 解析 SSE，映射 `ChatMessage`/`ToolCall` ↔ OpenAI 格式，从 env（`${{ }}` 由 `cordis-include` 插值）读取 key/url/model，注册到 `ctx.llm`，可热插拔。`FakeAdapter` 供测试。

### 2.4 会话日志即真相
`ctx.sessions.append(event)` 写追加日志并发 `session/event`；`derive_messages()` 从日志投影为 `Vec<ChatMessage>`；持久化到 `<session_root>/<id>.jsonl`；`fork(source, boundary?, child_id?)` 支持分叉/恢复。

---

## 3. Crate 布局（新增到根 workspace 的 `harness/`）

```
harness/
  dsh-runtime/        # ctx.runtime (tokio 桥接)
  dsh-core/           # session, system-prompt, tools, agent, agent-loop, scope
  dsh-llm/            # ctx.llm + OpenAiAdapter + FakeAdapter
  dsh-fs/             # ctx.fs
  dsh-shell/          # ctx.shell
  dsh-subprocess/     # ctx.subprocess
  dsh-sandbox/        # ctx.sandbox
  dsh-terminals/      # ctx.terminals
  dsh-jobs/           # ctx.jobs (+ job_* 工具)
  dsh-commands/       # ctx.commands
  dsh-goals/          # ctx.goals
  dsh-subagent/       # 子 agent 提供方
  dsh-session-title/  # ctx.sessionTitle (单例)
  dsh-telemetry/      # ctx.telemetry
  dsh-base/           # 组合层：注册上述核心插件
  dsh-headless/       # 一次性运行器 (profile 模板)
  dsh-web/            # axum HTTP + WS + SPA
  dsh-config/         # bundle/profile 注册 + 分层启动
  dsh-cli/            # `dsh` 二进制
  dsh-sdk/            # 编程式 API
  dsh-tests/          # 端到端对齐测试 (FakeAdapter)
```

新增依赖（仅 harness crate；`cordis` 核心保持零依赖，符合上游设计）：`tokio`、`reqwest`（stream 特性）、`serde`、`serde_json`、`futures`、`uuid`、`async-stream`、`axum`/`tower`（Web）、`json-patch`（patch）、`dirs`、`clap`（CLI）等。

---

## 4. 分阶段实施计划

- **M0 脚手架**：workspace 成员、`dsh-runtime`（`RuntimeService` + tokio 桥接 + `plugin_async_on_tokio` 帮助宏）、`dsh-config` 的 bundle/profile 注册表骨架、基础错误/类型 crate。可 `dsh run cordis.yml` 启动空 harness。

- **M1 核心数据面**：`dsh-core` 的 `ctx.sessions`（`SessionEvent` 枚举、`SessionStore`、`append`→`session/event`、`deriveMessages`、`fork`、jsonl 持久化）与 `ctx.systemPrompt`（段注册 + 渲染）。

- **M2 LLM 接缝**：`dsh-llm` 的 `ChatMessage`/`ToolCall`/`LlmStreamEvent`、`LlmAdapter` trait、`OpenAiAdapter`（流式 SSE）、`ctx.llm` 注册；`FakeAdapter` 供测试。

- **M3 工具 + 智能体循环（首个可跑通 agent）**：`ctx.tools`（注册、JSON schema、`tools/*` 守卫执行）、`ctx.agents` + `agent/*` 事件、`ctx.agentLoop`（请求 → 派生消息 → 流式 → 工具 → 循环 → `agent/turn-stopping`）、`ctx.scope`。先给 1~2 个真实工具（用 `std::process` 的 `bash` 占位，M4 替换为 `dsh-shell`）。`dsh-sdk` 的 `DeepSeekHarness::run(prompt, session_id)` 与 `dsh-headless` + `dsh-cli dsh run` 跑通端到端（先 FakeAdapter 测试，再接真实 OpenAI 兼容端点）。
  > **← 批准后先实现到 M3。**

- **M4 系统能力**：`dsh-subprocess` / `dsh-shell` / `dsh-fs` / `dsh-sandbox` / `dsh-terminals` 后端，替换占位工具，提供 `bash`、`read`/`write`/`edit`/`glob`/`grep` 等工具，支持沙箱 argv 包装。

- **M5 进阶能力**：`dsh-jobs`（+`job_*` 工具）、`dsh-commands`、`dsh-goals`、`dsh-subagent`（子 agent 提供方，复用 `ctx.agents` + isolate）、`dsh-session-title`、`dsh-telemetry`。

- **M6 配置成熟化**：`dsh-base` 完整组合、`dsh-headless`/`dsh-web` 作为 profile 模板、`cordis.patch.yml` 分层叠加、`--patch` 覆盖、loader `reload`/HMR。

- **M7 Web UI**：`dsh-web`（axum + 静态 SPA + `/ws`），订阅 `session/event`/`agent/*`，浏览器发 prompt、流式渲染、会话树/分叉；`dsh web` 启动。

- **M8 对齐与打磨**：以 deepseek-harness 官方子系统文档为 spec 写 `dsh-tests` 端到端用例；补充 README/架构文档；补齐遥测、错误分类、性能与稳定性。

---

## 5. 首阶段（M0–M3）交付细节

1. **`harness/dsh-runtime`**：`RuntimeService { handle: tokio::runtime::Handle }`，`provide` 到 root ctx；`plugin_async_on_tokio!` 宏在 tokio 上 spawn 长任务，返回 `PluginOutput::disposer` 以 abort。
2. **`harness/dsh-core/src/session.rs`**：`SessionEvent` 枚举（`user/message`、`assistant/chunk`、`assistant/message`、`tool/call`、`tool/result`、`step/*`、`turn/*`、`agent/*`）、`SessionStore::append` 发 `session/event`、`derive_messages()`、`fork()`、JSONL 持久化。
3. **`harness/dsh-llm`**：`LlmAdapter` trait + `OpenAiAdapter`（env 取 key/url/model）+ `FakeAdapter`。
4. **`harness/dsh-core/src/{tools,agent,agent_loop,system_prompt,scope}.rs`**：实现上述服务与事件；`agent_loop` 默认驱动跑通"请求→派生消息→流式→工具→循环"。
5. **`harness/dsh-sdk`**：`DeepSeekHarness` 编程式 API；**`harness/dsh-cli`**：`dsh run|headless`。
6. **`harness/dsh-tests`**：FakeAdapter 驱动的端到端测试（模型先调工具再给最终答案），验证循环、日志重建、工具执行。

**M0–M3 验收标准**：
- `dsh run --provider openai-compatible --model <m> -p "列出当前目录文件并统计行数"` 能端到端跑通：LLM 流式输出、调用 `bash` 工具、把结果回填、给出最终答案。
- `dsh-sdk` 编程式 `DeepSeekHarness::run(...)` 同等工作。
- `dsh-tests` 中 FakeAdapter 用例通过（断言会话日志可重建消息、工具被执行、最终答案生成）。
- 一份最小 `cordis.yml` 可配置驱动启动（配置驱动与编程式两条路均验证）。

---

## 6. 测试与对齐策略

- 每个 crate 单测；
- `dsh-tests` 做端到端（fake LLM 验证循环/工具/日志不变量）；
- 以 deepseek-harness 官方 reference/architecture 文档为 spec，逐子系统对齐 `ctx.*` 方法与事件模式（emit/parallel/serial/bail/waterfall）；
- 关键不变量自动化断言："model-visible 输入必落日志"。

---

## 7. 待确认的开放项（影响 M7 之前无需阻塞）

- **命名**：`dsh-rs` / `dsh` 是否可用，还是想用 `cordis-harness` 之类？
- **放置位置**：直接加进本仓库（同 workspace）还是另开仓库？
- **Web UI 技术栈**：`axum` + 原生 TS/JS 单页（零前端构建）还是引入 React/Vite？（影响 M7 工作量）

---

## 8. 参考（上游坐标）

- deepseek-harness 仓库：`https://github.com/deepseek-ai/deepseek-harness`
- 架构文档：`docs/architecture.md`（核心/session/tools/agent/llm 子系统表、`agent/*` 与 `tools/*` 事件模式）
- Cordis primer：`docs/cordis-primer.md`（插件即 Service、inject 依赖、可逆 effect、事件分发）
- 本仓库 `cordis-rs`：Cordis 4.x Rust 移植，提供 `Context`/`Events`/`Fiber`/`Service`/`Plugin`/`Reflect`/`Loader`（`crates/cordis/src`，`crates/cordis-loader`）。
