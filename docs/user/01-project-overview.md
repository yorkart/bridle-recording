# 项目介绍

bridle-recording 是一个面向模型应用和 Agent 的透明代理录制工具。它把真实客户端请求转发到上游模型服务，同时在旁路保存完整原始流量，后续用于会话观测、测试集沉淀和 mock 回放。

## 适用场景

- 观察 Agent 与模型服务之间的多轮交互。
- 保存真实请求和响应，作为后续回归测试资产。
- 将已确认的录制会话保存为测试集，供集成测试或 mock 服务使用。
- 在不改变线上请求语义的前提下，为调试、排查和复现实验提供依据。

## 核心原则

代理正确性是本项目第一优先级。

- 在线代理链路保持透明转发。
- 在线录制只保存 recorder 观测到的原始内容。
- 请求头、请求体、响应头、响应体、SSE 事件和 WebSocket 帧不在主链路中改写。
- 录制失败不应影响请求转发。
- 脱敏、裁剪、mock 匹配、回放准备等能力属于离线派生流程，不进入在线代理和原始录制链路。

## 基本工作流

```text
客户端或 Agent
  -> bridle-recording 透明代理
  -> 上游模型服务

旁路保存:
  原始录制 -> 观测页面 -> 保存为测试集 -> mock 或集成测试使用
```

推荐按下面顺序使用：

1. 启动 recorder。
2. 配置 Codex 或其他兼容客户端走 recorder。
3. 执行一次真实交互，让 recorder 生成会话录制。
4. 打开观测页面检查会话内容。
5. 将确认可用的会话保存为测试集。
6. 集成测试通过测试集接口读取可用输入，并通过 mock 服务回放。

## 主要概念

**Profile**

Profile 表示一个被录制的客户端或模型通道。当前内置示例包括 `codex-http` 和 `codex-websocket`。每个 profile 有自己的上游地址、录制目录和 mock 路由。

**Session**

Session 是一次或一组连续交互的录制单位。recorder 会根据会话 header 识别 session，并把请求按顺序写入对应目录。

**Recording**

Recording 是原始录制数据，保存在本机 `~/.bridle-recording/<profile>/recordings/<session_id>/` 下。它是事实来源，不应被编辑或脱敏。

**Observability UI**

观测页面用于浏览已经录制的会话。页面会把原始请求解析成更容易阅读的 turn、prompt、tools、tool call、metadata 等视图。

**Testset**

Testset 是保存到当前 Git 仓库中的测试资产。它包含测试集索引文件和一份原始录制副本。测试集以第一个用户输入作为业务唯一键，并用该输入的 sha256 作为目录 id。

## 本地目录约定

默认运行时目录：

```text
~/.bridle-recording/
  codex-http/
    recordings/
    derived/mock/
  codex-websocket/
    recordings/
    derived/mock/
```

`recordings/` 只包含在线录制的原始数据和必要元信息。mock 优先回放仓库中的测试集，未匹配到测试集时才回退到本地 recording，供临时回放使用。mock 的匹配索引与可选响应改写配置位于 `derived/mock/`，不得写回 recording session 或测试集 `raw/`。

仓库内测试集目录：

```text
testsets/
  <profile>/
    <first_user_input_sha256>/
      testset.json
      raw/
        <source_session_id>/
```

## 下一步

- 代理启动和客户端配置见 [代理使用](./02-proxy-usage.md)。
- 录制与页面操作见 [录制、页面使用](./03-recording-and-ui.md)。
- 测试集发现和集成测试接入见 [测试集接入使用](./04-testset-integration.md)。
