# 代理使用

本页说明如何启动 bridle-recording，并让 Codex、Claude Code 或 OpenAI 兼容客户端通过它访问上游模型服务。

## 启动 recorder

推荐使用项目脚本：

```sh
./scripts/run-recorder.sh
```

默认监听地址：

```text
http://127.0.0.1:8787
```

脚本默认设置：

```sh
BRIDLE_HOME_ROOT=~/.bridle-recording
HTTP_PROXY=http://127.0.0.1:7890
HTTPS_PROXY=http://127.0.0.1:7890
ALL_PROXY=socks5://127.0.0.1:7890
```

如果本机代理端口不同，可以在启动前覆盖：

```sh
HTTP_PROXY=http://127.0.0.1:7897 \
HTTPS_PROXY=http://127.0.0.1:7897 \
ALL_PROXY=socks5://127.0.0.1:7897 \
./scripts/run-recorder.sh
```

如果不需要上游代理，也可以清空这些环境变量后启动。

## 手动启动

```sh
BRIDLE_HOME_ROOT=~/.bridle-recording \
cargo run -- \
  --listen 127.0.0.1:8787
```

健康检查：

```sh
curl http://127.0.0.1:8787/health
```

正常返回：

```text
ok
```

## 服务自描述 `/help`

recorder 提供机器可读的帮助接口：

```sh
curl -s http://127.0.0.1:8787/help | jq .
```

它相当于 HTTP 服务的 `--help`，返回当前运行实例的 profile、录制与 mock 路径、会话 header 优先级，以及 `list_testsets` 等操作的 JSON Schema。路径均为相对路径，使用方可以基于实际 recorder origin 拼接；返回内容不会暴露 profile 的真实 upstream 或认证信息。

常见发现流程：

1. 请求 `GET /help`，读取 `active_profiles` 和 `operations`。
2. 从 `operations` 中按 `name` 查找 `record_proxy_request`、`list_testsets` 或 `mock_replay_request`。
3. 按操作中的 `http` 路径和 `input_schema` / `output_schema` 发起请求。

## 配置 Codex HTTP 录制

安装 profile 注册表条目（已有配置会被覆盖）：

```sh
./scripts/setup-profiles.sh
```

然后启动 Codex：

```sh
./scripts/run-recorder.sh
./scripts/run-agent.sh codex-http
```

`setup-profiles.sh` 会在 `~/.bridle-recording/codex-http/` 下生成
`bridle-profile.json`。注册表条目里的 `agent_home` 默认指向你自己的
`~/.codex`：不再复制 agent home，也不再复制 `auth.json`，登录态直接用你自己
的。

`run-agent.sh` 是纯通用启动器，不感知具体 agent：它只读取注册表条目，把
`{{recorder_base_url}}` / `{{agent_home}}` 替换成实际值，然后导出
`launch.env` 里的环境变量、按 `launch.args` 里的命令行参数集合执行
`command`。`codex-http/bridle-profile.json` 里声明了 `CODEX_HOME` 指向你的真实 `~/.codex`、
`NO_PROXY=127.0.0.1,localhost`，以及注入 `recorder-openai-http` provider 和
`http://127.0.0.1:8787/codex-http` 的 `--config` 参数。它不修改
`~/.codex/config.toml`；即使 Codex 更新了你 home 下的文件，通过脚本启动的
模型流量仍会经过 recorder。

需要改 recorder 地址或自己的 home 路径时，直接编辑
`~/.bridle-recording/codex-http/bridle-profile.json`，不需要改任何脚本。

`NO_PROXY` 很重要，它保证客户端访问本机 recorder 时不会再次绕到系统代理。

## 配置 WebSocket 录制

如果需要 WebSocket profile：

```sh
./scripts/run-agent.sh codex-websocket
```

`codex-websocket` 与 `codex-http` 两个目录里的配置区别只有 recorder 入口
（`/codex-websocket`）和 WebSocket 特性开关；`agent_home` 同样指向你自己的
`~/.codex`。

## 配置 Claude Code 录制

Claude profile 直接复用用户已有的 `~/.claude/settings.json`，不需要复制
profile 或认证令牌。先启动 recorder，再通过统一启动器启动 Claude Code：

```sh
./scripts/run-recorder.sh
./scripts/run-agent.sh claude
```

这条链路的配置职责如下：

- recorder 启动时自动发现 `~/.claude/settings.json`，从 `env.ANTHROPIC_BASE_URL` 读取真实上游；没有配置时使用 `https://api.anthropic.com`。
- `run-agent.sh claude` 读取 `claude/bridle-profile.json`，仍让 Claude Code 正常加载原始用户 settings，只把 `ANTHROPIC_BASE_URL` 覆盖为 `http://127.0.0.1:8787/claude`。
- `ANTHROPIC_AUTH_TOKEN` 或 `ANTHROPIC_API_KEY` 仍由 Claude Code 自己读取并形成认证 header。recorder 不提取或使用该凭据，只原样转发和录制该 header。
- recorder 使用 Claude Code 自带的 `x-claude-code-session-id` 对录制分组，无需增加自定义 header。

如果 Claude settings 不在默认位置，可以在启动 recorder 时指定：

```sh
BRIDLE_CLAUDE_SETTINGS_PATH=/path/to/settings.json ./scripts/run-recorder.sh
```

Claude profile 在 recorder 启动时发现，因此修改 settings 后需要重启 recorder。

## Profile 路由

recorder 通过路径前缀区分 profile。

```text
/{profile}/...
```

当前常用入口：

```text
http://127.0.0.1:8787/codex-http
http://127.0.0.1:8787/codex-websocket
http://127.0.0.1:8787/claude
```

OpenAI Responses API 客户端通常会请求：

```text
POST /responses
```

如果 base URL 配成：

```text
http://127.0.0.1:8787/codex-http
```

实际到达 recorder 的地址就是：

```text
POST http://127.0.0.1:8787/codex-http/responses
```

## 会话识别

recorder 会使用会话 header 识别 session。默认 header 来自项目配置中的默认值，也可以启动时覆盖：

```sh
RECORDER_SESSION_HEADER=x-bridle-session-id ./scripts/run-recorder.sh
```

如果请求没有可用会话标识，recorder 会把它归入 unknown 会话。观测页面默认不展示 unknown 会话。

## 透明代理注意事项

- 不要期待 recorder 在在线链路中替你修正请求格式。
- 不要依赖在线录制阶段做脱敏或字段裁剪。
- 上游认证仍由客户端配置负责，recorder 只负责转发和旁路保存。
- 如果某个服务必须通过改写真实流量才能工作，应放到离线派生流程处理，而不是放进代理主链路。

## 常见问题

**访问 recorder 失败**

确认 recorder 已启动，并检查端口：

```sh
curl http://127.0.0.1:8787/health
```

**Codex 请求没有进入 recorder**

确认通过 `./scripts/run-agent.sh codex-http` 启动，并检查
`~/.bridle-recording/codex-http/bridle-profile.json` 中 `recorder_base_url` 是
recorder 地址、`agent_home` 指向你自己的 Codex home。也可以加
`BRIDLE_LAUNCH_DEBUG=1` 查看实际注入的地址。

**Claude Code 请求没有进入 recorder**

确认通过 `./scripts/run-agent.sh claude` 启动，并访问 `/api/profiles` 检查
是否包含 `claude`。如果刚修改 Claude settings，需要重启 recorder。

**请求被系统代理绕走**

确认启动 Codex 时设置了：

```sh
NO_PROXY=127.0.0.1,localhost
no_proxy=127.0.0.1,localhost
```

**上游无法访问**

如果本机需要代理访问上游模型服务，确认 `HTTP_PROXY`、`HTTPS_PROXY` 或 `ALL_PROXY` 指向正确的本机代理端口。
