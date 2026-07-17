# 代理使用

本页说明如何启动 bridle-recording，并让 Codex 或 OpenAI 兼容客户端通过它访问上游模型服务。

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

## 配置 Codex HTTP 录制

第一次使用时，先把 profile 模板复制到本机运行目录，并复制 Codex 登录态：

```sh
mkdir -p ~/.bridle-recording
cp -R agent-home/codex-http ~/.bridle-recording/
cp ~/.codex/auth.json ~/.bridle-recording/codex-http/auth.json
```

然后启动 Codex：

```sh
./scripts/run-codex-http.sh
```

这个脚本会设置：

```sh
BRIDLE_AGENT_HOME=~/.bridle-recording/codex-http
CODEX_HOME=~/.bridle-recording/codex-http
NO_PROXY=127.0.0.1,localhost
```

脚本还会在每次启动时通过 Codex 命令行配置显式注入
`recorder-openai-http` provider 和
`http://127.0.0.1:8787/codex-http`。即使 Codex 更新了该 home 下的
`config.toml`，通过脚本启动的模型流量仍会经过 recorder。

`NO_PROXY` 很重要，它保证客户端访问本机 recorder 时不会再次绕到系统代理。

## 配置 WebSocket 录制

如果需要 WebSocket profile：

```sh
mkdir -p ~/.bridle-recording
cp -R agent-home/codex-websocket ~/.bridle-recording/
cp ~/.codex/auth.json ~/.bridle-recording/codex-websocket/auth.json

./scripts/run-codex-websocket.sh
```

## Profile 路由

recorder 通过路径前缀区分 profile。

```text
/{profile}/...
```

当前常用入口：

```text
http://127.0.0.1:8787/codex-http
http://127.0.0.1:8787/codex-websocket
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

确认 `CODEX_HOME` 指向 `~/.bridle-recording/codex-http`，并确认 profile 的 `config.toml` 中 `base_url` 是 recorder 地址。

**请求被系统代理绕走**

确认启动 Codex 时设置了：

```sh
NO_PROXY=127.0.0.1,localhost
no_proxy=127.0.0.1,localhost
```

**上游无法访问**

如果本机需要代理访问上游模型服务，确认 `HTTP_PROXY`、`HTTPS_PROXY` 或 `ALL_PROXY` 指向正确的本机代理端口。
