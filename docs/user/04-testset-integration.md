# 测试集接入使用

本页面向使用方和集成测试作者，说明如何发现测试集、读取用户输入列表，并通过 mock 服务进行回放。

## 测试集目录

测试集保存在当前 Git 仓库：

```text
testsets/<profile>/<first_user_input_sha256>/
```

每个测试集包含：

```text
testset.json
raw/<source_session_id>/
```

`testset.json` 是轻量索引，供页面、接口和集成测试读取。`raw/` 从原始录制派生：请求体、响应体、SSE、WebSocket 帧和其他文件按原始字节复制；`request_headers.json` 与 `response_headers.json` 保留既有结构和 Header key，但非白名单 Header 的 value 会替换为 `******`。源录制不受导出影响。

## 发现测试集

启动 recorder 后，使用方可以调用：

```text
GET http://127.0.0.1:8787/api/testsets
```

只查看某个 profile：

```text
GET http://127.0.0.1:8787/api/testsets/codex-http
```

示例：

```sh
curl -s http://127.0.0.1:8787/api/testsets/codex-http | jq .
```

返回结构：

```json
{
  "testsets": [
    {
      "profile": "codex-http",
      "id": "ea3cdaacff6a7a4270ec9b69415b2793db6e67978602b59f5588e620d621ef67",
      "source_session_id": "019f2a24-f268-7eb0-8518-10375ab58b97",
      "first_user_input": "list files",
      "user_inputs": ["list files", "hi"],
      "user_input_sha256": "ea3cdaacff6a7a4270ec9b69415b2793db6e67978602b59f5588e620d621ef67",
      "saved_at": "2026-07-04T01:44:27.522Z",
      "source_recording_path": "/Users/yorkart/.bridle-recording/codex-http/recordings/019f2a24-f268-7eb0-8518-10375ab58b97",
      "raw_recording_path": "raw/019f2a24-f268-7eb0-8518-10375ab58b97",
      "testset_path": "/path/to/repo/testsets/codex-http/ea3cdaacff6a7a4270ec9b69415b2793db6e67978602b59f5588e620d621ef67"
    }
  ]
}
```

## 字段说明

`profile`

测试集所属 profile，例如 `codex-http`。

`id`

测试集目录名，当前等于第一个用户输入的 sha256。

`source_session_id`

测试集来源录制会话。

`first_user_input`

测试集业务唯一值。保存测试集时，系统使用它判断是否冲突。

`user_inputs`

该测试集包含的用户输入列表。集成测试通常读取这个字段，把这些输入按顺序喂给被测客户端。

`raw_recording_path`

相对测试集目录的原始录制路径。

`testset_path`

当前仓库中的测试集绝对路径，便于本地工具定位文件。

## 集成测试推荐流程

1. 测试启动 recorder。
2. 调用 `/api/testsets/:profile` 获取可用测试集。
3. 选择目标测试集。
4. 读取 `user_inputs`。
5. 将客户端 base URL 指向 mock 服务。
6. 按 `user_inputs` 顺序驱动客户端执行。
7. mock 服务严格匹配请求，匹配成功才返回录制响应。

## mock 服务地址

mock base URL：

```text
http://127.0.0.1:8787/<profile>/mock
```

例如 `codex-http`：

```text
http://127.0.0.1:8787/codex-http/mock
```

OpenAI Responses API 请求：

```text
POST /responses
```

会到达：

```text
POST http://127.0.0.1:8787/codex-http/mock/responses
```

## 严格匹配行为

mock 服务会根据请求 body 提取用户输入，找到对应 session，然后对请求进行严格匹配校验。

只有满足匹配规则时才返回对应录制响应；否则返回错误。这样可以避免测试在请求结构已经漂移时仍然误判通过。

对使用方来说，这意味着：

- 测试输入应来自 `user_inputs`，不要手写近似文案。
- 被测客户端的模型参数、输入结构和关键请求字段应与录制时保持一致。
- 如果业务 prompt 或工具定义发生预期变更，应重新录制并保存新的测试集。

## Node.js 示例

```js
const recorder = "http://127.0.0.1:8787";
const profile = "codex-http";

const response = await fetch(`${recorder}/api/testsets/${profile}`);
const { testsets } = await response.json();
const target = testsets.find((item) => item.first_user_input === "list files");

if (!target) {
  throw new Error("missing required testset: list files");
}

for (const input of target.user_inputs) {
  await runClientTurn({
    baseUrl: `${recorder}/${profile}/mock`,
    input,
  });
}
```

`runClientTurn` 代表使用方自己的客户端调用逻辑。

## CI 使用建议

- 在 CI job 中先启动 recorder。
- 确认仓库已包含需要的 `testsets/` 目录。
- 测试启动前调用 `/api/testsets` 做一次资产检查。
- 每个测试用例显式声明依赖哪个 `profile` 和 `first_user_input`。
- 当 mock 返回匹配错误时，把错误视为请求契约变更信号，而不是简单重试。

## 更新测试集

当产品逻辑、提示词或工具调用发生预期变化时：

1. 使用真实上游重新录制。
2. 在观测页面检查新会话。
3. 保存为测试集。
4. 如果第一个用户输入相同，确认替换。
5. 提交更新后的 `testsets/` 内容。

当前阶段测试集保存原始数据，不做脱敏处理。提交前请确认仓库策略允许保存这些录制内容。
