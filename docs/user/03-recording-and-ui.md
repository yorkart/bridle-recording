# 录制、页面使用

本页说明如何生成录制、浏览会话，以及把确认后的会话保存为测试集。

## 生成一次录制

1. 启动 recorder：

```sh
./scripts/run-recorder.sh
```

2. 启动已配置好的 Codex：

```sh
./scripts/run-codex-http.sh
```

3. 在 Codex 中发起一次普通交互。

交互完成后，录制会写入：

```text
~/.bridle-recording/<profile>/recordings/<session_id>/
```

例如：

```text
~/.bridle-recording/codex-http/recordings/019f2a24-f268-7eb0-8518-10375ab58b97/
```

## 打开观测页面

浏览器访问：

```text
http://127.0.0.1:8787/ui
```

页面会展示可用 profile 和已录制 session。unknown 会话不会展示。

## 页面信息结构

观测页面按照会话和 turn 组织信息，适合像聊天记录一样查看模型交互过程。

常见区域：

- Session 列表：展示录制会话、更新时间、请求数量等摘要。
- Turn 视图：按用户输入聚合一轮或多轮模型请求。
- 模型请求详情：展示每次请求的模型、耗时、状态码、stream 信息和 token usage。
- Prompt：展示系统提示词、用户输入、环境上下文等 prompt 结构。
- Tools：展示工具定义、tool call 参数和返回内容。
- Metadata：展示请求和响应元信息、SSE 事件统计、原始文件位置等。

Prompt 默认展示摘要内容。需要完整排查时，可以在页面中展开完整文本；看完后也可以收起回到摘要视图。

Tools 和 Metadata 中的 JSON 会以语法高亮方式展示，便于复制、对比和排查。

## 推荐检查顺序

1. 先看 session 是否是本次操作产生的。
2. 再看 turn 中的用户输入是否符合预期。
3. 打开对应模型请求，检查 prompt 是否包含需要的系统提示词和上下文。
4. 检查 tools 列表是否符合本次任务。
5. 检查 tool call 参数和返回内容是否完整。
6. 检查响应状态码、SSE 事件和输出内容是否正常。

这套顺序接近常见模型观测工具的使用习惯：先定位会话，再定位 turn，最后进入单次请求细节。

## 保存为测试集

当一个录制会话确认可用于回归测试时，可以在页面中保存为测试集。

保存后的数据会写入当前 Git 仓库：

```text
testsets/<profile>/<first_user_input_sha256>/
```

目录中包含：

```text
testset.json
raw/<source_session_id>/
```

`raw/` 下保存的是录制内容副本；Header 会按测试集安全策略处理，body、SSE 和 WebSocket 数据按原始字节复制。源录制不会被修改，`request_match.json`、`response_rewrite.json` 等 mock 派生文件也不会混入副本。

## 唯一键和替换

测试集以第一个用户输入作为业务唯一值，并使用该输入的 sha256 作为目录 id。

如果保存时发现相同 profile 下已经存在同一个第一个用户输入，页面或接口会提示需要确认替换。确认替换后，旧测试集目录会被新保存内容替换。

## 当前阶段限制

当前阶段保存的是原始测试集，还没有执行敏感词过滤、tool 裁剪或 skill 裁剪。

后续剪裁模式会作为离线派生流程实现：

```text
原始录制 -> 页面剪裁 -> 剪裁后测试集 -> mock 服务
```

这意味着原始录制仍然保留为事实来源，剪裁结果是可提交、可回放的派生产物。

## 排查建议

**页面没有 session**

- 确认 recorder 正在运行。
- 确认客户端 base URL 指向 recorder。
- 确认当前请求带有可识别的 session 信息。
- unknown 会话默认不展示。

**Prompt 看起来不完整**

- 打开请求详情中的 Prompt 选项卡。
- 使用展开功能查看完整内容。
- 注意系统提示词通常来自 request body 的 `instructions` 字段。

**Tools 为空**

- 确认本次请求是否实际携带 tools。
- 有些请求可能只是纯文本模型调用，不一定包含工具定义或 tool call。

**保存为测试集冲突**

- 说明相同 profile 下已经存在同一个第一个用户输入。
- 如果这是同一个场景的更新版本，可以选择替换。
- 如果不是同一个场景，建议换一个更明确的首轮用户输入后重新录制。
