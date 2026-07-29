# agora_workflow

把 **语聚AI（集简云）聚合对话** 的消息推送落地成一个可看、可查、可自动化的会话工作台。

语聚汇聚了抖音企业号、企微代运营、微信公众号、微信客服、小红书私信/评论、钉钉、
快手、飞书、QQ、视频号等渠道的私信与评论。本程序订阅它的「聚合对话有新消息」
webhook，把各渠道报文归一化后入库，并提供实时会话界面与工作流引擎。

**零第三方依赖**，纯 Python 3 标准库。有 `python3` 就能跑。

```
语聚(集简云)
   │  POST  webhook
   ▼
app.py  ──jjy.py 归一化──▶  SQLite(store.py)  ──SSE──▶  会话工作台(index.html)
   │                                            └────▶  管理后台(admin.html)
   └──▶ 工作流引擎(workflow.py)  条件匹配 → 转发 / 调接口
```

---

## 快速开始

```bash
cp config.example.json config.json
```

编辑 `config.json`，至少填两项：

```jsonc
"jjy": {
  "enabled": true,
  "allow_company": ["你的公司ID"],   // 语聚控制台里能看到
  "bot_name": "你的机器人昵称",       // 群聊 @我 判定用
  "api_key": ""                     // 要发消息才需要，见下面「发送」一节
}
```

```bash
python3 app.py
```

打开 `http://127.0.0.1:9000/`（管理后台在 `/admin`）。

配置也可以在**管理后台 → 系统设置**里改，保存即写回 `config.json`。
大部分字段实时生效；标了「改动需重启」的（监听地址/端口、存储上限、去重窗口）
需要重启进程。


然后在语聚后台把回调地址填成 `http(s)://你的地址/gw/jjy-hook`，订阅「当聚合对话有新消息时」。

---

## 目录结构

```
app.py                后端：webhook 接入 + 入库 + 工作流触发 + 健康检查 + REST API + SSE
jjy.py                语聚报文归一化（各渠道差异都收敛在这里）
store.py              会话/消息持久化层（SQLite + WAL，游标分页、服务端去重）
workflow.py           工作流引擎（触发条件匹配 + 动作执行 + 限流/重试/运行记录）
index.html            会话工作台（首页 /）
admin.html            管理后台（工作流 / 运行记录 / 状态监控 / 系统设置，/admin）
config.example.json   配置模板
```

---

## 关于语聚的官方文档

**官方 OpenAPI 文档和实际推送对不上**，照文档写解析代码会全部取空。
实测差异（`jjy.py` 里按实际报文实现，并对两种形态都做了兼容）：

| 文档 | 实际推送 |
|---|---|
| `data.chat` / `data.message` / `data.user` / `data.source` | **`data.push_data.*`**，中间多一层 |
| `user_type` 枚举 1–6 | 实际出现 **7**（企微内部成员） |
| `source` 无 `rule_id` | 有，且类型不稳定：`"1557"` 和 `1557` 都出现过 |
| `source_account_id` 标 required | 实际是 `null` |
| `message_trigger_type: 1` = "API" | 实际 `message_trigger_name` 是「AI智能体回复」 |
| `message_type` 枚举跳过了 8 | **8 = 文件消息**，文档里根本没有这个号 |

**两种消息的 `message_content.text` 是一段 JSON 字符串，要再解一层**（文档只说是 text）：

```jsonc
// type 8 文件
{"text": "{\"fileUrl\":\"https://...\",\"name\":\"a.log\",\"size\":1036562}"}
// type 12 合并转发的聊天记录
{"text": "{\"chatHistoryList\":[...],\"title\":\"A和B的聊天记录\"}"}
```

**还有一个文档里没有的事件类型：`chat_finish`**（对话结束），和 `ai_assistant_receives_msg`
走同一个回调地址。本程序按 `event_type` 区分，计入 `ignored` 而不是当成解析失败。

另外两个实测结论：

- **`message_create_time` 是毫秒**，不是秒。
- **incoming / outgoing 可配对**：同一轮问答的 `message_id` 是同一个 UUID，
  incoming 那条末尾多一个 `i`。去掉后缀即可关联提问与回复。

---

## 几个必须知道的坑

**① 回环。** 语聚会把机器人自己的回复**原样推回来**（`message_forward_type: "outgoing"`）。
存库要存（不然对话不完整），但绝不能拿它触发工作流，否则自问自答无限循环。
本程序把 outgoing 标成 `is_self_msg=1`，`on_message()` 见到就只入库不触发。

**② 这个 webhook 没有签名。** 官方 spec 里 `security: []`，没有任何签名或 token。
`jjy.allow_company` 白名单是**唯一**的身份校验：

- 公网部署**必须**配置 `allow_company`
- 回调路径本身建议带一段随机串（靠反向代理转发到 `/gw/jjy-hook`）
- 校验失败一律回 `200` 而不是 `403` —— 不给扫描者任何"这里有东西"的反馈

**③ 必须立刻返回 200。** 语聚等不到响应就会重推。本程序在 HTTP 线程里只做
白名单校验 + `event_id` 去重 + 入队，重活全在 worker 线程，不会因为处理慢而收到重复消息。

**④ 媒体默认不下载。** 语聚给的图片/文件是**公开的 S3 对象**——不带签名参数、
无鉴权直接 200，实测不会过期。所以默认 `media.mode = "link"`：不落盘，
`/media/<id>` 用 302 跳到原始 URL，前端 `<img src="media/xxx">` 照常出图。

省掉了下载线程、磁盘占用和失败重试。想留档（比如担心对方删文件）就改成
`"mode": "download"`，同一个 `/media/<id>` 会改吐本地文件，前端无感知。

用 302 而不是 301，就是为了让这个切换能反悔——301 会被浏览器永久缓存。

---

## 发送消息

出站走语聚的 `POST /v1/openapi/aggregate/message/send`。填上 apiKey 就能用：

```jsonc
"jjy": {
  "api_key": "G-xxxxxxxx",   // 语聚「应用助手 → 集成配置」页获取
  "send_qps": 1,             // 全局发送速率
  "send_retry": 2
}
```

管理后台 →「系统设置 → 出站发送」也能填，旁边的 **校验 apiKey** 会拿一个不存在的
会话试发一次：认证过得去就会走到参数校验才失败，所以这条请求验得了凭证、
又发不出任何消息。

**⚠️ 填上之后工作台就是可写的**：手动发消息会真的发到客户那边，工作流的
「转发 / 回复」动作也一样。如果语聚后台已经配了 AI 自动回复规则，
**同一条消息会有两个机器人抢答** —— 开之前先去语聚把规则确认一遍。

几个实现上的取舍：

- **接口的响应字段是大写开头的**：`{"Code": 2000, "Data": ..., "Msg": ...}`，
  不是常见的 `code/data/msg`。成功是 `Code == 2000`。
- **`Code=4000` 不重试**（参数错、会话已关闭，重试还是一样的结果）；
  只有 **429 / 5xx / 网络错误**才退避重试。401/402 直接翻译成人话返回。
- **全局串行限流**。语聚 429 的文案是 "Too Many Requests in one second"，
  限的是每秒。工作流一次转发给 N 个目标是个 for 循环，不限流必撞 429。
- **apiKey 走 query string**（官方就这么设计的），所以代码里任何日志都不打完整 URL，
  `GET /gw/config` 返回给前端时也打码成 `********`；前端原样回传这个占位符
  就表示"不修改"。
- 发出去的消息**不在本地补录**，等语聚把它当 `outgoing` 推回来再入库
  （和另一个机器人的回复走同一条路），避免和回声重复。

发别的类型直接调底层函数，`message_content` 原样透传：

```python
jjy_send(sid, 9, {"url": "https://.../a.png"})   # 9=图片 8=文件 3=语音
```

图片/文件要的是**公网可访问的直链** —— 本项目的 `/media/<id>` 在反代鉴权后面，
不能直接给语聚，要用 `STORE.get_media_cdn(mid)["url"]` 取语聚原始那个。

---

## 反向代理部署（nginx）

```nginx
# webhook 回调：路径带随机串，不走站点原有的鉴权
location = /hook/<一段随机串> {
    auth_request off;                 # 站点若有 SSO，这行必须加，否则第三方回调会被跳登录页
    proxy_pass http://127.0.0.1:9000/gw/jjy-hook;
    proxy_set_header X-Real-IP $remote_addr;
}

# 工作台：保留站点原有鉴权，只让内部人看（里面是客户对话原文）
location = /workflow { return 301 /workflow/; }
location ^~ /workflow/ {
    proxy_pass http://127.0.0.1:9000/;
    proxy_set_header Host $host;

    proxy_buffering off;              # SSE 必须，否则消息不实时刷新
    proxy_read_timeout 3600s;         # SSE 是长连接，默认 60s 会被掐断
}
```

前端用的是**相对路径**，所以挂在根路径或子路径都能用。
但 `return 301` 不能省 —— 不带尾斜杠访问时相对路径会解析错，页面白屏。

`listen_addr` 默认 `127.0.0.1`，只绑本机，公网唯一入口是反向代理。

---

## 当前限制

| 能力 | 状态 |
|---|---|
| 收消息、入库、实时展示、历史检索 | ✅ |
| 图片 / 文件 / 视频 / 语音 展示 | ✅ 默认直链，可切换为下载归档 |
| 合并转发的聊天记录（type 12） | ✅ 展开成多行文本，原始结构存进 `rich` |
| 内部同事 / 外部客户 区分 | ✅ 由 `user_type` 判定 |
| 群聊 @我 检测 | ⚠️ 文本匹配（语聚报文无结构化 `at_list`） |
| 发送文本消息 | ✅ 填 `jjy.api_key` 后可用，工作台可写 |
| 发送图片 / 文件 / 语音 | ⚠️ 底层 `jjy_send()` 支持，但前端没做上传入口 |
| 引用回复 / @人 | ⚠️ 参数已透传，**只有企微代运营渠道支持**，其他渠道语聚会忽略 |
| 通讯录 / 群成员列表 | ❌ webhook 模式没有这类接口，显示名靠推送累积 |
| 历史消息回溯 | ❌ 语聚只推新消息，工作台从启用那一刻开始攒 |

---

## 设计取舍

- **SQLite + WAL 持久化**，重启不丢历史。
- **游标分页**（`before`/`after` + `limit`，Discord 风格），前台按需加载，不做 offset。
- **SSE 实时推送**（`GET /gw/events`），不轮询；断线重连用 `after=<last_seq>` 补差。
- **未读数走显式 `POST /gw/read`**（Chatwoot 收件箱模型），而不是"拉过就算已读"。
- **会话 ID 用 `R:`/`S:` 前缀包住语聚的 `chat_id`**，群聊/私聊一眼可分，
  且能无损还原回原始 id。
