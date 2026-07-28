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
  "bot_name": "你的机器人昵称"        // 群聊 @我 判定用
}
```

```bash
python3 app.py
```

打开 `http://127.0.0.1:9000/`（管理后台在 `/admin`），
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

**④ 媒体 URL 会过期。** 语聚给的图片/视频/语音是有时效的直连 URL，
过期后原件再也拉不回来。`media.keep_days` 控制本地留存，按需调大。

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
| 图片/视频/语音自动下载与内联展示 | ✅ |
| 内部同事 / 外部客户 区分 | ✅ 由 `user_type` 判定 |
| 群聊 @我 检测 | ⚠️ 文本匹配（语聚报文无结构化 `at_list`） |
| **发送消息** | ❌ **未接入** —— `send_text()` 是明确失败的桩，工作台目前只读 |
| 通讯录 / 群成员列表 | ❌ webhook 模式没有这类接口，显示名靠推送累积 |
| 历史消息回溯 | ❌ 语聚只推新消息，工作台从启用那一刻开始攒 |

要接发送通道，实现 `app.py` 的 `send_text(conversation_id, msg)` 即可，
上层（工作流、前端）无需改动。`jjy.raw_chat_id()` 可把内部 session id
还原成语聚的原始 `chat_id`。

---

## 设计取舍

- **SQLite + WAL 持久化**，重启不丢历史。
- **游标分页**（`before`/`after` + `limit`，Discord 风格），前台按需加载，不做 offset。
- **SSE 实时推送**（`GET /gw/events`），不轮询；断线重连用 `after=<last_seq>` 补差。
- **未读数走显式 `POST /gw/read`**（Chatwoot 收件箱模型），而不是"拉过就算已读"。
- **会话 ID 用 `R:`/`S:` 前缀包住语聚的 `chat_id`**，群聊/私聊一眼可分，
  且能无损还原回原始 id。
