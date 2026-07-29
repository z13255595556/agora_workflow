# -*- coding: utf-8 -*-
"""
jjy.py — 语聚AI(集简云)聚合对话 webhook 报文 -> 内部归一化消息
=============================================================
对接事件：event_type = ai_assistant_receives_msg

⚠️ 官方 OpenAPI 文档和实际推送**对不上**，本模块按实测报文写：

  文档写的                     实际推送
  data.chat                    data.push_data.chat
  data.message                 data.push_data.message
  data.user                    data.push_data.user
  data.source                  data.push_data.source
  data.channel_num             data.push_data.channel_num

  user_type 枚举 1..6          实际出现了 7(企微内部成员)
  source 无 rule_id            有，且类型不稳定："1557" 和 1557 都出现过
  source_account_id required   实际是 null

`_pick()` 两层都试，文档哪天改回去了也不会挂。

会话 ID 约定
------------
沿用 app.py 既有的 "R: = 群聊 / S: = 私聊" 前缀，把语聚的 chat_id 包一层：

    群聊  R:<chat_id>        私聊  S:<chat_id>

这样 store_message() 里的 `sid.startswith("R:")` 一行都不用改；
将来要调语聚的发消息接口，`raw_chat_id()` 把前缀剥掉就是它认的 chat_id。
"""

import json
import time
from urllib.parse import unquote

# ---- 内部 content_type，取值必须和 app.py 的 CT_* 保持一致 ----
MSG_TEXT = 2
CT_IMAGE = 101; CT_FILE = 102; CT_VIDEO = 103; CT_VOICE = 16
CT_LINK  = 13;  CT_LOCATION = 6; CT_CARD = 41
PUSH_CHAT = 100

# ---- 语聚 message_type -> (内部 content_type, 媒体种类|None) ----
# 媒体种类非 None 的会进媒体下载队列。语聚给的都是可直连的 https URL。
JJY_TYPE = {
    1:  (MSG_TEXT,    None),   # 系统消息
    2:  (MSG_TEXT,    None),   # 文本
    3:  (CT_VOICE,    "voice"),
    4:  (CT_CARD,     None),   # 名片
    5:  (CT_LOCATION, None),
    6:  (CT_VIDEO,    "video"),
    7:  (CT_LINK,     None),   # 卡片/链接
    8:  (CT_FILE,     "file"),  # ⚠️ 官方文档的枚举里没有 8，实测就是文件消息
    9:  (CT_IMAGE,    "image"),
    10: (MSG_TEXT,    None),   # 抖音进线
    11: (MSG_TEXT,    None),   # 广告留资
    12: (MSG_TEXT,    None),   # 历史消息
}

# 富文本要留档的字段，对齐 app.py 的 RICH_FIELDS
RICH_KEYS = ("title", "cover", "link", "url", "address", "name",
             "latitude", "longitude", "avatar", "nickname", "text")


def _unwrap(mtype, mc):
    """message_content.text 有时是**一段 JSON 字符串**，要再解一层。

        8  -> {"fileUrl": "...", "name": "xx.log", "size": 1036562}
        12 -> {"chatHistoryList": [...], "title": "A和B的聊天记录"}
        引用回复(任何类型都可能，实测 type 2 就有):
           -> {"text": "test", "quote_message": {"quote_message_id": "...",
                "sender": "...", "content": "...", "quote_message_type": "text"}}

    这几条都不在官方文档里(8 号连枚举都没有)，是从真实推送里扒出来的。
    不解这一层的话，界面上那条消息显示的就是这一整串 JSON 原文。

    ⚠️ 8/12 按类型确定；引用则**按有没有 quote_message 判定**，不能见到
    type 2 的 text 以 { 开头就解 —— 用户真发一段 JSON 文本会被拆掉。
    解析失败一律原样返回，退化成纯文本展示，不会丢消息。
    """
    if not isinstance(mc, dict):
        return mc
    t = mc.get("text")
    if not isinstance(t, str) or not t.lstrip().startswith("{"):
        return mc
    try:
        inner = json.loads(t)
    except Exception:
        return mc
    if not isinstance(inner, dict):
        return mc
    if mtype in (8, 12) or "quote_message" in inner:
        return dict(mc, **inner)
    return mc


def _history_text(mc):
    """合并转发的聊天记录 -> 多行可读文本。"""
    lst = mc.get("chatHistoryList") or []
    head = "[聊天记录] " + (mc.get("title") or "")
    lines = []
    for it in lst[:50]:                      # 只展开前 50 条，避免一条消息撑爆气泡
        who = it.get("senderName") or ""
        corp = it.get("corpName") or ""
        body = ((it.get("message") or {}).get("content") or "").replace("\n", " ")
        lines.append("%s%s：%s" % (who, "(%s)" % corp if corp else "", body))
    if len(lst) > 50:
        lines.append("…… 共 %d 条" % len(lst))
    return "\n".join([head.strip()] + lines)


def _pick(data, key):
    """兼容 data.push_data.X(实际) 和 data.X(文档)。"""
    pd = data.get("push_data")
    if isinstance(pd, dict) and key in pd:
        return pd.get(key)
    return data.get(key)


def raw_chat_id(session_id):
    """R:/S: 前缀的内部 session_id -> 语聚原始 chat_id(发消息接口要用)。"""
    s = str(session_id or "")
    return s[2:] if s[:2] in ("R:", "S:") else s


def _as_text(mtype, mc):
    """message_content(各类型结构不同) -> 一行可读文本。"""
    if not isinstance(mc, dict):
        return str(mc or "")
    if mtype == 3:                                    # 语音：语聚已带转写
        t = mc.get("text") or ""
        return t or "[语音 %s 秒]" % (mc.get("duration") or "?")
    if mtype == 4:
        return "[名片] %s" % (mc.get("name") or "")
    if mtype == 5:
        return "[位置] %s %s" % (mc.get("name") or "", mc.get("address") or "")
    if mtype == 6:
        return "[视频]"
    if mtype == 7:
        return "[卡片] %s" % (mc.get("title") or "")
    if mtype == 8:
        return "[文件] %s" % (mc.get("name") or "")
    if mtype == 9:
        return ""                                     # 图片正文留空，前端渲染媒体
    if mtype == 12:
        return _history_text(mc)
    if mtype == 11:
        # 广告留资：小红书/抖音两套结构都可能，捡关键字段拼一行
        bits = [mc.get(k) for k in ("advertiser_name", "campaign_name",
                                    "phone_num", "wechat", "remark", "leads_tag")]
        return "[广告留资] " + " ".join(str(b) for b in bits if b)
    return mc.get("text") or mc.get("title") or ""


def _media_of(kind, mc):
    """语聚的媒体 URL -> app.py 媒体条目。全部走 direct 直连下载。"""
    url = {"image": mc.get("url"),
           "video": mc.get("video_url"),
           "voice": mc.get("voice_url"),
           "file":  mc.get("fileUrl")}.get(kind) or ""
    if not url:
        return None
    ext = {"image": "jpg", "video": "mp4", "voice": "mp3", "file": "bin"}[kind]
    if kind == "file" and mc.get("name"):
        return {"kind": kind, "file_name": mc["name"],
                "size": int(mc.get("size") or 0), "md5": "",
                "cdn_type": 0, "cdn": {"url": url, "direct": True}, "file_type": 5}
    # 文件名取 URL 末段。必须 unquote —— S3 路径里的中文是百分号编码的，
    # 不解码会得到 image_%E4%BC%81%E4%B8%9A... 这种没法看的名字。
    tail = unquote(url.rsplit("/", 1)[-1].split("?")[0])[:60] or ("x." + ext)
    if "." not in tail:
        tail += "." + ext
    return {"kind": kind, "file_name": "%s_%s" % (kind, tail),
            "size": int(mc.get("size") or 0), "md5": "",
            "cdn_type": 0, "cdn": {"url": url, "direct": True},
            "file_type": 1}


def normalize(body, bot_name=""):
    """语聚 webhook body -> 内部归一化消息 dict。

    非本事件 / 缺关键字段时返回 None，调用方直接丢弃。
    bot_name 用于群聊 @我 判定(语聚报文没有结构化 at_list，只能文本匹配)。
    """
    if not isinstance(body, dict):
        return None
    if body.get("event_type") != "ai_assistant_receives_msg":
        return None

    data = body.get("data") or {}
    chat = _pick(data, "chat") or {}
    msg  = _pick(data, "message") or {}
    user = _pick(data, "user") or {}
    src  = _pick(data, "source") or {}

    chat_id = str(chat.get("chat_id") or "")
    if not chat_id:
        return None

    is_group = (chat.get("chat_type") == "群聊")
    sid = ("R:" if is_group else "S:") + chat_id

    mtype = int(msg.get("message_type") or 0)
    ct, kind = JJY_TYPE.get(mtype, (MSG_TEXT, None))
    mc = msg.get("message_content")
    if not isinstance(mc, dict):
        mc = {"text": str(mc or "")}
    mc = _unwrap(mtype, mc)          # type 8/12 的 text 里还套着一层 JSON
    content = _as_text(mtype, mc)

    # outgoing = 我方(人工客服/AI智能体)发出的，展示在右侧。
    # ⚠️ 这也是回环防线：语聚会把机器人自己的回复原样推回来，
    #    存库要存(不然对话不完整)，但绝不能拿它再去触发工作流。
    forward = msg.get("message_forward_type") or ""
    is_self = 1 if forward == "outgoing" else 0

    # user_type: 1=外部客户  7=企微内部成员  2=人工客服  3=AI  4=系统  5=AI流程
    # 文档只写到 6，7 是实测出来的(头像域名 wework.qpic.cn 即企微内部)。
    ut = int(user.get("user_type") or 0)
    external = 1 if ut == 1 else (0 if ut in (2, 3, 4, 5, 7) else None)

    # 语聚没给结构化 at_list，只能拿机器人昵称在正文里找
    at_me = 1 if (is_group and bot_name and isinstance(content, str)
                  and ("@" + bot_name) in content) else 0

    # message_create_time 是**毫秒**
    ts = int(msg.get("message_create_time") or 0) // 1000 or int(time.time())

    out = {
        "type": PUSH_CHAT,
        "msg_type": ct,
        "user_id": sid,
        "sender": str(user.get("user_id") or ""),
        "sender_name": user.get("user_name") or "",
        "content": content,
        "time_stamp": ts,
        "msg_id": str(msg.get("message_id") or ""),
        "is_self_msg": is_self,
        "at_me": at_me,
        "sender_external": external,
        # --- 以下是语聚特有的，存档用 ---
        "jjy": {
            "mt": mtype,                                # 语聚原始 message_type
            "chat_id": chat_id,
            "chat_title": chat.get("chat_title") or "",
            "channel_num": _pick(data, "channel_num"),
            "trigger_type": msg.get("message_trigger_type"),
            "trigger_name": msg.get("message_trigger_name") or "",
            "forward": forward,
            "user_type": ut,
            "rule_id": str(src.get("rule_id") or ""),   # 类型不稳定，一律转字符串
            "source_name": src.get("source_name") or "",
            # room_id/bot_wxid 是**企微群管理接口那套 id**(imRoomId/imBotId)，
            # 和上面的 chat_id 不是一个空间。这是唯一能把两边对上的地方 ——
            # 群列表接口返回的 imRoomId 全靠它映射回本地会话。私聊没有 room_id。
            "room_id": (src.get("source_addition") or {}).get("room_id") or "",
            "bot_id": str((src.get("source_addition") or {}).get("bot_wxid") or ""),
            "bot_name": (src.get("source_addition") or {}).get("bot_name") or "",
            # 渠道原生 id：微信客服/企微代运营等，回连自有身份体系的 join key
            "addition": user.get("user_addition") or {},
        },
    }

    m = _media_of(kind, mc) if kind else None
    if m:
        out["media"] = m

    rich = {k: mc[k] for k in RICH_KEYS if isinstance(mc, dict) and mc.get(k)}
    if mtype == 12 and mc.get("chatHistoryList"):
        out["rich"] = {"title": mc.get("title") or "",
                       "history": mc["chatHistoryList"][:50]}
    elif rich and mtype in (4, 5, 7, 11):
        out["rich"] = rich

    # 引用回复。⚠️ 官方文档里没有这个结构，实测才有：
    #   "message_content": {"text": "开启了吧？", "quote_message": {
    #       "quote_message_id": "...", "sender": "刘雨欣",
    #       "content": "传统降噪是默认开启的", "quote_message_type": "text"}}
    # 不存的话，一条引用回复看起来就是条普通消息，上下文全丢。
    # 任何消息类型都可能带，所以独立于上面那几个 mtype 分支处理。
    q = mc.get("quote_message") if isinstance(mc, dict) else None
    if isinstance(q, dict) and (q.get("content") or q.get("quote_message_id")):
        out.setdefault("rich", {})["quote"] = {
            "id": str(q.get("quote_message_id") or ""),
            "sender": q.get("sender") or "",
            "content": (q.get("content") or "").strip(),
            "mtype": q.get("quote_message_type") or "",
        }

    # 要引用这条消息时, quoteMessageId 该填哪个 —— 报文里有**两个** message_id:
    #   message.message_id          "api_<uuid>i"  ← 我们当 msg_id 用(去重/撤回)
    #   message_content.message_id  32 位 hex      ← 引用块里的 quote_message_id 就是这个格式
    # 实测同一条报文里 quote_message_id 和 message_content.message_id 同款(32位hex)，
    # 和外层那个完全不同 —— 所以引用要用内层的。
    inner_id = str(mc.get("message_id") or "") if isinstance(mc, dict) else ""
    if inner_id:
        out.setdefault("rich", {})["qid"] = inner_id

    return out


def session_title(body):
    """从报文里取会话标题，用于会话列表显示(通讯录接口没了，只能靠推送攒)。"""
    data = body.get("data") or {}
    chat = _pick(data, "chat") or {}
    return chat.get("chat_title") or ""
