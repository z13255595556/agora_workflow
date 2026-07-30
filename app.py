# -*- coding: utf-8 -*-
"""
语聚(集简云)聚合对话 消息网关 (带可视化会话工作台)
==================================================
把语聚聚合对话的 webhook 推送落地成一个可看、可查、可自动化的会话工作台。
语聚汇聚了抖音/企微/公众号/小红书/钉钉/快手/飞书/QQ/视频号等渠道的私信与评论，
本程序订阅它的「聚合对话有新消息」事件，归一化后入库并实时呈现。

一个进程做三件事：
  1. 收 webhook(POST /gw/jjy-hook) -> jjy.py 归一化 -> 入库 + SSE 广播；媒体异步下载
  2. 工作流引擎：按条件触发转发/调接口
  3. 提供页面 + REST API：
       GET /        会话工作台(index.html)
       GET /admin   管理后台(工作流/运行记录/状态监控/系统设置)
       /gw/*        REST API

存储与加载(参考成熟 IM 的设计)：
  * SQLite 持久化(store.py, WAL)——重启不丢历史
  * 消息接口游标分页(before/after + limit, Discord 风格)
  * GET /gw/events 是 SSE 实时推送；断线重连用 after=<last_seq> 补差
  * 未读数走显式 POST /gw/read(Chatwoot 收件箱模型)

出站发送走语聚 OpenAPI(POST /v1/openapi/aggregate/message/send)，见 jjy_send()。
需要在配置里填 `jjy.api_key`(语聚「应用助手 → 集成配置」页获取)，留空则发送不可用。

⚠️ 语聚 webhook 没有签名机制，`jjy.allow_company` 白名单是唯一的身份校验，
   公网部署务必配置，并把服务放在反向代理后面。

依赖：无。纯 Python3 标准库。
"""

import json
import os
import queue
import time
import uuid
import threading
import logging
import urllib.request
import urllib.error
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, quote

import jjy      # 语聚AI(集简云)聚合对话 webhook 报文归一化
from store import Store              # SQLite 会话/消息持久化层
from workflow import WorkflowEngine  # 消息工作流引擎(触发条件->转发/调接口)

# ============================ 路径 & 默认配置 ============================
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
INDEX_FILE  = os.path.join(BASE_DIR, "index.html")   # 会话消息页
ADMIN_FILE  = os.path.join(BASE_DIR, "admin.html")   # 管理后台页

DEFAULT_CONFIG = {
    "listen_addr": "127.0.0.1",           # 监听地址。默认只绑本机, 由反向代理(nginx)对外
    "listen_port": 9000,                  # 网关网页/接口端口
    "jjy": {
        # 语聚(集简云)聚合对话 webhook 入口: POST /gw/jjy-hook
        #
        # ⚠️ 这个 webhook **没有签名机制**(官方 spec 里 security: [])。
        #    allow_company 是唯一的身份校验 —— 留空 = 谁都能往里灌伪造消息。
        #    公网部署必须填, 并且回调路径本身也建议带一段随机串。
        "enabled": False,
        "allow_company": [],              # 语聚控制台里的公司 ID, 例: ["your-company-id"]
        "bot_name": "",                   # 托管机器人昵称。群聊 @我 判定靠文本匹配 ——
                                          # 语聚报文里没有结构化的 at_list
        "dedup_max": 4000,                # event_id 去重窗口(条)。语聚重推带同一个 id

        # ---- 出站发送: POST {api_base}/v1/openapi/aggregate/message/send ----
        # apiKey 在语聚「应用助手 → 集成配置」页获取。留空 = 发送不可用(工作台只读)。
        #
        # ⚠️ apiKey 是走 query string 的(官方接口就这么设计)，所以凡是打日志的地方
        #    都不能带完整 url；/gw/config 返回给前端时也做了打码, 见 _public_config()。
        "api_base": "https://chat.jijyun.cn",
        "api_key": "",
        # 语聚的接口分两套 base path, 文档里对 apiKey 的说法也不一样:
        #   /v1/openapi/...            "应用助手集成配置页面获取"   -> api_key
        #   /v1/api/ai/marketing/...   "语聚AI的API接口页面获取"    -> api_key_mkt
        # 是不是同一个 key 官方没说清。留空则回退用 api_key —— 真是同一个就不用填。
        "api_key_mkt": "",
        "send_qps": 1,                    # 全局发送速率(条/秒)。语聚的 429 文案是
                                          # "Too Many Requests in one second", 按秒限流
        "send_retry": 2,                  # 429/5xx/网络错误的重试次数(业务失败不重试)
        "send_timeout": 15
    },
    "storage": {
        "db_file": "gateway.db",          # 消息数据库(SQLite), 相对路径按本目录解析
        "max_msg_per_session": 5000,      # 每会话最多保留条数, 0=不限制(改动需重启)
        # 工作流运行明细保留条数。默认 0=全部保留 —— 运行记录是排查现场，
        # 裁剪是全局的(不分工作流)，一条高频工作流刷满就会把低频那条的历史挤没。
        # 表涨得受不了了再设个上限。改动需重启
        "max_workflow_runs": 0,
        # AI 回复上下文的保留天数。200 行/人 那个上限管不了"人越来越多"：
        # 一个客户问过一次就再不出现，那两行会永远躺着。按天过期是唯一会让
        # ai_context 缩回去的机制。0=不过期
        "ai_context_days": 7
    },
    "media": {
        "enabled": True,
        # link     = 不下载, /media/<id> 302 跳到语聚给的原始 URL(默认)
        # download = 下载归档到本地。语聚给的是公开 S3 对象、不带签名也不过期,
        #            所以 link 足够用; 只有"对方可能删文件、你要留证"时才需要 download
        "mode": "link",
        "dir": "media",                   # 落盘目录(仅 download 模式), 相对路径按本目录解析
        "image_auto": True,               # 图片自动下载并在会话里内联展示
        "auto_download_mb": 50,           # ≤此大小的自动下载; 超过的只存元信息, 点击再下
        "video_auto": False,              # 视频体积大, 默认不自动下
        "voice_auto": False,
        # 媒体保留天数, 0=不清理。**仅 download 模式生效** ——
        # link 模式下没有本地文件可回收, 删记录只会把链接弄丢。
        "keep_days": 3,
        "workers": 2,                     # 下载线程数。直连 HTTP, 可以开大点            
        "max_tries": 3
    },
    "health": {
        "enabled": True,
        "interval_sec": 60,               # 健康检查间隔
        "webhook_url": "",                # 告警用的机器人 webhook(企微/钉钉/飞书均可)
        "alert_on_recovery": True,
        "max_backlog": 200,               # 处理队列积压超过这个数 = 异常(worker 卡死)
        "stale_minutes": 0                # 多久没收到推送算异常。0=不检查
                                          # (半夜本来就没消息, 容易误报, 要开就设大点)
    },
    "debug": {
        # 把语聚推送原文留在内存里供管理后台查看。
        # ⚠️ 默认关 —— 原文是客户对话全文, 公网环境开之前想清楚。
        "raw_push": False,
        "raw_max": 300                    # 内存里最多保留多少条(只在内存, 不落库)
    }
}

# 全局消息库(main 里创建)
STORE = None
# 全局工作流引擎(main 里创建)
WF = None

# 内部归一化后的消息形态(与旧版一致，便于复用 store_message/do_forward):
#   type=PUSH_CHAT(100) 聊天消息; msg_type = 企微旗舰版的 content_type
CT_TEXT   = 2       # 企微旗舰版文本消息 content_type
MSG_TEXT  = 2       # 归一化后的文本子类型
PUSH_CHAT = 100     # 归一化后的外层 type: 聊天消息

# 旗舰版 content_type(见 接口整理.md 4.2)。注意和旧版推送码不是一套：
# 旧版图片=14/文件=15/视频=23，旗舰版是 101/102/103。
CT_IMAGE = 101; CT_FILE = 102; CT_VIDEO = 103; CT_GIF = 29; CT_VOICE = 16
CT_LINK  = 13;  CT_LOCATION = 6; CT_CARD = 41; CT_MINIAPP = 78
CT_REDPACKET = 26; CT_IMGTEXT = 123; CT_CHANNELS = 141; CT_CHANNELS_LIVE = 146

# 通知 type -> (content_type, 媒体种类)。媒体种类为 None 表示无需下载。
# 通知 type -> 中文名。用于原始推送面板把裸数字翻成人能看懂的东西。
# 覆盖 接口整理.md 第四章全部通知 + 第三章会走回调的查询响应。
TYPE_NAME = {          # 语聚 message_type -> 中文名(原始推送面板用)
    1:"系统消息", 2:"文本消息", 3:"语音消息", 4:"名片消息", 5:"位置消息",
    6:"视频消息", 7:"卡片消息", 9:"图片消息", 10:"抖音进线", 11:"广告留资",
    12:"历史消息",
}


# 富文本类要留档的结构化字段。不存这些的话，url/经纬度/名片 user_id 这类
# 只在推送里出现一次的信息就永久丢了 —— 和媒体过期是同一类问题。
# 注意不含 image_list —— 图文的图已经进 media 表了，再存一份等于把 CDN 凭证冗余两遍
RICH_FIELDS = ("title", "desc", "url", "image_url", "cover_url", "avatar",
               "address", "latitude", "longitude", "zoom",
               "nickname", "source", "user_id", "corp_id",
               "appid", "appname", "appicon", "page_path", "username",
               "money", "packet_id", "remark", "text_content")

# 媒体下载状态(store.media.state)
MS_PENDING = 0    # 待下载(已排队)
MS_DOING   = 1    # 下载中
MS_OK      = 2    # 已就绪
MS_FAIL    = 3    # 下载失败
MS_SKIP    = 4    # 超过自动下载阈值, 未下载(点击时按需下)

# ============================ 全局状态 ============================
_lock       = threading.RLock()
CONFIG      = {}
LOGS        = deque(maxlen=500)        # 供后台"状态监控"查看
MEMBER_CACHE = {}                      # {group_id: {user_id: name}} 转发时解析昵称
MEMBER_LIST  = {}                      # {group_id: [{id,name}]} 成员列表缓存(选人面板用)
NAME_MAP    = {}                       # {id: name}  群/好友/成员 id->显示名
CONTACT_TYPE = {}                      # {user_id: True=外部 / False=内部}  同步通讯录时填充
ROOM_LINKED = {}                       # {sid: (sid,room_id,bot_id)} 已落库的映射, 免得每条消息都 UPDATE
PEER_LINKED = {}                       # {sid: external_contact_id} 同上, 私聊那侧
STATS = {
    "started_at": time.time(),
    "received": 0,       # 收到的聊天消息数(本次启动以来)
    "forwarded": 0,      # 成功转发条数(本次启动以来)
    "last_msg_at": 0,
}
HEALTH = {
    "healthy": None,     # None=未知 True/False
    "logged_in": None,
    "last_check": 0,
    "last_error": "",
}

# 语聚 webhook：HTTP 线程只入队，重活交给 worker。
# 语聚等不到 200 就会重推 —— 在 HTTP 线程里做任何耗时的事(入库/下媒体/跑工作流)
# 都会变成重复消息。SEEN_EVENTS 是第二道防线，按 event_id 去重。
JJY_Q       = queue.Queue()
SEEN_EVENTS = deque(maxlen=4000)
SEEN_SET    = set()                    # 与 SEEN_EVENTS 同步, 用于 O(1) 判重
JJY_STATS   = {"received": 0, "dup": 0, "rejected": 0,
               "ignored": 0,          # 非 ai_assistant_receives_msg 的事件(如 chat_finish)
               "bad": 0,              # 是本事件但缺关键字段, 解析不出来
               "last_at": 0,
               "sent": 0,             # 出站发送成功条数(本次启动以来)
               "send_fail": 0,
               "send_err": ""}        # 最近一次发送失败的原因
JJY_EVENTS  = {}                      # {event_type: 条数} 被 ignored 的都是什么

# 原始推送调试缓冲(只在内存, 不落库)。默认关, 见 config.debug.raw_push
RAW_PUSHES  = deque(maxlen=1000)
_RAW_SEQ    = [0]

# 会话列表/气泡的占位文案。key 是 content_type。
MSG_TYPE_LABEL = {
    # 旗舰版 content_type
    101:"[图片]", 102:"[文件]", 103:"[视频]", 29:"[动图]", 16:"[语音]",
    13:"[链接]", 6:"[位置]", 41:"[名片]", 78:"[小程序]", 26:"[红包]",
    123:"[图文]", 141:"[视频号]", 146:"[视频号直播]",
    # 旧版推送码：老库里可能已有这些历史行，保留以免显示成 [未知消息]
    14:"[图片]", 15:"[文件]", 23:"[视频]", 4:"[合并消息]",
}

# 允许内联渲染的图片 MIME 白名单。
# 刻意不含 image/svg+xml —— SVG 会被浏览器当文档执行脚本，同源内联 = 存储型 XSS。
INLINE_MIME = {"image/jpeg", "image/png", "image/gif", "image/webp", "image/bmp"}

EXT_MIME = {
    ".jpg":"image/jpeg", ".jpeg":"image/jpeg", ".png":"image/png",
    ".gif":"image/gif", ".webp":"image/webp", ".bmp":"image/bmp",
    ".mp4":"video/mp4", ".mov":"video/quicktime", ".amr":"audio/amr",
    ".mp3":"audio/mpeg", ".silk":"audio/silk", ".pdf":"application/pdf",
}


# ============================ SSE 事件总线 ============================
class EventBus:
    """实时推送订阅中心：每个 SSE 连接一个队列，新消息到达时广播。"""
    def __init__(self):
        self._subs = set()
        self._slock = threading.Lock()

    def subscribe(self):
        q = queue.Queue(maxsize=500)
        with self._slock:
            self._subs.add(q)
        return q

    def unsubscribe(self, q):
        with self._slock:
            self._subs.discard(q)

    def publish(self, ev):
        with self._slock:
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(ev)
            except queue.Full:      # 某个客户端消费不动, 丢弃(它重连时会 gap-fill)
                pass

    def count(self):
        with self._slock:
            return len(self._subs)

BUS = EventBus()


# ============================ 日志(带环形缓冲) ============================
class RingHandler(logging.Handler):
    def emit(self, record):
        try:
            LOGS.append({
                "t": time.strftime("%m-%d %H:%M:%S", time.localtime(record.created)),
                "level": record.levelname,
                "msg": record.getMessage(),
            })
        except Exception:
            pass

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("gateway")
log.addHandler(RingHandler())


# ============================ 配置读写 ============================
def _deep_merge(base, override):
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def load_config():
    global CONFIG
    data = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            log.warning("读取 config.json 失败, 用默认配置: %s", e)
    CONFIG = _deep_merge(DEFAULT_CONFIG, data)

def save_config():
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(CONFIG, f, ensure_ascii=False, indent=2)

# 前端回填时用的占位符。管理页虽然在鉴权后面，但 apiKey 是能替别人发消息的凭证，
# 和 company_id 不是一个量级 —— 不往浏览器里吐原文。
API_KEY_MASK = "********"
SECRET_KEYS  = ("api_key", "api_key_mkt")

def _public_config():
    """给前端看的配置：apiKey 打码，其余原样。"""
    c = json.loads(json.dumps(CONFIG))       # 深拷贝, 别改到全局
    j = c.get("jjy")
    if isinstance(j, dict):
        for k in SECRET_KEYS:
            if j.get(k):
                j[k] = API_KEY_MASK
    return c

# ============================ 消息源适配 ============================
# 本项目的消息源是语聚(集简云)聚合对话 webhook —— 报文解析见 jjy.py。
# 通讯录/群成员/登录态这些原本由 IM 客户端提供的能力，webhook 模式下都没有：
# 显示名只能从每条推送里的 user_name / chat_title 累积（见 jjy_worker）。

SEND_GATE  = threading.Lock()          # 全局发送节流锁(工作台手发/工作流群发共用)
_send_next = [0.0]                     # 下一次允许发送的 monotonic 时刻

def _send_throttle():
    """按 jjy.send_qps 全局串行节流。

    语聚的 429 文案是 "Too Many Requests in one second" —— 限的是每秒并发。
    工作流一次转发给 N 个目标是个 for 循环，不限流必然连撞 429，
    所以这里拿锁串起来：慢一点，但不丢消息。
    """
    qps = float((CONFIG.get("jjy") or {}).get("send_qps") or 1)
    gap = 1.0 / max(0.1, min(qps, 20))
    with SEND_GATE:
        wait = _send_next[0] - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _send_next[0] = time.monotonic() + gap


def _jjy_url(path, api_key="", **params):
    """拼语聚接口 URL。apiKey 走 query string 是官方设计 —— 所以调用方
    **绝对不能把返回值写进日志**，出错只打 path。"""
    cfg = CONFIG.get("jjy") or {}
    # /v1/api/ai/marketing/ 那套单独一个 key(留空回退主 key), 见 DEFAULT_CONFIG
    if not api_key:
        api_key = ((cfg.get("api_key_mkt") if path.startswith("/v1/api/") else "")
                   or cfg.get("api_key") or "")
    base = (cfg.get("api_base") or "https://chat.jijyun.cn").rstrip("/")
    qs = "".join("&%s=%s" % (k, quote(str(v), safe=""))
                 for k, v in params.items() if v not in (None, ""))
    return "%s%s?apiKey=%s%s" % (base, path, quote(api_key.strip(), safe=""), qs)


def _jjy_call(path, body=None, api_key="", meta=None, throttle=False, **params):
    """调语聚接口，返回 (Data, err)。body=None 走 GET，否则 POST JSON。

    统一处理这套接口的三个共性：
      * 响应字段是**大写开头**的 Code/Data/Msg，成功是 Code==2000
      * Code=4000 是业务失败，重试没意义
      * 401/402 有专门的含义，翻成人话
    只有校验接口 /v1/openapi/check 不守这个约定(直接回 {"success": true})，
    那个单独处理。
    """
    meta = meta if isinstance(meta, dict) else {}
    cfg = CONFIG.get("jjy") or {}
    if not (api_key or cfg.get("api_key") or "").strip():
        return None, "未配置语聚 apiKey（管理后台 → 系统设置 → 出站发送）"
    url = _jjy_url(path, api_key, **params)
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    timeout = max(3, int(cfg.get("send_timeout") or 15))
    # 只有会改状态的调用(撤回)才排队 —— 拉列表要翻几十页, 套上 1 条/秒会慢到没法用
    if throttle:
        _send_throttle()
    try:
        req = urllib.request.Request(
            url, data=data, method="GET" if data is None else "POST",
            headers={"Content-Type": "application/json; charset=utf-8"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            meta["http"] = resp.status
            j = json.loads(resp.read().decode("utf-8", "ignore") or "{}")
    except urllib.error.HTTPError as e:
        meta["http"] = e.code
        return None, ("apiKey 无效或已失效（401）" if e.code == 401 else
                      "语聚资源包不足（402）" if e.code == 402 else "HTTP %s" % e.code)
    except Exception as e:
        return None, str(e)
    meta["code"] = j.get("Code")
    if j.get("Code") == 2000:
        return j.get("Data"), ""
    d = j.get("Data")
    return None, (j.get("Msg") or (d if isinstance(d, str) else "")
                  or "语聚返回 Code=%s" % j.get("Code"))


def _take_over(what, sid, prev):
    """身份被新会话行接管 = 聚合 chat_id 变过。喊一声 + 把历史搬过来。

    喊：否则漂移是无声的 —— 工作台看着像凭空多出来一个同名会话。
    搬：不搬的话取消息得跨会话行 IN 查，漂移 N 次列表就有 N 项；搬完恒为 1 项。
        旧行留成墓碑做重定向，不能删(老 sid 还散落在配置和前端里)。
    搬失败也不影响正确性：merged_into 仍是空，读取侧退化成合并模式。
    """
    if not prev:
        return
    log.warning("%s 的会话 id 变了: %s -> %s", what, "、".join(prev), sid)
    try:
        n = STORE.absorb(sid, prev)
        log.info("已把 %d 条历史消息并入新会话 %s（旧会话行留作重定向）", n, sid)
    except Exception as e:
        log.warning("历史消息并入失败，读取时会自动合并，不影响正确性: %s", e)


def resolve_target(target):
    """发送目标 -> **当前**可寻址的会话 id。

    收两种形态：
      R:/S: + 聚合 chat_id   本地会话 id（工作台、回复、转发目标都是它）
      纯数字                 「人」的 id(external_contact_id)，选人面板给的
    聚合 chat_id 是地址不是身份、会变，所以第一种也要过一次库：拿这行会话的
    身份(imRoomId / peer_uid)去取最新那行。查不到就原样返回，不在这儿拦。
    """
    t = str(target or "")
    if not (t and STORE):
        return t
    if t[:2] in ("R:", "S:"):
        sess = STORE.get_session(t)
        if not sess:            # 本地没这行：多半是群列表点进来的 imRoomId
            return STORE.latest_by_identity(room_id=t) or t
        return STORE.latest_by_identity(room_id=sess.get("room_id") or "",
                                        peer_uid=sess.get("peer_uid") or "") or t
    return STORE.latest_by_identity(peer_uid=t) or t


def jjy_send(conversation_id, message_type, message_content, meta=None, api_key=""):
    """调语聚「发送/回复聚合对话消息」。返回 (ok, err)。

    meta 传一个 dict 的话会被填上 {"http": <状态码>, "code": <业务 Code>}，
    用来区分"没发出去"的原因(网络断 / 认证挂了 / 参数不对)——只有 /gw/check-apikey
    需要这个粒度，正常发送看 ok 就够了。api_key 同理: 只有校验按钮会传，
    为的是验「输入框里刚填的那个」而不是「已经存下的那个」。

        POST {api_base}/v1/openapi/aggregate/message/send?apiKey=xxx
        {"chat_id": "...", "message_type": 2, "message_content": {...}}

    ⚠️ 响应体的键是**大写开头**的(Code/Data/Msg)，不是常见的 code/data/msg：
        {"Code": 2000, "Data": {"channel": "...", "chat_id": "..."}, "Msg": ""}
        {"Code": 4000, "Data": "<失败信息>", "Msg": "<错误具体原因>"}

    message_type: 2=文本 9=图片 8=文件 3=语音 10~16=各渠道素材。
    文本是 {"text": ...}；图片/文件/语音都是 {"url": "公网可访问的链接"}；
    素材是 {"material_id": ...}。所以要发图只需 jjy_send(sid, 9, {"url": ...})。
    """
    meta = meta if isinstance(meta, dict) else {}
    cfg = CONFIG.get("jjy") or {}
    key = (api_key or cfg.get("api_key") or "").strip()
    if not key:
        return False, "未配置语聚 apiKey（管理后台 → 系统设置 → 语聚接入）"
    target  = resolve_target(conversation_id)
    chat_id = jjy.raw_chat_id(target)
    if not chat_id:
        return False, "会话 id 为空"
    if target.isdigit():
        # 纯数字 = 「人」的 id，而且没能换成会话 id：这个人还没给托管账号发过
        # 消息。语聚对不认识的 chat_id 回的是 5xx，会触发重试白等几秒才失败，
        # 不如在这儿直接说清楚。
        return False, "这个人还没来过消息，拿不到会话 id（语聚只能回复已存在的会话）"

    base = (cfg.get("api_base") or "https://chat.jijyun.cn").rstrip("/")
    # apiKey 在 query string 里 —— 下面任何一条日志都不许带 url
    url = "%s/v1/openapi/aggregate/message/send?apiKey=%s" % (base, quote(key, safe=""))
    body = json.dumps({"chat_id": chat_id,
                       "message_type": int(message_type),
                       "message_content": message_content},
                      ensure_ascii=False).encode("utf-8")
    timeout = max(3, int(cfg.get("send_timeout") or 15))
    tries   = max(1, int(cfg.get("send_retry") or 0) + 1)

    err = ""
    for i in range(tries):
        _send_throttle()
        req = urllib.request.Request(
            url, data=body, method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                meta["http"] = resp.status
                raw = resp.read().decode("utf-8", "ignore")
            j = json.loads(raw or "{}")
            meta["code"] = j.get("Code")
            if j.get("Code") == 2000:
                return True, ""
            # 业务失败(4000)：参数不对、会话已关闭之类，重试也是一样的结果
            data = j.get("Data")
            return False, (j.get("Msg") or (data if isinstance(data, str) else "")
                           or "语聚返回 Code=%s" % j.get("Code"))
        except urllib.error.HTTPError as e:
            meta["http"] = e.code
            if e.code == 401:
                return False, "apiKey 无效或已失效（401）"
            if e.code == 402:
                return False, "语聚资源包不足（402）"
            err = "HTTP %s" % e.code
            if e.code != 429 and e.code < 500:      # 4xx 是我们发的请求有问题
                return False, err
        except Exception as e:                       # 超时/连接失败/响应不是 JSON
            err = str(e)
        if i < tries - 1:
            time.sleep(1.2 + i)                      # 429 是按秒限的, 退避要跨过这一秒
    return False, err or "发送失败"


def _quote_rich(conversation_id, quote_id):
    """自己发的引用消息，补录时也要带上引用块，不然入库那条只剩正文
    （乐观气泡上那个引用块会被入库的这条替换掉，看着像"引用没生效"）。

    被引用消息的发送人和原文从**自己库里**查，不让前端传 ——
    那是要展示给人看的内容，从库里取才和别处显示的一致。
    """
    if not (quote_id and STORE):
        return None
    src = STORE.get_by_msg_id(conversation_id, quote_id)
    if not src:
        return None
    kind = {CT_IMAGE: "image", CT_GIF: "image", CT_FILE: "file",
            CT_VIDEO: "video", CT_VOICE: "voice"}.get(src.get("msg_type"), "text")
    return {"quote": {"id": str(quote_id),
                      "sender": src.get("sender_name") or "",
                      "content": src.get("content") or "",
                      "mtype": kind}}


def _record_sent(conversation_id, text, quote_id=""):
    """把自己发出去的消息补录进库。

    ⚠️ 语聚**不会**把 OpenAPI 发出去的消息推回来（实测 sent +2 而 received 为 0，
    同期别的推送照常进来）。所以不补录的话，前端那个气泡只是本地占位，
    一刷新就没了。(语聚自己的 AI 规则发的回复倒是走消息事件，两条路不一样。)

    msg_id 用 local: 前缀的本地 id —— 发送接口的响应里没有 message_id，
    拿不到语聚那边的真 id 就引用不了也撤回不了，前端据此不给这两个按钮。
    """
    if not STORE:
        return
    try:
        store_message({
            "type": PUSH_CHAT, "msg_type": MSG_TEXT,
            "user_id": conversation_id, "sender": "", "sender_name": "我",
            "content": text, "time_stamp": int(time.time()),
            "msg_id": "local:" + uuid.uuid4().hex,
            "is_self_msg": 1, "at_me": 0, "sender_external": 0,
            "rich": _quote_rich(conversation_id, quote_id),
            "jjy": {"mt": 2, "chat_id": jjy.raw_chat_id(conversation_id),
                    "local": True},
        })
    except Exception as e:
        # 补录失败不该让"其实已经发出去了"变成"发送失败"
        log.warning("已发送但补录入库失败 %s: %s", conversation_id, e)


def send_text_ex(conversation_id, msg, quote_id="", mention=None):
    """发文本，返回 (ok, err)。前端要拿 err 显示，所以和 send_text 分成两个。"""
    text = str(msg or "")
    if not text:
        return False, "内容为空"
    mc = {"text": text}
    if quote_id:                       # 引用/@ 目前只有企微代运营渠道支持，
        mc["quoteMessageId"] = str(quote_id)   # 其他渠道语聚会忽略这两个字段
    if mention:
        mc["mention"] = [str(x) for x in mention]

    # 先把目标解析成当前会话 id：入参可能是「人」的 id，也可能是 chat_id 漂移前
    # 的旧会话 id。下面留档和日志都得用解析后的，否则自己发的消息会记到旧会话行上。
    # jjy_send 里还会再解析一次(幂等)，那是给其它调用方兜底的。
    conversation_id = resolve_target(conversation_id)
    ok, err = jjy_send(conversation_id, 2, mc)
    with _lock:
        if ok:
            JJY_STATS["sent"] += 1
        else:
            JJY_STATS["send_fail"] += 1
            JJY_STATS["send_err"] = err
    if ok:
        log.info("已发送 -> %s (%d 字)", conversation_id, len(text))
        _record_sent(conversation_id, text, quote_id)
    else:
        log.warning("发送失败 -> %s: %s", conversation_id, err)
    return ok, err


def send_text(conversation_id, msg):
    """工作流引擎约定的发送出口：send_text(sid, text) -> bool。"""
    return send_text_ex(conversation_id, msg)[0]


def jjy_revoke(conversation_id, message_id):
    """撤回一条消息。POST /v1/openapi/aggregate/message/revoke

    ⚠️ 官方只写了「支持小红书、企业微信聚合消息撤回」——别的渠道会返回 4000。
    文档没写时限，但各家 IM 基本都有(企微 2 分钟)，超时会被语聚那边挡掉。
    message_id 用的是**推送里带的那个**：发送接口的响应里没有 message_id，
    所以自己发的消息也得等回声推回来才撤得了。
    """
    chat_id = jjy.raw_chat_id(conversation_id)
    mid = str(message_id or "")
    if not (chat_id and mid):
        return False, "缺少会话或消息 id"
    _, err = _jjy_call("/v1/openapi/aggregate/message/revoke",
                       {"chat_id": chat_id, "message_id": mid}, throttle=True)
    if err:
        log.warning("撤回失败 %s/%s: %s", conversation_id, mid, err)
    return (not err), err


CONTACTS_CACHE = {"at": 0, "list": []}

# ---------------------------------------------------------------------------
# 通讯录/群列表：**只读本地，只有手动同步才回源**
#
# 这两份数据来自语聚接口。它们不是消息，不会自己变着变着就过期成错的，
# 所以不搞 TTL、不搞后台定时刷 —— 上游请求只发生在人明确点了按钮的时候：
#   「同步通讯录」 -> /gw/sync-contacts   (群列表 + 联系人)
#   「刷新群成员」 -> /gw/members refresh (群列表)
# 其余所有读取(打开管理页、开工作流编辑器、发消息前查名字)一律走 directory
# 表的快照 + 进程内缓存，一个网络请求都不发。
#
# 代价说清楚：新建的群、新加的人，不点同步就不会出现。这是有意的 ——
# 宁可你自己决定什么时候更新，也不要一次打开页面卡在那儿翻几十页。
def _dir_save(kind, items, full):
    """快照落库。失败只记一笔 —— 内存缓存已经更新了，不影响这次请求。"""
    if not STORE:
        return
    try:
        STORE.save_directory(kind, items, full=full)
    except Exception as e:
        log.warning("通讯录快照落库失败(%s): %s", kind, e)


def directory_boot():
    """启动时把库里的通讯录/群列表快照读进内存。不回源。"""
    if not STORE:
        return
    try:
        gl, gat = STORE.load_directory("group")
        cl, cat = STORE.load_directory("contact")
    except Exception as e:
        log.warning("读取通讯录快照失败: %s", e)
        return
    with _lock:
        GROUPS_CACHE.update({"at": gat, "list": gl})
        CONTACTS_CACHE.update({"at": cat, "list": cl})
    if gl or cl:
        log.info("通讯录快照已载入: 群 %d, 联系人 %d（上次同步 %s）",
                 len(gl), len(cl),
                 time.strftime("%m-%d %H:%M", time.localtime(max(gat, cat))))
    else:
        log.info("通讯录快照为空 —— 到管理后台点一次「同步通讯录」再用选人面板")


def jjy_contacts(force=False):
    """企微托管账号的联系人列表。GET /v1/api/ai/marketing/contacts/list

    ⚠️ 同样只覆盖**企微代运营**渠道，且返回的 wxid / externalUserId 和聚合对话
    推送里的 user_id 不是一个 id 空间。好在这个接口返回了 chatId ——
    那大概率就是聚合对话的会话 id，所以能直接映射(见 paired)。
    """
    with _lock:
        have = list(CONTACTS_CACHE["list"])
    if not force:                 # 只有点了同步(force)才回源，其余一律读快照
        return have, ""

    out, page, err = [], 0, ""
    while page < 20:                      # 20*500=1万人，够了；防翻页不收敛
        data, e = _jjy_call("/v1/api/ai/marketing/contacts/list",
                            current=page, pageSize=500)
        if e:
            err = e
            break
        batch = ((data or {}).get("data") or [])
        out.extend(x for x in batch if isinstance(x, dict))
        total = int((((data or {}).get("page") or {}).get("total")) or 0)
        if len(batch) < 500 or (total and len(out) >= total):
            break
        page += 1

    if err and not out:
        log.warning("拉取企微联系人失败: %s", err)
        # 库存还在内存和库里，不动它 —— 上游抽风不该把面板清空
        return have, err
    with _lock:
        CONTACTS_CACHE.update({"at": time.time(), "list": out})
    _dir_save("contact", [(str(c.get("wxid") or ""), c) for c in out], full=not err)
    log.info("企微联系人已刷新: %d 人%s", len(out), ("（部分失败: %s）" % err) if err else "")
    return out, err


def contacts_for_picker(force=False):
    """联系人列表 -> 选人面板形态。

    ⚠️ 一条记录里有**两个不同维度**的 id，别混：
      id    会话 id = "S:"+chatId。选「指定会话」和发消息用它(发送接口认 chatId)
      wxid  人   id。选「指定发送人」用它 —— 它和群成员接口的 imContactId 是
            同一个空间，也正是推送里 user_addition.external_contact_id 的值
    以前发送人面板拿的是 id(会话 id)，和推送里的人 id 根本不是一个空间，
    所以「指定发送人」永远匹配不上。
    """
    raw, err = jjy_contacts(force)
    out = []
    for c in raw:
        if c.get("deleted"):
            continue
        cid = str(c.get("chatId") or "")
        out.append({
            "id": ("S:" + cid) if cid else str(c.get("wxid") or ""),
            "paired": bool(cid),
            "name": c.get("alias") or c.get("nickName") or c.get("wxid") or "",
            # coworker=同公司员工 -> 内部; 其余按外部客户算
            "is_external": 0 if c.get("coworker") else 1,
            "avatar": c.get("avatarUrl") or "",
            "wxid": str(c.get("wxid") or ""),
            "external_id": str(c.get("externalUserId") or ""),
            "tags": (c.get("tags") or []) + (c.get("labels") or []),
        })
    out.sort(key=lambda x: (not x["paired"], x["name"]))
    return out, err


def sender_id_migration(contacts, apply=False):
    """存量工作流里「指定发送人」的老 id -> 人 id。返回待改写清单。

    发送人面板以前存的是 "S:"+chatId(会话 id)，和推送里的 external_contact_id
    压根不是一个空间 —— 那些条件**从来没匹配上过**，等于这些工作流一直是死的。

    ⚠️ 所以这件事默认只算不改(apply=False)。改完它们会**立刻开始触发** ——
    动作里可能有转发(把客户对话转到别处)或 http 回调，一条沉睡两个月的规则
    突然活过来是要人点头的，不能跟在"同步通讯录"后面静默发生。

    认不出的值原样留着，绝不删：_match 里 `if t.get("senders") and ...` 意味着
    空列表 = 不限发送人，删值会把"只对张三"劣化成"对所有人"。
    """
    if not WF:
        return []
    m = {c["id"]: c["wxid"] for c in contacts if c.get("id") and c.get("wxid")}
    if not m:
        return []
    wfs, plan = WF.list(), []
    for w in wfs:
        t = w.get("trigger") or {}
        out, hit = [], False
        for s in (t.get("senders") or []):
            v = m.get(s)
            if v:
                hit = True
                plan.append({"wf": w.get("name") or w.get("id"),
                             "old": s, "new": v})
            v = v or s
            if v not in out:
                out.append(v)
        if hit and apply:
            t["senders"] = out
    if plan and apply:
        WF.save(wfs)
        log.info("工作流「指定发送人」id 迁移: 改写 %d 项（联系人会话id -> 人id）",
                 len(plan))
    return plan


# ============================ 企微群列表 ============================
# GET /v1/openapi/wxwork/group/list —— 托管账号所在的企微群，**含群成员**。
#
# ⚠️ 两个必须记住的边界：
#   1) 只覆盖**企微代运营**渠道。抖音/小红书/钉钉/公众号那些聚合对话，
#      这个接口一个都不返回。
#   2) 它返回的 imRoomId 形如 "R:10942308524386327"，和聚合对话的 chat_id
#      (UUID 形态)**不是一个 id 空间**。要映射回本地会话只能靠推送里的
#      source_addition.room_id —— 也就是说，只有来过消息的群才对得上。
GROUPS_CACHE = {"at": 0, "list": [], "err": ""}   # at = 上次**同步**时间, 不是过期钟

def jjy_group_list(force=False):
    """拉全量群列表(自动翻页)。返回 (list, err)，list 里每项是接口原样的群对象。"""
    with _lock:
        have = list(GROUPS_CACHE["list"])
    if not force:                 # 同 jjy_contacts：只有点了同步才回源
        return have, ""

    out, page, err = [], 0, ""
    while page < 50:                     # 50*100=5000 个群，够了；防翻页不收敛
        # current 是**从 0 开始**的页码(文档: "默认为0，即第一页")
        data, e = _jjy_call("/v1/openapi/wxwork/group/list",
                            current=page, pageSize=100)
        if e:
            err = e
            break
        batch = ((data or {}).get("data") or [])
        out.extend(x for x in batch if isinstance(x, dict))
        total = int((data or {}).get("total") or 0)
        if len(batch) < 100 or (total and len(out) >= total):
            break
        page += 1

    if err and not out:
        with _lock:
            GROUPS_CACHE["err"] = err
        log.warning("拉取企微群列表失败: %s", err)
        return have, err               # 同上，保住库存
    with _lock:
        GROUPS_CACHE.update({"at": time.time(), "list": out, "err": err})
    _dir_save("group", [(str(g.get("imRoomId") or g.get("wecomChatId") or ""), g)
                        for g in out], full=not err)
    log.info("企微群列表已刷新: %d 个群%s", len(out), ("（部分失败: %s）" % err) if err else "")
    return out, err


def groups_for_picker(force=False):
    """群列表 -> 选人面板用的形态。

    关键在 paired：imRoomId 能映射到本地会话的群，id 用内部 session_id
    (发送接口认的是聚合对话 chat_id)；映射不上的原样给 imRoomId 并标记
    paired=False —— 前端置灰不让选，因为那个 id 大概率发不出去。
    """
    raw, err = jjy_group_list(force)
    rmap = STORE.room_map() if STORE else {}
    out = []
    for g in raw:
        rid = str(g.get("imRoomId") or g.get("wecomChatId") or "")
        sid = rmap.get(rid, "")
        out.append({
            "id": sid or rid,
            "room_id": rid,
            "paired": bool(sid),
            "name": g.get("name") or rid,
            "is_external": 1 if g.get("external") else 0,
            "notice": g.get("notice") or "",
            "owner": str(g.get("owner") or ""),
            "bot_id": str(g.get("imBotId") or ""),
            "members": len(g.get("memberList") or []),
        })
    out.sort(key=lambda x: (not x["paired"], x["name"]))
    return out, err


def _room_index(force=False):
    """{imRoomId/wecomChatId: 群对象}。批量查成员时只建一次，避免每个群都全表扫。"""
    raw, err = jjy_group_list(force)
    idx = {}
    for g in raw:
        for k in (g.get("imRoomId"), g.get("wecomChatId")):
            if k:
                idx.setdefault(str(k), g)
    return idx, err


def _members_in(g):
    """群对象 -> 成员列表(选人面板形态)。

    id 用 imContactId —— 那是**人**的 id，和联系人接口的 wxid、推送里的
    external_contact_id 同一个空间，所以一个人勾一次，他所在的每个群都能命中。
    """
    return [{
        "id": str(m.get("imContactId") or ""),
        "name": m.get("nickName") or "",
        "avatar": m.get("avatarUrl") or "",
        "corp": m.get("corpName") or "",
        "external_id": str(m.get("externalUserId") or ""),
        # type: 1=个人微信 3=企微用户。⚠️ 3 不等于"本司同事" —— 实测 787 个 type=3
        # 里有 105 个是他司企微(环信/千里千寻/麦驰…)，imContactId 一样 1688 开头。
        # 真要分内外部，判据是 corpName 是否本司 / externalUserId 是否非空。
        "type": m.get("type"),
        "is_owner": 1 if m.get("identity") == 2 else 0,
        "join_time": m.get("joinTime") or 0,
    } for m in (g.get("memberList") or []) if isinstance(m, dict)]


def _members_of(session_id, idx):
    """单个会话查成员。idx 由 _room_index() 建好。返回 (list, err)。"""
    sid = str(session_id or "")
    if sid.startswith("S:"):
        return [], "私聊没有群成员"
    sess = STORE.get_session(sid) if STORE else None
    rid = (sess or {}).get("room_id") or ""
    if not rid and not sess:
        # 本地压根没这个会话 —— 那前端传的多半就是 imRoomId(从群列表点进来的)。
        # 不靠格式猜: 对不上的话下面查索引自然会返回"没这个群"。
        rid = sid
    if not rid:
        return [], "该会话没有对应的企微群（非企微渠道，或还没收到过消息）"
    g = idx.get(rid)
    if not g:
        return [], "群列表里没有这个群（可能不在托管账号名下）"
    return _members_in(g), ""


def members_of(session_id, force=False):
    """某会话对应企微群的成员列表 -> [{id,name,...}]。"""
    idx, err = _room_index(force)
    lst, e = _members_of(session_id, idx)
    return lst, e or err


def members_of_many(session_ids, force=False):
    """批量查成员。返回 ({会话id: [成员]}, {会话id: err}, err)。

    ⚠️ 存在的理由是 force：members_of(g, True) 每调一次就把**整份群列表**重拉一遍
    (156 个群 = 2 页)，前端按群逐个调的话 10 个群就是 20 次上游请求。
    批量后无论多少个群都只刷一次，所以前端那边也就不需要再限制群数了。
    """
    idx, err = _room_index(force)
    out, errs = {}, {}
    for sid in session_ids:
        lst, e = _members_of(sid, idx)
        out[str(sid)] = lst
        if e:
            errs[str(sid)] = e
    return out, errs, err


def _private_peer_id(sid, self_id=""):
    """私聊会话 id -> 对端 id。

    语聚的 chat_id 里没有下划线，走到最后的 partition 兜底会原样返回，
    所以对 "S:<chat_id>" 这种形态等价于"剥掉前缀"。
    """
    if not sid.startswith("S:"):
        return ""
    body = sid[2:]
    if self_id:
        if body.startswith(self_id + "_"):
            return body[len(self_id) + 1:]
        if body.endswith("_" + self_id):
            return body[:-(len(self_id) + 1)]
    a, _sep, b = body.partition("_")
    return b or a


def resolve_name(group_id, user_id):
    """id -> 显示名。NAME_MAP 由收到的推送逐条填充，不是拉通讯录来的。"""
    return NAME_MAP.get(user_id, user_id)


def classify_sender(uid):
    """0=内部 1=外部 None=未知。由 jjy 侧的 user_type 判定后回填 CONTACT_TYPE。"""
    if not uid:
        return None
    t = CONTACT_TYPE.get(uid)
    return None if t is None else (1 if t else 0)


# ============================ 会话工作台：消息入库 + 广播 ============================
def _disp_content(msg):
    """把一条推送消息转成可显示文本(非文本类型给占位)。"""
    ct = msg.get("msg_type")
    if ct == MSG_TEXT:
        c = msg.get("content")
        return c if isinstance(c, str) else json.dumps(c, ensure_ascii=False)
    label = MSG_TYPE_LABEL.get(ct, "[未知消息]")
    c = msg.get("content")
    # 图文的 text_content 就是正文本身，直接当消息内容，不加 "[图文]" 前缀
    if ct == CT_IMGTEXT and isinstance(c, str) and c.strip():
        return c.strip()
    md = msg.get("media") or {}
    if md.get("kind") == "file" and md.get("file_name"):
        return "%s %s" % (label, md["file_name"])        # 会话列表里能直接看到文件名
    if isinstance(c, str) and c.strip():                 # 链接/名片等带标题
        return "%s %s" % (label, c.strip())
    return label

def store_message(msg):
    """把一条推送(含自己发的)持久化进 SQLite，并通过 SSE 广播给前台。"""
    sid = str(msg.get("user_id") or "")
    if not sid:
        return
    is_self  = msg.get("is_self_msg") == 1
    sender   = str(msg.get("sender") or "")
    is_group = sid.startswith("R:")          # 群 R: / 私聊 S:
    text = _disp_content(msg)
    if is_self:
        sname = "我"
    elif msg.get("sender_name"):             # 企微推送已带发送者姓名，优先用
        sname = msg["sender_name"]
    elif is_group and sender:
        sname = NAME_MAP.get(sender, sender)
    else:
        sname = NAME_MAP.get(sid, sid)
    self_id = ""            # webhook 模式没有"自己"这个账号概念
    with _lock:
        if is_group:
            disp_name = NAME_MAP.get(sid, "")                       # 群名 = 会话 id 直接命中
        else:
            disp_name = NAME_MAP.get(_private_peer_id(sid, self_id), "")  # 私聊 = 对端昵称
    # 私聊兜底：通讯录里没有(如外部用户未入库)，就用这条非自己发消息带的发送人名
    if not is_group and not disp_name and not is_self and msg.get("sender_name"):
        disp_name = msg["sender_name"]
    preview = ((sname + "：") if (is_group and not is_self) else "") + text

    # 媒体消息：先生成 uuid 当媒体 id(同时也是 /media/<id> 的路径)，
    # 这样消息行和媒体行能互相引用，不用两次写库。
    # 图文(11068)一条消息可以带多张图，所以统一按列表处理。
    mlist = msg.get("media_list") or ([msg["media"]] if msg.get("media") else [])
    mids = [uuid.uuid4().hex for _ in mlist]
    ts = int(msg.get("time_stamp") or time.time())

    item, sess = STORE.add_message(
        session_id=sid, msg_id=str(msg.get("msg_id") or ""),
        sender=sender, sender_name=sname, content=text,
        msg_type=msg.get("msg_type"), is_self=is_self, ts=ts,
        is_group=is_group, session_name=disp_name, preview=preview,
        media_id=(mids[0] if mids else ""), rich=msg.get("rich"))
    if item is None:            # 重复推送(命中唯一索引), 不广播
        return

    if mlist:
        regs = [_register_media(mids[i], item["seq"], sid, mlist[i], ts)
                for i in range(len(mlist))]
        item["media"] = regs[0]
        if len(regs) > 1:
            item["media_list"] = regs
    BUS.publish({"type": "message", "session_id": sid,
                 "message": item, "session": sess})


# ============================ 媒体：登记 / 下载 / 清理 ============================
MEDIA_Q = queue.Queue()

def _mcfg():
    return CONFIG.get("media") or DEFAULT_CONFIG["media"]

def _media_root():
    d = _resolve(_mcfg().get("dir") or "media")
    os.makedirs(d, exist_ok=True)
    return d

def _guess_mime(name, kind):
    ext = os.path.splitext(name or "")[1].lower()
    if ext in EXT_MIME:
        return EXT_MIME[ext]
    return {"image": "image/jpeg", "gif": "image/gif", "video": "video/mp4",
            "voice": "audio/amr"}.get(kind, "application/octet-stream")

def _auto_plan(kind, size, md=None):
    """按配置决定这条媒体的初始状态。

    link 模式：直接置 MS_OK —— 不下载，取用时由 /media/<id> 302 到原始 URL。
    前端因此一行都不用改：<img src="media/xxx"> 照样出图。

    文件的阈值是 auto_download_mb(默认 50MB)：超过的不自动下，但消息照常入库、
    会话里能看到文件卡片，点击时再按需下载。
    """
    c = _mcfg()
    if not c.get("enabled", True):
        return MS_SKIP
    # link 模式：只要拿得到直连 url 就当已就绪
    if c.get("mode", "link") == "link" and ((md or {}).get("cdn") or {}).get("url"):
        return MS_OK
    if kind in ("image", "gif"):
        return MS_PENDING if c.get("image_auto", True) else MS_SKIP
    if kind == "video":
        return MS_PENDING if c.get("video_auto", False) else MS_SKIP
    if kind == "voice":
        return MS_PENDING if c.get("voice_auto", False) else MS_SKIP
    # file
    cap = float(c.get("auto_download_mb") or 0)
    if cap <= 0:
        return MS_SKIP
    if size and size > cap * 1024 * 1024:
        return MS_SKIP
    return MS_PENDING

def _register_media(mid, seq, sid, md, ts):
    """把媒体登记进库，需要自动下载的排进队列。返回给前端的媒体 dict。"""
    kind = md.get("kind") or "file"
    name = md.get("file_name") or "file"
    size = int(md.get("size") or 0)
    state = _auto_plan(kind, size, md)
    STORE.add_media(mid=mid, msg_seq=seq, session_id=sid, kind=kind,
                    file_name=name, size=size, md5=md.get("md5") or "",
                    mime=_guess_mime(name, kind),
                    cdn_type=int(md.get("cdn_type") or 0),
                    cdn=dict(md.get("cdn") or {}, file_type=md.get("file_type") or 1),
                    state=state, ts=ts)
    if state == MS_PENDING:
        MEDIA_Q.put(mid)
    elif state == MS_OK:
        pass                       # link 模式：无需下载，取用时再 302
    else:
        log.info("媒体未自动下载(超阈值或按配置跳过) kind=%s size=%.1fMB name=%s",
                 kind, size / 1048576.0, name)
    return STORE.get_media(mid)

def _media_path(mid, name):
    """落盘路径：用 uuid 做文件名，规避中文名/重名/路径穿越；按前 2 位分片建目录。"""
    ext = os.path.splitext(name or "")[1].lower()
    if len(ext) > 12 or "/" in ext or "\\" in ext:
        ext = ""
    sub = os.path.join(_media_root(), mid[:2])
    os.makedirs(sub, exist_ok=True)
    return os.path.join(sub, mid + ext)

def _download_direct(url, save_path, timeout=60):
    """GIF(11048) 只给了裸 url、没有 cdn 凭证，走普通 HTTP 拉取。"""
    if not (url or "").lower().startswith(("http://", "https://")):
        return False, "", "无效的 url"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r, \
                open(save_path, "wb") as f:
            while True:
                chunk = r.read(65536)
                if not chunk:
                    break
                f.write(chunk)
        return (True, save_path, "") if os.path.getsize(save_path) > 0 \
            else (False, "", "下载内容为空")
    except Exception as e:
        return False, "", "直连下载失败: %s" % e

def _do_download(mid):
    m = STORE.get_media(mid)
    if not m or m["state"] == MS_OK:
        return
    cdn = STORE.get_media_cdn(mid)
    STORE.set_media(mid, state=MS_DOING, err="")
    _bcast_media(mid)
    path = _media_path(mid, m["file_name"])
    ft = int(cdn.get("file_type") or 1)

    if cdn.get("direct"):
        ok, real, err = _download_direct(cdn.get("url") or "", path)
    else:
        # 语聚给的媒体都是可直连的 https URL，一律走 direct。
        # 走到这里说明有条目没带 direct 标记 —— 是 jjy.py 的 bug，不是网络问题。
        ok, real, err = False, "", "媒体条目缺少直连 url(cdn.direct)"

    if ok:
        try:
            size = os.path.getsize(real)
        except OSError:
            size = m["size"]
        STORE.set_media(mid, state=MS_OK, path=real, size=size, err="")
        log.info("媒体已就绪 %s %s (%.1fKB)", m["kind"], m["file_name"], size / 1024.0)
    else:
        STORE.bump_media_try(mid)
        cur = STORE.get_media(mid) or {}
        tries = int(cur.get("tries") or 0)
        if tries < int(_mcfg().get("max_tries") or 3):
            STORE.set_media(mid, state=MS_PENDING, err=err)
            MEDIA_Q.put(mid)                       # 重排队重试
            log.warning("媒体下载失败(第%d次, 将重试) %s: %s", tries, m["file_name"], err)
        else:
            STORE.set_media(mid, state=MS_FAIL, err=err)
            log.error("媒体下载失败(已达重试上限) %s: %s", m["file_name"], err)
    _bcast_media(mid)

def _bcast_media(mid):
    """媒体状态变化 -> SSE 推给前台，把占位符换成真图/下载按钮。"""
    m = STORE.get_media(mid)
    if m:
        BUS.publish({"type": "media", "session_id": m["session_id"], "media": m})

def media_worker():
    while True:
        mid = MEDIA_Q.get()
        try:
            _do_download(mid)
        except Exception as e:
            log.exception("媒体下载线程异常 mid=%s: %s", mid, e)
        finally:
            MEDIA_Q.task_done()

def media_gc():
    """定期清理：孤儿媒体 + 超过保留天数的文件，顺带清过期的 AI 上下文。

    AI 上下文搭这趟车而不是自己起一个线程 —— 都是每小时一次的按天过期，
    没必要为了一条 DELETE 多养一个线程。
    """
    while True:
        time.sleep(3600)
        try:
            n = STORE.ai_ctx_gc((CONFIG.get("storage") or {}).get("ai_context_days", 7))
            if n:
                log.info("AI 上下文清理: 删除过期 %d 行", n)
        except Exception as e:
            log.warning("AI 上下文清理失败: %s", e)
        try:
            c = _mcfg()
            if not c.get("enabled", True):
                continue                       # 媒体关掉了，这趟只做上面的上下文清理
            keep = int(c.get("keep_days") or 0)
            # link 模式下本地根本没有文件, 按天数删记录只会把链接弄丢、还回收不了任何磁盘。
            # 传 0 让 orphan_media 只清"消息已被裁剪掉的孤儿", 不做时间过期。
            if c.get("mode", "link") == "link":
                keep = 0
            rows = STORE.orphan_media(keep)
            if not rows:
                continue
            root = os.path.realpath(_media_root())
            freed = 0
            for m in rows:
                p = m.get("path") or ""
                if not p:
                    continue
                rp = os.path.realpath(p)
                if not rp.startswith(root + os.sep):   # 不删目录外的东西
                    continue
                try:
                    freed += os.path.getsize(rp)
                    os.remove(rp)
                except OSError:
                    pass
            STORE.del_media([m["id"] for m in rows])
            log.info("媒体清理: 删除 %d 条, 释放 %.1fMB", len(rows), freed / 1048576.0)
        except Exception as e:
            log.warning("媒体清理异常: %s", e)


# ============================ 企微 webhook 告警 ============================
def push_webhook(text):
    url = (CONFIG.get("health") or {}).get("webhook_url", "").strip()
    if not url:
        return False, "未配置 webhook"
    body = json.dumps({"msgtype": "text", "text": {"content": text}}).encode("utf-8")
    req = urllib.request.Request(url, data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            j = json.loads(resp.read().decode("utf-8", "ignore") or "{}")
            return (j.get("errcode") == 0), j.get("errmsg", "")
    except Exception as e:
        return False, str(e)


# ============================ 健康检查线程 ============================
def health_loop():
    """webhook 模式没有"登录态"，改盯两个真正会出问题的信号。

      1) **队列积压** —— worker 卡死或处理不过来。这是硬故障：语聚还在推，
         但消息进不了库，用户看不到。
      2) **长时间收不到推送** —— 回调地址失效、语聚那边配置被改、网络断了。
         这一项很容易误报(半夜本来就没消息)，所以 stale_minutes 默认 0 = 不检查；
         要开的话按你的业务时段设一个明显偏大的值。
    """
    while True:
        hc = CONFIG.get("health") or {}
        interval = max(10, int(hc.get("interval_sec", 60) or 60))
        if hc.get("enabled", True) and (CONFIG.get("jjy") or {}).get("enabled"):
            backlog = JJY_Q.qsize()
            max_backlog = int(hc.get("max_backlog") or 200)
            stale_min = int(hc.get("stale_minutes") or 0)
            with _lock:
                last = JJY_STATS["last_at"]
            stale = bool(stale_min and last and
                         (time.time() - last) > stale_min * 60)

            healthy = backlog < max_backlog and not stale
            if backlog >= max_backlog:
                reason = "处理队列积压 %d 条(阈值 %d)，worker 可能已卡死" % (backlog, max_backlog)
            elif stale:
                reason = "已 %d 分钟没收到任何推送" % stale_min
            else:
                reason = ""

            with _lock:
                prev = HEALTH["healthy"]
                HEALTH.update({"healthy": healthy, "logged_in": None,
                               "last_check": time.time(), "last_error": reason})
            # 只在状态翻转时告警，避免每个周期刷一条
            if prev is not None and prev != healthy:
                if not healthy:
                    push_webhook("⚠️ 消息网关告警\n原因：%s\n时间：%s"
                                 % (reason, time.strftime("%Y-%m-%d %H:%M:%S")))
                    log.warning("健康检查: 异常 -> %s", reason)
                elif hc.get("alert_on_recovery", True):
                    push_webhook("✅ 消息网关恢复\n时间：%s"
                                 % time.strftime("%Y-%m-%d %H:%M:%S"))
                    log.info("健康检查: 已恢复正常")
        time.sleep(interval)


# ============================ 工作流引擎接入 ============================
def _wf_send(sid, text, quote_id="", mention=None):
    """工作流引擎的发送出口：发成功计入回复统计。

    quote_id = 触发消息的 msg_id，非空就引用它回复（AI 回复默认开着）。
    ⚠️ 引用只有企微代运营渠道支持，别的渠道语聚直接忽略这个字段 ——
    不会报错，就是没引用效果，所以这里不做渠道判断。

    mention 要的是**企微那套**人 id 的数组(external_contact_id)，实测过。
    语聚会拿它去群成员表里查，查不到就整条发送失败(503 "the mention user(x)
    is not in the room") —— 等于为了一个 @ 把整条回复搞没了。正常情况不会
    发生(要 @ 的人就是刚在这个群说话的人)，但人退群、id 对不上都可能踩到，
    所以带 mention 发失败时**摘掉它重发一次**：正文里的 "@张三" 前缀还在，
    只是没有真 @ 的红点。
    """
    ok, err = send_text_ex(sid, text, quote_id=quote_id, mention=mention)
    if not ok and mention:
        log.warning("带 @ 发送失败(%s)，摘掉 mention 重发 —— %s 多半已经不在这个"
                    "群里了（正文里的 @昵称 前缀不受影响）", err, mention)
        ok, _ = send_text_ex(sid, text, quote_id=quote_id)
    if ok:
        with _lock:
            STATS["forwarded"] += 1
    return ok


_PUSH_SEEN = False

def on_message(msg):
    """收到一条推送后的分发入口。"""
    global _PUSH_SEEN
    if not _PUSH_SEEN:
        _PUSH_SEEN = True
        log.info("首次收到语聚推送 (type=%s) —— 回调链路正常", msg.get("type"))
    outer = msg.get("type")
    if outer != PUSH_CHAT:              # 只处理聊天消息(100)，其余(好友申请/掉线等)先忽略
        return
    # 先入库+SSE 广播(含自己发的，用于展示)
    try:
        store_message(msg)
    except Exception as e:
        log.exception("存储消息异常: %s", e)
    if msg.get("is_self_msg") == 1:     # 自己发的: 不计接收、不触发工作流(防死循环)
        return
    with _lock:
        STATS["received"] += 1
        STATS["last_msg_at"] = time.time()
    # 工作流触发(条件在引擎里判断；动作在后台 worker 执行，不阻塞推送线程)
    if WF:
        try:
            WF.handle(msg)
        except Exception as e:
            log.exception("工作流触发异常: %s", e)


# ============================ 语聚(集简云)webhook 接入 ============================
def _jjy_seen(ev):
    """按 event_id 判重。返回 True = 这条已经处理过，丢弃。

    语聚收不到 200 会重推，重推带的是**同一个 event_id**，所以这是可靠的去重键。
    """
    if not ev:
        return False
    with _lock:
        if ev in SEEN_SET:
            return True
        # deque 满了会自动挤掉最老的一条，SEEN_SET 必须跟着扔，否则只涨不降
        if len(SEEN_EVENTS) == SEEN_EVENTS.maxlen:
            SEEN_SET.discard(SEEN_EVENTS[0])
        SEEN_EVENTS.append(ev)
        SEEN_SET.add(ev)
    return False


def _capture_jjy_raw(body, msg, reason="", raw_text=None):
    """留一份语聚推送原文给管理后台的"原始推送"面板。

    打开抓取后，**进到这个端口的每一条**都留档，包括三类原本会被静默丢掉的：
    未启用 / 白名单拒绝 / event_id 重复(reason 传拦截原因)。不然那三类就是
    黑箱 —— 看起来像"根本没收到推送"，其实收到了只是被挡了。
    注意只是"也记一份"，拦截逻辑本身不动。

    msg=None 且没有 reason，表示过了闸但归一化不出来(不是消息事件)。

    默认关闭(debug.raw_push) —— 原文含客户对话全文，公网环境开之前想清楚。
    ⚠️ 被拒绝的报文是**未经认证的外部输入**，前端渲染处已做 HTML 转义；
    环形缓冲有上限，最坏情况是被刷屏挤掉真数据，所以查完记得关。
    """
    dbg = CONFIG.get("debug") or {}
    if not dbg.get("raw_push"):
        return
    if raw_text is not None and not body:
        # 解析不出 JSON 的请求：解析结果是空 dict，原文只剩这里有。
        # 留一条空记录等于什么都没说 —— 这个面板的全部意义就是"到底收到了什么"。
        raw = raw_text or "(空请求体)"
    else:
        try:
            raw = json.dumps(body, ensure_ascii=False, indent=1)
        except Exception:
            raw = repr(body)
    if len(raw) > 20000:
        raw = raw[:20000] + "\n… (已截断)"
    typ = int(((msg or {}).get("jjy") or {}).get("mt") or 0)
    ev = str((body or {}).get("event_type") or "?") if isinstance(body, dict) else "?"
    name = ("已拦截 · " + reason if reason else
            TYPE_NAME.get(typ, "已归一化") if msg else "未处理 · " + ev)
    with _lock:
        _RAW_SEQ[0] += 1
        item = {"seq": _RAW_SEQ[0], "t": time.time(), "type": typ,
                "name": name, "handled": bool(msg), "raw": raw}
        RAW_PUSHES.append(item)
        cap = int(dbg.get("raw_max") or 300)
        while len(RAW_PUSHES) > cap:
            RAW_PUSHES.popleft()
    BUS.publish({"type": "raw_push", "item": item})


def jjy_worker():
    """语聚报文 -> 归一化 -> 入库/SSE/工作流。和 HTTP 线程解耦，慢了也不会拖出重推。"""
    while True:
        body = JJY_Q.get()
        try:
            cfg = CONFIG.get("jjy") or {}
            msg = jjy.normalize(body, bot_name=cfg.get("bot_name") or "")
            if not msg:
                # 语聚除了消息推送还会发别的事件(实测有 chat_finish=对话结束)，
                # 那不是解析失败, 单独计数, 免得看着像在丢消息
                ev = str(body.get("event_type") or "")
                other = ev != "ai_assistant_receives_msg"
                with _lock:
                    JJY_STATS["ignored" if other else "bad"] += 1
                    seen_n = JJY_EVENTS.get(ev, 0) if other else 1
                    if other:
                        JJY_EVENTS[ev] = seen_n + 1
                # 每种事件类型第一次出现时把报文打进日志。ignored 只是个计数，
                # 光看数字根本不知道被丢的是什么 —— 而"被丢的"里可能就有你要的东西。
                if other and seen_n == 0:
                    log.info("未处理的事件类型 event_type=%r，报文: %s", ev,
                             json.dumps(body, ensure_ascii=False)[:800])
                _capture_jjy_raw(body, None)
                continue
            # 会话显示名：DLL 那套通讯录接口没了，只能从每条推送里攒 chat_title。
            # 群聊 store_message 查的是 NAME_MAP[sid]，私聊查的是 _private_peer_id(sid)
            # —— 语聚的 chat_id 里没有下划线，剥掉前缀就是它，所以两个 key 都写一份。
            title = jjy.session_title(body)
            sid = msg["user_id"]
            with _lock:
                if title:
                    NAME_MAP[sid] = title
                    NAME_MAP[jjy.raw_chat_id(sid)] = title
                if msg.get("sender_name") and msg.get("sender"):
                    NAME_MAP[msg["sender"]] = msg["sender_name"]
                # 工作流按"内部/外部"过滤时查的是 CONTACT_TYPE，回填一份
                if msg.get("sender") and msg.get("sender_external") is not None:
                    CONTACT_TYPE[msg["sender"]] = bool(msg["sender_external"])
            _capture_jjy_raw(body, msg)
            on_message(msg)
            # 会话的**身份**只在推送里出现，见到就落一次库(值没变的不写)。
            # 必须放在 on_message 之后 —— 会话行是那时候才建出来的。
            # 群的身份是 imRoomId，私聊的身份是对端「人」的 id；会话行的主键
            # (聚合 chat_id)是**地址**，会变，身份不会 —— 详见 store.py 的
            # 「身份 ↔ 地址」一节。
            jy = msg.get("jjy") or {}
            if jy.get("room_id") and STORE:
                key = (sid, jy["room_id"], jy.get("bot_id") or "")
                with _lock:
                    known = ROOM_LINKED.get(sid) == key
                    if not known:
                        ROOM_LINKED[sid] = key
                if not known:
                    _take_over("群 " + (jy.get("room_id") or ""), sid,
                                STORE.link_room(sid, jy["room_id"],
                                                jy.get("bot_id") or ""))
            # 私聊：对端就是发送人。自己发的那条对端是机器人自己，跳过。
            peer = str((jy.get("addition") or {}).get("external_contact_id") or "")
            if peer and STORE and sid.startswith("S:") and not msg.get("is_self_msg"):
                with _lock:
                    known = PEER_LINKED.get(sid) == peer
                    if not known:
                        PEER_LINKED[sid] = peer
                if not known:
                    _take_over("联系人 " + peer, sid, STORE.link_peer(sid, peer))
        except Exception as e:
            log.exception("语聚报文处理异常: %s", e)
        finally:
            JJY_Q.task_done()


# ============================ HTTP 服务 ============================
def _qs_int(qs, key):
    v = (qs.get(key) or [None])[0]
    try:
        return int(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None

class Handler(BaseHTTPRequestHandler):
    # ---------- 工具 ----------
    def _send_json(self, obj, code=200):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_body_text(self):
        """请求体原文。webhook 入口要它 —— 解析失败时 body 是空 dict，
        原文只剩这里有，而"原始推送"面板的全部意义就是看到底收到了什么。"""
        n = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(n).decode("utf-8", "ignore") if n else ""

    def _read_json(self):
        raw = self._read_body_text()
        try:
            return json.loads(raw) if raw else {}
        except Exception:
            return {}

    def _send_html(self, file):
        try:
            with open(file, "rb") as f:
                data = f.read()
        except Exception:
            data = ("<h1>%s not found</h1>" % os.path.basename(file)).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        # 页面是每次请求现读磁盘的，就是为了 git pull 完立刻生效 —— 别让浏览器
        # 缓存把这点好处吃掉，不然又是"页面看着是新的、其实是旧的"那种鬼故事
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):   # 静音默认访问日志
        pass

    # ---------- 媒体 ----------
    def _serve_media(self, mid, qs):
        """GET /media/<uuid>[?dl=1] —— 回媒体本体。

        安全约束：
          * id 是 uuid，只能从库里查到路径，绝不把 URL 参数拼进文件路径
          * realpath 必须落在 media 目录内(防符号链接/穿越)
          * 只有白名单图片 MIME 才 inline，其余一律 attachment
            (SVG/HTML 内联渲染 = 同源存储型 XSS)
        """
        mid = (mid or "").strip("/")
        if not mid or not mid.isalnum() or len(mid) > 64:
            return self._send_json({"error": "bad id"}, 400)
        m = STORE.get_media(mid) if STORE else None
        if not m:
            return self._send_json({"error": "not found"}, 404)
        if m["state"] != MS_OK:
            # 还没下载好：回状态让前端显示进度/触发按需下载，而不是给个死链
            return self._send_json({"state": m["state"], "err": m["err"],
                                    "file_name": m["file_name"],
                                    "size": m["size"], "kind": m["kind"]}, 409)
        if not m["path"]:
            # link 模式：本地没有文件，302 到语聚给的原始 URL。
            # 用 302(临时)而非 301 —— 将来若改成 download 模式，同一个 /media/<id>
            # 要能改回吐本地文件；301 会被浏览器永久缓存，改不回来。
            url = (STORE.get_media_cdn(mid) or {}).get("url") or ""
            if url.startswith(("http://", "https://")):
                self.send_response(302)
                self.send_header("Location", url)
                self.send_header("Referrer-Policy", "no-referrer")   # 别把本站地址带给第三方
                self.send_header("Cache-Control", "private, max-age=300")
                self.end_headers()
                return
            return self._send_json({"state": m["state"], "err": "无本地文件也无直连 url",
                                    "file_name": m["file_name"],
                                    "size": m["size"], "kind": m["kind"]}, 409)
        rp = os.path.realpath(m["path"])
        root = os.path.realpath(_media_root())
        if not rp.startswith(root + os.sep):
            log.error("媒体路径越界，拒绝: %s", rp)
            return self._send_json({"error": "forbidden"}, 403)
        try:
            total = os.path.getsize(rp)
        except OSError:
            STORE.set_media(mid, state=MS_FAIL, err="本地文件已丢失")
            return self._send_json({"error": "file missing"}, 410)

        mime = m["mime"] or "application/octet-stream"
        inline = (not (qs.get("dl") or [""])[0]) and mime in INLINE_MIME
        name = m["file_name"] or mid
        disp = ("inline" if inline else "attachment") + \
               "; filename*=UTF-8''" + quote(name, safe="")

        # Range：大文件断点续传 / 视频拖动进度条
        start, end, code = 0, total - 1, 200
        rng = self.headers.get("Range", "")
        if rng.startswith("bytes="):
            try:
                s, _, e = rng[6:].split(",")[0].partition("-")
                if s:
                    start = int(s)
                    end = int(e) if e else total - 1
                else:                                  # bytes=-N 取末尾 N 字节
                    start = max(0, total - int(e))
                if start > end or start >= total:
                    self.send_response(416)
                    self.send_header("Content-Range", "bytes */%d" % total)
                    self.end_headers()
                    return
                end = min(end, total - 1)
                code = 206
            except (ValueError, TypeError):
                start, end, code = 0, total - 1, 200

        length = end - start + 1
        self.send_response(code)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(length))
        self.send_header("Content-Disposition", disp)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "private, max-age=86400")
        if code == 206:
            self.send_header("Content-Range",
                             "bytes %d-%d/%d" % (start, end, total))
        self.end_headers()
        try:
            with open(rp, "rb") as f:                  # 分块发，别把大文件读进内存
                f.seek(start)
                left = length
                while left > 0:
                    buf = f.read(min(262144, left))
                    if not buf:
                        break
                    self.wfile.write(buf)
                    left -= len(buf)
        except OSError:                                # 客户端中途断开
            pass

    # ---------- SSE ----------
    def _serve_events(self):
        """GET /gw/events —— SSE 长连接，实时把新消息推给前台。"""
        q = BUS.subscribe()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")   # 反代(nginx)下禁用缓冲
            self.end_headers()
            self.wfile.write(b"retry: 3000\n\n")          # 客户端断线 3s 后重连
            self.wfile.flush()
            while True:
                try:
                    ev = q.get(timeout=15)
                    buf = ("data: " + json.dumps(ev, ensure_ascii=False)
                           + "\n\n").encode("utf-8")
                except queue.Empty:
                    buf = b": ping\n\n"                   # 心跳, 顺便探测断连
                self.wfile.write(buf)
                self.wfile.flush()
        except OSError:      # 客户端断开(BrokenPipe/ConnectionReset/Aborted)
            pass
        finally:
            BUS.unsubscribe(q)

    # ---------- GET ----------
    def do_GET(self):
        u = urlparse(self.path)
        p = u.path.rstrip("/")
        if p in ("", "/"):
            return self._send_html(INDEX_FILE)
        if p == "/admin":
            return self._send_html(ADMIN_FILE)
        if p.startswith("/media/"):
            return self._serve_media(p[7:], parse_qs(u.query))
        if p == "/gw/events":
            return self._serve_events()
        if p == "/gw/status":
            with _lock:
                return self._send_json({
                    "health": dict(HEALTH),
                    "stats": dict(STATS),
                    "store": STORE.stats() if STORE else {},
                    "workflow": WF.stats() if WF else {},
                    "sse_clients": BUS.count(),
                    "jjy": dict(JJY_STATS, queued=JJY_Q.qsize(),
                                events=dict(JJY_EVENTS),
                                enabled=bool((CONFIG.get("jjy") or {}).get("enabled"))),
                    "uptime": int(time.time() - STATS["started_at"]),
                    "config": {"listen_addr": CONFIG.get("listen_addr"),
                               "listen_port": CONFIG.get("listen_port")},
                })
        if p == "/gw/config":
            return self._send_json(_public_config())
        if p == "/gw/workflows":
            return self._send_json({"workflows": WF.list() if WF else []})
        if p == "/gw/workflow-runs":
            # 游标分页：?limit=50[&before=seq][&wf=<工作流id>]
            qs = parse_qs(u.query)
            if not WF:
                return self._send_json({"runs": [], "has_more": False})
            return self._send_json(WF.run_list(
                before=_qs_int(qs, "before"),
                limit=_qs_int(qs, "limit") or 50,
                wf_id=(qs.get("wf") or [""])[0] or None))
        if p == "/gw/logs":
            return self._send_json({"logs": list(LOGS)[-200:]})
        if p == "/gw/raw-pushes":
            # ?type=11042 只看某一类; ?after=seq 增量拉(配合 SSE 兜底)
            qs = parse_qs(u.query)
            with _lock:
                items = list(RAW_PUSHES)
            ft = _qs_int(qs, "type")
            if ft is not None:
                items = [i for i in items if i["type"] == ft]
            af = _qs_int(qs, "after")
            if af is not None:
                items = [i for i in items if i["seq"] > af]
            # 类型直方图：面板上做筛选下拉用
            hist = {}
            with _lock:
                for i in RAW_PUSHES:
                    k = str(i["type"])
                    hist[k] = hist.get(k, 0) + 1
            return self._send_json({
                "list": items[-300:],
                "hist": hist,
                "names": {str(k): v for k, v in TYPE_NAME.items()},
                "on": bool((CONFIG.get("debug") or {}).get("raw_push"))})
        # 通讯录/群列表：webhook 模式下没有这类接口，返回空列表 + 说明，
        # 让前端页面能正常渲染而不是报错。显示名靠推送累积(见 NAME_MAP)。
        if p == "/gw/groups":
            force = (parse_qs(u.query).get("refresh") or ["0"])[0] == "1"
            lst, err = groups_for_picker(force)
            return self._send_json({"list": lst, "error": err,
                                    "unpaired": sum(1 for x in lst if not x["paired"])})
        if p == "/gw/contacts":
            force = (parse_qs(u.query).get("refresh") or ["0"])[0] == "1"
            lst, err = contacts_for_picker(force)
            return self._send_json({"list": lst, "error": err,
                                    "unpaired": sum(1 for x in lst if not x["paired"])})
        if p == "/gw/sessions":
            return self._send_json({"list": STORE.get_sessions()})
        if p == "/gw/messages":
            # 游标分页：?session=xx&limit=50[&before=seq|&after=seq]
            qs = parse_qs(u.query)
            sid = (qs.get("session") or [""])[0]
            if not sid:
                return self._send_json({"error": "缺少 session"}, 400)
            msgs, has_more = STORE.get_messages(
                sid, before=_qs_int(qs, "before"),
                after=_qs_int(qs, "after"),
                limit=_qs_int(qs, "limit") or 50)
            return self._send_json({"list": msgs, "has_more": has_more})
        return self._send_json({"error": "not found"}, 404)

    # ---------- POST ----------
    def do_POST(self):
        p = self.path.split("?")[0].rstrip("/")

        # ---------- 语聚(集简云)聚合对话推送入口 ----------
        # 三条铁律，缺一条都会出问题：
        #   1) company_id 白名单 —— 语聚不签名，这是唯一的身份校验
        #   2) event_id 去重     —— 重推带同一个 id
        #   3) 立刻回 200        —— ThreadingHTTPServer 一连接一线程，
        #                           在这儿做重活会超时->重推->消息重复
        if p == "/gw/jjy-hook":
            cfg = CONFIG.get("jjy") or {}
            # 先读 body：下面每一道闸被拦时都要留一份原文(开了抓取的话)，
            # 否则被拦的那些就是黑箱 —— 看着像没收到推送，其实是收到了被挡了。
            # text 单独留着：解析不出 JSON 时 body 是空 dict，原文只剩它。
            text = self._read_body_text()
            try:
                body = json.loads(text) if text.strip() else {}
            except Exception:
                body = {}
            if not isinstance(body, dict):
                body = {}
            if not cfg.get("enabled"):
                _capture_jjy_raw(body, None, "webhook 接收未启用", text)
                return self._send_json({}, 200)          # 未启用: 静默吞掉
            if not body:
                _capture_jjy_raw(body, None, "请求体不是合法 JSON", text)
                return self._send_json({}, 200)
            allow = cfg.get("allow_company") or []
            if allow and str(body.get("company_id") or "") not in allow:
                with _lock:
                    JJY_STATS["rejected"] += 1
                _capture_jjy_raw(body, None,
                                 "company_id 不在白名单: %r" % body.get("company_id"), text)
                # 故意回 200 而不是 403：不给扫描者任何"这里有东西"的反馈
                return self._send_json({}, 200)
            if _jjy_seen(str(body.get("event_id") or "")):
                with _lock:
                    JJY_STATS["dup"] += 1
                _capture_jjy_raw(body, None, "event_id 重复", text)
                return self._send_json({}, 200)
            with _lock:
                JJY_STATS["received"] += 1
                JJY_STATS["last_at"] = time.time()
            JJY_Q.put(body)
            return self._send_json({}, 200)

        # 管理 API
        if p == "/gw/config":
            body = self._read_json()
            # 前端拿到的 apiKey 是打过码的，原样回传时当"不修改"处理，
            # 否则用户改个别的字段就会把真 key 覆盖成一串星号。
            if isinstance(body.get("jjy"), dict):
                for k in SECRET_KEYS:
                    if body["jjy"].get(k) == API_KEY_MASK:
                        body["jjy"].pop(k)
            with _lock:
                for k, v in body.items():
                    if k not in DEFAULT_CONFIG:
                        continue
                    if isinstance(v, dict) and isinstance(CONFIG.get(k), dict):
                        CONFIG[k] = _deep_merge(CONFIG.get(k, {}), v)
                    else:
                        CONFIG[k] = v
                save_config()
            log.info("配置已更新")
            return self._send_json({"ok": True, "config": _public_config()})

        if p == "/gw/workflows":
            body = self._read_json()
            wfs = body.get("workflows")
            if not isinstance(wfs, list):
                return self._send_json({"ok": False, "error": "workflows 必须是数组"}, 400)
            for w in wfs:
                if not w.get("id"):
                    w["id"] = "wf_" + uuid.uuid4().hex[:8]
            saved = WF.save(wfs)
            log.info("工作流已保存, 共 %d 条", len(saved))
            # 回传用 list()：带上 next_fire / 累计统计，前端卡片才能直接显示"下次触发"
            return self._send_json({"ok": True, "workflows": WF.list()})

        if p == "/gw/workflow-test":
            body = self._read_json()
            wf = body.get("workflow")
            if not isinstance(wf, dict):
                return self._send_json({"ok": False, "error": "缺少 workflow"}, 400)
            try:
                out = WF.test(wf, body.get("sample") or {},
                              run_http=bool(body.get("run_http", True)))
                out["ok"] = True
                return self._send_json(out)
            except Exception as e:
                log.exception("工作流测试异常: %s", e)
                return self._send_json({"ok": False, "error": str(e)}, 500)

        if p == "/gw/members":
            b = self._read_json()
            gids = b.get("group_ids")
            if isinstance(gids, list):        # 批量：{map:{群id:[成员]}, errors:{群id:原因}}
                mp, errs, err = members_of_many(
                    [str(x) for x in gids if x], bool(b.get("refresh")))
                return self._send_json({"map": mp, "errors": errs, "error": err,
                                        "cached": not b.get("refresh")})
            lst, err = members_of(str(b.get("group_id") or ""), bool(b.get("refresh")))
            return self._send_json({"list": lst, "cached": not b.get("refresh"),
                                    "error": err})

        if p == "/gw/messages":       # 兼容旧调用形态(参数走 body)
            b = self._read_json()
            sid = str(b.get("session") or "")
            if not sid:
                return self._send_json({"error": "缺少 session"}, 400)
            msgs, has_more = STORE.get_messages(
                sid, before=b.get("before"), after=b.get("after"),
                limit=b.get("limit") or 50)
            return self._send_json({"list": msgs, "has_more": has_more})

        if p == "/gw/raw-clear":      # 清空原始推送缓冲
            with _lock:
                n = len(RAW_PUSHES)
                RAW_PUSHES.clear()
            return self._send_json({"ok": True, "cleared": n})

        if p == "/gw/media-fetch":    # 按需下载(超阈值的大文件 / 失败重试 / 取原图)
            b = self._read_json()
            mid = str(b.get("id") or "")
            m = STORE.get_media(mid) if (STORE and mid) else None
            if not m:
                return self._send_json({"ok": False, "error": "媒体不存在"}, 404)
            if m["state"] in (MS_OK, MS_DOING):
                return self._send_json({"ok": True, "state": m["state"]})
            # 立刻返回，真正的下载交给 worker；前端等 SSE 的 media 事件
            # (下载可能几十秒，绝不能阻塞 HTTP 线程 —— ThreadingHTTPServer 一连接一线程)
            STORE.set_media(mid, state=MS_PENDING, err="", tries=0)
            MEDIA_Q.put(mid)
            return self._send_json({"ok": True, "state": MS_PENDING})

        if p == "/gw/read":           # 显式清未读(打开会话/窗口聚焦时前台调用)
            sid = str(self._read_json().get("session") or "")
            if sid:
                STORE.mark_read(sid)
                BUS.publish({"type": "read", "session_id": sid})
            return self._send_json({"ok": True})

        if p == "/gw/send":
            b = self._read_json()
            sid = str(b.get("session", "")); text = b.get("text", "")
            if not sid or not text:
                return self._send_json({"ok": False, "error": "缺少 session 或 text"}, 400)
            ok, err = send_text_ex(sid, text, quote_id=b.get("quote") or "",
                                   mention=b.get("mention"))
            return self._send_json({"ok": ok, "error": err})

        if p == "/gw/revoke":
            b = self._read_json()
            sid = str(b.get("session") or ""); mid = str(b.get("msg_id") or "")
            ok, err = jjy_revoke(sid, mid)
            if ok:
                # 语聚不会给撤回回声，本地自己标记 + 广播，不然界面不动
                m, s = STORE.revoke_message(mid, sid)
                if m:
                    BUS.publish({"type": "revoke", "session_id": sid,
                                 "message": m, "session": s})
            return self._send_json({"ok": ok, "error": err})

        if p == "/gw/sync-contacts":
            gl, ge = groups_for_picker(force=True)
            cl, ce = contacts_for_picker(force=True)
            err = ge or ce
            return self._send_json({"ok": bool(gl or cl) or not err,
                                    "groups": len(gl), "friends": len(cl),
                                    # 只算不改，要不要改由前端问过用户再调下面那个
                                    "migratable": sender_id_migration(cl),
                                    "unpaired": sum(1 for x in gl + cl if not x["paired"]),
                                    "error": err})

        if p == "/gw/migrate-senders":    # 用户确认后才真改 workflows.json
            cl, ce = contacts_for_picker()
            plan = sender_id_migration(cl, apply=True)
            return self._send_json({"ok": True, "migrated": len(plan),
                                    "plan": plan, "error": ce})

        if p == "/gw/test-webhook":
            ok, err = push_webhook("🔔 vworkApi 网关 webhook 测试消息\n时间：%s"
                                   % time.strftime("%Y-%m-%d %H:%M:%S"))
            return self._send_json({"ok": ok, "error": err})

        if p == "/gw/test-send":
            b = self._read_json()
            ok, err = send_text_ex(b.get("session") or b.get("user_id", ""),
                                   b.get("msg", "测试消息"))
            return self._send_json({"ok": ok, "error": err})

        if p == "/gw/check-apikey":
            # 官方的校验接口。⚠️ 它**不守** Code/Data/Msg 那套约定，直接回
            # {"success": true} / 401 —— 所以这里不能走 _jjy_call。
            #
            # 前端传的是输入框里的当前值 —— 验"刚填的"而不是"已存的"，免得改完没保存
            # 就点校验，验的却是旧 key。传打码占位符(= 没动过输入框)时退回用已存的。
            key = str(self._read_json().get("api_key") or "").strip()
            if key == API_KEY_MASK:
                key = ""
            if not (key or ((CONFIG.get("jjy") or {}).get("api_key") or "").strip()):
                return self._send_json({"ok": False, "error": "还没填 apiKey"})
            try:
                req = urllib.request.Request(_jjy_url("/v1/openapi/check", key),
                                             method="GET")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    j = json.loads(resp.read().decode("utf-8", "ignore") or "{}")
                return self._send_json({"ok": bool(j.get("success")),
                                        "error": "" if j.get("success") else "语聚未确认有效"})
            except urllib.error.HTTPError as e:
                return self._send_json({"ok": False, "error": (
                    "apiKey 无效或权限不足（401）" if e.code == 401 else "HTTP %s" % e.code)})
            except Exception as e:
                return self._send_json({"ok": False, "error": "没连上语聚：%s" % e})

        return self._send_json({"error": "not found"}, 404)


def _resolve(path):
    """相对路径按本程序目录解析，绝对路径原样。"""
    if not path:
        return ""
    return path if os.path.isabs(path) else os.path.join(BASE_DIR, path)

def main():
    global STORE, WF, SEEN_EVENTS
    load_config()
    port = int(CONFIG.get("listen_port", 9000))
    st = CONFIG.get("storage") or {}
    STORE = Store(_resolve(st.get("db_file") or "gateway.db"),
                  st.get("max_msg_per_session", 5000),
                  st.get("max_workflow_runs", 0), log=log)
    directory_boot()        # 通讯录/群列表：先读库直接可用，后台再去上游更新
    try:                    # 启动先扫一次，别等第一个小时的定时清理
        n = STORE.ai_ctx_gc(st.get("ai_context_days", 7))
        if n:
            log.info("AI 上下文清理: 删除超过 %s 天的 %d 行",
                     st.get("ai_context_days", 7), n)
    except Exception as e:
        log.warning("AI 上下文清理失败: %s", e)
    WF = WorkflowEngine(BASE_DIR, _wf_send, log,
                        resolve_name=lambda i: NAME_MAP.get(i, i),
                        classify=classify_sender, store=STORE)
    WF.load()
    threading.Thread(target=health_loop, daemon=True).start()

    jc = CONFIG.get("jjy") or {}
    SEEN_EVENTS = deque(maxlen=max(100, int(jc.get("dedup_max") or 4000)))
    # worker 无条件常驻：enabled 是在 HTTP 入口处判断的，这样管理页上开关一拨就生效，
    # 不用重启。worker 平时阻塞在队列上，不占 CPU。
    threading.Thread(target=jjy_worker, name="jjy", daemon=True).start()
    if jc.get("enabled") and not (jc.get("allow_company") or []):
        log.warning("语聚 webhook 已启用但 allow_company 为空 —— 任何人 POST 都会被当成真消息，"
                    "公网环境务必配上 company_id 白名单")

    # 媒体下载 worker + 清理线程。重启后把上次没下完的重新排队(含 state=1 的中断项)
    # gc 线程无条件起：它现在还管 AI 上下文的按天过期，那件事和媒体开不开无关
    threading.Thread(target=media_gc, name="gc", daemon=True).start()
    mc = _mcfg()
    if mc.get("enabled", True):
        for i in range(max(1, int(mc.get("workers") or 1))):
            threading.Thread(target=media_worker, name="media-%d" % i,
                             daemon=True).start()
        resume = STORE.pending_media()
        for mid in resume:
            MEDIA_Q.put(mid)
        if resume:
            log.info("恢复未完成的媒体下载 %d 条", len(resume))

    s = STORE.stats()
    addr = CONFIG.get("listen_addr") or "0.0.0.0"
    log.info("=" * 48)
    log.info("聚合对话消息网关已启动")
    log.info("会话工作台:  http://%s:%s/", addr, port)
    log.info("管理后台  :  http://%s:%s/admin", addr, port)
    log.info("语聚回调  :  POST /gw/jjy-hook  (%s)",
             "已启用, 白名单 %s" % (jc.get("allow_company") or "**未配置(危险)**")
             if jc.get("enabled") else "未启用")
    log.info("消息库    :  %s (会话 %d, 历史消息 %d)",
             _resolve(st.get("db_file") or "gateway.db"), s["sessions"], s["messages"])
    # 保留策略当面说清楚：config.json 里显式写着 2000 的老配置，光改默认值没用
    mr = int(st.get("max_workflow_runs") or 0)
    log.info("运行记录  :  共 %d 条, %s", s["workflow_runs"],
             ("只保留最近 %d 条 —— 想全留把 storage.max_workflow_runs 设成 0" % mr)
             if mr > 0 else "全部保留")
    log.info("AI 上下文 :  %s",
             ("保留 %d 天" % st["ai_context_days"]) if int(st.get("ai_context_days") or 0) > 0
             else "不过期（这张表就没人打扫了）")
    if mc.get("enabled", True):
        log.info("媒体      :  %s (图片自动下载=%s, 文件≤%sMB 自动下载)",
                 _media_root(), "开" if mc.get("image_auto", True) else "关",
                 mc.get("auto_download_mb"))
    log.info("工作流    :  共 %d 条, 启用 %d 条", WF.stats()["count"], WF.stats()["enabled"])
    log.info("=" * 48)
    try:
        ThreadingHTTPServer((CONFIG.get("listen_addr") or "0.0.0.0", port),
                            Handler).serve_forever()
    finally:
        if STORE:
            STORE.close()


if __name__ == "__main__":
    main()
