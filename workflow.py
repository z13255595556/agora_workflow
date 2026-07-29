# -*- coding: utf-8 -*-
"""
workflow.py — 消息工作流引擎 (纯标准库)
=====================================
消息 = 触发器。每条工作流：

  触发条件(全部可选，不填=不限)：
    sessions   指定会话(群 R: / 私聊 S:)
    senders    指定发送人。聚合对话推送的 user_id 和企微群成员接口的
               imContactId 是两套 id，选人面板给的是后者 —— 所以两个都比(见 _match)
    at_me      是否@了托管账号: any(不限) / yes(必须@) / no(必须未@)   仅群聊有意义
    msg_types  消息类型(2=文本…)
    content    内容匹配: none / any(任一关键词) / all(全部关键词) / regex(正则)

  动作(按顺序执行，互相隔离，一个失败不影响其它)：
    reply      自定义回复(支持模板变量)。发到**来源会话** —— 不需要配任何 id，
               推送里带的 chat_id 永远是当前有效的那个
    http       调用接口(GET/POST、自定义头/体、超时、重试)，
               可把返回内容(支持 JSON 路径提取)按模板回复到来源会话
    ai         AI 回复。和 http 的区别只有一个：请求体不用你写，引擎按这个人
               和机器人之前聊过的话拼 messages[]。上下文存在 ai_context 表，
               键是(工作流, 会话, 人) —— 群里两个人同时@机器人各聊各的。
               默认走百炼(DashScope)应用接口，关掉开关就是 OpenAI 兼容形态

  高可用设计：
    * 动作在后台 worker 线程池执行，DLL 推送线程永不被 HTTP 阻塞
    * HTTP 超时 + 自动重试(退避)；响应体积/回复长度有上限
    * 每工作流冷却时间(cooldown_sec) + 每分钟触发上限(max_per_min)，防风暴/防死循环
    * 自己发出的消息永不触发(防回复死循环)
    * 运行记录(环形 200 条)供管理页排查

模板变量：{content} {sender} {sender_name} {source} {source_name} {time} {date}
回复模板额外支持 {result}(接口返回/提取值)。
"""

import datetime
import json
import os
import re
import time
import queue
import threading
import urllib.request
import urllib.error
import urllib.parse
from collections import deque

MAX_RESP_BYTES = 200_000     # 接口响应最多读取字节数
MAX_REPLY_LEN  = 1800        # 回复消息最大长度(超出截断)
WORKER_COUNT   = 3
SCHED_TICK     = 20          # 定时调度扫描间隔(秒)
SCHED_GRACE    = 600         # 错过超过这个秒数的时间槽就不再补触发(防重启后刷历史任务)


def _now():
    return int(time.time())


def _norm_hhmm(v, default="09:00"):
    """把用户填的时间规整成 HH:MM，非法则回落默认值。"""
    m = re.match(r"^\s*(\d{1,2})\s*[:：]\s*(\d{1,2})\s*$", str(v or ""))
    if not m:
        return default
    h, mi = int(m.group(1)), int(m.group(2))
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        return default
    return "%02d:%02d" % (h, mi)


def _norm_date(v):
    """规整成 YYYY-MM-DD，非法/空则返回空串。"""
    m = re.match(r"^\s*(\d{4})\D(\d{1,2})\D(\d{1,2})\s*$", str(v or ""))
    if not m:
        return ""
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return datetime.date(y, mo, d).strftime("%Y-%m-%d")
    except ValueError:
        return ""


class WorkflowEngine:
    def __init__(self, base_dir, send_text, log, resolve_name=None, classify=None, store=None):
        """send_text(sid, text, quote_id="", mention=None)->bool 发消息
        (quote_id 非空则引用原消息；mention 是要 @ 的人 id 列表)；
        resolve_name(id)->str 显示名；
        classify(uid)->0内部/1外部/None未知(用户类型条件用)；
        store: Store 实例，用于持久化运行记录/累计统计(None 则退化为内存 deque)。"""
        self.file       = os.path.join(base_dir, "workflows.json")
        self.rules_file = os.path.join(base_dir, "rules.json")
        self.send_text  = send_text
        self.log        = log
        self.resolve    = resolve_name or (lambda i: i)
        self.classify   = classify or (lambda i: None)
        self.store      = store

        self.workflows = []
        self.runs      = deque(maxlen=200)     # 运行记录
        self.counter   = {"runs": 0, "fails": 0}

        self._lock     = threading.RLock()
        self._q        = queue.Queue(maxsize=2000)
        self._cooldown = {}    # (wf_id, session) -> last_ts
        self._minute   = {}    # wf_id -> deque[ts] 最近一分钟触发
        self._re_cache = {}
        self._serial_q = {}    # wf_id -> Queue  串行工作流的专属队列(一条一条跑)
        self._slots    = {}    # wf_id -> 已触发的时间槽(内存镜像，启动时从库恢复)

        for i in range(WORKER_COUNT):
            threading.Thread(target=self._worker, daemon=True,
                             name="workflow-%d" % i).start()
        threading.Thread(target=self._sched_loop, daemon=True,
                         name="workflow-sched").start()

    # ============ 持久化 / 迁移 ============
    def load(self):
        if os.path.exists(self.file):
            try:
                with open(self.file, "r", encoding="utf-8") as f:
                    raw = json.load(f).get("workflows", [])
                self.workflows = [self._norm(w) for w in raw]
                self._seed_counters()
                return
            except Exception as e:
                self.log.warning("读取 workflows.json 失败: %s", e)
                self.workflows = []
                return
        # 老版 rules.json -> 自动迁移一次
        if os.path.exists(self.rules_file):
            try:
                with open(self.rules_file, "r", encoding="utf-8") as f:
                    rules = json.load(f).get("rules", [])
                self.workflows = [self._from_rule(r) for r in rules if r.get("source_group_id")]
                if self.workflows:
                    self._write()
                    self.log.info("已把 rules.json 的 %d 条转发规则迁移为工作流", len(self.workflows))
            except Exception as e:
                self.log.warning("迁移 rules.json 失败: %s", e)

    def _seed_counters(self):
        """老版本升级上来时，把 workflows.json 里的历史 stats 搬进 wf_counters。
        不搬的话，老工作流下次触发会从 0 重新计数，看起来像统计被清零了。
        只在计数行不存在时写入，重复启动不会累加。"""
        if not self.store:
            return
        try:
            have = self.store.wf_run_stats()
        except Exception as e:
            self.log.warning("读取累计统计失败，跳过历史迁移: %s", e)
            return
        moved = 0
        for w in self.workflows:
            st = w.get("stats") or {}
            if w["id"] in have or not (st.get("runs") or st.get("fails")):
                continue
            try:
                if self.store.seed_counter(w["id"], st.get("runs"),
                                           st.get("fails"), st.get("last_at")):
                    moved += 1
            except Exception as e:
                self.log.warning("迁移历史统计失败 %s: %s", w.get("name"), e)
        if moved:
            self.log.info("已把 %d 条工作流的历史触发统计迁入数据库", moved)

    def _from_rule(self, r):
        kws = [k for k in (r.get("keywords") or []) if k]
        return self._norm({
            "id": "wf_" + str(r.get("id") or "").lstrip("r_"),
            "name": r.get("name") or "迁移的转发规则",
            "enabled": r.get("enabled", True),
            "trigger": {
                "sessions": [r["source_group_id"]],
                "senders": r.get("source_user_ids") or [],
                "at_me": "any", "msg_types": [2],
                "content": {"mode": ("any" if kws else "none") if r.get("keyword_mode") != "all" else "all",
                            "keywords": kws, "regex": ""},
            },
            # 老 rules.json 的转发目标已不再支持(见 _norm)，模板留下，回到来源会话
            "actions": [{"type": "reply",
                         "template": r.get("template") or "【{source_name}】{sender_name}：{content}"}],
        })

    def _norm(self, w):
        """补默认值，保证结构完整。"""
        w = dict(w or {})
        w.setdefault("id", "wf_" + os.urandom(4).hex())
        w.setdefault("name", "未命名工作流")
        w.setdefault("enabled", True)
        t = dict(w.get("trigger") or {})
        # 触发方式：message=收到消息触发(默认，兼容老配置) / schedule=到点定时触发
        t["kind"] = t.get("kind") if t.get("kind") in ("message", "schedule") else "message"
        s = dict(t.get("schedule") or {})
        s["mode"]     = s.get("mode") if s.get("mode") in ("daily", "weekly", "once") else "daily"
        s["time"]     = _norm_hhmm(s.get("time"))
        # 星期留空就是“没选”——不猜默认值，宁可不触发也不在用户没选的日子跑
        s["weekdays"] = sorted({int(x) for x in (s.get("weekdays") or [])
                                if str(x).isdigit() and 1 <= int(x) <= 7})
        s["date"]     = _norm_date(s.get("date"))
        s["text"]     = str(s.get("text") or "")
        t["schedule"] = s
        t["sessions"]  = [s2 for s2 in (t.get("sessions") or []) if s2]
        t["chat_type"] = (t.get("chat_type")
                          if t.get("chat_type") in ("any", "group", "private") else "any")
        t["senders"]   = [s for s in (t.get("senders") or []) if s]
        t["at_me"]     = t.get("at_me") if t.get("at_me") in ("any", "yes", "no") else "any"
        t["sender_type"] = (t.get("sender_type")
                            if t.get("sender_type") in ("any", "internal", "external") else "any")
        t["msg_types"] = [int(x) for x in (t.get("msg_types") or []) if str(x).lstrip("-").isdigit()]
        c = dict(t.get("content") or {})
        c["mode"]     = c.get("mode") if c.get("mode") in ("none", "any", "all", "regex") else "none"
        c["keywords"] = [k for k in (c.get("keywords") or []) if k]
        c["regex"]    = str(c.get("regex") or "")
        t["content"] = c
        w["trigger"] = t
        acts = []
        for a in (w.get("actions") or []):
            a = dict(a or {})
            if a.get("type") == "reply":
                a.pop("targets", None)        # 老的 forward 迁过来时可能带着
                a.setdefault("template", "")
            elif a.get("type") == "http":
                a.setdefault("method", "GET")
                a["method"] = a["method"].upper() if a.get("method", "").upper() in ("GET", "POST") else "GET"
                a.setdefault("url", "")
                a.setdefault("headers", "")
                a.setdefault("body", "")
                a["timeout_sec"] = min(1900, max(1, int(a.get("timeout_sec") or 10)))
                r = a.get("retries")          # 注意: 显式配 0 时不能被默认值 1 吞掉
                a["retries"]     = min(3, max(0, 1 if r in (None, "") else int(r)))
                a["reply"]       = bool(a.get("reply"))
                a.setdefault("reply_path", "")
                a.setdefault("reply_template", "{result}")
                # 「回复到」已下线：接口返回只回消息来源那个会话。语聚发送接口
                # 认的是聚合对话 chat_id，选人面板给的联系人 id 发过去必 503。
                a.pop("reply_to", None)
            elif a.get("type") == "ai":
                # 模型不在这儿配 —— 百炼是在百炼应用/工作流里选的，通用模式
                # 想指定就写进 extra(`{"model":"qwen-plus"}`)，不值得单开一个字段
                a["bailian"] = bool(a.get("bailian", True))
                a.setdefault("url", "")
                a.setdefault("app_id", "")
                a.setdefault("api_key", "")
                a.setdefault("headers", "")
                a.setdefault("extra", "")
                a.setdefault("reply_path", "")
                a["quote"]   = bool(a.get("quote", True))
                a["mention"] = bool(a.get("mention", True))
                a["ctx_mode"] = (a.get("ctx_mode")
                                 if a.get("ctx_mode") in ("count", "time") else "count")
                a["ctx_count"]   = min(100, max(1, int(a.get("ctx_count") or 20)))
                a["ctx_minutes"] = min(1440, max(1, int(a.get("ctx_minutes") or 30)))
                # AI 接口慢，默认给 60 秒(http 动作是 10 秒)
                a["timeout_sec"] = min(1900, max(1, int(a.get("timeout_sec") or 60)))
                r = a.get("retries")
                a["retries"]     = min(3, max(0, 1 if r in (None, "") else int(r)))
                a.setdefault("reply_template", "{result}")
            elif a.get("type") == "forward":
                # 「转发到其他群/人」已下线：语聚只认聚合对话 chat_id，而那个 id
                # 只在对方先说话之后才存在，选人面板给的联系人 id 根本发不出去。
                # 老配置里的 forward 就地转成「自定义回复」——模板留着，目标丢掉。
                self.log.warning("工作流[%s] 的「转发消息」动作已下线，转成回复到"
                                 "来源会话（原转发目标 %s 被丢弃）",
                                 w.get("name") or w.get("id"), a.get("targets") or [])
                a = {"type": "reply", "template": a.get("template") or ""}
            else:
                # 认不出的动作类型只能丢，但**必须喊一声**：admin.html 是每次请求
                # 现读磁盘的，git pull 完不重启 = 新页面配着老引擎，新加的动作
                # 类型会在这儿被静默吃掉，表现成"保存了但动作没了"，极难查。
                self.log.warning("工作流[%s] 有认不出的动作类型 %r，已丢弃 —— "
                                 "多半是代码拉了但进程没重启",
                                 w.get("name") or w.get("id"), a.get("type"))
                continue
            acts.append(a)
        w["actions"] = acts
        # AI 回复靠 @ 触发：面板上这个条件是锁死置灰的，这里再兜一道，
        # 免得直接编辑 workflows.json 绕过去。私聊不受影响(见 _match)。
        if any(x.get("type") == "ai" for x in acts):
            t["at_me"] = "yes"
        w["cooldown_sec"] = min(86400, max(0, int(w.get("cooldown_sec") or 0)))
        w["max_per_min"]  = min(600, max(1, int(w.get("max_per_min") or 30)))
        w["serial"]       = bool(w.get("serial"))
        w.setdefault("stats", {})
        w["stats"] = {"runs": int(w["stats"].get("runs") or 0),
                      "fails": int(w["stats"].get("fails") or 0),
                      "last_at": int(w["stats"].get("last_at") or 0)}
        return w

    def save(self, workflows):
        """整表保存(管理页提交)。返回规范化后的列表。"""
        normed = [self._norm(w) for w in (workflows or [])]
        with self._lock:
            # 保留运行统计(前端不回传或回传旧值都以内存为准)
            old = {w["id"]: w["stats"] for w in self.workflows}
            for w in normed:
                if w["id"] in old:
                    w["stats"] = old[w["id"]]
            self.workflows = normed
            self._write()
        return normed

    def _write(self):
        with open(self.file, "w", encoding="utf-8") as f:
            json.dump({"workflows": self.workflows}, f, ensure_ascii=False, indent=2)

    # ============ 定时触发 ============
    def _sched_loop(self):
        """每 SCHED_TICK 秒扫一遍定时工作流。到点就跑，靠“时间槽”判重：
        同一个槽(如 2026-07-25 09:00)只会触发一次，状态落库，重启不重复也不漏。
        超过 SCHED_GRACE 秒的槽不再补触发——避免网关停了半天、一启动就刷一批历史任务。"""
        if self.store:
            try:
                self._slots = dict(self.store.sched_slots())
            except Exception as e:
                self.log.warning("读取定时状态失败: %s", e)
        while True:
            time.sleep(SCHED_TICK)
            try:
                self._sched_tick()
            except Exception as e:
                self.log.exception("定时调度异常: %s", e)

    def _sched_tick(self, now=None):
        now = now or time.time()
        with self._lock:
            wfs = [dict(w) for w in self.workflows
                   if w.get("enabled") and (w.get("trigger") or {}).get("kind") == "schedule"]
        for wf in wfs:
            slot, due_ts = self._due_slot(wf, now)
            if not slot:
                continue
            if now - due_ts > SCHED_GRACE:        # 错过太久，不补
                continue
            if self._slots.get(wf["id"]) == slot:  # 内存里已触发过
                continue
            if self.store:                         # 落库判重(原子)，抢到才执行
                try:
                    if not self.store.sched_mark(wf["id"], slot, int(now)):
                        self._slots[wf["id"]] = slot
                        continue
                except Exception as e:
                    self.log.warning("写定时状态失败: %s", e)
            self._slots[wf["id"]] = slot
            msg = self._sched_msg(wf, due_ts)
            self.log.info("定时触发工作流[%s] 槽=%s", wf.get("name"), slot)
            if wf.get("serial"):
                self._serial_put(wf, msg)
            else:
                try:
                    self._q.put_nowait((wf, msg))
                except queue.Full:
                    self.log.warning("工作流队列已满，丢弃定时触发: %s", wf.get("name"))
            if (wf["trigger"]["schedule"].get("mode") == "once"):
                self._disable_once(wf["id"])       # 一次性任务触发后自动停用

    @staticmethod
    def _due_slot(wf, now):
        """返回 (slot标识, 该槽的应触发时间戳)；未到点返回 (None, 0)。"""
        s = (wf.get("trigger") or {}).get("schedule") or {}
        hh, _, mm = _norm_hhmm(s.get("time")).partition(":")
        lt = time.localtime(now)
        today = datetime.date(lt.tm_year, lt.tm_mon, lt.tm_mday)
        mode = s.get("mode") or "daily"
        if mode == "once":
            d = _norm_date(s.get("date"))
            if not d:
                return None, 0
            day = datetime.datetime.strptime(d, "%Y-%m-%d").date()
        else:
            day = today
            if mode == "weekly" and day.isoweekday() not in (s.get("weekdays") or []):
                return None, 0
        due = time.mktime((day.year, day.month, day.day,
                           int(hh), int(mm), 0, 0, 0, -1))
        if now < due:
            return None, 0
        return "%s %s" % (day.strftime("%Y-%m-%d"), _norm_hhmm(s.get("time"))), due

    def _disable_once(self, wf_id):
        with self._lock:
            w = next((x for x in self.workflows if x["id"] == wf_id), None)
            if w is not None:
                w["enabled"] = False
                self._write()
        self.log.info("一次性定时任务已自动停用: %s", wf_id)

    @staticmethod
    def _sched_msg(wf, due_ts):
        """定时触发没有来源消息，造一条“虚拟消息”让动作链沿用同一套模板变量。"""
        s = (wf.get("trigger") or {}).get("schedule") or {}
        return {"user_id": "", "sender": "", "sender_name": "定时任务",
                "content": s.get("text") or "", "msg_type": 2,
                "time_stamp": int(due_ts), "at_me": 0, "is_sched": 1}

    def next_fire(self, wf):
        """给页面用：算出下一次触发时间(时间戳)，无则 0。"""
        s = (wf.get("trigger") or {}).get("schedule") or {}
        if (wf.get("trigger") or {}).get("kind") != "schedule":
            return 0
        hh, _, mm = _norm_hhmm(s.get("time")).partition(":")
        now = time.time()
        mode = s.get("mode") or "daily"
        if mode == "once":
            d = _norm_date(s.get("date"))
            if not d:
                return 0
            day = datetime.datetime.strptime(d, "%Y-%m-%d").date()
            due = time.mktime((day.year, day.month, day.day, int(hh), int(mm), 0, 0, 0, -1))
            return int(due) if due > now else 0
        lt = time.localtime(now)
        base = datetime.date(lt.tm_year, lt.tm_mon, lt.tm_mday)
        for i in range(0, 8):
            day = base + datetime.timedelta(days=i)
            if mode == "weekly" and day.isoweekday() not in (s.get("weekdays") or []):
                continue
            due = time.mktime((day.year, day.month, day.day, int(hh), int(mm), 0, 0, 0, -1))
            if due > now:
                return int(due)
        return 0

    # ============ 触发入口 ============
    def handle(self, msg):
        """on_message 调用。msg 为归一化消息(不含自己发的)。"""
        with self._lock:
            # 定时工作流不吃消息，只由 _sched_loop 到点触发
            wfs = [w for w in self.workflows if w.get("enabled")
                   and (w.get("trigger") or {}).get("kind") != "schedule"]
        for wf in wfs:
            ok, _ = self._match(wf, msg)
            if not ok:
                continue
            if not self._pass_limits(wf, msg):
                continue
            if wf.get("serial"):               # 串行：进该工作流的专属队列，逐条执行
                self._serial_put(wf, msg)
                continue
            try:
                self._q.put_nowait((wf, dict(msg)))
            except queue.Full:
                self.log.warning("工作流队列已满，丢弃触发: %s", wf.get("name"))

    def _serial_put(self, wf, msg):
        """串行工作流：每条工作流一个专属队列 + 一个专属线程，前一条跑完才跑下一条。
        适合并发受限的下游(如 Codex 代理并发=1)——同时来的触发会排队，不会互相打架。"""
        with self._lock:
            q = self._serial_q.get(wf["id"])
            if q is None:
                q = queue.Queue(maxsize=100)
                self._serial_q[wf["id"]] = q
                threading.Thread(target=self._serial_worker, args=(q,),
                                 daemon=True,
                                 name="workflow-serial-%s" % wf["id"]).start()
        try:
            q.put_nowait((wf, dict(msg)))
        except queue.Full:
            self.log.warning("工作流[%s] 串行队列已满(100)，丢弃触发", wf.get("name"))

    def _serial_worker(self, q):
        while True:
            try:
                wf, msg = q.get()
            except Exception:
                continue
            try:
                self._run(wf, msg)
            except Exception as e:
                self.log.exception("串行工作流[%s]执行异常: %s", wf.get("name"), e)

    def _pass_limits(self, wf, msg):
        """冷却 + 每分钟上限。"""
        now = time.time()
        wid, sid = wf["id"], str(msg.get("user_id") or "")
        with self._lock:
            cd = wf.get("cooldown_sec") or 0
            if cd and now - self._cooldown.get((wid, sid), 0) < cd:
                return False
            dq = self._minute.setdefault(wid, deque())
            while dq and now - dq[0] > 60:
                dq.popleft()
            if len(dq) >= (wf.get("max_per_min") or 30):
                self.log.warning("工作流[%s] 触发超过 %s次/分钟, 已限流",
                                 wf.get("name"), wf.get("max_per_min"))
                return False
            dq.append(now)
            self._cooldown[(wid, sid)] = now
        return True

    # ============ 条件匹配 ============
    def _match(self, wf, msg):
        t = wf.get("trigger") or {}
        sid     = str(msg.get("user_id") or "")
        sender  = str(msg.get("sender") or "")
        content = msg.get("content") if isinstance(msg.get("content"), str) else ""
        mtype   = msg.get("msg_type")
        at_me   = bool(msg.get("at_me"))

        # 「人」和「会话」是两个维度，各认各的 id，混用就是本来那个 bug。
        # 推送里的 addition 是唯一能把聚合对话的 id 和企微接口的 id 对上的桥：
        #   external_contact_id  人  = 联系人接口 wxid = 群成员接口 imContactId
        #                             (实测跨群稳定，同一个人在 99 个群里同一个值)
        #   external_chat_id     会话= 联系人接口 chatId。⚠️ 只在私聊有意义 ——
        #                             群聊时它是**群会话**自己的 id，不是发送人的
        #   room_id              群  = 群列表接口 imRoomId，连 "R:" 前缀一起相等
        add  = (msg.get("jjy") or {}).get("addition") or {}
        peer = str(add.get("external_contact_id") or "")
        alt_chat = "S:" + str(add.get("external_chat_id") or "")
        alt_room = str((msg.get("jjy") or {}).get("room_id") or "")

        # 会话面板列的是本地会话 id；企微那边有、但还没来过消息的群/联系人，
        # 列的是 imRoomId / "S:"+chatId / wxid —— 这几个别名让这类也能匹配上，
        # 所以「还没来过消息」不妨碍拿它当触发条件：第一条消息一到就对上了。
        # peer(=人 id) 只在私聊算会话别名 —— 群聊里它是**发言人**，
        # 认了就变成"张三在任何群说话都算这个会话"。
        if t.get("sessions"):
            alias = [sid, alt_chat, alt_room]
            if sid.startswith("S:"):
                alias.append(peer)
            if not any(x in t["sessions"] for x in alias if x):
                return False, "会话不匹配"
        ct = t.get("chat_type", "any")
        if ct == "group" and not sid.startswith("R:"):
            return False, "非群聊消息(条件要求仅群聊)"
        if ct == "private" and not sid.startswith("S:"):
            return False, "非私聊消息(条件要求仅私聊)"
        # 发送人只认「人」的 id：推送的 user_id，或 external_contact_id。
        if t.get("senders") and sender not in t["senders"] \
           and peer not in t["senders"]:
            return False, "发送人不匹配"
        stype = t.get("sender_type", "any")
        if stype in ("internal", "external"):
            se = msg.get("sender_external")
            if se is None:
                se = self.classify(sender)
            if se is None:
                return False, "无法判断发送人内外部身份(请先同步通讯录)"
            if stype == "internal" and se == 1:
                return False, "发送人是外部用户(条件要求内部成员)"
            if stype == "external" and se == 0:
                return False, "发送人是内部成员(条件要求外部用户)"
        # 「必须@」只在群聊成立。at_me 是拿机器人昵称在正文里找出来的(jjy.py)，
        # 私聊压根不参与计算、永远是 0 —— 硬卡的话配了这个条件的工作流在私聊
        # 永远不触发，而私聊本来就是一对一，不存在"要不要@我"这回事。
        if t.get("at_me") == "yes" and sid.startswith("R:") and not at_me:
            return False, "未 @ 托管账号"
        if t.get("at_me") == "no" and at_me:
            return False, "@ 了托管账号(条件要求未@)"
        if t.get("msg_types") and mtype not in t["msg_types"]:
            return False, "消息类型不匹配"

        c = t.get("content") or {}
        mode = c.get("mode", "none")
        if mode == "any":
            kws = c.get("keywords") or []
            if kws and not any(k in content for k in kws):
                return False, "未命中任一关键词"
        elif mode == "all":
            kws = c.get("keywords") or []
            if kws and not all(k in content for k in kws):
                return False, "未命中全部关键词"
        elif mode == "regex":
            pat = c.get("regex") or ""
            if pat:
                try:
                    rx = self._re_cache.get(pat)
                    if rx is None:
                        rx = re.compile(pat)
                        self._re_cache[pat] = rx
                    if not rx.search(content):
                        return False, "正则未匹配"
                except re.error as e:
                    return False, "正则表达式错误: %s" % e
        return True, "命中"

    # ============ 执行 ============
    def _worker(self):
        while True:
            try:
                wf, msg = self._q.get()
            except Exception:
                continue
            try:
                self._run(wf, msg)
            except Exception as e:
                self.log.exception("工作流[%s]执行异常: %s", wf.get("name"), e)

    def _ctx(self, msg):
        sid = str(msg.get("user_id") or "")
        ts  = int(msg.get("time_stamp") or time.time())
        sched = bool(msg.get("is_sched"))
        return {
            "content":     msg.get("content") if isinstance(msg.get("content"), str) else "",
            "sender":      str(msg.get("sender") or ""),
            "sender_name": str(msg.get("sender_name") or msg.get("sender") or ""),
            "source":      sid,
            # 定时触发没有来源会话，给个可读名字，别在运行记录里显示成空
            "source_name": ("定时触发" if sched else str(self.resolve(sid) or sid)),
            "time":        time.strftime("%H:%M:%S", time.localtime(ts)),
            "date":        time.strftime("%Y-%m-%d", time.localtime(ts)),
        }

    @staticmethod
    def _render(tmpl, ctx, urlencode=False):
        out = str(tmpl or "")
        for k, v in ctx.items():
            v = str(v)
            if urlencode:
                v = urllib.parse.quote(v)
            out = out.replace("{%s}" % k, v)
        return out

    def _run(self, wf, msg, dry=False, run_http=True):
        """执行一条工作流的所有动作。返回 results 列表。dry=True 时转发/回复只预览不发送。"""
        ctx = self._ctx(msg)
        results = []
        for a in wf.get("actions") or []:
            try:
                if a.get("type") == "reply":
                    results.append(self._do_reply(a, ctx, dry))
                elif a.get("type") == "http":
                    if dry and not run_http:
                        results.append({"action": "http", "ok": True,
                                        "detail": "预览: %s %s" % (a.get("method"),
                                                  self._render(a.get("url"), ctx, urlencode=True))})
                    else:
                        results.append(self._do_http(a, ctx, msg, dry))
                elif a.get("type") == "ai":
                    # AI 回复真调模型是要花钱的，所以试跑一律只拼请求体不发出去
                    results.append(self._do_ai(a, ctx, msg, wf, dry=True)
                                   if dry else self._do_ai(a, ctx, msg, wf))
            except Exception as e:
                results.append({"action": a.get("type") or "?", "ok": False,
                                "detail": "异常: %s" % e})
        if not dry:
            ok = all(r.get("ok") for r in results) if results else True
            run = {
                "t": _now(), "wf": wf.get("name"), "wf_id": wf.get("id"),
                "source": ctx["source"], "source_name": ctx["source_name"],
                "sender": ctx["sender_name"],
                "content": ctx["content"][:80],
                "ok": ok, "results": results,
            }
            with self._lock:
                self.counter["runs"] += 1
                if not ok:
                    self.counter["fails"] += 1
                wf_live = next((x for x in self.workflows if x["id"] == wf["id"]), None)
                if wf_live is not None:
                    wf_live["stats"]["runs"] += 1
                    wf_live["stats"]["last_at"] = run["t"]
                    if not ok:
                        wf_live["stats"]["fails"] += 1
                self.runs.append(run)
            if self.store:                       # 持久化(明细 + 累计计数)，重启不丢
                try:
                    self.store.add_run(run)
                except Exception as e:
                    self.log.warning("运行记录持久化失败: %s", e)
        return results

    def _do_reply(self, a, ctx, dry=False):
        """自定义回复：把模板渲染后发回**来源会话**。

        不需要配任何目标 id —— 目标就是这条消息所在的会话，而推送里带的
        chat_id 永远是当前有效的那个。这也是为什么它不会踩「联系人 id 发不
        出去」那个坑：根本不经过选人面板。

        ⚠️ 定时工作流没有来源会话(_sched_msg 的 user_id 是空)，所以它配这个
        动作发不出去 —— 定时任务要发消息只能走 http 动作。
        """
        text = self._render(a.get("template"), ctx)[:MAX_REPLY_LEN]
        if not text.strip():
            return {"action": "reply", "ok": False, "detail": "回复内容为空"}
        tgt = ctx.get("source") or ""
        if not tgt:
            return {"action": "reply", "ok": False,
                    "detail": "没有来源会话可回（定时工作流请改用「调用接口」）"}
        if dry:
            return {"action": "reply", "ok": True,
                    "detail": "预览 → %s：%s" % (ctx.get("source_name") or tgt, text[:120])}
        ok = self.send_text(tgt, text)
        return {"action": "reply", "ok": ok,
                "detail": ("已回复：" if ok else "回复失败：") + text[:80]}

    def _request(self, url, method="GET", headers=None, body=None,
                 timeout=10, retries=0):
        """带退避重试的 HTTP 调用。返回 (status, raw, err, tries)。

        err 非空 = 这次没拿到 2xx 响应，raw 可能有(错误响应体)也可能没有。
        「调用接口」和「AI 回复」共用这一份，别再抄一遍重试逻辑。
        """
        attempts = 1 + int(retries or 0)
        last_err, raw, status, tries = "", None, 0, 0
        for i in range(attempts):
            tries = i + 1
            try:
                req = urllib.request.Request(url, data=body,
                                             headers=headers or {}, method=method)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    status = resp.status
                    raw = resp.read(MAX_RESP_BYTES).decode("utf-8", "ignore")
                if 200 <= status < 300:
                    return status, raw, "", tries
                last_err = "HTTP %s" % status
            except urllib.error.HTTPError as e:
                status = e.code
                try:
                    raw = e.read(MAX_RESP_BYTES).decode("utf-8", "ignore")
                except Exception:
                    raw = None
                last_err = "HTTP %s %s" % (e.code, (raw or "")[:120])
                # 客户端类错误(参数/鉴权/路径/超限)重试也不会成功，直接放弃
                if e.code in (400, 401, 403, 404, 405, 411, 413):
                    break
            except Exception as e:
                last_err = str(e) or e.__class__.__name__
            if i < attempts - 1:
                time.sleep(5 * (i + 1))    # 重试退避(兼顾 429 busy 场景)
        return status, raw, (last_err or "无响应"), tries

    def _do_http(self, a, ctx, msg, dry=False):
        url = self._render(a.get("url"), ctx, urlencode=True)
        if not url.startswith(("http://", "https://")):
            return {"action": "http", "ok": False, "detail": "URL 必须以 http(s):// 开头"}
        method = a.get("method", "GET")
        headers = {}
        for line in (a.get("headers") or "").splitlines():
            if ":" in line:
                k, _, v = line.partition(":")
                if k.strip():
                    headers[k.strip()] = self._render(v.strip(), ctx)
        body = None
        if method == "POST":
            # {x_json} = JSON 转义后的值(不带引号)，放在模板的双引号内可安全拼 JSON，
            # 消息里的引号/换行/反斜杠不会打坏请求体
            jctx = dict(ctx)
            for k, v in ctx.items():
                jctx[k + "_json"] = json.dumps(str(v), ensure_ascii=False)[1:-1]
            body = self._render(a.get("body"), jctx).encode("utf-8")
            if body and "Content-Type" not in headers:
                headers["Content-Type"] = "application/json"
        headers.setdefault("User-Agent", "wxwork-gateway-workflow/1.0")

        status, raw, err, tries = self._request(
            url, method, headers, body,
            a.get("timeout_sec") or 10, a.get("retries") or 0)
        if err:
            return {"action": "http", "ok": False,
                    "detail": "调用失败(尝试%d次): %s" % (tries, err)}

        detail = "HTTP %s %s" % (status, url[:80])
        # ---- 提取返回内容 ----
        result = raw
        path = (a.get("reply_path") or "").strip()
        if path:
            try:
                data = json.loads(raw)
                val = self._extract(data, path)
                if val is None:
                    return {"action": "http", "ok": False,
                            "detail": detail + " · 响应中提取不到 %s" % path}
                result = val if isinstance(val, str) else json.dumps(val, ensure_ascii=False)
            except json.JSONDecodeError:
                return {"action": "http", "ok": False,
                        "detail": detail + " · 响应不是 JSON, 无法按路径提取"}

        # ---- 回复 ----
        if a.get("reply"):
            rctx = dict(ctx); rctx["result"] = result
            text = self._render(a.get("reply_template") or "{result}", rctx).strip()
            if len(text) > MAX_REPLY_LEN:
                text = text[:MAX_REPLY_LEN] + "…"
            # 只回消息来源那个会话。定时触发没有来源，回不了
            tgt = ctx.get("source") or ""
            if not tgt:
                return {"action": "http", "ok": False,
                        "detail": detail + " · 定时触发没有来源会话，回复不了（请关掉回复开关）"}
            if dry:
                return {"action": "http", "ok": True,
                        "detail": detail + " · 回复预览 → %s：%s" % (
                            ctx.get("source_name") or tgt, text[:200])}
            if not self.send_text(tgt, text):
                return {"action": "http", "ok": False, "detail": detail + " · 回复失败"}
            detail += " · 已回复 " + (ctx.get("source_name") or tgt)
        elif dry:
            detail += " · 返回(前200字): %s" % result[:200]
        return {"action": "http", "ok": True, "detail": detail}

    # ============ AI 回复 ============
    DASHSCOPE_URL = "https://dashscope.aliyuncs.com/api/v1/apps/%s/completion"

    def _do_ai(self, a, ctx, msg, wf, dry=False):
        """带上下文调模型，把回答发回来源会话。

        和「调用接口」的区别只有一个：请求体不用你写，引擎按这个人和机器人
        之前聊过的话拼 messages[]。上下文存在 ai_context 表里，键是
        (工作流, 会话, 人) —— 群里两个人同时 @ 机器人，各聊各的。
        """
        sid  = ctx.get("source") or ""
        peer = str(msg.get("sender") or "")
        if not sid:
            return {"action": "ai", "ok": False,
                    "detail": "没有来源会话（定时工作流用不了 AI 回复）"}
        ask = (ctx.get("content") or "").strip()
        if not ask:
            return {"action": "ai", "ok": False, "detail": "触发消息没有文本内容"}

        # ---- 上下文：历史 + 当前这句。历史读失败就按第一轮处理，不让它挡着回话 ----
        hist = []
        if self.store:
            try:
                hist = self.store.ai_ctx_read(
                    wf.get("id") or "", sid, peer,
                    mode=a.get("ctx_mode"), count=a.get("ctx_count"),
                    minutes=a.get("ctx_minutes"))
            except Exception as e:
                self.log.warning("读取 AI 上下文失败，这轮按无上下文处理: %s", e)
        messages = hist + [{"role": "user", "content": ask}]

        # ---- 附加参数：浅合并进请求体顶层；system 单独拎出来放消息列表最前 ----
        extra = {}
        if (a.get("extra") or "").strip():
            try:
                extra = json.loads(self._render(a["extra"], ctx))
            except json.JSONDecodeError as e:
                return {"action": "ai", "ok": False,
                        "detail": "附加参数不是合法 JSON: %s" % e}
            if not isinstance(extra, dict):
                return {"action": "ai", "ok": False, "detail": "附加参数必须是 JSON 对象"}
        sysmsg = str(extra.pop("system", "") or "").strip()
        if sysmsg:
            messages = [{"role": "system", "content": sysmsg}] + messages

        bailian = bool(a.get("bailian"))
        url = self._render(a.get("url"), ctx, urlencode=True).strip()
        if bailian and not url:
            app_id = (a.get("app_id") or "").strip()
            if not app_id:
                return {"action": "ai", "ok": False, "detail": "百炼模式要填应用 ID"}
            url = self.DASHSCOPE_URL % urllib.parse.quote(app_id, safe="")
        if not url.startswith(("http://", "https://")):
            return {"action": "ai", "ok": False, "detail": "接口地址必须以 http(s):// 开头"}

        body = ({"input": {"messages": messages}, "parameters": {}, "debug": {}}
                if bailian else {"messages": messages})
        body.update(extra)

        headers = {"Content-Type": "application/json"}
        for line in (a.get("headers") or "").splitlines():
            if ":" in line:
                k, _, v = line.partition(":")
                if k.strip():
                    headers[k.strip()] = self._render(v.strip(), ctx)
        key = (a.get("api_key") or "").strip()
        if key and "Authorization" not in headers:
            headers["Authorization"] = "Bearer " + key
        headers.setdefault("User-Agent", "wxwork-gateway-workflow/1.0")

        if dry:
            return {"action": "ai", "ok": True,
                    "detail": "预览 · %s · 上下文 %d 条 · 请求体 %s" % (
                        url[:60], len(messages),
                        json.dumps(body, ensure_ascii=False)[:400])}

        status, raw, err, tries = self._request(
            url, "POST", headers, json.dumps(body, ensure_ascii=False).encode("utf-8"),
            a.get("timeout_sec") or 60, a.get("retries") or 0)
        if err:
            return {"action": "ai", "ok": False,
                    "detail": "调用失败(尝试%d次): %s" % (tries, err)}
        detail = "HTTP %s · 上下文 %d 条" % (status, len(messages))

        result, perr = self._ai_answer(raw, bailian, a.get("reply_path"))
        if perr:
            return {"action": "ai", "ok": False, "detail": detail + " · " + perr}

        text = self._render(a.get("reply_template") or "{result}",
                            dict(ctx, result=result)).strip()
        # 引用 + @ 都只在群里做。私聊是一对一：引用谁、@ 谁都是明摆着的，
        # 加了只是噪音。
        #
        # @ 为什么正文前缀和结构化 mention 都给：前缀保证「看得见」——
        # 语聚推过来的 @ 本来就是正文里的纯文本，at_me 就是这么认出来的；
        # mention 争取「真被@到、有红点」，它不被认时 _wf_send 会摘掉重发，
        # 不影响这条回复发出去。
        ment, qid = None, ""
        if sid.startswith("R:"):
            if a.get("mention") and peer:
                text = "@%s %s" % (ctx.get("sender_name") or peer, text)
                ment = [peer]
            if a.get("quote"):
                qid = str(msg.get("msg_id") or "")
        if len(text) > MAX_REPLY_LEN:
            text = text[:MAX_REPLY_LEN] + "…"
        if not self.send_text(sid, text, qid, ment):
            return {"action": "ai", "ok": False, "detail": detail + " · 回复失败"}

        # 发出去了才落库。模型答了但没发成也不写 —— 对方根本没看见这句，
        # 记进上下文只会让下一轮基于一段从未发生过的对话往下编。
        if self.store:
            try:
                self.store.ai_ctx_append(wf.get("id") or "", sid, peer,
                                         [("user", ask), ("assistant", result)])
            except Exception as e:
                self.log.warning("AI 上下文落库失败(这轮不影响回复): %s", e)
        return {"action": "ai", "ok": True, "detail": detail + " · 已回复：" + text[:80]}

    def _ai_answer(self, raw, bailian, path=""):
        """从响应里取回复文本。返回 (text, err)。"""
        try:
            data = json.loads(raw or "")
        except json.JSONDecodeError:
            return "", "响应不是 JSON: %s" % (raw or "")[:120]
        if bailian:
            # 官方形态是 output.text；应用编排(工作流)走的是 workflow_message
            out = data.get("output") or {}
            txt = str(out.get("text") or "").strip()
            if not txt:
                wm = (out.get("workflow_message") or {}).get("message") or {}
                txt = str(wm.get("content") or "").strip()
            if not txt:
                return "", "响应里没有回复内容: %s" % json.dumps(
                    data, ensure_ascii=False)[:160]
            return txt, ""
        val = self._extract(data, (path or "choices.0.message.content").strip())
        if val is None:
            return "", "响应中提取不到 %s" % (path or "choices.0.message.content")
        txt = (val if isinstance(val, str) else
               json.dumps(val, ensure_ascii=False)).strip()
        return (txt, "") if txt else ("", "提取到的回复是空的")

    @staticmethod
    def _extract(data, path):
        """按点路径提取 JSON 字段, 支持数组下标: data.choices.0.message.content"""
        cur = data
        for part in path.split("."):
            if isinstance(cur, dict):
                if part not in cur:
                    return None
                cur = cur[part]
            elif isinstance(cur, list):
                if not part.lstrip("-").isdigit():
                    return None
                idx = int(part)
                if idx >= len(cur) or idx < -len(cur):
                    return None
                cur = cur[idx]
            else:
                return None
        return cur

    # ============ 测试(管理页"试跑") ============
    def test(self, wf, sample, run_http=True):
        """dry-run：只匹配 + 预览动作；run_http=True 时真实调用接口(不发消息)。"""
        wf = self._norm(wf)
        # 定时工作流没有触发条件可匹配：直接按“到点了”预览动作
        if wf["trigger"].get("kind") == "schedule":
            msg = self._sched_msg(wf, _now())
            nf = self.next_fire(wf)
            return {"matched": True,
                    "reason": "定时触发 · 下次 %s" % (
                        time.strftime("%Y-%m-%d %H:%M", time.localtime(nf)) if nf else "无(检查日期/星期设置)"),
                    "results": self._run(wf, msg, dry=True, run_http=run_http)}
        msg = {
            "user_id":     sample.get("session") or "R:test",
            "sender":      sample.get("sender") or "u_test",
            "sender_name": sample.get("sender_name") or "测试用户",
            "content":     sample.get("content") or "",
            "msg_type":    int(sample.get("msg_type") or 2),
            "at_me":       1 if sample.get("at_me") else 0,
            "time_stamp":  _now(),
            # 试跑面板只给一个"发送人 id"，可真实报文里人有两个 id(推送 user_id /
            # addition.external_contact_id)，条件里存的是哪个取决于从哪个面板选的。
            # 不把它也放进 addition 的话，从选人面板挑的人试跑必报"发送人不匹配",
            # 用户会以为没修好 —— 这是反馈回路，不是锦上添花。
            "jjy": {"addition": {
                "external_contact_id": sample.get("sender") or ""}},
        }
        # 发送人身份：样例显式指定(0/1)优先，否则按 id 自动判断
        se = sample.get("sender_external")
        msg["sender_external"] = int(se) if se in (0, 1, "0", "1") \
            else self.classify(msg["sender"])
        ok, reason = self._match(wf, msg)
        out = {"matched": ok, "reason": reason, "results": []}
        if ok:
            out["results"] = self._run(wf, msg, dry=True, run_http=run_http)
        return out

    # ============ 查询 ============
    def list(self):
        """工作流列表。若有持久化 store，卡片统计用库里累计值(重启不丢)覆盖内存值。"""
        with self._lock:
            out = [dict(w) for w in self.workflows]
        if self.store:
            try:
                per = self.store.wf_run_stats()
                for w in out:
                    s = per.get(w["id"])
                    if s:
                        w["stats"] = dict(s)
            except Exception as e:
                self.log.warning("读取工作流累计统计失败: %s", e)
        for w in out:                      # 定时工作流带上“下次触发”给页面显示
            w["next_fire"] = self.next_fire(w)
        return out

    def run_list(self, before=None, limit=50, wf_id=None):
        """运行记录，游标分页(最新在前)。返回 {runs, has_more}。"""
        if self.store:
            runs, has_more = self.store.get_runs(before=before, wf_id=wf_id, limit=limit)
            return {"runs": runs, "has_more": has_more}
        # 无持久化兜底：内存 deque，不支持真正游标，仅返回最近 limit 条
        with self._lock:
            items = list(reversed(self.runs))
        limit = max(1, min(200, int(limit or 50)))
        return {"runs": items[:limit], "has_more": len(items) > limit}

    def stats(self):
        with self._lock:
            base = {"count": len(self.workflows),
                    "enabled": sum(1 for w in self.workflows if w.get("enabled")),
                    "runs": self.counter["runs"], "fails": self.counter["fails"],
                    "queue": self._q.qsize()}
        if self.store:                            # 累计数字以库为准(重启不丢)
            try:
                c = self.store.run_counter()
                base["runs"], base["fails"] = c["runs"], c["fails"]
            except Exception as e:
                self.log.warning("读取工作流累计计数失败: %s", e)
        return base
