# -*- coding: utf-8 -*-
"""
store.py — 会话/消息持久化层 (SQLite, 纯标准库)
=====================================
参考成熟 IM 的存储与分页设计：
  * 消息表自增 seq 作为全局游标(Discord/Matrix 风格)：
    分页用 before/after + limit，不用 offset(offset 大了慢、且新消息到达时会漂移)。
  * (session_id, msg_id) 唯一索引做服务端去重，替代旧版"最近40条"滑动窗口。
  * WAL 日志模式：读写不互相阻塞；进程崩溃/断电不丢已提交数据。
  * 会话表冗余 last_msg/last_time/unread(Chatwoot 收件箱模型)，
    会话列表 O(会话数) 直出，不用扫消息表聚合。

对上层暴露(所有方法线程安全)：
  Store(db_file, max_per_session)
    .add_message(...) -> (msg_dict, session_dict) | (None, None)重复
    .get_messages(sid, before=None, after=None, limit=50) -> (list, has_more)
    .get_sessions() -> list
    .get_session(sid) -> dict | None
    .mark_read(sid)
    .update_names({id: name}) -> 更新条数
    .stats() -> {"messages": n, "sessions": m}
"""

import json
import time
import sqlite3
import threading

_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages(
  seq         INTEGER PRIMARY KEY AUTOINCREMENT,   -- 全局游标(分页/去重锚点)
  session_id  TEXT    NOT NULL,
  msg_id      TEXT    NOT NULL DEFAULT '',         -- 企微 server_id, 可为空
  sender      TEXT    NOT NULL DEFAULT '',
  sender_name TEXT    NOT NULL DEFAULT '',
  content     TEXT    NOT NULL DEFAULT '',
  msg_type    INTEGER,
  is_self     INTEGER NOT NULL DEFAULT 0,
  ts          INTEGER NOT NULL DEFAULT 0
);
-- 服务端去重：同一会话内相同 msg_id 只入库一次(msg_id 为空则不参与去重)
CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_dedup
  ON messages(session_id, msg_id) WHERE msg_id != '';
-- 会话内按游标翻页的核心索引
CREATE INDEX IF NOT EXISTS ix_messages_session_seq
  ON messages(session_id, seq);

-- 媒体附件(图片/文件/视频/语音)。单开一张表而不是往 messages 加列，因为：
--   * 图文消息(11068)的 image_list[] 是一对多
--   * 下载是有状态的生命周期(待下/下载中/成功/失败+重试)，塞进消息行会让消息接口变脏
--   * 重试、按会话清理、迁移到对象存储都只动这张表
CREATE TABLE IF NOT EXISTS media(
  id          TEXT PRIMARY KEY,                    -- uuid4，直接当 URL 用(不可枚举)
  msg_seq     INTEGER NOT NULL DEFAULT 0,          -- 关联 messages.seq
  session_id  TEXT    NOT NULL DEFAULT '',
  kind        TEXT    NOT NULL DEFAULT '',         -- image/file/video/voice/gif
  file_name   TEXT    NOT NULL DEFAULT '',
  size        INTEGER NOT NULL DEFAULT 0,
  md5         TEXT    NOT NULL DEFAULT '',
  mime        TEXT    NOT NULL DEFAULT '',
  cdn_type    INTEGER NOT NULL DEFAULT 0,
  cdn         TEXT    NOT NULL DEFAULT '{}',       -- 原始 cdn 凭证(JSON)，留着重试/按需下载
  path        TEXT    NOT NULL DEFAULT '',         -- 本地落盘路径(将来可换成对象存储 key)
  state       INTEGER NOT NULL DEFAULT 0,          -- 0待下载 1下载中 2成功 3失败 4超限未下载
  err         TEXT    NOT NULL DEFAULT '',
  tries       INTEGER NOT NULL DEFAULT 0,
  ts          INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_media_msg   ON media(msg_seq);
CREATE INDEX IF NOT EXISTS ix_media_state ON media(state);

CREATE TABLE IF NOT EXISTS sessions(
  id        TEXT PRIMARY KEY,                      -- 群 R:xxx / 私聊 S:xxx
  name      TEXT    NOT NULL DEFAULT '',
  is_group  INTEGER NOT NULL DEFAULT 0,
  last_msg  TEXT    NOT NULL DEFAULT '',
  last_time INTEGER NOT NULL DEFAULT 0,
  unread    INTEGER NOT NULL DEFAULT 0
);

-- 工作流运行记录(明细，供分页浏览)。按 max_runs 只保留最近若干条。
CREATE TABLE IF NOT EXISTS workflow_runs(
  seq         INTEGER PRIMARY KEY AUTOINCREMENT,   -- 全局游标(分页锚点)
  t           INTEGER NOT NULL DEFAULT 0,          -- 触发时间(秒)
  wf_id       TEXT    NOT NULL DEFAULT '',
  wf_name     TEXT    NOT NULL DEFAULT '',
  source      TEXT    NOT NULL DEFAULT '',
  source_name TEXT    NOT NULL DEFAULT '',
  sender      TEXT    NOT NULL DEFAULT '',
  content     TEXT    NOT NULL DEFAULT '',
  ok          INTEGER NOT NULL DEFAULT 1,
  results     TEXT    NOT NULL DEFAULT '[]'         -- 逐动作结果(JSON)
);
CREATE INDEX IF NOT EXISTS ix_runs_wf ON workflow_runs(wf_id, seq);

-- 工作流累计计数(不裁剪，重启不丢；卡片/状态页的统计数字来源)
CREATE TABLE IF NOT EXISTS wf_counters(
  wf_id   TEXT PRIMARY KEY,
  runs    INTEGER NOT NULL DEFAULT 0,
  fails   INTEGER NOT NULL DEFAULT 0,
  last_at INTEGER NOT NULL DEFAULT 0
);

-- 定时工作流的触发状态：记录“已触发到哪个时间槽”，重启后不重复触发、不漏触发
CREATE TABLE IF NOT EXISTS wf_schedule(
  wf_id     TEXT PRIMARY KEY,
  last_slot TEXT    NOT NULL DEFAULT '',   -- 已触发的时间槽标识, 如 2026-07-25 09:00
  last_fire INTEGER NOT NULL DEFAULT 0
);
"""


class Store:
    def __init__(self, db_file, max_per_session=5000, max_runs=2000):
        self.db_file = db_file
        self.max_per_session = int(max_per_session or 0)   # 0 = 不限制
        self.max_runs = int(max_runs or 0)                 # 工作流运行明细保留条数, 0=不限制
        self._lock = threading.RLock()
        self._db = sqlite3.connect(db_file, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        with self._lock:
            self._db.executescript(_SCHEMA)
            self._migrate()
            self._db.commit()

    def _migrate(self):
        """幂等迁移：CREATE TABLE IF NOT EXISTS 加不了列，已有库要单独 ALTER。"""
        cols = {r["name"] for r in self._db.execute("PRAGMA table_info(messages)")}
        add = [
            ("media_id", "TEXT NOT NULL DEFAULT ''"),
            # 链接/位置/名片/小程序等的结构化字段(JSON)。不存的话 url、经纬度、
            # 名片 user_id 这些推送里才有的信息就永久丢了，事后补不回来。
            ("rich",     "TEXT NOT NULL DEFAULT ''"),
            # 撤回标记：企微 11123 只给消息 id，原消息仍要留档，只是展示成"已撤回"
            ("revoked",  "INTEGER NOT NULL DEFAULT 0"),
        ]
        for name, decl in add:
            if name not in cols:
                self._db.execute(
                    "ALTER TABLE messages ADD COLUMN %s %s" % (name, decl))

        # 群管理接口那套 id。聚合对话的 chat_id 和企微群管理的 imRoomId 是
        # **两个不同的 id 空间**(UUID 形态 vs "R:10942308524386327")，只有推送里的
        # source_addition.room_id 能把它们对上 —— 所以见到一次就得存下来，
        # 否则重启后拿群列表接口的返回没法映射回本地会话。
        scols = {r["name"] for r in self._db.execute("PRAGMA table_info(sessions)")}
        for name, decl in (("room_id", "TEXT NOT NULL DEFAULT ''"),   # = imRoomId
                           ("bot_id",  "TEXT NOT NULL DEFAULT ''")):  # = imBotId
            if name not in scols:
                self._db.execute(
                    "ALTER TABLE sessions ADD COLUMN %s %s" % (name, decl))

    def link_room(self, session_id, room_id, bot_id=""):
        """记下会话对应的企微 imRoomId / imBotId。只在值有变化时写。"""
        if not (session_id and room_id):
            return
        with self._lock:
            self._db.execute(
                "UPDATE sessions SET room_id=?, bot_id=? WHERE id=?"
                " AND (room_id!=? OR bot_id!=?)",
                (room_id, bot_id or "", session_id, room_id, bot_id or ""))
            self._db.commit()

    def get_by_msg_id(self, session_id, msg_id):
        """按 msg_id 取一条消息。走 (session_id, msg_id) 唯一索引。"""
        if not (session_id and msg_id):
            return None
        with self._lock:
            r = self._db.execute(
                "SELECT * FROM messages WHERE session_id=? AND msg_id=?",
                (session_id, str(msg_id))).fetchone()
        return self._msg_dict(r) if r else None

    def room_map(self):
        """{imRoomId: 内部 session_id}。群列表接口的返回靠它映射回本地会话。"""
        with self._lock:
            rows = self._db.execute(
                "SELECT id, room_id FROM sessions WHERE room_id!=''").fetchall()
        return {r["room_id"]: r["id"] for r in rows}

    # ---------- 写 ----------
    def add_message(self, session_id, msg_id, sender, sender_name, content,
                    msg_type, is_self, ts, is_group, session_name, preview,
                    media_id="", rich=None):
        """入库一条消息并联动更新会话。
        重复消息(同 session+msg_id 已存在)返回 (None, None)。
        成功返回 (消息dict, 更新后的会话dict)。"""
        with self._lock:
            rj = "" if not rich else (
                rich if isinstance(rich, str)
                else json.dumps(rich, ensure_ascii=False))
            cur = self._db.execute(
                "INSERT OR IGNORE INTO messages"
                "(session_id,msg_id,sender,sender_name,content,msg_type,"
                " is_self,ts,media_id,rich)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (session_id, msg_id or "", sender or "", sender_name or "",
                 content or "", msg_type, 1 if is_self else 0, int(ts or 0),
                 media_id or "", rj))
            if cur.rowcount == 0:              # 命中唯一索引 -> 重复推送
                self._db.commit()
                return None, None
            seq = cur.lastrowid

            # 会话 upsert：名字非空才覆盖，未读只对他人消息 +1
            inc = 0 if is_self else 1
            row = self._db.execute(
                "SELECT name FROM sessions WHERE id=?", (session_id,)).fetchone()
            if row is None:
                self._db.execute(
                    "INSERT INTO sessions(id,name,is_group,last_msg,last_time,unread)"
                    " VALUES(?,?,?,?,?,?)",
                    (session_id, session_name or "", 1 if is_group else 0,
                     preview or "", int(ts or 0), inc))
            else:
                self._db.execute(
                    "UPDATE sessions SET name=?, is_group=?, last_msg=?,"
                    " last_time=?, unread=unread+? WHERE id=?",
                    (session_name or row["name"] or "", 1 if is_group else 0,
                     preview or "", int(ts or 0), inc, session_id))

            # 按会话裁剪：只保留最新 max_per_session 条(0=不裁剪)
            if self.max_per_session > 0:
                self._db.execute(
                    "DELETE FROM messages WHERE session_id=? AND seq <= COALESCE("
                    " (SELECT seq FROM messages WHERE session_id=?"
                    "  ORDER BY seq DESC LIMIT 1 OFFSET ?), 0)",
                    (session_id, session_id, self.max_per_session))
            self._db.commit()

            item = self._msg_dict(self._db.execute(
                "SELECT * FROM messages WHERE seq=?", (seq,)).fetchone())
            sess = self.get_session(session_id)
        return item, sess

    def revoke_message(self, msg_id, session_id=None):
        """把一条消息标记为已撤回(企微 11123)。原消息保留，只是展示成"已撤回"。

        单聊的撤回通知里 room_id=0，拿不到会话 id，这时只能按 msg_id 全库找。
        返回 (消息dict, 会话dict)；找不到返回 (None, None)。
        """
        msg_id = str(msg_id or "")
        if not msg_id:
            return None, None
        with self._lock:
            if session_id:
                row = self._db.execute(
                    "SELECT * FROM messages WHERE session_id=? AND msg_id=?",
                    (session_id, msg_id)).fetchone()
            else:
                row = self._db.execute(
                    "SELECT * FROM messages WHERE msg_id=? ORDER BY seq DESC LIMIT 1",
                    (msg_id,)).fetchone()
            if row is None:
                return None, None
            if row["revoked"]:                       # 重复通知，不再广播
                return None, None
            sid = row["session_id"]
            self._db.execute(
                "UPDATE messages SET revoked=1 WHERE seq=?", (row["seq"],))
            # 撤回的是会话里最后一条时，同步刷新列表预览
            last = self._db.execute(
                "SELECT seq FROM messages WHERE session_id=?"
                " ORDER BY seq DESC LIMIT 1", (sid,)).fetchone()
            if last and last["seq"] == row["seq"]:
                self._db.execute(
                    "UPDATE sessions SET last_msg=? WHERE id=?", ("[消息已撤回]", sid))
            self._db.commit()
            item = self._msg_dict(self._db.execute(
                "SELECT * FROM messages WHERE seq=?", (row["seq"],)).fetchone())
            sess = self.get_session(sid)
        return item, sess

    def mark_read(self, session_id):
        with self._lock:
            self._db.execute(
                "UPDATE sessions SET unread=0 WHERE id=?", (session_id,))
            self._db.commit()

    def update_names(self, name_map):
        """同步通讯录后，把已知 id->name 回填到会话表。返回更新条数。"""
        if not name_map:
            return 0
        n = 0
        with self._lock:
            rows = self._db.execute("SELECT id, name FROM sessions").fetchall()
            for r in rows:
                new = name_map.get(r["id"])
                if new and new != r["name"]:
                    self._db.execute(
                        "UPDATE sessions SET name=? WHERE id=?", (new, r["id"]))
                    n += 1
            if n:
                self._db.commit()
        return n

    # ---------- 读 ----------
    def get_messages(self, session_id, before=None, after=None, limit=50):
        """游标分页(Discord 风格)。返回 (按 seq 升序的列表, has_more)。
          * 默认取最新 limit 条(打开会话)
          * before=seq  取更早的一页(向上翻历史)，has_more 指是否还有更早
          * after=seq   取更新的消息(断线重连补差)，has_more 指是否还有更新
        """
        limit = max(1, min(int(limit or 50), 200))
        with self._lock:
            if after is not None:
                rows = self._db.execute(
                    "SELECT * FROM messages WHERE session_id=? AND seq>?"
                    " ORDER BY seq ASC LIMIT ?",
                    (session_id, int(after), limit + 1)).fetchall()
                has_more = len(rows) > limit
                rows = rows[:limit]
            else:
                cond, args = "", [session_id]
                if before is not None:
                    cond = " AND seq<?"
                    args.append(int(before))
                args.append(limit + 1)
                rows = self._db.execute(
                    "SELECT * FROM messages WHERE session_id=?" + cond +
                    " ORDER BY seq DESC LIMIT ?", args).fetchall()
                has_more = len(rows) > limit
                rows = list(reversed(rows[:limit]))
        out = [self._msg_dict(r) for r in rows]
        # 一次批量把媒体附上，前端不用为每条消息再发一次请求
        mm = self.media_for_seqs([m["seq"] for m in out if m.get("media_id")])
        for m in out:
            if not m.get("media_id"):
                continue
            lst = mm.get(m["seq"]) or []
            m["media"] = lst[0] if lst else None      # 单媒体沿用这个字段
            if len(lst) > 1:
                m["media_list"] = lst                 # 图文多图才有
        return out, has_more

    def get_sessions(self):
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM sessions ORDER BY last_time DESC").fetchall()
        return [self._sess_dict(r) for r in rows]

    def get_session(self, session_id):
        with self._lock:
            r = self._db.execute(
                "SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
        return self._sess_dict(r) if r else None

    # ---------- 媒体附件 ----------
    def add_media(self, mid, msg_seq, session_id, kind, file_name, size,
                  md5, mime, cdn_type, cdn, state, ts):
        """登记一个媒体附件。cdn 是原始凭证 dict，原样存 JSON 供下载/重试用。"""
        with self._lock:
            self._db.execute(
                "INSERT OR REPLACE INTO media"
                "(id,msg_seq,session_id,kind,file_name,size,md5,mime,"
                " cdn_type,cdn,path,state,err,tries,ts)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,'',?,'',0,?)",
                (str(mid), int(msg_seq or 0), session_id or "", kind or "",
                 file_name or "", int(size or 0), md5 or "", mime or "",
                 int(cdn_type or 0),
                 json.dumps(cdn or {}, ensure_ascii=False),
                 int(state or 0), int(ts or 0)))
            self._db.commit()

    def get_media(self, mid):
        with self._lock:
            r = self._db.execute("SELECT * FROM media WHERE id=?", (str(mid),)).fetchone()
        return self._media_dict(r) if r else None

    def get_media_cdn(self, mid):
        """取原始 cdn 凭证(下载器用)。"""
        with self._lock:
            r = self._db.execute("SELECT cdn FROM media WHERE id=?", (str(mid),)).fetchone()
        if not r:
            return {}
        try:
            return json.loads(r["cdn"] or "{}")
        except Exception:
            return {}

    def set_media(self, mid, **f):
        """更新媒体状态。只允许改白名单字段。"""
        allow = ("path", "state", "err", "tries", "size", "mime", "file_name")
        sets, args = [], []
        for k in allow:
            if k in f:
                sets.append("%s=?" % k)
                args.append(f[k])
        if not sets:
            return
        args.append(str(mid))
        with self._lock:
            self._db.execute(
                "UPDATE media SET %s WHERE id=?" % ",".join(sets), args)
            self._db.commit()

    def bump_media_try(self, mid):
        with self._lock:
            self._db.execute(
                "UPDATE media SET tries=tries+1 WHERE id=?", (str(mid),))
            self._db.commit()

    def media_for_seqs(self, seqs):
        """批量取媒体，避免消息列表 N+1 查询。返回 {msg_seq: [media_dict, ...]}。

        一条消息可以挂多个媒体 —— 图文(11068)的 image_list 就是多图，
        按 rowid 升序返回，顺序即入库顺序 = 原推送里的图片顺序。
        """
        seqs = [int(s) for s in (seqs or [])]
        if not seqs:
            return {}
        out = {}
        with self._lock:
            for i in range(0, len(seqs), 400):      # 避开 SQLite 变量数上限
                chunk = seqs[i:i + 400]
                rows = self._db.execute(
                    "SELECT * FROM media WHERE msg_seq IN (%s) ORDER BY rowid"
                    % ",".join("?" * len(chunk)), chunk).fetchall()
                for r in rows:
                    out.setdefault(r["msg_seq"], []).append(self._media_dict(r))
        return out

    def pending_media(self, limit=500):
        """重启后恢复：取还没下完的(0待下载 / 1下载中——上次进程挂了)。"""
        with self._lock:
            rows = self._db.execute(
                "SELECT id FROM media WHERE state IN (0,1) ORDER BY ts ASC LIMIT ?",
                (int(limit),)).fetchall()
        return [r["id"] for r in rows]

    def orphan_media(self, keep_days=0):
        """返回可清理的媒体行(消息已被裁剪掉的孤儿 + 超过保留天数的)。
        只返回记录，实际删文件由上层做(store 不碰文件系统)。"""
        conds = ["msg_seq NOT IN (SELECT seq FROM messages)"]
        args = []
        if keep_days and int(keep_days) > 0:
            conds.append("ts < ?")
            args.append(int(time.time()) - int(keep_days) * 86400)
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM media WHERE %s" % " OR ".join(conds), args).fetchall()
        return [self._media_dict(r) for r in rows]

    def del_media(self, mids):
        mids = [str(m) for m in (mids or [])]
        if not mids:
            return 0
        n = 0
        with self._lock:
            for i in range(0, len(mids), 400):
                chunk = mids[i:i + 400]
                cur = self._db.execute(
                    "DELETE FROM media WHERE id IN (%s)"
                    % ",".join("?" * len(chunk)), chunk)
                n += cur.rowcount
            self._db.commit()
        return n

    # ---------- 工作流运行记录 ----------
    def add_run(self, run):
        """写一条工作流运行记录 + 累计计数，并按 max_runs 裁剪最旧明细。返回新记录 seq。"""
        res = run.get("results")
        rj = res if isinstance(res, str) else json.dumps(res or [], ensure_ascii=False)
        t   = int(run.get("t") or 0)
        wid = str(run.get("wf_id") or "")
        fail = 0 if run.get("ok") else 1
        with self._lock:
            cur = self._db.execute(
                "INSERT INTO workflow_runs"
                "(t,wf_id,wf_name,source,source_name,sender,content,ok,results)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (t, wid, str(run.get("wf") or ""), str(run.get("source") or ""),
                 str(run.get("source_name") or ""), str(run.get("sender") or ""),
                 str(run.get("content") or ""), 1 - fail, rj))
            seq = cur.lastrowid
            # 累计计数(不裁剪) —— 手动 upsert，兼容老版 sqlite
            u = self._db.execute(
                "UPDATE wf_counters SET runs=runs+1, fails=fails+?, last_at=? WHERE wf_id=?",
                (fail, t, wid))
            if u.rowcount == 0:
                self._db.execute(
                    "INSERT INTO wf_counters(wf_id,runs,fails,last_at) VALUES(?,1,?,?)",
                    (wid, fail, t))
            # 明细裁剪：只留最新 max_runs 条
            if self.max_runs > 0:
                self._db.execute(
                    "DELETE FROM workflow_runs WHERE seq <= COALESCE("
                    " (SELECT seq FROM workflow_runs ORDER BY seq DESC LIMIT 1 OFFSET ?), 0)",
                    (self.max_runs,))
            self._db.commit()
        return seq

    @staticmethod
    def _run_dict(r):
        try:
            results = json.loads(r["results"] or "[]")
        except Exception:
            results = []
        return {"seq": r["seq"], "t": r["t"], "wf_id": r["wf_id"], "wf": r["wf_name"],
                "source": r["source"], "source_name": r["source_name"],
                "sender": r["sender"], "content": r["content"],
                "ok": bool(r["ok"]), "results": results}

    def get_runs(self, before=None, wf_id=None, limit=50):
        """游标分页取运行明细(最新在前)。before=seq 取更早的。返回 (list, has_more)。"""
        limit = max(1, min(200, int(limit or 50)))
        where, args = [], []
        if wf_id:
            where.append("wf_id=?"); args.append(str(wf_id))
        if before:
            where.append("seq<?"); args.append(int(before))
        wsql = ("WHERE " + " AND ".join(where)) if where else ""
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM workflow_runs %s ORDER BY seq DESC LIMIT ?" % wsql,
                (*args, limit + 1)).fetchall()
        has_more = len(rows) > limit
        return [self._run_dict(r) for r in rows[:limit]], has_more

    def seed_counter(self, wf_id, runs, fails, last_at):
        """迁移用：仅当该工作流还没有计数行时，写入历史累计值(已有则原样不动)。
        返回 True=写入了。用于从旧版 workflows.json 的 stats 搬迁历史次数。"""
        with self._lock:
            try:
                self._db.execute(
                    "INSERT INTO wf_counters(wf_id,runs,fails,last_at) VALUES(?,?,?,?)",
                    (str(wf_id), int(runs or 0), int(fails or 0), int(last_at or 0)))
            except sqlite3.IntegrityError:
                return False
            self._db.commit()
        return True

    def run_counter(self):
        """全局累计触发/失败数(来自 wf_counters，重启不丢)。"""
        with self._lock:
            r = self._db.execute(
                "SELECT COALESCE(SUM(runs),0) runs, COALESCE(SUM(fails),0) fails"
                " FROM wf_counters").fetchone()
        return {"runs": r["runs"], "fails": r["fails"]}

    def wf_run_stats(self):
        """每条工作流累计统计 {wf_id: {runs, fails, last_at}}。"""
        with self._lock:
            rows = self._db.execute(
                "SELECT wf_id, runs, fails, last_at FROM wf_counters").fetchall()
        return {r["wf_id"]: {"runs": r["runs"], "fails": r["fails"], "last_at": r["last_at"]}
                for r in rows}

    # ---------- 定时触发状态 ----------
    def sched_slots(self):
        """{wf_id: last_slot} 已触发过的时间槽，供调度器判重。"""
        with self._lock:
            rows = self._db.execute("SELECT wf_id, last_slot FROM wf_schedule").fetchall()
        return {r["wf_id"]: r["last_slot"] for r in rows}

    def sched_mark(self, wf_id, slot, ts):
        """标记某工作流已触发到 slot。返回 True=本次抢到(之前没触发过这个槽)。
        用 UPDATE ... WHERE last_slot<>? 做原子判重，多线程同时进来也只会有一个成功。"""
        with self._lock:
            u = self._db.execute(
                "UPDATE wf_schedule SET last_slot=?, last_fire=?"
                " WHERE wf_id=? AND last_slot<>?",
                (slot, int(ts), str(wf_id), slot))
            if u.rowcount == 0:
                try:
                    self._db.execute(
                        "INSERT INTO wf_schedule(wf_id,last_slot,last_fire) VALUES(?,?,?)",
                        (str(wf_id), slot, int(ts)))
                except sqlite3.IntegrityError:
                    self._db.commit()
                    return False          # 已存在且 last_slot 就是 slot -> 已触发过
            self._db.commit()
        return True

    def sched_clear(self, wf_id):
        with self._lock:
            self._db.execute("DELETE FROM wf_schedule WHERE wf_id=?", (str(wf_id),))
            self._db.commit()

    def stats(self):
        with self._lock:
            m = self._db.execute("SELECT COUNT(*) c FROM messages").fetchone()["c"]
            s = self._db.execute("SELECT COUNT(*) c FROM sessions").fetchone()["c"]
            wr = self._db.execute("SELECT COUNT(*) c FROM workflow_runs").fetchone()["c"]
        return {"messages": m, "sessions": s, "workflow_runs": wr}

    def close(self):
        with self._lock:
            self._db.close()

    # ---------- 行 -> dict ----------
    @staticmethod
    def _msg_dict(r):
        k = r.keys()
        d = {
            "seq": r["seq"], "id": r["msg_id"],
            "sender": r["sender"], "sender_name": r["sender_name"],
            "content": r["content"], "msg_type": r["msg_type"],
            "is_self": bool(r["is_self"]), "time": r["ts"],
            "media_id": (r["media_id"] if "media_id" in k else "") or "",
            "revoked": bool(r["revoked"]) if "revoked" in k else False,
        }
        raw = (r["rich"] if "rich" in k else "") or ""
        if raw:
            try:
                d["rich"] = json.loads(raw)
            except Exception:
                pass
        return d

    @staticmethod
    def _media_dict(r):
        return {
            "id": r["id"], "msg_seq": r["msg_seq"], "session_id": r["session_id"],
            "kind": r["kind"], "file_name": r["file_name"], "size": r["size"],
            "md5": r["md5"], "mime": r["mime"], "cdn_type": r["cdn_type"],
            "path": r["path"], "state": r["state"], "err": r["err"],
            "tries": r["tries"], "ts": r["ts"],
        }

    @staticmethod
    def _sess_dict(r):
        k = r.keys()
        return {
            "id": r["id"], "name": r["name"] or r["id"],
            "is_group": bool(r["is_group"]), "last_msg": r["last_msg"],
            "last_time": r["last_time"], "unread": r["unread"],
            # 迁移前的老库没有这两列，用 keys() 兜一下
            "room_id": (r["room_id"] if "room_id" in k else "") or "",
            "bot_id":  (r["bot_id"]  if "bot_id"  in k else "") or "",
        }
