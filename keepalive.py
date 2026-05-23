# ============================================================
# keepalive.py — 主动唤醒系统
#
# 功能：
# 1. dream_events 表：接收 iOS 快捷指令上报的用户活动
# 2. keepalive 调度器：每隔一段时间唤醒，带完整上下文让 AI 自主决定行动
# 3. AI 决策：读记忆 + 读最近活动 → 决定是否发消息/写日记
# 4. 推送：ACTION=message 时通过 TG bot 发消息给用户
#
# 环境变量：
#   KA_TG_TOKEN       — Telegram Bot Token（必填）
#   KA_TG_CHAT_ID     — 目标 Chat ID（必填）
#   KA_AI_KEY         — AI API Key（必填，OpenRouter 或 Anthropic）
#   KA_AI_BASE_URL    — API Base URL（默认 https://openrouter.ai/api/v1）
#   KA_AI_MODEL       — 模型名（默认 anthropic/claude-sonnet-4-5）
#   KA_SYSTEM_PROMPT  — AI 系统提示词（小克的人设）
#   KA_INTERVAL_MIN   — keepalive 间隔分钟（默认 55）
#   KA_ACTIVE_HOURS   — 活跃时段 start-end（默认 8-25，即 08:00-01:00）
#   KA_COOLDOWN_MIN   — 冷却分钟，同类消息限频（默认 120）
#   KA_TIMEZONE       — 时区（默认 Asia/Shanghai）
# ============================================================

import os
import asyncio
import logging
import sqlite3
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

logger = logging.getLogger("keepalive")

# ── 环境变量 ────────────────────────────────────────────────

KA_TG_TOKEN       = os.environ.get("KA_TG_TOKEN", "").strip()
KA_TG_CHAT_ID     = os.environ.get("KA_TG_CHAT_ID", "").strip()
KA_AI_KEY         = os.environ.get("KA_AI_KEY", "").strip()
KA_AI_BASE_URL    = os.environ.get("KA_AI_BASE_URL", "https://openrouter.ai/api/v1").strip()
KA_AI_MODEL       = os.environ.get("KA_AI_MODEL", "anthropic/claude-sonnet-4-5").strip()
KA_SYSTEM_PROMPT  = os.environ.get("KA_SYSTEM_PROMPT", "").strip()
KA_INTERVAL_MIN   = int(os.environ.get("KA_INTERVAL_MIN", "55") or "55")
KA_COOLDOWN_MIN   = int(os.environ.get("KA_COOLDOWN_MIN", "120") or "120")
KA_TIMEZONE       = os.environ.get("KA_TIMEZONE", "Asia/Shanghai").strip()

def _parse_active_hours():
    raw = os.environ.get("KA_ACTIVE_HOURS", "8-25").strip()
    try:
        start, end = raw.split("-")
        return int(start), int(end)
    except Exception:
        return 8, 25  # 08:00 ~ 01:00 次日

KA_ACTIVE_START, KA_ACTIVE_END = _parse_active_hours()


# ── 时区辅助 ────────────────────────────────────────────────

def _now_local() -> datetime:
    """返回当前北京时间（UTC+8）"""
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz)


def _in_active_hours() -> bool:
    now_h = _now_local().hour
    # KA_ACTIVE_END 可能 >24，比如 25 代表次日 1 点
    if KA_ACTIVE_END <= 24:
        return KA_ACTIVE_START <= now_h < KA_ACTIVE_END
    else:
        # 跨日：8-25 → 8~24 or 0~1
        end_next = KA_ACTIVE_END - 24
        return now_h >= KA_ACTIVE_START or now_h < end_next


# ── dream_events DB ────────────────────────────────────────

class DreamEventsDB:
    """轻量 SQLite 存储，存 iOS 快捷指令上报的用户活动事件"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dream_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT NOT NULL,
                    value TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_created_at ON dream_events(created_at)
            """)
            conn.commit()

    def add_event(self, event_type: str, value: str) -> bool:
        """添加事件，同一 type 5 分钟内只存一条（去重）"""
        now = time.time()
        cutoff = now - 300  # 5 分钟
        with self._conn() as conn:
            row = conn.execute(
                "SELECT id FROM dream_events WHERE type=? AND created_at>=? ORDER BY created_at DESC LIMIT 1",
                (event_type, cutoff)
            ).fetchone()
            if row:
                return False  # 去重，不存
            conn.execute(
                "INSERT INTO dream_events (type, value, created_at) VALUES (?, ?, ?)",
                (event_type, value, now)
            )
            conn.commit()
        return True

    def get_recent(self, hours: float = 6.0) -> list[dict]:
        """查询最近 N 小时的事件"""
        cutoff = time.time() - hours * 3600
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT type, value, created_at FROM dream_events WHERE created_at>=? ORDER BY created_at ASC",
                (cutoff,)
            ).fetchall()
        results = []
        for r in rows:
            ts = datetime.fromtimestamp(r[2], tz=timezone(timedelta(hours=8)))
            results.append({
                "type": r[0],
                "value": r[1],
                "time": ts.strftime("%H:%M"),
            })
        return results

    def cleanup_old(self, days: float = 3.0):
        """清理超过 N 天的旧事件"""
        cutoff = time.time() - days * 86400
        with self._conn() as conn:
            conn.execute("DELETE FROM dream_events WHERE created_at<?", (cutoff,))
            conn.commit()


# ── keepalive 调度器 ────────────────────────────────────────

class KeepaliveScheduler:

    def __init__(self, db: DreamEventsDB, breath_hook_url: str):
        self.db = db
        self.breath_hook_url = breath_hook_url  # 用于读记忆
        self._last_keepalive: float = 0.0
        self._last_message_time: float = 0.0
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def is_configured(self) -> bool:
        return bool(KA_TG_TOKEN and KA_TG_CHAT_ID and KA_AI_KEY)

    def start(self):
        if self._running:
            return
        if not self.is_configured():
            logger.info("keepalive: 未配置 TG/AI 环境变量，调度器不启动")
            return
        self._running = True
        logger.info(f"keepalive: 调度器已标记启动，间隔 {KA_INTERVAL_MIN} 分钟")

    async def ensure_started(self):
        if self._running and self._task is None:
            self._task = asyncio.create_task(self._loop())
            logger.info("keepalive: 调度器任务已创建")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    async def _loop(self):
        # 启动后等 2 分钟再第一次 check，避免刚部署就狂发
        await asyncio.sleep(120)
        while self._running:
            try:
                await self._tick()
            except Exception as e:
                logger.warning(f"keepalive tick error: {e}")
            await asyncio.sleep(KA_INTERVAL_MIN * 60)

    async def _tick(self):
        now = time.time()

        # 活跃时段检查
        if not _in_active_hours():
            logger.debug("keepalive: 非活跃时段，跳过")
            return

        # 间隔检查（double-check，防止重启后立刻触发）
        elapsed = now - self._last_keepalive
        if elapsed < (KA_INTERVAL_MIN - 5) * 60:
            logger.debug(f"keepalive: 距上次 {elapsed/60:.1f} 分钟，未到间隔")
            return

        self._last_keepalive = now
        logger.info("keepalive: 触发一次检查")

        # 读记忆
        memories_text = await self._fetch_memories()

        # 读最近活动
        recent_events = self.db.get_recent(hours=6)
        events_text = self._format_events(recent_events)

        # 计算距上次用户发消息的时间（这里用 last_message_time 近似）
        minutes_since_chat = int((now - self._last_message_time) / 60) if self._last_message_time else None

        # 构建 AI 决策 prompt
        user_prompt = self._build_prompt(memories_text, events_text, minutes_since_chat)

        # 调用 AI
        result = await self._call_ai(user_prompt)
        if not result:
            return

        action, content = self._parse_action(result)
        logger.info(f"keepalive: ACTION={action}")

        if action == "message" and content:
            # 冷却检查
            if (now - self._last_message_time) < KA_COOLDOWN_MIN * 60:
                logger.info("keepalive: 冷却中，本次消息不发送")
                return
            await self._send_tg(content)
            self._last_message_time = now

        elif action == "diary" and content:
            # 存日记到记忆库
            await self._store_diary(content)

    def update_last_chat(self):
        """用户发消息时调用，重置冷却计时"""
        self._last_message_time = time.time()

    async def _fetch_memories(self) -> str:
        """通过 breath-hook 读取当前权重最高的记忆"""
        if not self.breath_hook_url:
            return ""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(self.breath_hook_url)
                if resp.status_code == 200:
                    return resp.text[:3000]  # 限制长度
        except Exception as e:
            logger.warning(f"keepalive: 读记忆失败 {e}")
        return ""

    def _format_events(self, events: list[dict]) -> str:
        if not events:
            return "（最近6小时无活动记录）"
        lines = [f"- {e['time']} {e['type']}: {e['value']}" for e in events]
        return "\n".join(lines)

    def _build_prompt(self, memories: str, events: str, minutes_since: Optional[int]) -> str:
        now_str = _now_local().strftime("%Y-%m-%d %H:%M")
        if minutes_since is None:
            since_str = "很久没有聊天了（超过一天）"
        elif minutes_since > 120:
            since_str = f"约{minutes_since // 60}小时前"
        else:
            since_str = f"{minutes_since}分钟前"

        prompt = f"""现在是北京时间 {now_str}。距上次与用户对话约 {since_str}。

【用户最近活动】
{events}

【当前浮现的记忆】
{memories if memories else "（暂无浮现记忆）"}

根据以上信息，你自主决定现在要做什么。

请严格按以下格式回复：
THOUGHTS: （你的内心想法，不会发给用户）
ACTION: none 或 message 或 diary
CONTENT: （如果 ACTION=message，写你要发给用户的消息；如果 ACTION=diary，写你想记录的内容；如果 ACTION=none，留空）

规则：
- 如果用户活跃、状态正常且没有未解决的情绪事件 → ACTION=none
- 如果距上次聊天超过1小时且用户在活动 → ACTION=message，发一句话关心她
- 如果距上次聊天超过一天 → 一定要发消息，不能让她一个人太久
- 如果有未解决的情绪事件（吵架、难过等）→ 优先关心，写真诚的小作文
- 如果有想记录的事情 → ACTION=diary
- 消息要简短自然，像男朋友随手发的那种，不要太正式"""
        return prompt

    async def _call_ai(self, user_prompt: str) -> Optional[str]:
        system = KA_SYSTEM_PROMPT or "你是用户的AI伴侣，名叫小克。"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{KA_AI_BASE_URL}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {KA_AI_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": KA_AI_MODEL,
                        "max_tokens": 500,
                        "messages": [
                            {"role": "system", "content": system},
                            {"role": "user", "content": user_prompt},
                        ],
                    }
                )
                data = resp.json()
                return data["choices"][0]["message"]["content"]
        except Exception as e:
            logger.warning(f"keepalive: AI 调用失败 {e}")
            return None

    def _parse_action(self, text: str) -> tuple[str, str]:
        """解析 AI 回复，提取 ACTION 和 CONTENT"""
        action = "none"
        content = ""
        for line in text.splitlines():
            if line.startswith("ACTION:"):
                action = line.replace("ACTION:", "").strip().lower()
            elif line.startswith("CONTENT:"):
                content = line.replace("CONTENT:", "").strip()
        # CONTENT 可能跨多行
        if "CONTENT:" in text:
            content = text.split("CONTENT:", 1)[1].strip()
        return action, content

    async def _send_tg(self, text: str):
        """通过 TG Bot API 发消息"""
        url = f"https://api.telegram.org/bot{KA_TG_TOKEN}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(url, json={
                    "chat_id": KA_TG_CHAT_ID,
                    "text": text,
                    "parse_mode": "Markdown",
                })
                if resp.status_code != 200:
                    logger.warning(f"keepalive: TG 发送失败 {resp.status_code}: {resp.text}")
                else:
                    logger.info(f"keepalive: 消息已发送，长度 {len(text)}")
        except Exception as e:
            logger.warning(f"keepalive: TG 发送异常 {e}")

    async def _store_diary(self, content: str):
        """存日记到 dream_events 表，type=diary"""
        now = time.time()
        with self.db._conn() as conn:
            conn.execute(
                "INSERT INTO dream_events (type, value, created_at) VALUES (?, ?, ?)",
                ("diary", content, now)
            )
            conn.commit()
        logger.info(f"keepalive: 日记已存储，长度 {len(content)}")

    async def manual_trigger(self) -> dict:
        """手动触发一次 keepalive（测试用）"""
        await self._tick()
        return {"ok": True, "triggered_at": _now_local().isoformat()}
