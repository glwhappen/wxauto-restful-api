"""
智能会话管理器

自动管理微信子窗口的生命周期：
- 用户/群主动发来消息时自动开启子窗口并注册监听
- 超过指定时间不活跃后自动关闭子窗口
- 子窗口数量超过上限时关闭最久未活跃的
- 可分别控制好友 / 群聊是否自动激活
"""
import asyncio
import logging
import time
from typing import Dict, Literal, Optional

logger = logging.getLogger(__name__)

ChatType = Literal["friend", "group", "unknown"]


class Session:
    __slots__ = ("who", "chat_type", "last_active")

    def __init__(self, who: str, chat_type: ChatType):
        self.who = who
        self.chat_type: ChatType = chat_type
        self.last_active: float = time.time()

    def touch(self):
        self.last_active = time.time()


class SessionManager:
    _instance: Optional["SessionManager"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized"):
            return
        self._initialized = True

        # who -> Session
        self.sessions: Dict[str, Session] = {}

        self.config: Dict = {
            "enabled": False,
            "timeout_minutes": 5,        # 不活跃多久后关闭
            "max_sessions": 10,          # 最多同时保留几个子窗口
            "poll_interval_seconds": 2,  # 主动轮询间隔
            "filter_mute": False,        # 是否忽略免打扰消息
            "listen_friends": True,      # 是否自动监听好友消息
            "listen_groups": True,       # 是否自动监听群消息
            "only_at_in_groups": False,  # 群消息只在 @自己 时才激活
            "wxname": "",                # 微信实例名（多开时用）
        }

        self._task: Optional[asyncio.Task] = None
        # 缓存自己的微信名，用于 @检测
        self._my_name: Optional[str] = None

    # ------------------------------------------------------------------ #
    # 控制接口                                                              #
    # ------------------------------------------------------------------ #

    def start(self) -> bool:
        if self._task and not self._task.done():
            return False
        self.config["enabled"] = True
        self._task = asyncio.create_task(self._run())
        logger.info("SessionManager started")
        return True

    def stop(self):
        self.config["enabled"] = False
        if self._task:
            self._task.cancel()
        logger.info("SessionManager stopped")

    def running(self) -> bool:
        return bool(self._task and not self._task.done())

    def update_config(self, **kwargs):
        for k, v in kwargs.items():
            if k in self.config:
                self.config[k] = v

    def status(self) -> dict:
        now = time.time()
        timeout_secs = self.config["timeout_minutes"] * 60
        sessions_info = [
            {
                "who": s.who,
                "chat_type": s.chat_type,
                "last_active": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(s.last_active)),
                "idle_seconds": int(now - s.last_active),
                "will_close_in": max(0, int(timeout_secs - (now - s.last_active))),
            }
            for s in sorted(self.sessions.values(), key=lambda x: -x.last_active)
        ]
        friends = sum(1 for s in self.sessions.values() if s.chat_type == "friend")
        groups = sum(1 for s in self.sessions.values() if s.chat_type == "group")
        return {
            "running": self.running(),
            "config": self.config,
            "session_count": len(self.sessions),
            "friend_sessions": friends,
            "group_sessions": groups,
            "sessions": sessions_info,
        }

    # ------------------------------------------------------------------ #
    # 会话操作                                                              #
    # ------------------------------------------------------------------ #

    def touch(self, who: str, chat_type: ChatType = "unknown"):
        if who in self.sessions:
            self.sessions[who].touch()
            if chat_type != "unknown":
                self.sessions[who].chat_type = chat_type
        else:
            self.sessions[who] = Session(who, chat_type)

    async def _ensure_listening(self, who: str):
        from app.services.listen_service import manager as ws_manager
        if who in ws_manager.callbacks:
            return
        try:
            from app.services.listen_service import ListenService
            result = ListenService().start_listen(who=who, wxname=self.config["wxname"] or None)
            if result.success:
                logger.info(f"[SessionManager] 自动开启监听: {who}")
            else:
                logger.warning(f"[SessionManager] 开启监听失败 {who}: {result.message}")
        except Exception as e:
            logger.error(f"[SessionManager] 开启监听异常 {who}: {e}")

    def _push_to_queue(self, who: str, chat_type: ChatType, messages: list):
        """将 getnextnewmessage 拿到的消息补推进 HTTP 队列"""
        from app.services.listen_service import _http_message_queue
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        for m in messages:
            if not isinstance(m, dict) or m.get("type") == "time":
                continue
            entry = {
                **m,
                "chat_name": who,
                "chat_type": chat_type,
                "listen_who": who,
                "received_at": now,
            }
            _http_message_queue.appendleft(entry)

    async def _close_session(self, who: str):
        try:
            from app.services.listen_service import ListenService
            ListenService().stop_listen(who=who, wxname=self.config["wxname"] or None)
            logger.info(f"[SessionManager] 已关闭会话: {who}")
        except Exception as e:
            logger.error(f"[SessionManager] 关闭会话异常 {who}: {e}")
        self.sessions.pop(who, None)

    # ------------------------------------------------------------------ #
    # 后台主循环                                                            #
    # ------------------------------------------------------------------ #

    async def _run(self):
        from app.services.wechat_service import WeChatService
        wx_service = WeChatService()
        await self._fetch_my_name(wx_service)

        while self.config["enabled"]:
            try:
                await self._poll_and_activate(wx_service)
                await self._cleanup()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[SessionManager] 循环异常: {e}")

            await asyncio.sleep(self.config["poll_interval_seconds"])

        logger.info("SessionManager loop exited")

    async def _fetch_my_name(self, wx_service):
        """启动时获取自己的微信名，用于群 @ 检测"""
        try:
            result = await wx_service.get_my_info()
            if result.success and result.data:
                self._my_name = result.data.get("item", {}).get("display_name", "")
                logger.info(f"[SessionManager] 我的微信名: {self._my_name}")
        except Exception as e:
            logger.warning(f"[SessionManager] 获取自己信息失败: {e}")

    async def _poll_and_activate(self, wx_service):
        result = await wx_service.get_next_new_message(
            filter_mute=self.config["filter_mute"],
            wxname=self.config["wxname"] or None,
        )
        if not result.success or not result.data:
            return

        chat_info = result.data.get("chat_info", {})
        messages = result.data.get("messages", [])
        who = chat_info.get("chat_name", "")
        chat_type: ChatType = chat_info.get("chat_type", "unknown")  # "friend" / "group"

        if not who:
            return

        incoming = [m for m in messages if m.get("src") == "friend"]
        if not incoming:
            return

        # 根据类型决定是否激活
        if chat_type == "friend" and not self.config["listen_friends"]:
            return
        if chat_type == "group" and not self.config["listen_groups"]:
            return

        # 群聊：only_at_in_groups 开启时，只在消息含 @ 自己时激活
        if chat_type == "group" and self.config["only_at_in_groups"]:
            my_name = self._my_name or ""
            mentioned = any(
                (f"@{my_name}" in (m.get("content") or "") or
                 my_name in (m.get("at_list") or []))
                for m in incoming
            )
            if not mentioned:
                return

        type_label = "好友" if chat_type == "friend" else "群"
        logger.info(f"[SessionManager] 检测到{type_label}消息: {who}，激活会话")
        self.touch(who, chat_type)

        # 将触发激活的消息手动推入 HTTP 队列（子窗口尚未开启时回调不会触发）
        self._push_to_queue(who, chat_type, messages)

        await self._ensure_listening(who)

    async def _cleanup(self):
        if not self.sessions:
            return

        now = time.time()
        timeout_secs = self.config["timeout_minutes"] * 60
        max_s = self.config["max_sessions"]

        ordered = sorted(self.sessions.values(), key=lambda s: s.last_active)
        to_close: set = set()

        for s in ordered:
            if now - s.last_active > timeout_secs:
                to_close.add(s.who)

        active = [s.who for s in ordered if s.who not in to_close]
        if len(active) > max_s:
            for who in active[: len(active) - max_s]:
                to_close.add(who)

        for who in to_close:
            chat_type = self.sessions[who].chat_type if who in self.sessions else "?"
            logger.info(f"[SessionManager] 自动关闭会话: {who}（{chat_type}，超时或超量）")
            await self._close_session(who)


# 全局单例
session_manager = SessionManager()
