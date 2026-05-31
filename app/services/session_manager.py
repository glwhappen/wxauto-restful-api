"""
智能会话管理器

自动管理微信子窗口的生命周期：
- 用户主动发来消息时自动开启子窗口并注册监听
- 超过指定时间不活跃后自动关闭子窗口
- 子窗口数量超过上限时关闭最久未活跃的
"""
import asyncio
import logging
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)


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

        # who -> 最后活跃时间戳
        self.sessions: Dict[str, float] = {}

        self.config: Dict = {
            "enabled": False,
            "timeout_minutes": 5,       # 不活跃多久后关闭
            "max_sessions": 10,         # 最多同时保留几个子窗口
            "poll_interval_seconds": 2, # 主动轮询间隔
            "filter_mute": False,       # 是否忽略免打扰消息
            "wxname": "",               # 微信实例名
        }

        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------ #
    # 控制接口                                                             #
    # ------------------------------------------------------------------ #

    def start(self) -> bool:
        """启动会话管理器，返回是否成功启动（已运行则返回 False）"""
        if self._task and not self._task.done():
            return False
        self.config["enabled"] = True
        self._task = asyncio.create_task(self._run())
        logger.info("SessionManager started")
        return True

    def stop(self):
        """停止会话管理器"""
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
                "who": who,
                "last_active": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)),
                "idle_seconds": int(now - ts),
                "will_close_in": max(0, int(timeout_secs - (now - ts))),
            }
            for who, ts in sorted(self.sessions.items(), key=lambda x: -x[1])
        ]
        return {
            "running": self.running(),
            "config": self.config,
            "session_count": len(self.sessions),
            "sessions": sessions_info,
        }

    # ------------------------------------------------------------------ #
    # 会话操作                                                             #
    # ------------------------------------------------------------------ #

    def touch(self, who: str):
        """更新联系人的最后活跃时间"""
        self.sessions[who] = time.time()

    async def _ensure_listening(self, who: str):
        """如果该联系人还没有子窗口，自动开启"""
        from app.services.listen_service import manager as ws_manager
        if who in ws_manager.callbacks:
            return  # 已在监听
        try:
            from app.services.listen_service import ListenService
            result = ListenService().start_listen(who=who, wxname=self.config["wxname"] or None)
            if result.success:
                logger.info(f"[SessionManager] 自动开启监听: {who}")
            else:
                logger.warning(f"[SessionManager] 开启监听失败 {who}: {result.message}")
        except Exception as e:
            logger.error(f"[SessionManager] 开启监听异常 {who}: {e}")

    async def _close_session(self, who: str):
        """关闭指定联系人的监听和子窗口"""
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

    async def _poll_and_activate(self, wx_service):
        """轮询一次新消息，如有好友发言则激活其会话"""
        result = await wx_service.get_next_new_message(
            filter_mute=self.config["filter_mute"],
            wxname=self.config["wxname"] or None,
        )
        if not result.success or not result.data:
            return

        chat_info = result.data.get("chat_info", {})
        messages = result.data.get("messages", [])
        who = chat_info.get("chat_name", "")

        if not who:
            return

        has_incoming = any(m.get("src") == "friend" for m in messages)
        if not has_incoming:
            return

        logger.info(f"[SessionManager] 检测到 {who} 新消息，激活会话")
        self.touch(who)
        await self._ensure_listening(who)

    async def _cleanup(self):
        """关闭超时会话 & 超量会话"""
        if not self.sessions:
            return

        now = time.time()
        timeout_secs = self.config["timeout_minutes"] * 60
        max_s = self.config["max_sessions"]

        # 按最后活跃时间排序（最旧在前）
        ordered = sorted(self.sessions.items(), key=lambda x: x[1])

        to_close = set()

        # 超时
        for who, ts in ordered:
            if now - ts > timeout_secs:
                to_close.add(who)

        # 超量（从最旧的开始关）
        active = [w for w, _ in ordered if w not in to_close]
        if len(active) > max_s:
            for who in active[: len(active) - max_s]:
                to_close.add(who)

        for who in to_close:
            logger.info(f"[SessionManager] 自动关闭会话: {who}（超时或超量）")
            await self._close_session(who)


# 全局单例
session_manager = SessionManager()
