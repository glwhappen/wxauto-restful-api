"""
智能会话管理 API

自动管理微信子窗口生命周期：用户发消息时自动开子窗口，超时或超量时自动关闭。
"""
from fastapi import APIRouter
from app.models.response import APIResponse
from app.models.request.session import SessionConfigRequest, SessionActivateRequest
from app.services.session_manager import session_manager

router = APIRouter()


@router.post(
    "/start",
    operation_id="[session]启动会话管理器",
    response_model=APIResponse,
    summary="启动智能会话管理器（自动开关子窗口）",
)
async def start_session_manager():
    """启动会话管理器。

    启动后行为：
    - 自动轮询新消息（`getnextnewmessage`）
    - 检测到好友发来消息时，自动调用 `addlistenchat` 打开子窗口
    - 超过 `timeout_minutes` 分钟无新消息，自动关闭该子窗口
    - 子窗口数量超过 `max_sessions`，自动关闭最久未活跃的

    客户端只需轮询 `GET /v1/listen/messages` 即可收到所有消息，无需手动管理子窗口。
    """
    ok = session_manager.start()
    if not ok:
        return APIResponse(success=False, message="会话管理器已在运行中", data=session_manager.status())
    return APIResponse(success=True, message="会话管理器已启动", data=session_manager.status())


@router.post(
    "/stop",
    operation_id="[session]停止会话管理器",
    response_model=APIResponse,
    summary="停止智能会话管理器",
)
async def stop_session_manager():
    """停止会话管理器（已开启的子窗口不会立即关闭，需手动清理）"""
    session_manager.stop()
    return APIResponse(success=True, message="会话管理器已停止", data=session_manager.status())


@router.get(
    "/status",
    operation_id="[session]查看会话状态",
    response_model=APIResponse,
    summary="查看当前所有活跃会话及管理器状态",
)
async def get_session_status():
    """返回管理器运行状态、配置，以及每个子窗口的活跃时间和剩余倒计时。"""
    return APIResponse(success=True, message="", data=session_manager.status())


@router.post(
    "/config",
    operation_id="[session]更新配置",
    response_model=APIResponse,
    summary="更新会话管理器配置（不需要重启）",
)
async def update_config(request: SessionConfigRequest):
    """动态更新配置，立即生效，不需要重启管理器。

    | 参数 | 说明 | 默认值 |
    |------|------|--------|
    | timeout_minutes | 不活跃多少分钟后自动关闭 | 5 |
    | max_sessions | 最多同时保留几个子窗口 | 10 |
    | poll_interval_seconds | 轮询新消息的间隔（秒）| 2 |
    | filter_mute | 是否忽略免打扰会话 | false |
    """
    updates = {k: v for k, v in request.model_dump().items() if v is not None}
    session_manager.update_config(**updates)
    return APIResponse(success=True, message="配置已更新", data=session_manager.status())


@router.post(
    "/activate",
    operation_id="[session]手动激活会话",
    response_model=APIResponse,
    summary="手动激活指定联系人的会话（立即开子窗口）",
)
async def activate_session(request: SessionActivateRequest):
    """手动为指定联系人打开子窗口并重置活跃计时器。
    适用于机器人主动发起对话、或提前预热某个重要联系人的会话。
    """
    session_manager.touch(request.who)
    await session_manager._ensure_listening(request.who)
    return APIResponse(
        success=True,
        message=f"已激活会话: {request.who}",
        data=session_manager.status(),
    )


@router.delete(
    "/sessions/{who}",
    operation_id="[session]关闭指定会话",
    response_model=APIResponse,
    summary="立即关闭指定联系人的子窗口",
)
async def close_session(who: str):
    """立即关闭指定联系人的子窗口，不等超时。"""
    if who not in session_manager.sessions:
        return APIResponse(success=False, message=f"会话不存在: {who}")
    await session_manager._close_session(who)
    return APIResponse(success=True, message=f"已关闭会话: {who}", data=session_manager.status())
