from pydantic import BaseModel, Field
from typing import Optional


class SessionConfigRequest(BaseModel):
    timeout_minutes: Optional[int] = Field(None, ge=1, le=60, description="不活跃多少分钟后自动关闭（1-60）")
    max_sessions: Optional[int] = Field(None, ge=1, le=50, description="最大同时子窗口数量（1-50）")
    poll_interval_seconds: Optional[int] = Field(None, ge=1, le=30, description="轮询间隔秒数（1-30）")
    filter_mute: Optional[bool] = Field(None, description="是否忽略免打扰消息")
    wxname: Optional[str] = Field(None, description="微信实例名，单开留空")


class SessionActivateRequest(BaseModel):
    who: str = Field(..., description="手动激活指定联系人的会话")
    wxname: Optional[str] = Field(None, description="微信实例名，单开留空")
