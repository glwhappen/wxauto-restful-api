from typing import Optional
from app.models.response import APIResponse
from .wechat_service import get_wechat_subwin, check_wechat_alive
from .init import WeChat, WxClient

def get_wechat(wxname: str) -> 'WeChat':
    """获取微信实例（带缓存和健康检查）"""
    # 如果没有指定 wxname，使用第一个缓存的实例
    if not wxname:
        if WxClient:
            # 获取第一个有效的实例
            for cached_wx in WxClient.values():
                if check_wechat_alive(cached_wx):
                    print('获取缓存的实例', flush=True)
                    return cached_wx
            # 如果没有有效实例，创建新的
            print('没有有效实例，创建新的实例', flush=True)
            wx = WeChat()
            WxClient[wx.nickname] = wx
            return wx
        else:
            # 缓存为空，创建新实例
            print('缓存为空，创建新实例', flush=True)
            wx = WeChat()
            WxClient[wx.nickname] = wx
            return wx

    # 如果指定了 wxname
    if wxname in WxClient:
        wx = WxClient[wxname]
        # 检查缓存的实例是否有效
        if check_wechat_alive(wx):
            return wx
        else:
            # 实例已失效，重新创建
            print(f"微信实例 {wxname} 已失效，重新创建", flush=True)
            wx = WeChat(nickname=wxname)
            WxClient[wxname] = wx
            return wx
    else:
        # 缓存中没有，创建新实例并缓存
        wx = WeChat(nickname=wxname)
        WxClient[wxname] = wx
        return wx

class ChatService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ChatService, cls).__new__(cls)
        return cls._instance
    
    def __repr__(self):
        return f'<Chat Service object at {id(self)}>'

    def send_message(
        self,
        msg: str,
        who: str,
        clear: bool = True,
        at: Optional[str | list] = None,
        wxname: Optional[str] = None
    ) -> APIResponse:
        subwin = get_wechat_subwin(wxname, who)
        if subwin:
            result = subwin.SendMsg(msg=msg, clear=clear, at=at)
            message = result.get('message') or '操作成功'
            return APIResponse(success=bool(result), message=message, data=result.get('data'))
        else:
            return APIResponse(success=False, message='找不到该聊天窗口')
        
    def get_all_message(
            self,
            who: str,
            wxname: Optional[str] = None
        ) -> APIResponse:
        subwin = get_wechat_subwin(wxname, who)
        if subwin:
            result = subwin.ChatInfo()
            result['msg'] = [msg.raw for msg in subwin.GetAllMessage()]
            return APIResponse(success=True, message='', data=result)
        else:
            return APIResponse(success=False, message='找不到该聊天窗口')

    def get_new_message(
            self,
            who: str,
            wxname: Optional[str] = None
        ) -> APIResponse:
        subwin = get_wechat_subwin(wxname, who)
        if subwin:
            result = subwin.ChatInfo()
            result['msg'] = [msg.raw for msg in subwin.GetNewMessage()]
            return APIResponse(success=True, message='', data=result)
        else:
            return APIResponse(success=False, message='找不到该聊天窗口')
        
    def _get_msg_by_id(
            self,
            msg_id: str,
            who: str,
            wxname: Optional[str] = None
        ) -> APIResponse:
        subwin = get_wechat_subwin(wxname, who)
        if subwin:
            msg = subwin.GetMessageById(msg_id)
            return msg
        else:
            return None
        
    def send_quote_by_id(
            self,
            content: str,
            msg_id: str,
            who: str,
            wxname: Optional[str] = None
        ) -> APIResponse:
        msg = self._get_msg_by_id(msg_id, who, wxname)
        if msg and msg.attr in ('self', 'friend'):
            result = msg.quote(content)
            message = result.get('message') or '操作成功'
            return APIResponse(success=bool(result), message=message, data=result.get('data'))
        else:
            return APIResponse(success=False, message='找不到消息')

    def get_chat_info(self, who: str, wxname: Optional[str] = None) -> APIResponse:
        """获取聊天信息"""
        try:
            subwin = get_wechat_subwin(wxname, who)
            result = subwin.ChatInfo()
            return APIResponse(success=True, message='', data=result)
        except Exception as e:
            return APIResponse(success=False, message=str(e))
        
    def close_sub_window(self, who: str, wxname: Optional[str] = None) -> APIResponse:
        try:
            subwin = get_wechat_subwin(wxname, who)
            if subwin is None:
                return APIResponse(success=False, message=f'窗口不存在：{who}')
            subwin.Close()
            return APIResponse(success=True, message='')

        except Exception as e:
            return APIResponse(success=False, message=str(e))
