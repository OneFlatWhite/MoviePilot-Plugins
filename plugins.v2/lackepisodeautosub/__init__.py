# -*- coding: utf-8 -*-
"""
MoviePilot V2 自定义插件：缺集自动补齐（LackEpisodeAutoSub）

功能：定时扫描 Emby 剧集库，对比 TMDB 找出缺集的剧，按「优先级策略 + 每日配额」
     自动添加 MP 订阅；并通过「下载验证回环」持续盯梢：订阅后复查 Emby 是否真入库、
     qBittorrent 是否有死任务、下载目录磁盘是否告急，形成完整闭环。

参考实现（均已核对真实源码）：
  - jxxghp/MoviePilot-Plugins plugins.v2/doubanrank     （官方插件骨架、cron 服务注册、SubscribeChain.add 用法）
  - jxxghp/MoviePilot-Plugins plugins.v2/torrentremover （DownloaderHelper 获取下载器实例、get_torrents 读任务、
                                                         delete_torrents 删任务、settings.TORRENT_TAG 即 MP 任务标签、
                                                         qb 任务字段 .hash/.name/.size/.added_on/.progress）
  - FUJIWARESHINE/MoviePilot-Plugins plugins.v2/mediamissingsubscribe_me （Emby 库遍历、TMDB 缺集对比、SubscribeOper 去重）
  - jxxghp/MoviePilot v2 主程序源码（核实 API 真实签名）：
      app/chain/subscribe.py      SubscribeChain.add(title, year, mtype, tmdbid, season, exist_ok, username, message, **kwargs)
      app/db/subscribe_oper.py    SubscribeOper.exists(tmdbid, doubanid, ..., season=...)
      app/db/models/subscribe.py  Subscribe 模型含 best_version 字段（经 **kwargs 透传）
      app/chain/mediaserver.py    MediaServerChain.librarys/items/episodes
      app/schemas/mediaserver.py  MediaServerSeasonInfo.season / .episodes
      app/core/context.py         MediaInfo 含 original_language / origin_country / genre_ids / vote_average
                                  （recognize_media 一次调用即带回，无需为优先级额外请求 TMDB 详情）

版本历史：
  v1.4.0  新增「爱影 115 通道」（实验功能）：
          ①插件内嵌 TG 用户态会话管理器（Telethon + 独立 daemon 线程跑 asyncio
            事件循环，同步代码经 run_coroutine_threadsafe 调用），会话文件存插件
            数据目录，Telethon 未安装/未登录时整体降级为「未启用」，绝不影响 PT 主流程；
          ②缺集订阅前先问资源机器人（默认 @ayclub_bot）拿 ed2k/115 链接，
            转发给 SA 转存机器人自动离线到 115，全部补齐则不再走 MP 订阅，
            部分补齐则剩余集落回 PT 兜底（history 标注混合渠道）；
          ③新增 /tg_send_code /tg_verify /tg_status 三个登录 API，
            配置页提供一次性开关完成「发验证码/完成登录」全流程；
          ④验证回环核销时，115 渠道已补齐的剧自动退订本插件此前添加的 PT 订阅，
            避免重复下载；通知与历史记录区分 [爱影115] / [PT下载] 来源
  v1.3.2  修复严重 bug：订阅阶段与扫描阶段共用同一个超时计时，
          大库扫满超时预算后订阅阶段被秒判「超时收尾」，候选剧一部都订不出去；
          现改为订阅阶段从进入时独立计时（时长仍=配置的扫描超时分钟数）
  v1.3.1  修复：msChain.items() 返回生成器时被进度统计提前消费，
          导致整轮扫描 0 部的严重 bug（先 list() 物化再统计）
  v1.3.0  实时进度与可见性大修：
          ①详情页顶部新增「运行进度卡片」——进度条 + 当前正在扫的剧名 +
            本轮实时计数（已扫描/发现缺集/已订阅/跳过/失败），扫描没跑完也能看到数字；
          ②历史记录改为每产生一条就立刻落盘（原来整轮结束才保存，中途看是空的）；
          ③订阅阶段开始前明确打印候选数与当日剩余配额，订阅中每部更新进度；
          ④GET /status API 同步返回 progress 字段，方便外部轮询进度条
  v1.2.2  扫描周期改为小白友好的「频率下拉 + 几点几分」组合，内部仍生成 cron；
          老配置的 cron 字段自动迁移（能解析映射为对应频率，不能则落入自定义）
  v1.2.1  补上抽象方法 get_api（修复真机加载失败），注册 /scan 与 /status 两个 API
  v1.2.0  新增下载验证回环：入库验证状态机、qb 死任务检测、磁盘告警
  v1.1.0  新增优先级策略（地区/类型分层 + 排序规则）与风控三连
  v1.0.0  首版：Emby 缺集扫描 + 每日配额自动订阅
"""
import datetime
import os
import re
import threading
import time
import traceback
from threading import Event as ThreadEvent
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# FastAPI Body：插件 API 接收 JSON body 用（MP 用 app.add_api_route 直接注册端点，
# 支持 FastAPI 依赖注入）；极端情况下导入失败则退化为普通查询参数
try:
    from fastapi import Body
except Exception:
    Body = None

from app.chain.media import MediaChain
from app.chain.mediaserver import MediaServerChain
from app.chain.subscribe import SubscribeChain
from app.chain.tmdb import TmdbChain
from app.core.config import settings
from app.db.subscribe_oper import SubscribeOper
from app.helper.downloader import DownloaderHelper
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaType, NotificationType

# ---------------------------------------------------------------------------
# 优先级策略常量
# ---------------------------------------------------------------------------

# 可选优先地区：值 -> 显示名（基于 TMDB original_language / origin_country 判断）
REGION_OPTIONS: Dict[str, str] = {
    "cn": "中国大陆",
    "hk": "中国香港",
    "tw": "中国台湾",
    "jp": "日本",
    "kr": "韩国",
    "west": "欧美",
    "other": "其他",
}

# 归入「欧美」的 origin_country 白名单（original_language == en 也归欧美）
WEST_COUNTRIES: Set[str] = {
    "US", "GB", "FR", "DE", "IT", "ES", "CA", "AU", "NL", "SE",
    "NO", "DK", "FI", "BE", "CH", "AT", "IE", "PT", "PL", "RU",
}

# TMDB 类型 ID（TMDB 官方定义，动漫=16，纪录片=99）
GENRE_ID_ANIMATION = 16
GENRE_ID_DOCUMENTARY = 99

# 排序规则
SORT_RULES: Dict[str, str] = {
    "least_missing": "缺集少的优先",
    "most_missing": "缺集多的优先",
    "top_rated": "评分高的优先",
}

# 扫描频率选项（v1.2.2 新增，替代裸 cron 输入框）
SCAN_FREQ_OPTIONS: Dict[str, str] = {
    "daily": "每天一次",
    "12h": "每 12 小时",
    "8h": "每 8 小时",
    "6h": "每 6 小时",
    "custom": "自定义（高级）",
}

# 时间字符串格式（订阅时间戳持久化用）
TIME_FMT = "%Y-%m-%d %H:%M:%S"

# ---------------------------------------------------------------------------
# 爱影 115 通道：TG 用户态会话（v1.4.0 新增）
# ---------------------------------------------------------------------------

# Telegram Desktop 官方开源公开凭证（github.com/telegramdesktop/tdesktop 源码内）
TG_API_ID = 2040
TG_API_HASH = "b18441a1ff607e10a989891a5462e627"
# 默认代理（NAS 本地代理，Telegram 直连不可达时需要）
TG_DEFAULT_PROXY = "http://192.168.31.40:7890"

# 爱影资源列表行：🧲 [国漫] 斗罗大陆Ⅱ绝世唐门 (2023) {tmdb-228429} S01E171 4K TX WEB-DL 1.57G
_AIYING_LINE_RE = re.compile(r"\{tmdb-(\d+)\}\s*S(\d+)E(\d+)\b(.*)", re.I)
# 行尾文件大小（1.57G / 800M / 500K）
_AIYING_SIZE_RE = re.compile(r"([\d.]+)\s*([GMK])B?\s*[-\s]*$", re.I)
# 回复里的「本月剩余次数：997」
_AIYING_QUOTA_RE = re.compile(r"本月剩余次数[：:]\s*(\d+)")
# 115 分享链接
_LINK_115_RE = re.compile(r"https?://(?:115\.com|115cdn\.com|115cdn\.net)/s/[A-Za-z0-9]+[^\s<>\"']*")
# ed2k 链接
_ED2K_RE = re.compile(r"ed2k://\|file\|[^\s]+")
# 按钮文字里的季集号（兼容 S1E171 / S01E171 两种写法）
_BTN_SE_RE = re.compile(r"S0*(\d+)E0*(\d+)", re.I)

# Telethon 兜底导入：依赖未装上时插件照常加载，爱影通道整体降级为「未启用」
try:
    import asyncio
    from telethon import TelegramClient
    from telethon.errors import SessionPasswordNeededError
    _TG_LIB_OK = True
except Exception:
    asyncio = None
    TelegramClient = None
    SessionPasswordNeededError = Exception
    _TG_LIB_OK = False


def _parse_aiying_lines(text: str) -> List[Dict[str, Any]]:
    """
    解析爱影资源列表文本，返回条目列表：
      {"tmdbid": int, "season": int, "episode": int, "qrank": int, "size": float(MB)}
    qrank：画质档位（2160p/4K=3，1080p=2，720p=1，其他=0）；size 统一折算成 MB 便于比较。
    纯函数，不依赖 MP/TG 环境，可独立测试。
    """
    entries: List[Dict[str, Any]] = []
    for line in (text or "").splitlines():
        m = _AIYING_LINE_RE.search(line)
        if not m:
            continue
        tail = m.group(4) or ""
        # 解析文件大小，统一折算成 MB
        size_mb = 0.0
        sm = _AIYING_SIZE_RE.search(tail.strip())
        if sm:
            try:
                size_mb = float(sm.group(1))
                unit = sm.group(2).upper()
                if unit == "G":
                    size_mb *= 1024
                elif unit == "K":
                    size_mb /= 1024
            except (TypeError, ValueError):
                size_mb = 0.0
        # 解析画质档位
        upper = tail.upper()
        if "2160P" in upper or "4K" in upper:
            qrank = 3
        elif "1080P" in upper:
            qrank = 2
        elif "720P" in upper:
            qrank = 1
        else:
            qrank = 0
        entries.append({
            "tmdbid": int(m.group(1)),
            "season": int(m.group(2)),
            "episode": int(m.group(3)),
            "qrank": qrank,
            "size": size_mb,
        })
    return entries


class _AiyingTgManager:
    """
    TG 用户态会话管理器（进程内单例，v1.4.0 新增）。

    独立 daemon 线程跑 asyncio 事件循环，插件同步代码经
    asyncio.run_coroutine_threadsafe 提交协程调用；Telethon 客户端只在该
    循环线程内创建与使用，避免跨线程/跨循环问题。
    所有 public 方法自带 try/except 兜底，任何异常都只返回错误字典，
    绝不向上抛，确保 PT 订阅主流程不受影响。
    """

    def __init__(self):
        self._loop = None                  # 独立线程里的 asyncio 事件循环
        self._thread: Optional[threading.Thread] = None
        self._client = None                # Telethon 客户端（只在循环线程内使用）
        self._session_path: str = ""       # 会话文件路径
        self._proxy_url: str = ""          # 代理地址
        self._phone_code_hash: Optional[str] = None  # 登录中间态
        self._login_phone: str = ""

    # ------------------------- 同步封装（插件线程调用） -------------------------
    def configure(self, session_path: str, proxy_url: str):
        """配置会话文件与代理；配置变化时丢弃旧客户端，下次使用自动重连"""
        try:
            session_path = str(session_path or "")
            proxy_url = str(proxy_url or "")
            if session_path != self._session_path or proxy_url != self._proxy_url:
                self._disconnect()
            self._session_path = session_path
            self._proxy_url = proxy_url
        except Exception:
            pass

    def status(self) -> Dict[str, Any]:
        """查询登录状态：{ok, logged_in, username, first_name, phone, error}"""
        if not _TG_LIB_OK:
            return {"ok": False, "logged_in": False, "error": "telethon 未安装"}
        try:
            return self.__run(self.__status_async(), timeout=60)
        except Exception as e:
            return {"ok": False, "logged_in": False, "error": str(e)}

    def send_code(self, phone: str) -> Dict[str, Any]:
        """发送登录验证码"""
        if not _TG_LIB_OK:
            return {"ok": False, "error": "telethon 未安装"}
        try:
            return self.__run(self.__send_code_async(phone), timeout=60)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def verify(self, phone: str, code: str, password: str = "") -> Dict[str, Any]:
        """提交验证码完成登录（支持两步验证密码）"""
        if not _TG_LIB_OK:
            return {"ok": False, "error": "telethon 未安装"}
        try:
            return self.__run(self.__verify_async(phone, code, password), timeout=60)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def collect(self, bot: str, keyword: str, tmdbid: int,
                lack_eps: Set[Tuple[int, int]], max_pages: int = 5,
                interval: int = 3, click_budget: int = 100) -> Dict[str, Any]:
        """
        给爱影机器人发关键词，分页收集缺集条目并逐集点击按钮拿 ed2k/115 链接。
        返回：{ok, links: {(季,集): url}, quota_left, clicks, error}
        """
        if not _TG_LIB_OK:
            return {"ok": False, "error": "telethon 未安装"}
        try:
            return self.__run(
                self.__collect_async(bot, keyword, tmdbid, lack_eps,
                                     max_pages, interval, click_budget),
                timeout=600)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def submit(self, sa_bot: str, items: List[Tuple[str, str]],
               interval: int = 3) -> Dict[str, Any]:
        """
        把 ed2k/115 链接逐条发给 SA 转存机器人。
        items: [(集标签如 S01E03, 链接)]；返回 {ok, results: {标签: {ok, msg}}}
        判定：回复含「失败」且不含「任务已存在」记失败，其余（含超时无回复前的成功回复）记成功。
        """
        if not _TG_LIB_OK:
            return {"ok": False, "error": "telethon 未安装"}
        try:
            return self.__run(self.__submit_async(sa_bot, items, interval), timeout=900)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def shutdown(self):
        """优雅关闭：断开客户端并停止事件循环线程（下次使用自动重建）"""
        try:
            self._disconnect()
            loop = self._loop
            if loop and loop.is_running():
                loop.call_soon_threadsafe(loop.stop)
            self._loop = None
            self._thread = None
        except Exception:
            pass

    # ------------------------- 内部：事件循环与客户端 -------------------------
    def _disconnect(self):
        """在当前配置下断开并丢弃 Telethon 客户端"""
        try:
            if self._client and self._loop and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._client.disconnect(), self._loop).result(15)
        except Exception:
            pass
        self._client = None

    def _ensure_loop(self):
        """确保独立 daemon 线程与事件循环已启动"""
        if self._loop and self._loop.is_running():
            return
        self._loop = None

        def _thread_main():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            loop.run_forever()

        self._thread = threading.Thread(
            target=_thread_main, name="aiying-tg-loop", daemon=True)
        self._thread.start()
        # 等循环就绪（最多 5 秒）
        for _ in range(50):
            if self._loop and self._loop.is_running():
                break
            time.sleep(0.1)
        if not (self._loop and self._loop.is_running()):
            raise RuntimeError("TG 事件循环启动失败")

    def __run(self, coro, timeout: int = 120):
        """把协程提交到独立事件循环并同步等待结果"""
        self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout)

    async def __ensure_client(self):
        """在循环线程内确保 Telethon 客户端已创建并连接"""
        if self._client is None:
            proxy = None
            m = re.match(r"^(https?|socks5?)://([^:/]+):(\d+)$",
                         (self._proxy_url or "").strip())
            if m:
                scheme = "socks5" if m.group(1).startswith("socks") else "http"
                proxy = (scheme, m.group(2), int(m.group(3)))
            self._client = TelegramClient(
                self._session_path, TG_API_ID, TG_API_HASH, proxy=proxy)
        if not self._client.is_connected():
            await self._client.connect()
        return self._client

    # ------------------------- 内部：协程实现 -------------------------
    async def __status_async(self) -> Dict[str, Any]:
        client = await self.__ensure_client()
        if not await client.is_user_authorized():
            return {"ok": True, "logged_in": False}
        me = await client.get_me()
        return {
            "ok": True,
            "logged_in": True,
            "username": getattr(me, "username", "") or "",
            "first_name": getattr(me, "first_name", "") or "",
            "phone": getattr(me, "phone", "") or "",
        }

    async def __send_code_async(self, phone: str) -> Dict[str, Any]:
        client = await self.__ensure_client()
        result = await client.send_code_request(phone)
        # phone_code_hash 存内存单例，/tg_verify 时用
        self._phone_code_hash = result.phone_code_hash
        self._login_phone = phone
        return {"ok": True}

    async def __verify_async(self, phone: str, code: str,
                             password: str = "") -> Dict[str, Any]:
        client = await self.__ensure_client()
        # TG 验证码常被用户带空格/横杠复制，先清洗
        code = re.sub(r"[\s-]+", "", code or "")
        try:
            await client.sign_in(phone, code,
                                 phone_code_hash=self._phone_code_hash)
        except SessionPasswordNeededError:
            if not password:
                return {"ok": False,
                        "error": "账号开启了两步验证，请填写两步验证密码后重试"}
            await client.sign_in(password=password)
        me = await client.get_me()
        return {
            "ok": True,
            "logged_in": True,
            "username": getattr(me, "username", "") or "",
            "first_name": getattr(me, "first_name", "") or "",
            "phone": getattr(me, "phone", "") or "",
        }

    async def __wait_reply(self, client, bot: str, sent, timeout: int = 40):
        """等机器人回复：每 2 秒拉一次最新消息，取发送之后的第一条机器人消息"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            await asyncio.sleep(2)
            msgs = await client.get_messages(bot, limit=5)
            for m in msgs:
                if not m.out and sent is not None and m.id > sent.id:
                    return m
        return None

    async def __wait_link(self, client, bot: str,
                          before_ids: Set[int], timeout: int = 15) -> Optional[str]:
        """点击按钮后等机器人发含 ed2k/115 链接的新消息，返回第一个链接"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            await asyncio.sleep(2)
            async for m in client.iter_messages(bot, limit=10):
                if m.id in before_ids:
                    break
                if m.out:
                    continue
                text = m.text or ""
                ed2k = _ED2K_RE.findall(text)
                if ed2k:
                    return ed2k[0]
                links115 = _LINK_115_RE.findall(text)
                if links115:
                    return links115[0]
        return None

    async def __collect_async(self, bot: str, keyword: str, tmdbid: int,
                              lack_eps: Set[Tuple[int, int]], max_pages: int,
                              interval: int, click_budget: int) -> Dict[str, Any]:
        client = await self.__ensure_client()
        if not await client.is_user_authorized():
            return {"ok": False, "error": "TG 未登录"}
        bot = (bot or "").lstrip("@")
        sent = await client.send_message(bot, keyword)
        reply = await self.__wait_reply(client, bot, sent, timeout=40)
        if reply is None:
            return {"ok": False, "error": "等待机器人回复超时"}

        quota_left: Optional[int] = None
        links: Dict[Tuple[int, int], str] = {}
        clicks = 0
        page_msg = reply

        for _page in range(max(1, max_pages)):
            if not page_msg:
                break
            text = page_msg.text or ""
            # 每页都尝试刷新「本月剩余次数」（翻页后可能变化）
            qm = _AIYING_QUOTA_RE.search(text)
            if qm:
                quota_left = int(qm.group(1))

            # 本页按钮：{(季,集): 按钮文字}
            btn_map: Dict[Tuple[int, int], str] = {}
            if page_msg.buttons:
                for row in page_msg.buttons:
                    for btn in row:
                        bm = _BTN_SE_RE.search(btn.text or "")
                        if bm:
                            btn_map[(int(bm.group(1)), int(bm.group(2)))] = btn.text

            # 本页条目过滤：tmdbid 一致 + 属于缺集 + 还没拿到链接 + 有对应按钮；
            # 同一集多条时选码率最高（qrank 优先，其次文件大者优先）
            best: Dict[Tuple[int, int], Dict[str, Any]] = {}
            for entry in _parse_aiying_lines(text):
                key = (entry["season"], entry["episode"])
                if entry["tmdbid"] != tmdbid:
                    continue
                if key not in lack_eps or key in links or key not in btn_map:
                    continue
                cur = best.get(key)
                if cur is None or (entry["qrank"], entry["size"]) > (cur["qrank"], cur["size"]):
                    best[key] = entry

            # 逐集点击按钮拿 ed2k/115 链接
            for key in sorted(best.keys()):
                if clicks >= click_budget:
                    break
                before_ids = {m.id async for m in client.iter_messages(bot, limit=10)}
                try:
                    await page_msg.click(text=btn_map[key])
                except Exception:
                    continue
                clicks += 1
                # 风控：每次点击后间隔，防点爆爱影次数
                await asyncio.sleep(max(1, interval))
                link = await self.__wait_link(client, bot, before_ids, timeout=15)
                if link:
                    links[key] = link

            # 缺集全部覆盖或点击预算用尽：停
            if all(k in links for k in lack_eps) or clicks >= click_budget:
                break

            # 翻页：点「下一页」按钮，消息会被编辑，重新拉取
            next_text = None
            if page_msg.buttons:
                for row in page_msg.buttons:
                    for btn in row:
                        if "下一页" in (btn.text or ""):
                            next_text = btn.text
                            break
                    if next_text:
                        break
            if not next_text:
                break
            try:
                await page_msg.click(text=next_text)
            except Exception:
                break
            await asyncio.sleep(2)
            try:
                page_msg = await client.get_messages(bot, ids=page_msg.id)
            except Exception:
                break

        return {"ok": True, "links": links, "quota_left": quota_left,
                "clicks": clicks}

    async def __submit_async(self, sa_bot: str, items: List[Tuple[str, str]],
                             interval: int) -> Dict[str, Any]:
        client = await self.__ensure_client()
        if not await client.is_user_authorized():
            return {"ok": False, "error": "TG 未登录"}
        bot = (sa_bot or "").lstrip("@")
        results: Dict[str, Dict[str, Any]] = {}
        for label, url in items:
            try:
                sent = await client.send_message(bot, url)
                reply = await self.__wait_reply(client, bot, sent, timeout=30)
                text = (reply.text or "") if reply else ""
                # 「115离线下载失败：任务已存在」也算成功（说明 115 已有该文件）
                if "失败" in text and "任务已存在" not in text:
                    results[label] = {"ok": False, "msg": (text or "无回复")[:120]}
                else:
                    results[label] = {"ok": True, "msg": text[:120]}
            except Exception as e:
                results[label] = {"ok": False, "msg": str(e)[:120]}
            # 风控：逐条间隔
            await asyncio.sleep(max(1, interval))
        return {"ok": True, "results": results}


# TG 会话管理器进程内单例
_TG_MANAGER: Optional[_AiyingTgManager] = None
_TG_MANAGER_LOCK = threading.Lock()


def _get_tg_manager() -> Optional[_AiyingTgManager]:
    """获取 TG 会话管理器单例；Telethon 未安装时返回 None（功能降级）"""
    global _TG_MANAGER
    if not _TG_LIB_OK:
        return None
    with _TG_MANAGER_LOCK:
        if _TG_MANAGER is None:
            _TG_MANAGER = _AiyingTgManager()
    return _TG_MANAGER


class LackEpisodeAutoSub(_PluginBase):
    # 插件名称
    plugin_name = "缺集自动补齐"
    # 插件描述
    plugin_desc = "定时扫描 Emby 剧集库找出缺集的剧，按优先级与每日配额自动订阅，并验证下载入库闭环。"
    # 插件图标（本仓库 icons/ 目录）
    plugin_icon = "https://raw.githubusercontent.com/OneFlatWhite/MoviePilot-Plugins/main/icons/lackepisodeautosub.png"
    # 插件版本
    plugin_version = "1.4.0"
    # 插件作者
    plugin_author = "coldbrew"
    # 作者主页
    author_url = "https://github.com/OneFlatWhite/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "lackepisodeautosub_"
    # 加载顺序
    plugin_order = 30
    # 可使用的用户级别（1 = 所有用户可见可用）
    auth_level = 1

    # ------------------------------------------------------------------
    # 私有属性
    # ------------------------------------------------------------------
    _scheduler: Optional[BackgroundScheduler] = None
    # 立即停止一次性任务用的中断标志（注意必须用 threading 的 Event，
    # 不能与 app.core.event.Event 同名混淆）
    _event: ThreadEvent = ThreadEvent()

    # MP 功能链 / 操作类（init_plugin 时重建，热重载安全）
    _subChain: Optional[SubscribeChain] = None      # 订阅链：添加订阅
    _subOper: Optional[SubscribeOper] = None        # 订阅表操作：判断订阅是否已存在
    _mediaChain: Optional[MediaChain] = None        # 媒体链：按 TMDB ID 识别媒体信息
    _tmdbChain: Optional[TmdbChain] = None          # TMDB 链：获取某季分集信息
    _msChain: Optional[MediaServerChain] = None     # 媒体服务器链：遍历 Emby 库
    _msHelper: Optional[MediaServerHelper] = None   # 媒体服务器助手：获取已配置服务器

    # ------------------------- 配置项默认值 -------------------------
    # 【基础】
    _enabled: bool = False               # 启用开关
    _onlyonce: bool = False              # 立即运行一次
    # 扫描周期（v1.2.2 起改为频率+时刻组合，内部仍生成 cron 字符串 _cron）
    _scan_freq: str = "daily"            # 扫描频率：daily/12h/8h/6h/custom
    _scan_hour: int = 4                  # 几点跑（0-23，仅"每天一次"生效）
    _scan_minute: int = 17               # 几分跑（0-59，各频率通用）
    _cron_custom: str = ""               # 自定义 cron（仅"自定义"频率生效）
    _cron: str = "17 4 * * *"            # 由上面四个字段生成的最终 cron（注册定时任务用）
    _dry_run: bool = True                # 调试模式：仅记录不订阅（默认开，首次先跑一轮看日志）
    _notify: bool = True                 # 每轮跑完是否发送汇总通知
    _clear_history: bool = False         # 清空历史记录与已处理清单（一次性）

    # 【过滤】
    _max_missing: int = 100              # 单部剧缺集数上限，超过则跳过（防止整部误识别）
    _ignore_s0: bool = True              # 忽略特别篇 S00
    _ignore_unfinished_latest: bool = True  # 未完结剧集忽略其最新季（还在更新，不算缺集）
    _exclude_keywords: str = ""          # 排除关键词，逗号分隔，命中剧名则跳过
    _best_version: bool = False          # 订阅时是否洗版

    # 【优先级策略】（v1.1 新增；老配置没有这些 key 时 get() 默认值兜底，向后兼容）
    _priority_regions: List[str] = []    # 优先地区多选（REGION_OPTIONS 的 key），空=不区分
    _priority_anime: bool = False        # 动漫优先（TMDB genre 含 Animation id=16）
    _priority_documentary: bool = False  # 纪录片优先（TMDB genre 含 Documentary id=99）
    _sort_rule: str = "least_missing"    # 同层排序规则：缺集少/缺集多/评分高

    # 【风控】（v1.1 新增）
    _daily_quota: int = 200              # 每日最大订阅数（部/天，建议不超过 300）
    _subscribe_interval: int = 2         # 每订阅一部的间隔秒数
    _max_consecutive_failures: int = 5   # 连续失败熔断次数
    _scan_timeout: int = 60              # 单轮扫描超时（分钟）

    # 【下载验证回环】（v1.2 新增；同样默认值兜底，向后兼容）
    _verify_alert_days: int = 7          # 订阅超过 N 天未入库则告警（只提醒，不自动退订）
    _verify_success_notify: bool = False # 剧集补齐核销时是否单独发 ✅ 通知
    _dead_task_hours: int = 6            # qb 任务 0 进度超过 N 小时视为疑似死任务
    _dead_task_auto_delete: bool = False # 是否自动删除死任务（默认关，原因见 README）
    _disk_check_path: str = "/video/downloads"  # 磁盘告警检查的容器内路径
    _disk_alert_gb: int = 200            # 剩余空间低于该 GB 数则告警

    # 【爱影 115 通道】（v1.4.0 新增；默认全部兜底，Telethon 缺失时整体不启用）
    _aiying_enabled: bool = False        # 爱影115通道开关
    _tg_phone: str = ""                  # TG 手机号（登录你本人 TG 账号）
    _tg_code: str = ""                   # TG 验证码（一次性，保存后清空）
    _tg_password: str = ""               # TG 两步验证密码（可空）
    _tg_proxy: str = TG_DEFAULT_PROXY    # TG 代理地址
    _tg_send_code_once: bool = False     # 一次性开关：保存配置时发送验证码
    _tg_verify_once: bool = False        # 一次性开关：保存配置时完成登录
    _aiying_bot: str = "ayclub_bot"      # 爱影资源机器人用户名
    _sa_bot: str = ""                    # SA 转存机器人用户名（你的 Symedia 机器人）
    _aiying_interval: int = 3            # 每集点击/发送间隔秒数（风控）
    _aiying_max_eps: int = 30            # 每剧经此通道最多补集数（风控）

    # 持久化数据的 key
    _DATA_PROCESSED = "processed"        # 已处理（已成功订阅）的剧 {tmdbid: {...}}
    _DATA_HISTORY = "history"            # 运行历史列表（最多保留 200 条）
    _DATA_DAILY = "daily"                # 当日配额计数 {"date": "YYYY-MM-DD", "count": n}
    _DATA_STATS = "stats"                # 累计统计
    _DATA_PENDING = "pending_verify"     # 已订阅未核销的剧 {tmdbid: 快照}
    _DATA_DEAD = "dead_tasks"            # 最近一轮疑似死任务快照（详情页展示用）
    _DATA_PROGRESS = "progress"          # 本轮实时进度快照（详情页进度条 / API 轮询用）
    _DATA_TG_LOGIN = "tg_login"          # TG 登录状态缓存 {logged_in, username, phone, ...}
    _DATA_AIYING = "aiying"              # 爱影通道状态 {quota_left: 本月剩余次数, updated: 时间}

    # ==================================================================
    # 插件生命周期
    # ==================================================================
    def init_plugin(self, config: dict = None):
        """
        插件初始化 / 配置保存后热重载都会走这里。
        注意：任何异常都不能向上抛，否则宿主会放弃加载本插件
        （表现为「装了但列表里看不到」），所以整体 try/except 兜底。
        """
        # 先复位运行期状态，再重建功能链
        self._scheduler = None
        try:
            self._subChain = SubscribeChain()
            self._subOper = SubscribeOper()
            self._mediaChain = MediaChain()
            self._tmdbChain = TmdbChain()
            self._msChain = MediaServerChain()
            self._msHelper = MediaServerHelper()

            if config:
                self._load_config(config)

            # 停止旧任务
            self.stop_service()

            # 处理「清空历史」一次性开关
            if self._clear_history:
                self.save_data(self._DATA_PROCESSED, {})
                self.save_data(self._DATA_HISTORY, [])
                self.save_data(self._DATA_DAILY, {})
                self.save_data(self._DATA_STATS, {})
                self.save_data(self._DATA_PENDING, {})
                self.save_data(self._DATA_DEAD, [])
                self.save_data(self._DATA_PROGRESS, {})
                self._clear_history = False
                logger.info(f"【{self.plugin_name}】历史记录与已处理清单已清空")
                self.__update_config()

            # 处理「爱影115通道」TG 登录一次性开关（发验证码/完成登录）
            if self._tg_send_code_once or self._tg_verify_once:
                try:
                    self.__handle_tg_login_actions()
                except Exception as e:
                    logger.error(f"【{self.plugin_name}】TG 登录动作处理失败（不影响主流程）: {e}")

            # 「立即运行一次」：用本地调度器 3 秒后触发一次，与 cron 服务互不影响
            if self._onlyonce:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                logger.info(f"【{self.plugin_name}】立即运行一次已触发")
                self._scheduler.add_job(
                    func=self.__scan,
                    trigger="date",
                    run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                    + datetime.timedelta(seconds=3),
                )
                if self._scheduler.get_jobs():
                    self._scheduler.print_jobs()
                    self._scheduler.start()
                # 一次性开关跑完即关，并回写配置（否则每次保存都会再跑一遍）
                self._onlyonce = False
                self.__update_config()

        except Exception as e:
            logger.error(f"【{self.plugin_name}】初始化失败（已降级不抛出，避免宿主放弃加载）: {e}")
            logger.error(traceback.format_exc())

    def _load_config(self, config: dict):
        """从配置字典加载各项设置，全部给默认值兜底（老配置缺新字段时用默认值，向后兼容）"""
        # 基础
        self._enabled = bool(config.get("enabled", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        # 扫描周期：v1.2.2 起为频率组合字段；老配置只有 cron 字段时自动迁移
        if "scan_freq" in config:
            # 新配置：直接读四个字段
            freq = str(config.get("scan_freq") or "daily")
            self._scan_freq = freq if freq in SCAN_FREQ_OPTIONS else "daily"
            self._scan_hour = min(23, max(0, self.__to_int(config.get("scan_hour"), 4)))
            self._scan_minute = min(59, max(0, self.__to_int(config.get("scan_minute"), 17)))
            self._cron_custom = str(config.get("cron_custom") or "").strip()
            # 自定义但表达式为空：尝试沿用老 cron 字段，否则回退默认
            if self._scan_freq == "custom" and not self._cron_custom:
                self._cron_custom = str(config.get("cron") or "").strip()
        else:
            # 老配置迁移：解析旧 cron 字段，能映射就映射，不能就落入自定义
            legacy_cron = str(config.get("cron") or "").strip()
            freq, hour, minute, custom = self.__parse_cron_legacy(legacy_cron)
            self._scan_freq = freq
            self._scan_hour = hour
            self._scan_minute = minute
            self._cron_custom = custom
            if legacy_cron:
                logger.info(f"【{self.plugin_name}】老配置 cron「{legacy_cron}」已迁移为："
                            f"{SCAN_FREQ_OPTIONS[freq]}"
                            + (f"（{hour} 点 {minute} 分）" if freq == "daily" else "")
                            + (f"（自定义表达式保留原文）" if freq == "custom" else ""))
        # 由组合字段生成最终 cron 并校验合法性（非法则回退默认，防止注册失败）
        self._cron = self.__build_cron(
            self._scan_freq, self._scan_hour, self._scan_minute, self._cron_custom)
        try:
            CronTrigger.from_crontab(self._cron)
        except Exception:
            logger.warning(f"【{self.plugin_name}】cron 表达式「{self._cron}」不合法，"
                           f"已回退为默认 17 4 * * *")
            self._cron = "17 4 * * *"
        self._dry_run = bool(config.get("dry_run", True))
        self._notify = bool(config.get("notify", True))
        self._clear_history = bool(config.get("clear_history", False))

        # 过滤
        self._max_missing = self.__to_int(config.get("max_missing"), 100)
        self._ignore_s0 = bool(config.get("ignore_s0", True))
        self._ignore_unfinished_latest = bool(config.get("ignore_unfinished_latest", True))
        self._exclude_keywords = str(config.get("exclude_keywords") or "")
        self._best_version = bool(config.get("best_version", False))

        # 优先级策略（VSelect multiple 存的是 list；兼容老版本存成逗号字符串的情况）
        regions = config.get("priority_regions") or []
        if isinstance(regions, str):
            regions = [r.strip() for r in regions.split(",") if r.strip()]
        # 只保留合法的地区 key，防止脏数据
        self._priority_regions = [r for r in regions if r in REGION_OPTIONS]
        self._priority_anime = bool(config.get("priority_anime", False))
        self._priority_documentary = bool(config.get("priority_documentary", False))
        sort_rule = str(config.get("sort_rule") or "least_missing")
        self._sort_rule = sort_rule if sort_rule in SORT_RULES else "least_missing"

        # 风控
        self._daily_quota = self.__to_int(config.get("daily_quota"), 200)
        self._subscribe_interval = max(0, self.__to_int(config.get("subscribe_interval"), 2))
        self._max_consecutive_failures = max(
            1, self.__to_int(config.get("max_consecutive_failures"), 5))
        self._scan_timeout = max(1, self.__to_int(config.get("scan_timeout"), 60))

        # 下载验证回环
        self._verify_alert_days = max(1, self.__to_int(config.get("verify_alert_days"), 7))
        self._verify_success_notify = bool(config.get("verify_success_notify", False))
        self._dead_task_hours = max(1, self.__to_int(config.get("dead_task_hours"), 6))
        self._dead_task_auto_delete = bool(config.get("dead_task_auto_delete", False))
        self._disk_check_path = str(
            config.get("disk_check_path") or "/video/downloads").strip()
        self._disk_alert_gb = max(1, self.__to_int(config.get("disk_alert_gb"), 200))

        # 爱影 115 通道（v1.4.0 新增；老配置缺字段时默认值兜底，向后兼容）
        self._aiying_enabled = bool(config.get("aiying_enabled", False))
        self._tg_phone = str(config.get("tg_phone") or "").strip()
        self._tg_code = str(config.get("tg_code") or "").strip()
        self._tg_password = str(config.get("tg_password") or "")
        self._tg_proxy = str(config.get("tg_proxy") or TG_DEFAULT_PROXY).strip()
        self._tg_send_code_once = bool(config.get("tg_send_code_once", False))
        self._tg_verify_once = bool(config.get("tg_verify_once", False))
        self._aiying_bot = str(config.get("aiying_bot") or "ayclub_bot").strip().lstrip("@")
        self._sa_bot = str(config.get("sa_bot") or "").strip().lstrip("@")
        self._aiying_interval = max(1, self.__to_int(config.get("aiying_interval"), 3))
        self._aiying_max_eps = max(1, self.__to_int(config.get("aiying_max_eps"), 30))

    @staticmethod
    def __to_int(value: Any, default: int) -> int:
        """把配置值安全转成 int，失败用默认值"""
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def __build_cron(scan_freq: str, scan_hour: int, scan_minute: int,
                     cron_custom: str) -> str:
        """
        由频率组合字段生成 cron 表达式（纯函数，可独立测试）：
          每天一次   -> {分} {时} * * *
          每 N 小时  -> {分} */N * * *（N=12/8/6）
          自定义     -> cron_custom 原文（为空回退默认）
        """
        if scan_freq == "custom":
            return cron_custom.strip() or "17 4 * * *"
        minute = min(59, max(0, int(scan_minute)))
        if scan_freq == "daily":
            hour = min(23, max(0, int(scan_hour)))
            return f"{minute} {hour} * * *"
        hours = {"12h": 12, "8h": 8, "6h": 6}.get(scan_freq, 24)
        return f"{minute} */{hours} * * *"

    @staticmethod
    def __parse_cron_legacy(cron: str) -> Tuple[str, int, int, str]:
        """
        解析老配置的 cron 字段，迁移为频率组合（纯函数，可独立测试）。
        返回 (scan_freq, scan_hour, scan_minute, cron_custom)：
          「17 4 * * *」这类每天定点 -> ("daily", 4, 17, "")
          「17 */8 * * *」这类每 N 小时（N=12/8/6）-> ("8h", 4, 17, "")
          其余无法解析的 -> ("custom", 4, 17, 原文)，原文保留进自定义框不丢配置
        """
        parts = (cron or "").split()
        # 空配置（全新安装）：直接给默认"每天一次 4:17"
        if not parts:
            return "daily", 4, 17, ""
        if len(parts) == 5 and parts[2] == "*" and parts[3] == "*" and parts[4] == "*":
            mi, hh = parts[0], parts[1]
            # 每天定点：分、时都是纯数字
            if mi.isdigit() and hh.isdigit():
                return "daily", int(hh) % 24, int(mi) % 60, ""
            # 每 N 小时：分是纯数字，时为 */N
            m = re.fullmatch(r"\*/(\d+)", hh)
            if mi.isdigit() and m and int(m.group(1)) in (12, 8, 6):
                return f"{m.group(1)}h", 4, int(mi) % 60, ""
        # 无法解析：落入自定义，保留原文
        return "custom", 4, 17, (cron or "").strip()

    def __update_config(self):
        """把当前内存中的配置回写到 MP 配置存储"""
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "scan_freq": self._scan_freq,
            "scan_hour": self._scan_hour,
            "scan_minute": self._scan_minute,
            "cron_custom": self._cron_custom,
            "cron": self._cron,   # 生成结果也回写，兼容旧版本插件读取
            "dry_run": self._dry_run,
            "notify": self._notify,
            "clear_history": self._clear_history,
            "max_missing": self._max_missing,
            "ignore_s0": self._ignore_s0,
            "ignore_unfinished_latest": self._ignore_unfinished_latest,
            "exclude_keywords": self._exclude_keywords,
            "best_version": self._best_version,
            "priority_regions": self._priority_regions,
            "priority_anime": self._priority_anime,
            "priority_documentary": self._priority_documentary,
            "sort_rule": self._sort_rule,
            "daily_quota": self._daily_quota,
            "subscribe_interval": self._subscribe_interval,
            "max_consecutive_failures": self._max_consecutive_failures,
            "scan_timeout": self._scan_timeout,
            "verify_alert_days": self._verify_alert_days,
            "verify_success_notify": self._verify_success_notify,
            "dead_task_hours": self._dead_task_hours,
            "dead_task_auto_delete": self._dead_task_auto_delete,
            "disk_check_path": self._disk_check_path,
            "disk_alert_gb": self._disk_alert_gb,
            "aiying_enabled": self._aiying_enabled,
            "tg_phone": self._tg_phone,
            # 验证码为一次性输入，回写时清空，避免残留
            "tg_code": "",
            "tg_password": self._tg_password,
            "tg_proxy": self._tg_proxy,
            "tg_send_code_once": self._tg_send_code_once,
            "tg_verify_once": self._tg_verify_once,
            "aiying_bot": self._aiying_bot,
            "sa_bot": self._sa_bot,
            "aiying_interval": self._aiying_interval,
            "aiying_max_eps": self._aiying_max_eps,
        })

    def get_state(self) -> bool:
        """插件启停状态，驱动插件页的开关显示"""
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务（cron 定时任务）。
        MP 会在插件启用时按这里返回的 trigger 定时调用 func。
        """
        if self._enabled and self._cron:
            return [{
                "id": "LackEpisodeAutoSub",
                "name": f"{self.plugin_name}定时扫描",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.__scan,
                "kwargs": {},
            }]
        elif self._enabled:
            # 用户没填 cron 时兜底：每天凌晨 4:17
            return [{
                "id": "LackEpisodeAutoSub",
                "name": f"{self.plugin_name}定时扫描",
                "trigger": CronTrigger.from_crontab("17 4 * * *"),
                "func": self.__scan,
                "kwargs": {},
            }]
        return []

    def stop_service(self):
        """停止一次性任务的本地调度器（cron 服务由 MP 托管，无需处理）；
        同时优雅关闭爱影 TG 会话（下次使用自动重建连接）"""
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            logger.error(f"【{self.plugin_name}】停止服务出错: {e}")
        # 优雅关闭 TG 会话管理器（Telethon 未装/未初始化时静默跳过）
        try:
            mgr = _get_tg_manager()
            if mgr:
                mgr.shutdown()
        except Exception:
            pass

    # ==================================================================
    # 插件 API（_PluginBase 抽象方法，必须实现，否则插件加载失败）
    # ==================================================================
    def get_api(self) -> List[Dict[str, Any]]:
        """
        注册插件 API，挂载在 /api/v1/plugin/LackEpisodeAutoSub/ 下：
          GET /scan   手动触发一轮扫描（等价于「立即运行一次」），返回本轮摘要
          GET /status 查询当前统计（待验证/已核销/今日已订阅/今日配额等）
          POST /tg_send_code 爱影115通道：发送 TG 登录验证码（body: {"phone": "+86..."}）
          POST /tg_verify     爱影115通道：提交验证码完成登录（body: {"phone", "code", "password"?}）
          GET /tg_status      爱影115通道：查询 TG 登录状态
        鉴权方式 apikey：调用时带 ?apikey=你的MP_API_TOKEN
        """
        return [
            {
                "path": "/scan",
                "endpoint": self.api_scan,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "手动触发一轮扫描",
                "description": "同步执行一轮完整任务（入库验证+死任务检测+磁盘检查+扫描订阅），"
                               "大库会比较久，调用方需耐心等响应",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "查询插件当前状态",
                "description": "返回统计信息：待验证数/已核销数/超时未补齐数/今日已订阅数/今日配额等",
            },
            {
                "path": "/tg_send_code",
                "endpoint": self.api_tg_send_code,
                "methods": ["POST"],
                "auth": "apikey",
                "summary": "发送 TG 登录验证码",
                "description": "爱影115通道登录第一步：body 传 {\"phone\": \"+86...\"}，"
                               "验证码发到 TG 内「Telegram」官方会话（不是短信）",
            },
            {
                "path": "/tg_verify",
                "endpoint": self.api_tg_verify,
                "methods": ["POST"],
                "auth": "apikey",
                "summary": "提交验证码完成 TG 登录",
                "description": "爱影115通道登录第二步：body 传 {\"phone\", \"code\", \"password\"(可空)}；"
                               "账号开两步验证时必须带 password",
            },
            {
                "path": "/tg_status",
                "endpoint": self.api_tg_status,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "查询 TG 登录状态",
                "description": "返回 {logged_in, username, phone}；未登录时先去配置页或调 /tg_send_code",
            },
        ]

    def api_scan(self) -> Dict[str, Any]:
        """API 端点：手动触发一轮扫描，返回本轮前后的统计对比"""
        logger.info(f"【{self.plugin_name}】收到 API 触发扫描请求")
        try:
            before = dict(self.get_data(self._DATA_STATS) or {})
            self.__scan()
            after = self.get_data(self._DATA_STATS) or {}
            return {
                "success": True,
                "message": "本轮任务已完成，明细见插件日志与详情页",
                "data": {
                    "dry_run": self._dry_run,
                    "last_run": after.get("last_run", ""),
                    "round_subscribed": (after.get("total_subscribed", 0)
                                         - before.get("total_subscribed", 0)),
                    "round_failed": (after.get("total_failed", 0)
                                     - before.get("total_failed", 0)),
                    "round_verified": (after.get("total_verified", 0)
                                       - before.get("total_verified", 0)),
                    "pending_verify": len(self.get_data(self._DATA_PENDING) or {}),
                },
            }
        except Exception as e:
            logger.error(f"【{self.plugin_name}】API 触发扫描失败: {e}")
            return {"success": False, "message": f"扫描失败: {e}", "data": None}

    def api_status(self) -> Dict[str, Any]:
        """API 端点：返回当前统计快照（只读，不触发任何任务）"""
        try:
            stats = self.get_data(self._DATA_STATS) or {}
            pending = self.get_data(self._DATA_PENDING) or {}
            daily = self.get_data(self._DATA_DAILY) or {}
            today_count = (int(daily.get("count", 0))
                           if daily.get("date") == self.__today_str() else 0)
            return {
                "success": True,
                "message": "",
                "data": {
                    "enabled": self._enabled,
                    "dry_run": self._dry_run,
                    "last_run": stats.get("last_run", "尚未运行"),
                    "total_scanned": stats.get("total_scanned", 0),
                    "total_subscribed": stats.get("total_subscribed", 0),
                    "total_failed": stats.get("total_failed", 0),
                    "pending_verify": len(pending),                 # 待验证数
                    "total_verified": stats.get("total_verified", 0),  # 已核销数
                    "total_timeout": stats.get("total_timeout", 0),    # 超时未补齐数
                    "today_subscribed": today_count,                # 今日已订阅数
                    "daily_quota": self._daily_quota,               # 今日配额
                    "progress": self.get_data(self._DATA_PROGRESS) or {},  # 实时进度快照
                },
            }
        except Exception as e:
            logger.error(f"【{self.plugin_name}】API 查询状态失败: {e}")
            return {"success": False, "message": str(e), "data": None}

    # FastAPI Body 参数（导入失败时退化为普通默认参数，查询字符串也能传）
    _BODY_STR = Body(default="", embed=True) if Body else ""

    def api_tg_send_code(self, phone: str = _BODY_STR) -> Dict[str, Any]:
        """API 端点：爱影115通道登录第一步，发送 TG 验证码"""
        try:
            phone = (phone or "").strip() or self._tg_phone
            if not phone:
                return {"success": False,
                        "message": "请提供手机号（body 传 phone，或在配置页填 TG 手机号）",
                        "data": None}
            mgr = self.__get_tg()
            if not mgr:
                return {"success": False,
                        "message": "telethon 未安装，爱影通道不可用（PT 订阅不受影响）",
                        "data": None}
            res = mgr.send_code(phone)
            if res.get("ok"):
                self._tg_phone = phone
                logger.info(f"【{self.plugin_name}】TG 验证码已发送至 {phone}")
                return {"success": True,
                        "message": "验证码已发送，请到 TG 内「Telegram」官方会话查看（不是短信），"
                                   "然后调 /tg_verify 或在配置页勾「完成登录」",
                        "data": None}
            logger.error(f"【{self.plugin_name}】TG 发送验证码失败: {res.get('error')}")
            return {"success": False,
                    "message": f"发送失败: {res.get('error')}", "data": None}
        except Exception as e:
            logger.error(f"【{self.plugin_name}】API 发送验证码失败: {e}")
            return {"success": False, "message": str(e), "data": None}

    def api_tg_verify(self, phone: str = _BODY_STR, code: str = _BODY_STR,
                      password: str = _BODY_STR) -> Dict[str, Any]:
        """API 端点：爱影115通道登录第二步，提交验证码（可选两步验证密码）"""
        try:
            phone = (phone or "").strip() or self._tg_phone
            code = (code or "").strip() or self._tg_code
            password = password or self._tg_password
            if not phone or not code:
                return {"success": False,
                        "message": "请提供 phone 与 code（先调 /tg_send_code）",
                        "data": None}
            mgr = self.__get_tg()
            if not mgr:
                return {"success": False,
                        "message": "telethon 未安装，爱影通道不可用（PT 订阅不受影响）",
                        "data": None}
            res = mgr.verify(phone, code, password)
            if res.get("ok"):
                self.__persist_tg_login(res)
                # 验证码一次性使用，成功后清空
                self._tg_code = ""
                logger.info(f"【{self.plugin_name}】TG 登录成功: "
                            f"{res.get('first_name')} (@{res.get('username')})")
                return {"success": True,
                        "message": f"登录成功：{res.get('first_name')} (@{res.get('username')})",
                        "data": res}
            logger.error(f"【{self.plugin_name}】TG 登录失败: {res.get('error')}")
            return {"success": False,
                    "message": f"登录失败: {res.get('error')}", "data": None}
        except Exception as e:
            logger.error(f"【{self.plugin_name}】API 登录验证失败: {e}")
            return {"success": False, "message": str(e), "data": None}

    def api_tg_status(self) -> Dict[str, Any]:
        """API 端点：查询 TG 登录状态（实时查询并刷新缓存）"""
        try:
            mgr = self.__get_tg()
            if not mgr:
                # telethon 未装：返回缓存状态 + 提示
                cached = self.get_data(self._DATA_TG_LOGIN) or {}
                return {"success": False,
                        "message": "telethon 未安装，爱影通道不可用（PT 订阅不受影响）",
                        "data": {"logged_in": bool(cached.get("logged_in")),
                                 "username": cached.get("username", ""),
                                 "phone": cached.get("phone", "")}}
            res = mgr.status()
            if res.get("ok"):
                self.__persist_tg_login(res)
                return {"success": True, "message": "",
                        "data": {"logged_in": bool(res.get("logged_in")),
                                 "username": res.get("username", ""),
                                 "first_name": res.get("first_name", ""),
                                 "phone": res.get("phone", "")}}
            return {"success": False,
                    "message": res.get("error", "查询失败"), "data": None}
        except Exception as e:
            logger.error(f"【{self.plugin_name}】API 查询 TG 状态失败: {e}")
            return {"success": False, "message": str(e), "data": None}

    # ==================================================================
    # 核心主流程
    # ==================================================================
    def __save_progress(self, progress: Dict[str, Any]):
        """
        实时进度快照落盘（v1.3.0 新增）。
        详情页进度卡片与 /status API 都读它；写盘是几 KB 的 pickle，
        扫描循环里每 10 部调一次、关键事件（发现缺集/订阅成败）立即调一次，
        开销可忽略，换来「扫到一半也能看到数字」。
        """
        try:
            self.save_data(self._DATA_PROGRESS, progress)
        except Exception as e:
            logger.debug(f"【{self.plugin_name}】保存进度快照失败（不影响主流程）: {e}")

    @staticmethod
    def __calc_percent(progress: Dict[str, Any]) -> int:
        """计算进度百分比：扫描阶段=已扫/剧集总数；订阅阶段=已处理/候选数"""
        try:
            if progress.get("phase") == "subscribing":
                total = int(progress.get("candidates", 0))
                done = int(progress.get("sub_done", 0))
            else:
                total = int(progress.get("total", 0))
                done = int(progress.get("scanned", 0))
            if total <= 0:
                return 0
            return min(100, int(done * 100 / total))
        except Exception:
            return 0

    def __scan(self):
        """
        主流程（每轮 cron 触发）：
          0. 下载验证回环：复查已订阅未核销的剧是否入库
          1. qb 死任务检测
          2. 下载目录磁盘告警
          3. 扫描 Emby 剧集库 -> 对比 TMDB 找缺集 -> 优先级排序 -> 按配额订阅
          4. 保存数据 + 汇总通知
        每一步都独立 try/except，任何一步失败不影响其他步骤。
        """
        start_time = datetime.datetime.now(tz=pytz.timezone(settings.TZ))
        logger.info(f"【{self.plugin_name}】===== 开始一轮任务 ====="
                    f"{'（调试模式：仅记录不订阅）' if self._dry_run else ''}")

        # 加载持久化数据
        processed: Dict[str, Any] = self.get_data(self._DATA_PROCESSED) or {}
        history: List[Dict[str, Any]] = self.get_data(self._DATA_HISTORY) or []
        stats: Dict[str, Any] = self.get_data(self._DATA_STATS) or {}

        # ---------- 0. 下载验证回环：复查已订阅未核销的剧 ----------
        try:
            self.__verify_pending(history, stats, start_time)
        except Exception as e:
            logger.error(f"【{self.plugin_name}】入库验证环节出错（不影响后续流程）: {e}")

        # ---------- 1. qb 死任务检测 ----------
        try:
            self.__check_dead_tasks(history)
        except Exception as e:
            logger.error(f"【{self.plugin_name}】死任务检测出错（不影响后续流程）: {e}")

        # ---------- 2. 下载目录磁盘告警 ----------
        try:
            self.__check_disk()
        except Exception as e:
            logger.error(f"【{self.plugin_name}】磁盘检查出错（不影响后续流程）: {e}")

        # 本轮计数器
        scanned = 0        # 扫描到的剧集总数
        missing_shows = 0  # 发现缺集的剧数
        subscribed = 0     # 本轮新增订阅数
        skipped = 0        # 跳过数（关键词/超限/已订阅/已处理）
        failed = 0         # 识别/请求/订阅失败数
        subscribed_titles: List[str] = []   # 本轮订阅成功的剧名（通知用）
        timeout_hit = False      # 是否触发扫描超时收尾
        circuit_broken = False   # 是否触发连续失败熔断

        # 排除关键词列表
        exclude_words = [w.strip() for w in self._exclude_keywords.split(",")
                         if w.strip()] if self._exclude_keywords else []

        # 获取当日剩余配额
        remaining_quota = self.__get_remaining_quota()

        # ---------- 实时进度快照初始化（v1.3.0）----------
        progress: Dict[str, Any] = {
            "running": True,                 # 是否正在跑
            "phase": "scanning",             # scanning=扫描中 / subscribing=订阅中 / done=完成
            "phase_label": "扫描媒体库中",
            "total": 0,                      # 已发现的剧集总数（随媒体库读取逐步增加）
            "scanned": 0,                    # 已扫描部数
            "missing": 0,                    # 发现缺集部数
            "candidates": 0,                 # 进入候选部数
            "subscribed": 0,                 # 本轮已订阅部数
            "skipped": 0,                    # 本轮跳过部数
            "failed": 0,                     # 本轮失败部数
            "sub_done": 0,                   # 订阅阶段已处理候选数
            "aiying": 0,                     # 本轮爱影115通道补齐部数（v1.4.0）
            "current": "",                   # 当前正在处理的剧名
            "quota_left": remaining_quota,   # 当日剩余配额
            "percent": 0,
            "dry_run": self._dry_run,
            "started_at": start_time.strftime(TIME_FMT),
            "finished_at": "",
            "elapsed_sec": 0,
        }
        self.__save_progress(progress)

        # ---------- 3. 遍历媒体服务器 ----------
        try:
            mediaservers = self._msHelper.get_services()
        except Exception as e:
            logger.error(f"【{self.plugin_name}】获取媒体服务器失败: {e}")
            mediaservers = None
        if not mediaservers:
            logger.warning(f"【{self.plugin_name}】未配置任何媒体服务器，本轮结束")
            # 验证/死任务/磁盘环节已执行过，仍需保存历史后返回
            self.save_data(self._DATA_HISTORY, history[-200:])
            self.save_data(self._DATA_STATS, stats)
            progress.update({"running": False, "phase": "done",
                             "phase_label": "本轮已完成",
                             "current": "",
                             "finished_at": datetime.datetime.now(
                                 tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT)})
            self.__save_progress(progress)
            return

        # 候选清单：本轮发现缺集、且通过所有过滤的剧（dict 列表，含优先级判定所需字段）
        candidates: List[Dict[str, Any]] = []

        for server_name in mediaservers:
            if timeout_hit:
                break
            if not server_name:
                continue
            logger.info(f"【{self.plugin_name}】开始扫描媒体服务器: {server_name}")

            try:
                librarys = self._msChain.librarys(server_name)
            except Exception as e:
                logger.error(f"【{self.plugin_name}】获取 {server_name} 媒体库列表失败: {e}")
                continue
            if not librarys:
                continue

            for library in librarys:
                if timeout_hit:
                    break
                if not library or not library.id:
                    continue
                try:
                    items = self._msChain.items(server_name, library.id)
                except Exception as e:
                    logger.error(
                        f"【{self.plugin_name}】获取媒体库 {library.name} 内容失败: {e}")
                    continue
                if not items:
                    continue

                # 关键：msChain.items() 可能返回生成器，先物化成列表，
                # 否则下面统计总数的 sum() 会把生成器消费掉，循环就扫不到任何剧
                items = list(items)

                # 进度：把本库的剧集数计入总数（总数随媒体库读取逐步增加）
                try:
                    progress["total"] += sum(
                        1 for it in items
                        if it and it.item_type in ["Series", "show"])
                    self.__save_progress(progress)
                except Exception:
                    pass

                for item in items:
                    # 【风控】单轮超时保护：超时就收尾，已扫描结果保留
                    if self.__is_timeout(start_time):
                        timeout_hit = True
                        logger.warning(
                            f"【{self.plugin_name}】扫描超过 {self._scan_timeout} 分钟，"
                            f"触发超时保护，本轮提前收尾")
                        break

                    # 只处理剧集（Series/show）
                    if not item or item.item_type not in ["Series", "show"]:
                        continue
                    scanned += 1
                    title = item.title or item.original_title or f"ItemID:{item.item_id}"

                    # 进度：每 10 部落盘一次（关键事件另有立即落盘）
                    progress["scanned"] = scanned
                    progress["current"] = title
                    progress["missing"] = missing_shows
                    progress["candidates"] = len(candidates)
                    progress["skipped"] = skipped
                    progress["failed"] = failed
                    progress["percent"] = self.__calc_percent(progress)
                    if scanned % 10 == 0:
                        self.__save_progress(progress)

                    try:
                        # ---- 过滤 1：排除关键词 ----
                        if exclude_words and any(w in title for w in exclude_words):
                            logger.info(f"【{title}】命中排除关键词，跳过")
                            skipped += 1
                            continue

                        # ---- 过滤 2：无 TMDB ID 无法对比 ----
                        tmdbid = item.tmdbid
                        if not tmdbid:
                            logger.debug(f"【{title}】无 TMDB ID，跳过")
                            continue

                        # ---- 过滤 3：本插件已处理过 ----
                        if str(tmdbid) in processed:
                            logger.debug(f"【{title}】已在已处理清单中，跳过")
                            continue

                        # ---- 3.1 取 Emby 已有季集 ----
                        # seasoninfo: {季号: [已有集号,...]}
                        seasoninfo: Dict[int, List[int]] = {}
                        try:
                            episodes_info = self._msChain.episodes(server_name, item.item_id) or []
                            for ep_info in episodes_info:
                                seasoninfo[ep_info.season] = ep_info.episodes or []
                        except Exception as e:
                            logger.error(f"【{title}】获取 Emby 季集信息失败: {e}")
                            failed += 1
                            continue

                        # ---- 3.2 对比 TMDB 得出缺集（同时拿回 mediainfo 供优先级判定） ----
                        lack_info, tmdbinfo = self.__find_lack_episodes(
                            tmdbid=tmdbid, title=title, seasoninfo=seasoninfo)
                        if lack_info is None:
                            # 识别/TMDB 请求失败
                            failed += 1
                            continue
                        if not lack_info:
                            # 不缺集
                            continue

                        total_missing = sum(len(eps) for eps in lack_info.values())
                        missing_shows += 1
                        progress["missing"] = missing_shows
                        self.__save_progress(progress)

                        # ---- 过滤 4：缺集数超过上限（可能整部识别错误） ----
                        if self._max_missing > 0 and total_missing > self._max_missing:
                            logger.warning(
                                f"【{title}】缺集 {total_missing} 集超过上限 "
                                f"{self._max_missing}，跳过（请人工确认是否识别错误）")
                            skipped += 1
                            self.__append_history(
                                history, title=title,
                                year=str(getattr(item, "year", "") or ""),
                                tmdbid=tmdbid, lack_info=lack_info,
                                missing_count=total_missing,
                                result="跳过-缺集过多", message="超过单部上限")
                            continue

                        candidates.append({
                            "title": title,
                            "year": str(getattr(item, "year", "") or ""),
                            "tmdbid": tmdbid,
                            "lack_info": lack_info,
                            "missing": total_missing,
                            # 验证回环所需：记录 Emby 定位信息，之后复查无需全库扫描
                            "server": server_name,
                            "item_id": item.item_id,
                            # 优先级判定字段：均来自 recognize_media 一次调用返回的
                            # MediaInfo（见 app/core/context.py），不额外请求 TMDB。
                            # 个别剧缺字段时取空值，自然落入普通层级，不影响主流程。
                            "vote": float(getattr(tmdbinfo, "vote_average", 0) or 0),
                            "genre_ids": list(getattr(tmdbinfo, "genre_ids", None) or []),
                            "original_language": getattr(tmdbinfo, "original_language", None),
                            "origin_country": list(getattr(tmdbinfo, "origin_country", None) or []),
                        })
                        logger.info(f"【{title}】发现缺集 {total_missing} 集，"
                                    f"涉及季: {sorted(lack_info.keys())}")

                    except Exception as e:
                        # 单部剧处理异常不中断整轮
                        logger.error(f"【{title}】处理出错: {e}")
                        failed += 1
                        continue

        logger.info(f"【{self.plugin_name}】扫描完成：共扫描 {scanned} 部剧，"
                    f"发现缺集 {missing_shows} 部，进入候选 {len(candidates)} 部"
                    f"{'（超时提前收尾）' if timeout_hit else ''}")

        # ---------- 3.3 优先级排序 ----------
        candidates = self.__sort_candidates(candidates)

        # ---------- 3.3.1 爱影115通道可用性检查（v1.4.0，每轮只查一次 TG 状态） ----------
        aiying_usable = False
        if not self._dry_run:
            try:
                aiying_usable = self.__aiying_ready()
            except Exception as e:
                logger.error(f"【{self.plugin_name}】爱影通道检测异常（不影响 PT 订阅）: {e}")
                aiying_usable = False
        if aiying_usable:
            logger.info(f"【{self.plugin_name}】爱影115通道已启用，缺集将优先尝试 115 离线")
        aiying_clicks = 0        # 本轮爱影累计点击数（熔断用，上限 100）
        aiying_round = 0         # 本轮爱影补齐剧数
        aiying_fuse_logged = False  # 熔断提示是否已记录

        # ---------- 3.4 按配额 + 风控订阅 ----------
        # 进度切换到订阅阶段，并明确打印配额（之前没有这行，看起来像"卡住没订阅"）
        progress.update({
            "phase": "subscribing", "phase_label": "订阅候选剧中",
            "candidates": len(candidates), "sub_done": 0, "percent": 0,
        })
        self.__save_progress(progress)
        if self._dry_run:
            logger.info(f"【{self.plugin_name}】调试模式：{len(candidates)} 部候选"
                        f"仅记录不订阅")
        elif not candidates:
            logger.info(f"【{self.plugin_name}】本轮无候选剧需要订阅")
        else:
            logger.info(f"【{self.plugin_name}】进入订阅阶段：候选 {len(candidates)} 部，"
                        f"今日剩余配额 {remaining_quota} 部，"
                        f"每部间隔 {self._subscribe_interval} 秒")

        consecutive_failures = 0  # 连续失败计数（成功即清零）
        # v1.3.2 修复：订阅阶段使用独立时间窗（从订阅阶段开始重新计时），
        # 不再与扫描阶段共用 start_time——大库扫满 60 分钟后，
        # 旧逻辑会让订阅阶段 2 秒内就被判超时，334 部候选一部都订不出去
        subscribe_start = datetime.datetime.now(tz=pytz.timezone(settings.TZ))
        for index, cand in enumerate(candidates):
            # 【风控】订阅阶段超时保护：从订阅阶段开始独立计时
            if self.__is_timeout(subscribe_start):
                timeout_hit = True
                logger.warning(f"【{self.plugin_name}】订阅阶段超过 "
                               f"{self._scan_timeout} 分钟，本轮提前收尾")
                self.__append_history(
                    history, title="（系统）", year="", tmdbid=0, lack_info={},
                    missing_count=0, result="超时收尾",
                    message=f"订阅阶段超过 {self._scan_timeout} 分钟，剩余 {len(candidates) - index} 部留待下轮")
                break

            title = cand["title"]
            progress["current"] = title
            progress["quota_left"] = remaining_quota

            # 调试模式：只记录，不订阅、不消耗配额、不标记已处理、不进入验证回环
            if self._dry_run:
                dry_msg = "调试模式未真正订阅"
                if self._aiying_enabled and self._sa_bot:
                    # 调试下不发任何 TG 消息，只记录爱影将尝试的集数
                    dry_try = min(cand["missing"], self._aiying_max_eps)
                    logger.info(f"【{title}】[调试] 爱影将尝试 {dry_try} 集")
                    dry_msg += f"；爱影将尝试 {dry_try} 集"
                self.__append_history(
                    history, title=title, year=cand["year"], tmdbid=cand["tmdbid"],
                    lack_info=cand["lack_info"], missing_count=cand["missing"],
                    result="调试-待订阅", message=dry_msg)
                progress["sub_done"] = index + 1
                progress["percent"] = self.__calc_percent(progress)
                self.__save_progress(progress)
                continue

            # 【风控】每日配额控制：超出配额的剧不标记已处理，留到明天继续
            if remaining_quota <= 0:
                logger.info(f"【{self.plugin_name}】当日配额 {self._daily_quota} 已用完，"
                            f"【{title}】及之后候选留待下一轮")
                break

            # ---------- 爱影115通道（v1.4.0）：MP 订阅之前优先尝试 ----------
            channel = "pt"   # pt / aiying / mixed
            ok, msg = False, ""
            ay = None
            if aiying_usable:
                # 【风控】单轮爱影总点击数熔断：超过 100 次本轮停止使用爱影，剩余走 PT
                if aiying_clicks >= 100:
                    if not aiying_fuse_logged:
                        aiying_fuse_logged = True
                        logger.warning(f"【{self.plugin_name}】爱影本轮点击已达 100 次上限，"
                                       f"触发熔断，剩余候选全部转 PT 订阅")
                        self.__append_history(
                            history, title="（系统）", year="", tmdbid=0, lack_info={},
                            missing_count=0, result="爱影熔断",
                            message="本轮爱影点击超过 100 次，剩余候选转 PT")
                else:
                    try:
                        ay = self.__aiying_fill(cand, 100 - aiying_clicks)
                        if ay:
                            aiying_clicks += ay.get("clicks", 0)
                    except Exception as e:
                        logger.error(f"【{title}】爱影通道异常（静默转 PT 兜底）: {e}")
                        ay = None

            if ay and ay.get("status") == "all":
                # 全部缺集都经 115 拿到：不再调 __subscribe_show
                ok, channel = True, "aiying"
                aiying_round += 1
                msg = f"[爱影115] 已提交 {ay['got']} 集到 115 离线"
                if ay.get("quota_left") is not None:
                    msg += f"（本月剩余次数 {ay['quota_left']}）"
            elif ay and ay.get("status") == "partial":
                # 部分集拿到：拿不到的集仍走 MP 订阅（MP 会自己比对只补缺集）
                ok_pt, msg_pt = self.__subscribe_show(
                    title, cand["year"], cand["tmdbid"], cand["lack_info"])
                ok, channel = True, "mixed"
                aiying_round += 1
                msg = (f"[爱影115] {ay['got']} 集已提交 115；"
                       f"剩余 {cand['missing'] - ay['got']} 集转 PT：{msg_pt}")
                if not ok_pt:
                    logger.warning(f"【{title}】爱影已补 {ay['got']} 集，"
                                   f"剩余集 PT 订阅未成功: {msg_pt}")
            else:
                # 爱影完全没资源/超时/异常：静默落到 MP 订阅（PT 兜底）
                prefix = ""
                if aiying_usable and aiying_clicks < 100:
                    prefix = ("爱影无资源，转 PT：" if ay is not None
                              else "爱影通道异常，转 PT：")
                # 逐季添加订阅（MP 订阅后自己会比对媒体库只补缺集）
                ok, msg_pt = self.__subscribe_show(
                    title, cand["year"], cand["tmdbid"], cand["lack_info"])
                msg = prefix + (msg_pt or "")

            # 【风控】订阅间隔：无论成败都 sleep，避免瞬间打爆 MP/TMDB/PT 站
            if self._subscribe_interval > 0:
                time.sleep(self._subscribe_interval)

            if ok:
                subscribed += 1
                consecutive_failures = 0  # 成功一次，连续失败清零
                remaining_quota -= 1
                progress["subscribed"] = subscribed
                progress["aiying"] = aiying_round
                progress["sub_done"] = index + 1
                progress["quota_left"] = remaining_quota
                progress["percent"] = self.__calc_percent(progress)
                self.__save_progress(progress)
                self.__incr_daily_quota()
                # 通知里区分来源渠道
                channel_tag = {"aiying": "[爱影115]", "mixed": "[爱影+PT]"}.get(
                    channel, "[PT下载]")
                subscribed_titles.append(f"{channel_tag} {title}（缺 {cand['missing']} 集）")
                # 标记已处理，下一轮不再重复
                processed[str(cand["tmdbid"])] = {
                    "title": title,
                    "time": datetime.datetime.now(
                        tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
                    "seasons": sorted(cand["lack_info"].keys()),
                }
                # 【验证回环】登记"已订阅未核销"快照，之后每轮复查入库情况
                self.__register_pending(cand, channel=channel)
                self.__append_history(
                    history, title=title, year=cand["year"], tmdbid=cand["tmdbid"],
                    lack_info=cand["lack_info"], missing_count=cand["missing"],
                    result="已订阅", message=msg)
            else:
                failed += 1
                consecutive_failures += 1
                progress["failed"] = failed
                progress["sub_done"] = index + 1
                progress["percent"] = self.__calc_percent(progress)
                self.__save_progress(progress)
                self.__append_history(
                    history, title=title, year=cand["year"], tmdbid=cand["tmdbid"],
                    lack_info=cand["lack_info"], missing_count=cand["missing"],
                    result="订阅失败", message=msg)

                # 【风控】连续失败熔断：MP/TMDB/网络很可能已出问题，及时止损
                if consecutive_failures >= self._max_consecutive_failures:
                    circuit_broken = True
                    logger.error(
                        f"【{self.plugin_name}】连续失败 {consecutive_failures} 次，"
                        f"触发熔断，本轮停止订阅（剩余 {len(candidates) - index - 1} 部留待下轮）")
                    self.__append_history(
                        history, title="（系统）", year="", tmdbid=0, lack_info={},
                        missing_count=0, result="熔断停止",
                        message=f"连续失败 {consecutive_failures} 次，本轮停止")
                    break

        # ---------- 4. 保存数据 ----------
        stats["total_scanned"] = stats.get("total_scanned", 0) + scanned
        stats["total_missing"] = stats.get("total_missing", 0) + missing_shows
        stats["total_subscribed"] = stats.get("total_subscribed", 0) + subscribed
        stats["total_skipped"] = stats.get("total_skipped", 0) + skipped
        stats["total_failed"] = stats.get("total_failed", 0) + failed
        stats["total_aiying"] = stats.get("total_aiying", 0) + aiying_round  # 累计爱影补齐
        stats["last_aiying"] = aiying_round                                  # 本轮爱影补齐
        stats["last_run"] = start_time.strftime(TIME_FMT)

        self.save_data(self._DATA_PROCESSED, processed)
        self.save_data(self._DATA_HISTORY, history[-200:])  # 最多保留 200 条
        self.save_data(self._DATA_STATS, stats)

        elapsed = (datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                   - start_time).total_seconds()

        # 进度收尾：标记完成，页面进度条显示本轮最终结果
        progress.update({
            "running": False, "phase": "done", "phase_label": "本轮已完成",
            "scanned": scanned, "missing": missing_shows,
            "candidates": len(candidates), "subscribed": subscribed,
            "skipped": skipped, "failed": failed,
            "aiying": aiying_round,
            "current": "", "quota_left": remaining_quota,
            "percent": 100,
            "finished_at": datetime.datetime.now(
                tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
            "elapsed_sec": int(elapsed),
        })
        self.__save_progress(progress)

        logger.info(f"【{self.plugin_name}】===== 本轮结束，耗时 {elapsed:.0f} 秒："
                    f"新增订阅 {subscribed} 部，跳过 {skipped} 部，失败 {failed} 部"
                    f"{'，已熔断' if circuit_broken else ''}"
                    f"{'，超时收尾' if timeout_hit else ''} =====")

        # ---------- 5. 汇总通知 ----------
        if self._notify:
            self.__send_summary(scanned=scanned, missing_shows=missing_shows,
                                subscribed=subscribed, skipped=skipped, failed=failed,
                                subscribed_titles=subscribed_titles,
                                elapsed=elapsed,
                                circuit_broken=circuit_broken,
                                timeout_hit=timeout_hit)

    def __is_timeout(self, start_time: datetime.datetime) -> bool:
        """单轮任务是否已超过配置的超时分钟数"""
        elapsed = (datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                   - start_time).total_seconds()
        return elapsed > self._scan_timeout * 60

    # ==================================================================
    # 下载验证回环 0：订阅后入库验证
    # ==================================================================
    def __register_pending(self, cand: Dict[str, Any], channel: str = "pt"):
        """订阅成功后登记快照：之后每轮复查 Emby 是否真入库。
        channel：补齐渠道（pt=纯 PT 订阅 / aiying=纯爱影115 / mixed=爱影+PT 混合），
        v1.4.0 新增，核销时 115 渠道会退订本插件此前添加的 PT 订阅"""
        pending: Dict[str, Any] = self.get_data(self._DATA_PENDING) or {}
        # 快照内容：tmdbid、剧名、年份、缺集列表、订阅时间、Emby 定位信息、渠道
        pending[str(cand["tmdbid"])] = {
            "title": cand["title"],
            "year": cand["year"],
            "tmdbid": cand["tmdbid"],
            "server": cand["server"],        # Emby 服务器名（复查时直接定位）
            "item_id": cand["item_id"],      # Emby 剧集 ID（复查时直接定位）
            "seasons": {str(s): list(eps) for s, eps in cand["lack_info"].items()},
            "remaining": {str(s): list(eps) for s, eps in cand["lack_info"].items()},
            "channel": channel,              # 补齐渠道（v1.4.0）
            "subscribe_time": datetime.datetime.now(
                tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
            "alerted": False,                # 是否已发过"超时未补齐"告警（只告警一次）
        }
        self.save_data(self._DATA_PENDING, pending)
        logger.info(f"【{cand['title']}】已登记入库验证快照，"
                    f"缺集 {cand['missing']} 集，之后每轮复查")

    def __verify_pending(self, history: List[Dict[str, Any]],
                         stats: Dict[str, Any], start_time: datetime.datetime):
        """
        复查所有「已订阅未核销」的剧：
          全部补齐 -> 核销 + 统计 +（可选）✅ 通知
          部分补齐 -> 更新剩余缺集，继续等
          超过 verify_alert_days 未补齐 -> 告警通知（只一次），历史标记「超时未补齐」
        单部复查失败跳过该部，不中断整轮复查。
        """
        pending: Dict[str, Any] = self.get_data(self._DATA_PENDING) or {}
        if not pending:
            logger.debug(f"【{self.plugin_name}】无待验证的订阅，跳过入库复查")
            return

        logger.info(f"【{self.plugin_name}】开始入库验证：共 {len(pending)} 部待复查")
        now = datetime.datetime.now(tz=pytz.timezone(settings.TZ))
        verified_titles: List[str] = []   # 本轮核销的剧名
        timeout_titles: List[str] = []    # 本轮新告警的剧名
        changed = False

        for key, entry in list(pending.items()):
            # 超时保护：复查阶段也受单轮超时约束
            if self.__is_timeout(start_time):
                logger.warning(f"【{self.plugin_name}】入库验证阶段超时，"
                               f"剩余 {len(pending)} 部下轮再查")
                break

            title = entry.get("title", key)
            server = entry.get("server")
            item_id = entry.get("item_id")
            if not server or not item_id:
                # 快照缺定位信息（可能是老版本数据），无法复查则移除，避免永远挂着
                logger.warning(f"【{title}】验证快照缺少 Emby 定位信息，移除待验证记录")
                del pending[key]
                changed = True
                continue

            # 重新拉 Emby 该剧的季集信息
            try:
                episodes_info = self._msChain.episodes(server, item_id) or []
                seasoninfo: Dict[int, List[int]] = {}
                for ep_info in episodes_info:
                    seasoninfo[ep_info.season] = ep_info.episodes or []
            except Exception as e:
                # Emby 请求失败：跳过该部（保持原状下轮再查），不中断整轮
                logger.error(f"【{title}】复查 Emby 季集失败（下轮再查）: {e}")
                continue

            # 状态机迁移（纯函数，便于独立测试）
            status, new_remaining, wait_days = self.__verify_transition(
                remaining=entry.get("remaining") or {},
                seasoninfo=seasoninfo,
                subscribe_time=entry.get("subscribe_time", ""),
                now=now,
                alert_days=self._verify_alert_days,
            )

            if status == "done":
                # 全部补齐 -> 核销
                del pending[key]
                changed = True
                stats["total_verified"] = stats.get("total_verified", 0) + 1
                verified_titles.append(title)
                logger.info(f"【{title}】缺集已全部补齐入库，核销 ✅（等待 {wait_days} 天）")
                self.__append_history(
                    history, title=title, year=str(entry.get("year", "")),
                    tmdbid=int(entry.get("tmdbid") or 0), lack_info={},
                    missing_count=0, result="已补齐核销",
                    message=f"订阅后第 {wait_days} 天确认全部入库")
                # v1.4.0：115 渠道补齐的剧，退订本插件此前添加的 PT 订阅，避免重复下载
                try:
                    self.__unsub_pt_if_115(entry, title, history)
                except Exception as e:
                    logger.error(f"【{title}】退订 PT 订阅检查失败（不影响核销）: {e}")
            elif status == "timeout" and not entry.get("alerted"):
                # 超时未补齐 -> 告警一次（不自动退订，只提醒）
                entry["remaining"] = new_remaining
                entry["alerted"] = True
                changed = True
                stats["total_timeout"] = stats.get("total_timeout", 0) + 1
                remaining_count = sum(len(v) for v in new_remaining.values())
                timeout_titles.append(f"{title}（已等 {wait_days} 天，仍缺 {remaining_count} 集）")
                logger.warning(f"【{title}】订阅 {wait_days} 天未完全入库，"
                               f"仍缺 {remaining_count} 集，发送告警")
                self.__append_history(
                    history, title=title, year=str(entry.get("year", "")),
                    tmdbid=int(entry.get("tmdbid") or 0), lack_info={},
                    missing_count=remaining_count, result="超时未补齐",
                    message=f"订阅 {wait_days} 天未入库，请人工检查资源")
            else:
                # waiting 或已告警过的 timeout：更新剩余缺集，继续等
                if new_remaining != entry.get("remaining"):
                    entry["remaining"] = new_remaining
                    changed = True
                logger.debug(f"【{title}】仍有 {sum(len(v) for v in new_remaining.values())} "
                             f"集未入库，继续等待（第 {wait_days} 天）")

        if changed:
            self.save_data(self._DATA_PENDING, pending)

        logger.info(f"【{self.plugin_name}】入库验证完成：核销 {len(verified_titles)} 部，"
                    f"新告警 {len(timeout_titles)} 部，剩余待验证 {len(pending)} 部")

        # 核销 ✅ 通知（可选，默认关，避免补得多时刷屏）
        if verified_titles and self._verify_success_notify:
            try:
                text = "\n".join(f"· {t}" for t in verified_titles[:10])
                if len(verified_titles) > 10:
                    text += f"\n· …等共 {len(verified_titles)} 部"
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title=f"【{self.plugin_name}】剧集补齐 ✅",
                    text=f"以下剧集缺集已全部入库：\n{text}",
                )
            except Exception as e:
                logger.error(f"【{self.plugin_name}】发送补齐通知失败: {e}")

        # 超时告警通知（重要，始终发送；每部剧只告警一次）
        if timeout_titles:
            try:
                text = "\n".join(f"· {t}" for t in timeout_titles[:10])
                if len(timeout_titles) > 10:
                    text += f"\n· …等共 {len(timeout_titles)} 部"
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title=f"【{self.plugin_name}】订阅超时未入库 ⚠️",
                    text=(f"以下剧集订阅超过 {self._verify_alert_days} 天仍未补齐，"
                          f"可能全网无资源或死种，请人工检查：\n{text}"),
                )
            except Exception as e:
                logger.error(f"【{self.plugin_name}】发送超时告警失败: {e}")

    @staticmethod
    def __verify_transition(remaining: Dict[str, List[int]],
                            seasoninfo: Dict[int, List[int]],
                            subscribe_time: str,
                            now: datetime.datetime,
                            alert_days: int) -> Tuple[str, Dict[str, List[int]], int]:
        """
        入库验证状态机（纯函数，不依赖 MP 环境，可独立测试）。

        入参：
          remaining      上次记录的剩余缺集 {"1": [3, 5], "2": [1]}
          seasoninfo     Emby 当前季集 {1: [1,2,3,4,5], ...}
          subscribe_time 订阅时间字符串（TIME_FMT），解析失败按 0 天计
          now            当前时间
          alert_days     告警阈值天数
        返回：(状态, 新剩余缺集, 已等待天数)
          done    全部补齐（新剩余为空）
          waiting 仍有缺集但未超期
          timeout 仍有缺集且已超期
        """
        # 1. 用 Emby 最新季集核销剩余缺集
        new_remaining: Dict[str, List[int]] = {}
        for season_str, eps in (remaining or {}).items():
            try:
                season_no = int(season_str)
            except (TypeError, ValueError):
                continue
            exist = set(seasoninfo.get(season_no) or [])
            still = sorted(set(eps) - exist)
            if still:
                new_remaining[str(season_no)] = still

        # 2. 计算已等待天数（时间解析失败保守按 0 天，不误告警）
        try:
            sub_dt = datetime.datetime.strptime(subscribe_time, TIME_FMT)
            wait_days = (now.replace(tzinfo=None) - sub_dt).days
        except (TypeError, ValueError):
            wait_days = 0

        # 3. 状态迁移
        if not new_remaining:
            return "done", {}, wait_days
        if wait_days >= alert_days:
            return "timeout", new_remaining, wait_days
        return "waiting", new_remaining, wait_days

    # ==================================================================
    # 下载验证回环 1：qb 死任务检测
    # ==================================================================
    def __check_dead_tasks(self, history: List[Dict[str, Any]]):
        """
        检测下载器中疑似死任务：带 MP 标签、进度为 0、添加时间超过 dead_task_hours。
        默认只通知；dead_task_auto_delete 开启后才自动删除（不删文件）。
        拿不到下载器/任务列表时静默跳过并记日志，不中断主流程。
        """
        # DownloaderHelper 用法先例：官方插件 torrentremover
        try:
            services = DownloaderHelper().get_services()
        except Exception as e:
            logger.error(f"【{self.plugin_name}】获取下载器实例失败，跳过死任务检测: {e}")
            return
        if not services:
            logger.debug(f"【{self.plugin_name}】未配置下载器，跳过死任务检测")
            return

        dead_tasks: List[Dict[str, Any]] = []
        now_ts = time.time()

        for dl_name, service_info in services.items():
            try:
                instance = service_info.instance
                # 未连接的下载器跳过（torrentremover 同款判断）
                if instance.is_inactive():
                    logger.warning(f"【{self.plugin_name}】下载器 {dl_name} 未连接，跳过")
                    continue
                # 只查带 MP 标签的任务；settings.TORRENT_TAG 即 "MOVIEPILOT"
                torrents, error_flag = instance.get_torrents(tags=[settings.TORRENT_TAG])
                if error_flag or not torrents:
                    continue
                for torrent in torrents:
                    # qBittorrent 字段：progress(0~1)/added_on(unix 秒)/hash/name/state
                    # 其他下载器（TR 等）字段不同，getattr 取不到 progress 就跳过该任务
                    progress = getattr(torrent, "progress", None)
                    added_on = getattr(torrent, "added_on", 0) or 0
                    if progress is None:
                        continue
                    if progress > 0:
                        continue  # 有进度的不算死任务
                    age_hours = (now_ts - added_on) / 3600 if added_on else 0
                    if age_hours < self._dead_task_hours:
                        continue  # 挂得还不够久
                    dead_tasks.append({
                        "downloader": dl_name,
                        "hash": getattr(torrent, "hash", ""),
                        "name": getattr(torrent, "name", "未知任务"),
                        "age_hours": round(age_hours, 1),
                        "state": getattr(torrent, "state", ""),
                    })
            except Exception as e:
                logger.error(f"【{self.plugin_name}】检查下载器 {dl_name} 出错，跳过: {e}")
                continue

        # 快照存档，供详情页展示
        self.save_data(self._DATA_DEAD, {
            "check_time": datetime.datetime.now(
                tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
            "tasks": dead_tasks,
        })

        if not dead_tasks:
            logger.info(f"【{self.plugin_name}】死任务检测完成：未发现疑似死任务")
            return

        logger.warning(f"【{self.plugin_name}】发现 {len(dead_tasks)} 个疑似死任务："
                       + "；".join(f"{t['name']}（已挂 {t['age_hours']} 小时）"
                                  for t in dead_tasks[:5]))

        # 历史记录（一轮一条汇总，避免刷屏）
        self.__append_history(
            history, title="（下载器）", year="", tmdbid=0, lack_info={},
            missing_count=len(dead_tasks), result="死任务告警",
            message="；".join(f"{t['name']} 已挂 {t['age_hours']}h" for t in dead_tasks[:5])
                    + ("……" if len(dead_tasks) > 5 else ""))

        # 可选：自动删除死任务（不删文件）。默认关闭——删除后 MP 订阅记录可能仍
        # 标记已下载（MP 已知行为），需到 MP 订阅页手动点搜索重新触发，见 README。
        deleted_note = ""
        if self._dead_task_auto_delete:
            deleted = 0
            # 按下载器分组删除
            by_downloader: Dict[str, List[str]] = {}
            for t in dead_tasks:
                if t["hash"]:
                    by_downloader.setdefault(t["downloader"], []).append(t["hash"])
            for dl_name, hashes in by_downloader.items():
                try:
                    services[dl_name].instance.delete_torrents(delete_file=False, ids=hashes)
                    deleted += len(hashes)
                    logger.info(f"【{self.plugin_name}】已从 {dl_name} 删除 {len(hashes)} 个死任务")
                except Exception as e:
                    logger.error(f"【{self.plugin_name}】删除 {dl_name} 死任务失败: {e}")
            deleted_note = f"\n已自动删除 {deleted} 个（未删文件）。请到 MP 订阅页手动点搜索重新触发。"

        # 通知
        if self._notify:
            try:
                lines = [f"发现 {len(dead_tasks)} 个疑似死任务（0 进度超过 "
                         f"{self._dead_task_hours} 小时）："]
                for t in dead_tasks[:8]:
                    lines.append(f"· {t['name']}（{t['downloader']}，已挂 {t['age_hours']} 小时）")
                if len(dead_tasks) > 8:
                    lines.append(f"· …等共 {len(dead_tasks)} 个")
                lines.append("建议：到下载器里查看是否无种/tracker 失效，"
                             "确认后删除并到 MP 订阅页手动点一次搜索重新触发。")
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title=f"【{self.plugin_name}】疑似死任务提醒",
                    text="\n".join(lines) + deleted_note,
                )
            except Exception as e:
                logger.error(f"【{self.plugin_name}】发送死任务通知失败: {e}")

    # ==================================================================
    # 下载验证回环 2：下载目录磁盘告警
    # ==================================================================
    def __check_disk(self):
        """用 os.statvfs 检查下载目录剩余空间，低于阈值告警（每轮最多一次）"""
        path = self._disk_check_path
        if not path:
            return
        try:
            st = os.statvfs(path)
        except OSError:
            # 路径不存在（比如没挂载进容器）：跳过，记 DEBUG，不打扰用户
            logger.debug(f"【{self.plugin_name}】磁盘检查路径不存在，跳过: {path}")
            return
        free_gb = st.f_bavail * st.f_frsize / (1024 ** 3)
        logger.info(f"【{self.plugin_name}】下载目录 {path} 剩余 {free_gb:.1f} GB")
        if free_gb >= self._disk_alert_gb:
            return
        logger.warning(f"【{self.plugin_name}】下载目录剩余空间 {free_gb:.1f} GB "
                       f"低于阈值 {self._disk_alert_gb} GB")
        if self._notify:
            try:
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title=f"【{self.plugin_name}】磁盘空间告急 ⚠️",
                    text=(f"下载目录 {path} 剩余空间仅 {free_gb:.1f} GB，"
                          f"低于阈值 {self._disk_alert_gb} GB。\n"
                          f"缺集补齐会持续产生下载，请及时清理或扩容，"
                          f"避免下载失败。"),
                )
            except Exception as e:
                logger.error(f"【{self.plugin_name}】发送磁盘告警失败: {e}")

    # ==================================================================
    # 优先级策略
    # ==================================================================
    def __sort_candidates(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        候选剧排序：
          第一层：优先级层级（命中优先地区 +1，命中优先类型各 +1，都命中更靠前）
          第二层：同层内按排序规则（缺集少/缺集多/评分高）
          第三层：剧名稳定排序兜底
        每部剧的层级判定写入日志（前 30 部 INFO 级，其余 DEBUG 级），可追踪。
        """
        if not candidates:
            return candidates

        # 为每部候选计算层级与判定依据
        for cand in candidates:
            tier = 0
            reasons: List[str] = []

            # --- 优先地区判定 ---
            region_tags = self.__region_tags(
                cand.get("original_language"), cand.get("origin_country"))
            cand["region_tags"] = region_tags
            if self._priority_regions and region_tags & set(self._priority_regions):
                tier += 1
                hit_names = [REGION_OPTIONS[t] for t in sorted(
                    region_tags & set(self._priority_regions))]
                reasons.append(f"地区命中({'/'.join(hit_names)})")

            # --- 优先类型判定 ---
            genre_ids = set(cand.get("genre_ids") or [])
            if self._priority_anime and GENRE_ID_ANIMATION in genre_ids:
                tier += 1
                reasons.append("动漫优先")
            if self._priority_documentary and GENRE_ID_DOCUMENTARY in genre_ids:
                tier += 1
                reasons.append("纪录片优先")

            cand["tier"] = tier
            cand["tier_reason"] = "+".join(reasons) if reasons else "普通"

        # --- 排序规则键 ---
        def __rule_key(cand: Dict[str, Any]) -> Any:
            if self._sort_rule == "most_missing":
                return -cand["missing"]       # 缺集多的优先
            if self._sort_rule == "top_rated":
                return -cand.get("vote", 0)   # 评分高的优先
            return cand["missing"]            # 默认：缺集少的优先（快速见效）

        # 层级高的在前；同层按规则；再按剧名稳定兜底
        candidates.sort(key=lambda c: (-c["tier"], __rule_key(c), c["title"]))

        # --- 日志输出：层级分布 + 队列前 30 部明细（完整明细 DEBUG 级） ---
        tier_counts: Dict[int, int] = {}
        for cand in candidates:
            tier_counts[cand["tier"]] = tier_counts.get(cand["tier"], 0) + 1
        tier_desc = "，".join(
            f"层级{tier} {tier_counts[tier]} 部" for tier in sorted(tier_counts, reverse=True))
        logger.info(f"【{self.plugin_name}】优先级分层结果：{tier_desc}；"
                    f"排序规则：{SORT_RULES.get(self._sort_rule)}")
        for i, cand in enumerate(candidates):
            line = (f"优先级队列 #{i + 1}: 【{cand['title']}】"
                    f"层级{cand['tier']}({cand['tier_reason']}) "
                    f"缺集 {cand['missing']} 集，评分 {cand.get('vote', 0)}，"
                    f"地区标签 {sorted(cand['region_tags'])}")
            if i < 30:
                logger.info(line)
            else:
                logger.debug(line)
        if len(candidates) > 30:
            logger.info(f"【{self.plugin_name}】…其余 {len(candidates) - 30} 部"
                        f"队列明细见 DEBUG 日志")

        return candidates

    @staticmethod
    def __region_tags(original_language: Optional[str],
                      origin_country: Optional[List[str]]) -> Set[str]:
        """
        根据 TMDB 的 original_language / origin_country 给剧打地区标签。
        一部剧可能有多个标签（如港台合拍）；都不沾边的归「其他」。
        """
        tags: Set[str] = set()
        countries = set(origin_country or [])
        lang = (original_language or "").lower()

        if "CN" in countries:
            tags.add("cn")
        if "HK" in countries:
            tags.add("hk")
        if "TW" in countries:
            tags.add("tw")
        if "JP" in countries or lang == "ja":
            tags.add("jp")
        if "KR" in countries or lang == "ko":
            tags.add("kr")
        if lang == "en" or countries & WEST_COUNTRIES:
            tags.add("west")
        if not tags:
            tags.add("other")
        return tags

    # ==================================================================
    # 缺集对比 / 订阅
    # ==================================================================
    def __find_lack_episodes(
        self, tmdbid: int, title: str, seasoninfo: Dict[int, List[int]]
    ) -> Tuple[Optional[Dict[int, List[int]]], Any]:
        """
        对比 TMDB 与 Emby，返回 ({季号: [缺集号,...]}, mediainfo)。
        返回 (None, None) 表示识别/请求失败；返回 ({}, mediainfo) 表示不缺集。
        mediainfo 顺带返回给调用方做优先级判定（避免重复识别）。
        """
        # 用 MP 媒体链按 TMDB ID 识别（走 MP 内置缓存，速度快）
        try:
            tmdbinfo = self._mediaChain.recognize_media(mtype=MediaType.TV, tmdbid=tmdbid)
        except Exception as e:
            logger.error(f"【{title}】识别媒体信息失败: {e}")
            return None, None
        if not tmdbinfo or not getattr(tmdbinfo, "seasons", None):
            logger.warning(f"【{title}】未获取到 TMDB 季集信息，跳过")
            return None, None

        show_status = getattr(tmdbinfo, "status", "") or ""
        tmdb_seasons = tmdbinfo.seasons  # Dict[int, List[int]]：{季号: [集号,...]}

        # 未完结剧集的最新季号（进行中剧集的最新季还在更新，默认不算缺集）
        finished = show_status in ("Ended", "Canceled")
        normal_seasons = [s for s in tmdb_seasons.keys() if s != 0]
        latest_season = max(normal_seasons) if normal_seasons else None

        today = datetime.datetime.now(tz=pytz.timezone(settings.TZ)).date()
        lack: Dict[int, List[int]] = {}

        for season in sorted(tmdb_seasons.keys()):
            # 忽略特别篇 S00
            if season == 0 and self._ignore_s0:
                continue
            # 未完结剧集忽略最新季
            if (self._ignore_unfinished_latest and not finished
                    and latest_season is not None and season == latest_season):
                logger.debug(f"【{title}】第 {season} 季为未完结剧最新季，跳过")
                continue
            # 该季已被用户手动订阅过则跳过（避免与已有订阅重复）
            try:
                if self._subOper.exists(tmdbid, None, season=season):
                    logger.info(f"【{title}】第 {season} 季已存在订阅，跳过")
                    continue
            except Exception as e:
                logger.error(f"【{title}】查询订阅状态失败: {e}")
                continue

            # 取 TMDB 该季分集，只统计「已播出」的集（未播出的不算缺）
            aired_episodes = self.__get_aired_episodes(tmdbid, season, title, today)
            if not aired_episodes:
                continue

            exist_episodes = seasoninfo.get(season) or []
            lack_episodes = sorted(set(aired_episodes) - set(exist_episodes))
            if lack_episodes:
                lack[season] = lack_episodes

        return lack, tmdbinfo

    def __get_aired_episodes(
        self, tmdbid: int, season: int, title: str, today: datetime.date
    ) -> List[int]:
        """获取 TMDB 某季中已播出的集号列表；请求失败返回空列表（宁可少订不误订）"""
        try:
            episodes_info = self._tmdbChain.tmdb_episodes(tmdbid=tmdbid, season=season)
        except Exception as e:
            logger.error(f"【{title}】获取 TMDB 第 {season} 季分集失败: {e}")
            return []
        if not episodes_info:
            return []

        aired: List[int] = []
        for ep in episodes_info:
            if not ep or not ep.episode_number:
                continue
            if ep.air_date:
                try:
                    air_date = datetime.datetime.strptime(ep.air_date, "%Y-%m-%d").date()
                    if air_date <= today:
                        aired.append(ep.episode_number)
                    continue
                except (ValueError, TypeError):
                    pass
            # 无播出日期或日期格式异常：保守处理，视为未播出不计入
            logger.debug(f"【{title}】S{season:02d}E{ep.episode_number:02d} "
                         f"无播出日期，视为未播出")
        return aired

    def __subscribe_show(
        self, title: str, year: str, tmdbid: int, lack_info: Dict[int, List[int]]
    ) -> Tuple[bool, str]:
        """
        对一部剧的缺集季逐季添加 MP 订阅。
        只要有一季成功即视为成功（MP 订阅后自行比对媒体库只下载缺集）。
        """
        success_seasons: List[int] = []
        last_msg = ""
        for season in sorted(lack_info.keys()):
            try:
                sid, msg = self._subChain.add(
                    title=title,
                    year=year,
                    mtype=MediaType.TV,
                    tmdbid=tmdbid,
                    season=season,
                    exist_ok=True,            # 已存在订阅则复用，不报错
                    username=self.plugin_name,
                    message=False,            # 不逐季发 MP 订阅通知，由本插件汇总通知
                    best_version=1 if self._best_version else 0,
                )
                last_msg = msg or ""
                if sid:
                    success_seasons.append(season)
                    logger.info(f"【{title}】第 {season} 季订阅添加成功 (sid={sid})")
                else:
                    logger.warning(f"【{title}】第 {season} 季订阅添加失败: {msg}")
            except Exception as e:
                last_msg = str(e)
                logger.error(f"【{title}】第 {season} 季订阅异常: {e}")

        if success_seasons:
            return True, f"已订阅季: {success_seasons}"
        return False, f"所有缺集季订阅失败，最后错误: {last_msg}"

    # ==================================================================
    # 配额 / 历史 / 通知 工具函数
    # ==================================================================
    def __today_str(self) -> str:
        return datetime.datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d")

    def __get_remaining_quota(self) -> int:
        """读取当日已用配额，返回剩余数"""
        daily = self.get_data(self._DATA_DAILY) or {}
        if daily.get("date") != self.__today_str():
            return self._daily_quota  # 新的一天，配额重置
        return max(0, self._daily_quota - int(daily.get("count", 0)))

    def __incr_daily_quota(self):
        """当日配额计数 +1"""
        daily = self.get_data(self._DATA_DAILY) or {}
        if daily.get("date") != self.__today_str():
            daily = {"date": self.__today_str(), "count": 0}
        daily["count"] = int(daily.get("count", 0)) + 1
        self.save_data(self._DATA_DAILY, daily)

    def __append_history(self, history: List[Dict[str, Any]], title: str, year: str,
                         tmdbid: int, lack_info: Dict[int, List[int]],
                         missing_count: int, result: str, message: str):
        """追加一条历史记录并立即落盘（v1.3.0：原来整轮结束才保存，
        扫描中途打开详情页看不到任何记录，现在每产生一条就能在页面看到）"""
        history.append({
            "time": datetime.datetime.now(
                tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
            "title": title,
            "year": year or "",
            "tmdbid": tmdbid,
            "seasons": ",".join(f"S{s:02d}" for s in sorted(lack_info.keys())),
            "missing_count": missing_count,
            "result": result,
            "message": message or "",
        })
        try:
            self.save_data(self._DATA_HISTORY, history[-200:])
        except Exception as e:
            logger.debug(f"【{self.plugin_name}】保存历史记录失败（不影响主流程）: {e}")

    def __send_summary(self, scanned: int, missing_shows: int, subscribed: int,
                       skipped: int, failed: int,
                       subscribed_titles: List[str], elapsed: float,
                       circuit_broken: bool, timeout_hit: bool):
        """每轮结束发送汇总通知"""
        try:
            mode = "调试模式（未真正订阅）" if self._dry_run else "正式订阅"
            lines = [
                f"模式：{mode}",
                f"扫描剧集：{scanned} 部，发现缺集：{missing_shows} 部",
                f"新增订阅：{subscribed} 部，跳过：{skipped} 部，失败：{failed} 部",
                f"耗时：{elapsed:.0f} 秒",
            ]
            if circuit_broken:
                lines.append(f"⚠️ 已连续失败 {self._max_consecutive_failures} 次触发熔断，"
                             f"本轮提前停止，请检查 MP/TMDB/网络状态")
            if timeout_hit:
                lines.append(f"⚠️ 超过 {self._scan_timeout} 分钟触发超时保护，"
                             f"本轮提前收尾，剩余剧留待下轮")
            if subscribed_titles:
                lines.append("")
                lines.append("本轮订阅：")
                # 通知里最多列 10 部，避免刷屏
                for t in subscribed_titles[:10]:
                    lines.append(f"· {t}")
                if len(subscribed_titles) > 10:
                    lines.append(f"· …等共 {len(subscribed_titles)} 部")
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title=f"【{self.plugin_name}】本轮扫描完成",
                text="\n".join(lines),
            )
        except Exception as e:
            logger.error(f"【{self.plugin_name}】发送汇总通知失败: {e}")

    # ==================================================================
    # 爱影 115 通道（v1.4.0 新增）：TG 会话 / 登录 / 缺集补齐 / PT 退订
    # ==================================================================
    def __get_tg(self) -> Optional[_AiyingTgManager]:
        """获取 TG 会话管理器并按当前配置初始化；Telethon 未安装时返回 None"""
        mgr = _get_tg_manager()
        if not mgr:
            return None
        try:
            mgr.configure(self.__tg_session_path(), self._tg_proxy)
        except Exception as e:
            logger.error(f"【{self.plugin_name}】TG 管理器配置失败: {e}")
            return None
        return mgr

    def __tg_session_path(self) -> str:
        """TG 会话文件路径：优先插件数据目录（get_data_path），兜底固定路径"""
        try:
            return str(self.get_data_path() / "aiying.session")
        except Exception:
            return "/config/plugins/lackepisodeautosub/aiying.session"

    def __persist_tg_login(self, info: Dict[str, Any]):
        """持久化 TG 登录状态（详情页/状态 API 展示用）"""
        try:
            self.save_data(self._DATA_TG_LOGIN, {
                "logged_in": bool(info.get("logged_in")),
                "username": info.get("username", ""),
                "first_name": info.get("first_name", ""),
                "phone": info.get("phone", ""),
                "checked_at": datetime.datetime.now(
                    tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
            })
        except Exception as e:
            logger.debug(f"【{self.plugin_name}】保存 TG 登录状态失败: {e}")

    def __handle_tg_login_actions(self):
        """
        处理配置页的两个一次性开关（保存配置即触发，结果写日志与详情页状态区）：
          tg_send_code_once：给 _tg_phone 发送验证码
          tg_verify_once   ：用 _tg_code（+_tg_password）完成登录
        处理完复位开关并回写配置（否则每次保存都会重复触发）。
        """
        mgr = self.__get_tg()
        if not mgr:
            logger.warning(f"【{self.plugin_name}】telethon 未安装，无法执行 TG 登录动作"
                           f"（请确认插件目录 requirements.txt 依赖已安装）")
            self._tg_send_code_once = False
            self._tg_verify_once = False
            self._tg_code = ""
            self.__update_config()
            return

        changed = False
        if self._tg_send_code_once:
            if not self._tg_phone:
                logger.warning(f"【{self.plugin_name}】请先填写 TG 手机号再发送验证码")
            else:
                res = mgr.send_code(self._tg_phone)
                if res.get("ok"):
                    logger.info(f"【{self.plugin_name}】TG 验证码已发送至 {self._tg_phone}，"
                                f"请到 TG 内「Telegram」官方会话查看（不是短信），"
                                f"然后填验证码并勾「完成登录」再保存一次")
                else:
                    logger.error(f"【{self.plugin_name}】TG 发送验证码失败: {res.get('error')}")
            self._tg_send_code_once = False
            changed = True

        if self._tg_verify_once:
            if not self._tg_phone or not self._tg_code:
                logger.warning(f"【{self.plugin_name}】请先填写手机号与验证码再完成登录")
            else:
                res = mgr.verify(self._tg_phone, self._tg_code, self._tg_password)
                if res.get("ok"):
                    self.__persist_tg_login(res)
                    logger.info(f"【{self.plugin_name}】TG 登录成功: "
                                f"{res.get('first_name')} (@{res.get('username')})")
                else:
                    logger.error(f"【{self.plugin_name}】TG 登录失败: {res.get('error')}")
            self._tg_verify_once = False
            self._tg_code = ""   # 验证码一次性使用，保存后清空
            changed = True

        if changed:
            self.__update_config()

    def __aiying_ready(self) -> bool:
        """
        爱影通道是否可用：开关开 + telethon 可用 + TG 已登录 + SA 机器人已配置。
        任何一步不满足都返回 False，调用方静默落 PT 兜底。
        """
        if not self._aiying_enabled:
            return False
        if not _TG_LIB_OK:
            logger.warning(f"【{self.plugin_name}】telethon 未安装，爱影通道不可用"
                           f"（PT 订阅不受影响）")
            return False
        if not self._sa_bot:
            logger.warning(f"【{self.plugin_name}】爱影通道已开启但未配置 SA 转存机器人，"
                           f"本轮跳过爱影（PT 兜底）")
            return False
        mgr = self.__get_tg()
        if not mgr:
            return False
        st = mgr.status()
        if st.get("logged_in"):
            self.__persist_tg_login(st)
            return True
        logger.warning(f"【{self.plugin_name}】TG 未登录（{st.get('error') or '会话失效'}），"
                       f"爱影通道不可用，请到配置页完成登录")
        self.__persist_tg_login({"logged_in": False})
        return False

    def __aiying_fill(self, cand: Dict[str, Any],
                      click_budget_left: int) -> Optional[Dict[str, Any]]:
        """
        爱影115通道尝试补齐一部剧（同步方法，内部经 TG 管理器提交协程）。
        返回 None 表示通道异常（调用方静默落 PT）；否则返回：
          {"status": "all"/"partial"/"none", "got": 成功集数, "clicks": 点击数,
           "quota_left": 爱影本月剩余次数, "sa_failed": [失败集标签]}
        """
        mgr = self.__get_tg()
        if not mgr:
            return None
        title = cand["title"]
        tmdbid = int(cand["tmdbid"])
        # 缺集集合 {(季, 集)}
        lack_eps = {(int(s), int(e))
                    for s, eps in cand["lack_info"].items() for e in eps}
        # 【风控】每剧经此通道最多补 _aiying_max_eps 集，超出部分留给 PT
        if len(lack_eps) > self._aiying_max_eps:
            logger.info(f"【{title}】缺集 {len(lack_eps)} 集超过爱影单剧上限 "
                        f"{self._aiying_max_eps}，仅尝试前 {self._aiying_max_eps} 集，"
                        f"剩余转 PT")
            lack_try = set(sorted(lack_eps)[:self._aiying_max_eps])
        else:
            lack_try = lack_eps

        # 搜索关键词：有年份发「剧名 年份」，否则发 tmdbid（机器人支持 tmdbid）
        keyword = f"{title} {cand['year']}".strip() if cand.get("year") else str(tmdbid)
        logger.info(f"【{title}】爱影通道：向 @{self._aiying_bot} 查询「{keyword}」，"
                    f"缺集 {len(lack_try)} 集")

        res = mgr.collect(self._aiying_bot, keyword, tmdbid, lack_try,
                          max_pages=5, interval=self._aiying_interval,
                          click_budget=max(1, click_budget_left))
        # 剧名+年份没搜到资源时，用 tmdbid 兜底再试一次
        if res.get("ok") and not (res.get("links") or {}) and keyword != str(tmdbid):
            logger.info(f"【{title}】按剧名未找到缺集资源，改用 tmdbid={tmdbid} 再试")
            res2 = mgr.collect(self._aiying_bot, str(tmdbid), tmdbid, lack_try,
                               max_pages=5, interval=self._aiying_interval,
                               click_budget=max(1, click_budget_left
                                                - int(res.get("clicks", 0))))
            if res2.get("ok"):
                res2["clicks"] = int(res2.get("clicks", 0)) + int(res.get("clicks", 0))
                if res2.get("quota_left") is None:
                    res2["quota_left"] = res.get("quota_left")
                res = res2

        if not res.get("ok"):
            logger.info(f"【{title}】爱影查询失败：{res.get('error')}（转 PT）")
            return {"status": "none", "got": 0, "clicks": int(res.get("clicks", 0)),
                    "quota_left": None, "sa_failed": []}

        # 持久化爱影本月剩余次数（详情页展示）
        if res.get("quota_left") is not None:
            try:
                self.save_data(self._DATA_AIYING, {
                    "quota_left": res["quota_left"],
                    "updated": datetime.datetime.now(
                        tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
                })
            except Exception:
                pass

        links: Dict[Tuple[int, int], str] = res.get("links") or {}
        if not links:
            logger.info(f"【{title}】爱影无本剧缺集资源（转 PT）")
            return {"status": "none", "got": 0, "clicks": int(res.get("clicks", 0)),
                    "quota_left": res.get("quota_left"), "sa_failed": []}

        # 把拿到的 ed2k/115 链接逐条发给 SA 转存机器人
        items = [(f"S{s:02d}E{e:02d}", url) for (s, e), url in sorted(links.items())]
        logger.info(f"【{title}】爱影拿到 {len(items)} 集链接，逐条转发给 "
                    f"@{self._sa_bot} 离线到 115")
        sub = mgr.submit(self._sa_bot, items, interval=self._aiying_interval)
        if not sub.get("ok"):
            logger.error(f"【{title}】SA 转存提交异常：{sub.get('error')}（已拿到的集按失败处理，转 PT）")
            return {"status": "none", "got": 0, "clicks": int(res.get("clicks", 0)),
                    "quota_left": res.get("quota_left"), "sa_failed": []}

        results = sub.get("results") or {}
        ok_eps = {(s, e) for (s, e) in links
                  if (results.get(f"S{s:02d}E{e:02d}") or {}).get("ok")}
        sa_failed = [label for label, r in results.items() if not r.get("ok")]
        got = len(ok_eps)
        for label in sa_failed:
            logger.warning(f"【{title}】{label} SA 转存失败: "
                           f"{(results.get(label) or {}).get('msg', '')}")

        if ok_eps >= lack_eps:
            status = "all"
        elif got > 0:
            status = "partial"
        else:
            status = "none"
        logger.info(f"【{title}】爱影通道结果：{status}（成功 {got} / 缺集 "
                    f"{len(lack_eps)} 集，点击 {res.get('clicks', 0)} 次）")
        return {"status": status, "got": got, "clicks": int(res.get("clicks", 0)),
                "quota_left": res.get("quota_left"), "sa_failed": sa_failed}

    def __unsub_pt_if_115(self, entry: Dict[str, Any], title: str,
                          history: List[Dict[str, Any]]):
        """
        核销时调用：channel 含 115 渠道的剧，若本插件此前给它加过 MP PT 订阅，
        核销后自动删除该订阅（115 已补齐，避免 PT 重复下载）。
        只删除本插件自己添加的订阅（username=插件名），用户手动订阅不动。
        """
        channel = entry.get("channel", "pt")
        if channel not in ("aiying", "mixed"):
            return
        try:
            tmdbid = int(entry.get("tmdbid") or 0)
        except (TypeError, ValueError):
            return
        if not tmdbid:
            return
        seasons: List[int] = []
        for s in (entry.get("seasons") or {}).keys():
            try:
                seasons.append(int(s))
            except (TypeError, ValueError):
                continue

        removed = 0
        for season in seasons:
            try:
                if not self._subOper.exists(tmdbid, None, season=season):
                    continue
                # 只删本插件自己加的订阅，避免误删用户手动订阅
                my_subs = [sub for sub in
                           (self._subOper.list_by_username(self.plugin_name) or [])
                           if getattr(sub, "tmdbid", None) == tmdbid
                           and getattr(sub, "season", None) == season]
                if not my_subs:
                    logger.info(f"【{title}】第 {season} 季存在 PT 订阅但非本插件添加，保留不动")
                    continue
                for sub in my_subs:
                    self._subOper.delete(sub.id)
                    removed += 1
                    logger.info(f"【{title}】第 {season} 季 PT 订阅已退订 (sid={sub.id})")
            except Exception as e:
                logger.error(f"【{title}】第 {season} 季退订 PT 失败: {e}")
        if removed:
            logger.info(f"【{title}】115 已补齐，退订 PT 订阅 {removed} 季")
            self.__append_history(
                history, title=title, year=str(entry.get("year", "")),
                tmdbid=tmdbid, lack_info={}, missing_count=0,
                result="退订PT", message=f"115 已补齐，退订 PT 订阅 {removed} 季")

    # ==================================================================
    # 配置表单 / 详情页
    # ==================================================================
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置页表单：开关 + 数字框 + 文本框 + 下拉多选"""
        return [
            {
                'component': 'VForm',
                'content': [
                    # ---- 第一行：主开关 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'enabled', 'label': '启用插件'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'onlyonce', 'label': '立即运行一次'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'dry_run',
                                              'label': '调试模式（仅记录不订阅）'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'notify', 'label': '汇总通知'}
                                }]
                            },
                        ]
                    },
                    # ---- 第二行：扫描周期（频率+时刻组合，v1.2.2 替代裸 cron 输入框） ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSelect',
                                    'props': {
                                        'model': 'scan_freq',
                                        'label': '扫描频率（建议每天一次即可，扫太勤没意义还费 TMDB 配额）',
                                        'items': [
                                            {'title': name, 'value': key}
                                            for key, name in SCAN_FREQ_OPTIONS.items()
                                        ],
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 2},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'scan_hour',
                                              'label': '几点跑（0-23，仅每天一次）',
                                              'type': 'number', 'placeholder': '4'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 2},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'scan_minute',
                                              'label': '几分跑（0-59）',
                                              'type': 'number', 'placeholder': '17'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'cron_custom',
                                              'label': '高级用户：自定义 cron 表达式（仅频率选自定义时生效）',
                                              'placeholder': '17 4 * * *'}
                                }]
                            },
                        ]
                    },
                    # ---- 第三行：配额与上限 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'daily_quota',
                                              'label': '每日最大订阅数（部/天，建议不超过300）',
                                              'type': 'number', 'placeholder': '200'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'max_missing',
                                              'label': '单部剧缺集数上限（超过跳过）',
                                              'type': 'number', 'placeholder': '100'}
                                }]
                            },
                        ]
                    },
                    # ---- 第四行：风控参数 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'subscribe_interval',
                                              'label': '订阅间隔（秒）',
                                              'type': 'number', 'placeholder': '2'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'max_consecutive_failures',
                                              'label': '连续失败熔断次数',
                                              'type': 'number', 'placeholder': '5'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'scan_timeout',
                                              'label': '单轮超时保护（分钟）',
                                              'type': 'number', 'placeholder': '60'}
                                }]
                            },
                        ]
                    },
                    # ---- 第四行：优先级策略 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VSelect',
                                    'props': {
                                        'model': 'priority_regions',
                                        'label': '优先地区（多选，留空=不区分）',
                                        'multiple': True,
                                        'chips': True,
                                        'items': [
                                            {'title': name, 'value': key}
                                            for key, name in REGION_OPTIONS.items()
                                        ],
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VSelect',
                                    'props': {
                                        'model': 'sort_rule',
                                        'label': '同层排序规则',
                                        'items': [
                                            {'title': name, 'value': key}
                                            for key, name in SORT_RULES.items()
                                        ],
                                    }
                                }]
                            },
                        ]
                    },
                    # ---- 第五行：优先类型与过滤开关 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'priority_anime',
                                              'label': '动漫优先'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'priority_documentary',
                                              'label': '纪录片优先'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'ignore_s0',
                                              'label': '忽略特别篇 S00'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'ignore_unfinished_latest',
                                              'label': '未完结剧忽略最新季'}
                                }]
                            },
                        ]
                    },
                    # ---- 第六行：验证回环参数 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'verify_alert_days',
                                              'label': '未入库告警天数',
                                              'type': 'number', 'placeholder': '7'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'dead_task_hours',
                                              'label': '死任务判定（小时）',
                                              'type': 'number', 'placeholder': '6'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'verify_success_notify',
                                              'label': '补齐时发 ✅ 通知'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'dead_task_auto_delete',
                                              'label': '自动删除死任务（慎开）'}
                                }]
                            },
                        ]
                    },
                    # ---- 第七行：磁盘告警 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'disk_check_path',
                                              'label': '磁盘告警路径（MP 容器内）',
                                              'placeholder': '/video/downloads'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'disk_alert_gb',
                                              'label': '剩余空间告警阈值（GB）',
                                              'type': 'number', 'placeholder': '200'}
                                }]
                            },
                        ]
                    },
                    # ---- 第八行：其余开关 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'best_version', 'label': '订阅时洗版'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'clear_history',
                                              'label': '清空历史与已处理清单'}
                                }]
                            },
                        ]
                    },
                    # ---- 第九行：排除关键词 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'exclude_keywords',
                                              'label': '排除关键词（逗号分隔，命中剧名则跳过）',
                                              'placeholder': '例如：纪录片,演唱会'}
                                }]
                            },
                        ]
                    },
                    # ---- 第十行：爱影115通道说明 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [{
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'warning',
                                        'variant': 'tonal',
                                        'text': '【爱影115通道】（实验功能）开启后，缺集会先问爱影资源机器人'
                                                '拿 ed2k/115 链接，发给你的 SA 转存机器人自动离线到 115；'
                                                '拿不到的集仍走原有 PT 订阅兜底。'
                                                '需要：①你的 TG 账号完成下方登录；②填写你自己的 Symedia '
                                                '转存机器人（Symedia 转存助手配置见 symedia.top 文档）。'
                                                'Telethon 依赖未装上或 TG 未登录时，本通道自动停用，'
                                                '不影响原有 PT 订阅。'
                                    }
                                }]
                            },
                        ]
                    },
                    # ---- 第十一行：爱影开关 + 机器人配置 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'aiying_enabled',
                                              'label': '启用爱影115通道',
                                              'hint': '缺集优先走 115 离线，拿不到再落 PT 兜底',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'aiying_bot',
                                              'label': '爱影资源机器人用户名',
                                              'placeholder': 'ayclub_bot',
                                              'hint': '默认 ayclub_bot，不用改；不带 @',
                                              'persistent-hint': True}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 5},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'sa_bot',
                                              'label': 'SA 转存机器人用户名（你自己的，不带 @）',
                                              'placeholder': '例如 ColdSymMedia_bot',
                                              'hint': '填你自己 Symedia 的 TG 机器人；插件把 115/ed2k 链接发给它自动离线到 115。Symedia 转存助手配置见 symedia.top 文档',
                                              'persistent-hint': True}
                                }]
                            },
                        ]
                    },
                    # ---- 第十二行：TG 登录信息 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'tg_phone',
                                              'label': 'TG 手机号（带国家区号）',
                                              'placeholder': '+8613800138000',
                                              'hint': '登录你本人 TG 账号，会话文件只存在你自己 NAS 的插件数据目录里',
                                              'persistent-hint': True}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'tg_code',
                                              'label': 'TG 验证码（一次性，保存后清空）',
                                              'placeholder': '12345',
                                              'hint': '先勾「发送验证码」保存一次，到 TG 的「Telegram」官方会话里收码（不是短信），填到这里再勾「完成登录」保存',
                                              'persistent-hint': True}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'tg_password',
                                              'label': '两步验证密码（可空）',
                                              'type': 'password',
                                              'hint': 'TG 账号开了两步验证才需要填',
                                              'persistent-hint': True}
                                }]
                            },
                        ]
                    },
                    # ---- 第十三行：登录动作开关 + 代理 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'tg_send_code_once',
                                              'label': '发送验证码（保存即触发）',
                                              'hint': '一次性开关：填好手机号后勾上并保存，结果看日志',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'tg_verify_once',
                                              'label': '完成登录（保存即触发）',
                                              'hint': '一次性开关：填好验证码后勾上并保存，登录状态看详情页顶部',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'tg_proxy',
                                              'label': 'TG 代理地址',
                                              'placeholder': TG_DEFAULT_PROXY,
                                              'hint': 'Telegram 需要代理才能连；也可以调 API 登录：POST /api/v1/plugin/LackEpisodeAutoSub/tg_send_code 与 /tg_verify（见 /docs）',
                                              'persistent-hint': True}
                                }]
                            },
                        ]
                    },
                    # ---- 第十四行：爱影风控参数 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'aiying_interval',
                                              'label': '爱影每集间隔（秒）',
                                              'type': 'number', 'placeholder': '3',
                                              'hint': '点按钮/发链接的间隔，太小容易被 TG 风控',
                                              'persistent-hint': True}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'aiying_max_eps',
                                              'label': '每剧经爱影最多补集数',
                                              'type': 'number', 'placeholder': '30',
                                              'hint': '防点爆爱影次数；超出的集仍走 PT 订阅',
                                              'persistent-hint': True}
                                }]
                            },
                        ]
                    },
                    # ---- 提示 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [{
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'info',
                                        'variant': 'tonal',
                                        'text': '首次使用建议保持「调试模式」开启并点「立即运行一次」，'
                                                '到日志和详情页确认识别无误后，再关闭调试模式正式订阅。'
                                                '优先级逻辑：命中「优先地区」+1 层，命中「动漫/纪录片优先」'
                                                '各 +1 层，层级越高越先订；同层内按「同层排序规则」排序。'
                                                '订阅后插件会每轮复查入库情况，超时未入库会告警（不自动退订）。'
                                                '「清空历史与已处理清单」会让所有剧重新参与订阅，请谨慎使用。'
                                    }
                                }]
                            },
                        ]
                    },
                ]
            }
        ], {
            # 表单默认值（首次打开时显示）
            "enabled": False,
            "onlyonce": False,
            "dry_run": True,
            "notify": True,
            "clear_history": False,
            "scan_freq": "daily",
            "scan_hour": 4,
            "scan_minute": 17,
            "cron_custom": "",
            "daily_quota": 200,
            "max_missing": 100,
            "subscribe_interval": 2,
            "max_consecutive_failures": 5,
            "scan_timeout": 60,
            "priority_regions": [],
            "priority_anime": False,
            "priority_documentary": False,
            "sort_rule": "least_missing",
            "ignore_s0": True,
            "ignore_unfinished_latest": True,
            "best_version": False,
            "exclude_keywords": "",
            "verify_alert_days": 7,
            "verify_success_notify": False,
            "dead_task_hours": 6,
            "dead_task_auto_delete": False,
            "disk_check_path": "/video/downloads",
            "disk_alert_gb": 200,
            "aiying_enabled": False,
            "tg_phone": "",
            "tg_code": "",
            "tg_password": "",
            "tg_proxy": TG_DEFAULT_PROXY,
            "tg_send_code_once": False,
            "tg_verify_once": False,
            "aiying_bot": "ayclub_bot",
            "sa_bot": "",
            "aiying_interval": 3,
            "aiying_max_eps": 30,
        }

    def get_page(self) -> List[dict]:
        """详情页：统计卡片 + 补齐验证区块 + 疑似死任务区块 + 最近 50 条历史表格"""
        stats = self.get_data(self._DATA_STATS) or {}
        history = self.get_data(self._DATA_HISTORY) or []
        processed = self.get_data(self._DATA_PROCESSED) or {}
        pending = self.get_data(self._DATA_PENDING) or {}
        dead_snapshot = self.get_data(self._DATA_DEAD) or {}
        recent = list(reversed(history[-50:]))  # 最新在前
        now = datetime.datetime.now(tz=pytz.timezone(settings.TZ))

        def __stat_card(title: str, value: Any, color: str) -> dict:
            return {
                'component': 'VCol',
                'props': {'cols': 6, 'md': 2},
                'content': [{
                    'component': 'VCard',
                    'props': {'variant': 'tonal', 'color': color},
                    'content': [
                        {
                            'component': 'VCardText',
                            'props': {'class': 'text-center pa-3'},
                            'content': [
                                {'component': 'div',
                                 'props': {'class': 'text-h5 font-weight-bold'},
                                 'text': str(value)},
                                {'component': 'div',
                                 'props': {'class': 'text-caption'},
                                 'text': title},
                            ]
                        }
                    ]
                }]
            }

        # ---- 实时进度卡片（v1.3.0）：扫描没跑完也能看到数字与进度条 ----
        progress_data = self.get_data(self._DATA_PROGRESS) or {}
        progress_rows: List[dict] = []
        if progress_data.get("running"):
            _pct = int(progress_data.get("percent", 0))
            _phase = progress_data.get("phase_label", "运行中")
            _cur = progress_data.get("current", "")
            _total = int(progress_data.get("total", 0))
            _scanned = int(progress_data.get("scanned", 0))
            _bar_props = {
                'color': 'primary', 'height': 20, 'striped': True,
                'class': 'mb-2',
            }
            if _total > 0:
                _bar_props['model-value'] = _pct
            else:
                # 总数还没读出来（正在读第一个媒体库），显示不定态滚动条
                _bar_props['indeterminate'] = True
            progress_rows.append({
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [{
                        'component': 'VCard',
                        'props': {'variant': 'tonal', 'color': 'primary'},
                        'content': [
                            {
                                'component': 'VCardTitle',
                                'props': {'class': 'text-subtitle-1'},
                                'text': f"⏳ 正在运行：{_phase}"
                                        f"{'（调试模式，不会真订阅）' if progress_data.get('dry_run') else ''}"
                            },
                            {
                                'component': 'VCardText',
                                'content': [
                                    {'component': 'VProgressLinear',
                                     'props': _bar_props},
                                    {'component': 'div',
                                     'props': {'class': 'text-body-2 mb-1'},
                                     'text': (f"进度 {_pct}%：已扫描 {_scanned}"
                                              f"{f' / {_total}' if _total else ''} 部剧"
                                              f"{'（总数随媒体库读取逐步增加）' if progress_data.get('phase') == 'scanning' else ''}")},
                                    {'component': 'div',
                                     'props': {'class': 'text-body-2 mb-1'},
                                     'text': f"正在处理：{_cur}" if _cur else "正在处理：…"},
                                    {'component': 'div',
                                     'props': {'class': 'text-body-2 mb-1'},
                                     'text': (f"本轮实时：发现缺集 {progress_data.get('missing', 0)} 部 · "
                                              f"候选 {progress_data.get('candidates', 0)} 部 · "
                                              f"已订阅 {progress_data.get('subscribed', 0)} 部 · "
                                              f"爱影补齐 {progress_data.get('aiying', 0)} 部 · "
                                              f"跳过 {progress_data.get('skipped', 0)} 部 · "
                                              f"失败 {progress_data.get('failed', 0)} 部 · "
                                              f"今日剩余配额 {progress_data.get('quota_left', 0)} 部")},
                                    {'component': 'div',
                                     'props': {'class': 'text-caption text-grey'},
                                     'text': (f"开始于 {progress_data.get('started_at', '')}。"
                                              f"页面不会自动刷新，重新打开本卡片即可查看最新进度。")},
                                ]
                            },
                        ]
                    }]
                }]
            })
        elif progress_data.get("finished_at"):
            _min = round(int(progress_data.get("elapsed_sec", 0)) / 60, 1)
            progress_rows.append({
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [{
                        'component': 'VAlert',
                        'props': {
                            'type': 'success', 'variant': 'tonal', 'density': 'compact',
                            'text': (f"上一轮已于 {progress_data.get('finished_at')} 完成："
                                     f"扫描 {progress_data.get('scanned', 0)} 部 · "
                                     f"发现缺集 {progress_data.get('missing', 0)} 部 · "
                                     f"订阅 {progress_data.get('subscribed', 0)} 部 · "
                                     f"跳过 {progress_data.get('skipped', 0)} 部 · "
                                     f"失败 {progress_data.get('failed', 0)} 部 · "
                                     f"耗时 {_min} 分钟")
                        }
                    }]
                }]
            })

        # ---- TG 登录状态行（v1.4.0）：爱影通道启用或登录过才显示 ----
        tg_login = self.get_data(self._DATA_TG_LOGIN) or {}
        tg_rows: List[dict] = []
        if self._aiying_enabled or tg_login:
            if tg_login.get("logged_in"):
                _acc = (f"{tg_login.get('first_name', '')} "
                        f"(@{tg_login.get('username', '')})").strip()
                tg_rows.append({
                    'component': 'VRow',
                    'content': [{
                        'component': 'VCol',
                        'props': {'cols': 12},
                        'content': [{
                            'component': 'VAlert',
                            'props': {
                                'type': 'success', 'variant': 'tonal', 'density': 'compact',
                                'text': (f"爱影115通道：TG 已登录（{_acc}）"
                                         f"{'，通道已启用' if self._aiying_enabled else '，通道未启用（到配置页打开开关）'}"
                                         f"（状态更新于 {tg_login.get('checked_at', '未知')}）")
                            }
                        }]
                    }]
                })
            else:
                tg_rows.append({
                    'component': 'VRow',
                    'content': [{
                        'component': 'VCol',
                        'props': {'cols': 12},
                        'content': [{
                            'component': 'VAlert',
                            'props': {
                                'type': 'warning', 'variant': 'tonal', 'density': 'compact',
                                'text': ('爱影115通道：TG 未登录。请到配置页填手机号，'
                                         '勾「发送验证码」保存，再到 TG 收码后填验证码、'
                                         '勾「完成登录」保存；登录成功后本行会显示账号名。')
                            }
                        }]
                    }]
                })

        page = tg_rows + progress_rows + [
            # ---- 统计卡片（两行：扫描订阅类 + 验证回环类） ----
            {
                'component': 'VRow',
                'content': [
                    __stat_card("累计扫描", stats.get("total_scanned", 0), "blue-grey"),
                    __stat_card("发现缺集", stats.get("total_missing", 0), "orange"),
                    __stat_card("累计订阅", stats.get("total_subscribed", 0), "green"),
                    __stat_card("累计跳过", stats.get("total_skipped", 0), "grey"),
                    __stat_card("累计失败", stats.get("total_failed", 0), "red"),
                    __stat_card("已处理剧目", len(processed), "purple"),
                ]
            },
            {
                'component': 'VRow',
                'content': [
                    __stat_card("待验证", len(pending), "blue"),
                    __stat_card("已核销", stats.get("total_verified", 0), "teal"),
                    __stat_card("超时未补齐", stats.get("total_timeout", 0), "deep-orange"),
                ]
            },
            # ---- 爱影统计卡片（v1.4.0，通道启用才显示）----
            *([{
                'component': 'VRow',
                'content': [
                    __stat_card("本轮爱影补齐", stats.get("last_aiying", 0), "cyan"),
                    __stat_card("累计爱影补齐", stats.get("total_aiying", 0), "teal"),
                    __stat_card(
                        "爱影剩余次数",
                        (self.get_data(self._DATA_AIYING) or {}).get("quota_left", "未知"),
                        "indigo"),
                ]
            }] if self._aiying_enabled else []),
            # ---- 上次运行时间 ----
            {
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [{
                        'component': 'VAlert',
                        'props': {
                            'type': 'info', 'variant': 'text', 'density': 'compact',
                            'text': f"上次运行：{stats.get('last_run', '尚未运行')}"
                                    f"{'，当前为调试模式（仅记录不订阅）' if self._dry_run else ''}"
                        }
                    }]
                }]
            },
        ]

        # ---- 补齐验证区块：等待入库列表 ----
        if pending:
            verify_items = []
            for entry in pending.values():
                try:
                    sub_dt = datetime.datetime.strptime(
                        entry.get("subscribe_time", ""), TIME_FMT)
                    wait_days = (now.replace(tzinfo=None) - sub_dt).days
                except (TypeError, ValueError):
                    wait_days = 0
                remaining_eps = sum(len(v) for v in (entry.get("remaining") or {}).values())
                # 状态色：已告警的超时=红，等待中=蓝
                verify_items.append({
                    "title": entry.get("title", ""),
                    "subscribe_time": entry.get("subscribe_time", ""),
                    "wait_days": wait_days,
                    "remaining": remaining_eps,
                    "status": "超时未补齐" if entry.get("alerted") else "等待入库",
                })
            # 等待天数长的排前面，最需要关注的在最上
            verify_items.sort(key=lambda x: -x["wait_days"])
            page.append({
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {
                            'component': 'VCard',
                            'props': {'variant': 'outlined'},
                            'content': [
                                {
                                    'component': 'VCardTitle',
                                    'props': {'class': 'text-subtitle-1'},
                                    'text': f'补齐验证（{len(pending)} 部订阅后等待入库）'
                                },
                                {
                                    'component': 'VCardText',
                                    'content': [{
                                        'component': 'VDataTable',
                                        'props': {
                                            'headers': [
                                                {'title': '剧名', 'key': 'title', 'sortable': False},
                                                {'title': '订阅日期', 'key': 'subscribe_time', 'sortable': False},
                                                {'title': '已等天数', 'key': 'wait_days', 'sortable': False},
                                                {'title': '剩余缺集', 'key': 'remaining', 'sortable': False},
                                                {'title': '状态', 'key': 'status', 'sortable': False},
                                            ],
                                            'items': verify_items[:50],
                                            'items-per-page': 50,
                                            'density': 'compact',
                                        }
                                    }]
                                },
                            ]
                        }
                    ]
                }]
            })

        # ---- 疑似死任务区块 ----
        dead_tasks = (dead_snapshot or {}).get("tasks") or []
        if dead_tasks:
            page.append({
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {
                            'component': 'VCard',
                            'props': {'variant': 'outlined', 'color': 'warning'},
                            'content': [
                                {
                                    'component': 'VCardTitle',
                                    'props': {'class': 'text-subtitle-1'},
                                    'text': f'疑似死任务（{len(dead_tasks)} 个，'
                                            f'检测于 {dead_snapshot.get("check_time", "未知")}）'
                                },
                                {
                                    'component': 'VCardText',
                                    'content': [{
                                        'component': 'VDataTable',
                                        'props': {
                                            'headers': [
                                                {'title': '任务名', 'key': 'name', 'sortable': False},
                                                {'title': '下载器', 'key': 'downloader', 'sortable': False},
                                                {'title': '已挂时长(小时)', 'key': 'age_hours', 'sortable': False},
                                                {'title': '状态', 'key': 'state', 'sortable': False},
                                            ],
                                            'items': dead_tasks[:50],
                                            'items-per-page': 50,
                                            'density': 'compact',
                                        }
                                    }]
                                },
                            ]
                        }
                    ]
                }]
            })

        # ---- 历史表格 ----
        if recent:
            page.append({
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [{
                        'component': 'VDataTable',
                        'props': {
                            'headers': [
                                {'title': '时间', 'key': 'time', 'sortable': False},
                                {'title': '剧名', 'key': 'title', 'sortable': False},
                                {'title': '年份', 'key': 'year', 'sortable': False},
                                {'title': '缺集季', 'key': 'seasons', 'sortable': False},
                                {'title': '缺集数', 'key': 'missing_count', 'sortable': False},
                                {'title': '结果', 'key': 'result', 'sortable': False},
                                {'title': '备注', 'key': 'message', 'sortable': False},
                            ],
                            'items': recent,
                            'items-per-page': 50,
                            'density': 'compact',
                            'class': 'elevation-1',
                        }
                    }]
                }]
            })
        else:
            page.append({
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [{
                        'component': 'VAlert',
                        'props': {'type': 'info', 'variant': 'tonal',
                                  'text': '暂无历史记录，开启插件并运行一次后再来看看。'}
                    }]
                }]
            })
        return page
