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
  v1.9.5  可观测性与 AY 鉴权诊断增强：
          ①失败计数细分为 TMDB、Emby、订阅、其他并在进度页展示；
          ②AY API 请求增加目标、状态码、耗时与脱敏 token 日志；
          ③每轮扫描开始时执行 AY 鉴权预检，失败自动回退且不影响 PT；
          ④配置页新增「测试 AY 连通性」保存即触发开关，通知结果后自动复位
  v1.9.4  遗留收尾：
          ①清理废弃死代码 __ay_estimate_cover（v1.7.0 估算覆盖，已被 v1.9.0
            的 __ay_resource_cover 精确选包取代，全文无调用）；
          ②联调 API GET /ay_api_test?save=1 提交改走 __sa_submit 三级回退
            （直连 API → 企微 → TG），与主流程一致；sa_result 增加 via 通道字段
  v1.9.3  详情页明暗主题适配：
          ①详情页「补齐验证/疑似死任务/最近历史」HTML 表格颜色全部改用
            Vuetify 主题变量（rgb(var(--v-theme-on-surface))、
            rgb(var(--v-theme-surface))、rgba(var(--v-border-color),
            var(--v-border-opacity)) 等），明暗主题自动适配，修复黑暗模式下
            表格发白看不清
  v1.9.2  SA 转存改直连 API + 公开仓库脱敏：
          ①SA 提交新增「直连 API」最高优先级：登录拿 JWT（缓存 25 天，
            401 自动重登重试），POST /plugin/115/offline 一次提交全部链接
            （ed2k 与 115 分享链接同接口），目标目录自动取 folders 第一项；
            三级回退：直连 API → 企微回调 → TG 机器人；
          ②公开仓库脱敏：AY API 地址/token、企微回调四件套等内置默认值
            全部清空或泛化（用户在 MP 数据库里的已存配置不受影响，
            空值时对应通道优雅降级）；全文件中文「AY」统一命名
          ③新增联调 API GET /sa_api_test（登录+目录检查，可真实提交一条）
  v1.9.1  修复详情页表格显示 bug：MP 前端 v2.15.6 的 VDataTable 渲染
          thead/tbody 全空（只剩分页器，后端 JSON 正常），「补齐验证/
          疑似死任务/最近历史」三个表格全部改为裸 HTML 经 v-html 渲染；
          状态列着色、渠道 chips 着色、单元格全量 HTML 转义
  v1.9.0  四项优化：
          ①AY API 选包改为「按缺集精确选包」的贪心集合覆盖：逐条解析资源
            name/notes 的集数范围（S01E06-S01E10/S1-S5/单集/全集/无标识），
            与缺集求交，无关包直接淘汰；每轮选新增有效覆盖最大的包，
            平局按 115 分享链接优先 > 溢出更小 > size 更小；
            缺 2 集不再拖回整季大包，选包理由写入 channel_detail/日志；
          ②部分季订阅失败的首轮成功季纳入验证回环：__register_pending
            支持按成功季登记与合并（同一部剧只占一条，剩余缺集并集更新）；
          ③即时搜索触发前探 Emby 就绪（/emby/System/Info/Public，5 秒超时，
            5 分钟 TTL 缓存，失败不缓存下条重探）：未就绪跳过触发，
            避免 Emby 重启时 MP 误判整季缺失拖整季包（大明风华 131G 事故）；
          ④/ay_api_test 增加 eps 参数模拟缺集，返回选包明细与理由
  v1.8.0  SA 直连 HTTP 转存：
          ①SA（Symedia）提交从「Telethon 发 TG 机器人」升级为 HTTP 直连
            （SA 的消息 API 唯一被处理的形态是企业微信回调协议，已实现
            WXBizMsgCrypt：AES-256-CBC + sha1 签名；明文 POST 会被 SA 静默
            丢弃，「消息已接收」≠「消息处理成功」，后者才算成功）；
          ②TG 会话从必需降级为可选兜底：SA HTTP 通道配置齐全时，
            TG 未登录也能走「AY API 查询 → HTTP 转存」全链路；
          ③新增包装层 __sa_submit：HTTP 优先、失败自动回退 TG 提交，
            渠道明细人话区分 SA(HTTP)/SA(TG)；
          ④提交不等离线结果（SA 异步处理），统一标「已提交待验证」，
            由入库验证回环兜底确认（设计意图）；
          ⑤新增联调 API GET /sa_http_test；api_status 增加 sa_channel 字段
  v1.7.0  AY HTTP API 通道 + 渠道详情记录：
          ①AY查询从 TG 点按钮流升级为 HTTP API 主通道（POST /api/user，
            返回 115 分享链接与当月额度 times），链接经现有 SA 通道（Telethon
            发 SA 机器人）离线到 115；API 请求异常自动回退原 TG 点按钮流程，
            API 明确无资源则直接转 PT 不再走 TG；
          ②链接选择：优先整季整剧标识（全/Complete/S1-S5 等），其次按大小降序，
            单剧最多提交 aiying_api_max_links 条；集数覆盖从 name/notes 尽力估算，
            估不出写「覆盖未知，待入库验证」，真实补齐仍以验证回环为准；
          ③历史记录扩展 channel（pt/aiying_api/aiying_tg/mixed）与 channel_detail
            人话明细（PT 附 sid、115 附资源 notes/大小/SA 回执），汇总通知前缀
            扩为 [PT]/[115·API]/[115·TG]/[混合]，详情页历史表新增渠道列；
          ④新增插件 API GET /ay_api_test?tmdb_id=xx&save=0/1 用于联调测试；
          ⑤页面统计区显示「AYAPI剩余次数」（取最近一次响应 times）
  v1.6.0  新增「增量扫描」（完结账本驱动）：
          ①新增持久化「完结账本」done_ledger：只收录「TMDB 已完结(Ended/Canceled)
            且当前无缺集」的剧；扫描评估无缺集时写入（status 取同一次识别结果，
            不额外请求），验证回环核销已完结剧时也写入；
          ②增量模式（默认开）下，账本内且不在待验证清单的剧本轮直接跳过，
            不再逐部调 Emby/TMDB——几千部的大库一轮从 60~70 分钟降到几分钟；
            判断只用 Emby 条目自带的 tmdbid，不为账本多调接口；
          ③自愈：任何模式下发现「有缺集」或「订阅未全部成功」的剧自动移出账本，
            完结剧出新季/用户删集下轮自动回归；
          ④全量兜底：新增「每周全量扫描日」（默认周日）与「本轮强制全量扫描」
            （一次性开关，跑完自动关）；「清空历史与已处理清单」连带清空账本；
          ⑤进度与页面展示扫描模式/账本跳过部数/账本总量，汇总通知带「增量跳过 X 部」
  v1.5.0  修复与闭环增强：
          ①修复严重 bug：部分季订阅失败却整部销账（mixed 分支AY部分成功也
            抵消 PT 失败）——现在只有全部缺集季订阅成功才写已处理清单，
            部分失败下轮重试，历史记录区分「已订阅/部分季订阅失败（附季号）」；
          ②新增「订阅后立即搜索」（默认开）：订阅成功即触发 MP 单订阅即时搜索，
            不再干等 MP 周期搜索（订阅间随机休眠 60~300 秒）；
          ③修复验证回环与扫描阶段共用超时预算的问题：验证阶段独立计时，
            预算仍取「单轮超时保护」配置值，两个阶段互不挤占；
          ④修复「立即停止」死代码：扫描/订阅循环每轮检查 _event 停止标志，
            置位时写「手动停止」历史记录并优雅收尾；
          ⑤新增「超时自动重置订阅」（默认开，note 清空 + lack_episode 重置为
            total_episode，让 MP 重新搜索）与「订阅丢失自动补订」（默认关，
            尊重手动退订）；两者只处理 username=本插件名 的订阅；
          ⑥页面修正：缺集统计卡标注「累计（含重复轮次）」口径、补齐验证表
            状态列按已等天数实时计算超期、顶部加静态快照提示
  v1.4.0  新增「AY 115 通道」（实验功能）：
          ①插件内嵌 TG 用户态会话管理器（Telethon + 独立 daemon 线程跑 asyncio
            事件循环，同步代码经 run_coroutine_threadsafe 调用），会话文件存插件
            数据目录，Telethon 未安装/未登录时整体降级为「未启用」，绝不影响 PT 主流程；
          ②缺集订阅前先问资源机器人（默认 @ayclub_bot）拿 ed2k/115 链接，
            转发给 SA 转存机器人自动离线到 115，全部补齐则不再走 MP 订阅，
            部分补齐则剩余集落回 PT 兜底（history 标注混合渠道）；
          ③新增 /tg_send_code /tg_verify /tg_status 三个登录 API，
            配置页提供一次性开关完成「发验证码/完成登录」全流程；
          ④验证回环核销时，115 渠道已补齐的剧自动退订本插件此前添加的 PT 订阅，
            避免重复下载；通知与历史记录区分 [AY115] / [PT下载] 来源
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
import base64
import datetime
import hashlib
import os
import random
import re
import struct
import threading
import time
import traceback
from threading import Event as ThreadEvent
from typing import Any, Dict, List, Optional, Set, Tuple
from html import escape as _escape_html
from urllib.parse import urlparse

import pytz
import requests
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
# AY 115 通道：TG 用户态会话（v1.4.0 新增）
# ---------------------------------------------------------------------------

# Telegram Desktop 官方开源公开凭证（github.com/telegramdesktop/tdesktop 源码内）
TG_API_ID = 2040
TG_API_HASH = "b18441a1ff607e10a989891a5462e627"
# 默认代理（NAS 本地代理，Telegram 直连不可达时需要）
TG_DEFAULT_PROXY = "http://192.168.31.40:7890"

# AY资源列表行：🧲 [国漫] 斗罗大陆Ⅱ绝世唐门 (2023) {tmdb-228429} S01E171 4K TX WEB-DL 1.57G
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

# Telethon 兜底导入：依赖未装上时插件照常加载，AY通道整体降级为「未启用」
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

# pycryptodome 兜底导入（v1.8.0，SA HTTP 通道的企业微信回调加解密用）：
# 依赖未装上时插件照常加载，SA HTTP 通道禁用并回退 TG 提交
try:
    from Crypto.Cipher import AES as _AES
    _CRYPTO_OK = True
except Exception:
    _AES = None
    _CRYPTO_OK = False


def _parse_aiying_lines(text: str) -> List[Dict[str, Any]]:
    """
    解析AY资源列表文本，返回条目列表：
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


def _html_table(headers: List[str], rows: List[List[Any]],
                col_styles: Optional[Dict[int, str]] = None) -> str:
    """
    裸 HTML 表格渲染（v1.9.1）。
    背景：MP 前端 v2.15.6 的 VDataTable 对本插件数据渲染 thead/tbody 全空
    （只剩分页器，后端 JSON 完全正常），改用 {'component': 'div', 'html': ...}
    的 v-html 通道绕过，这是最可靠的方式。
    headers: 列名列表；rows: 单元格二维列表——单元格为 ("raw", html) 元组时
    原样输出（用于状态着色/渠道 chips），其余一律 HTML 转义防注入；
    col_styles: {列号: 追加的 td 样式}（如渠道详情列允许换行）。
    纯函数，不依赖 MP 环境，可独立测试。
    """
    # v1.9.3：颜色全部改用 Vuetify 主题变量（--v-theme-on-surface/--v-border-color），
    # 明暗主题自动适配，不再硬编码浅色（修复黑暗模式下表格发白看不清）。
    _border = "rgba(var(--v-border-color),var(--v-border-opacity))"
    _fg = "rgb(var(--v-theme-on-surface))"
    th_style = (f"text-align:left;padding:6px 8px;border-bottom:2px solid {_border};"
                f"position:sticky;top:0;background:rgb(var(--v-theme-surface));"
                f"color:{_fg};z-index:1")
    td_style = (f"padding:5px 8px;border-bottom:1px solid {_border};"
                f"vertical-align:top;color:{_fg}")
    parts = [
        '<div style="overflow-x:auto;max-height:560px;overflow-y:auto">',
        f'<table style="width:100%;border-collapse:collapse;font-size:12.5px;'
        f'white-space:nowrap;color:{_fg};background:transparent">',
        '<thead><tr>',
    ]
    for h in headers:
        parts.append(f'<th style="{th_style}">{_escape_html(str(h))}</th>')
    parts.append('</tr></thead><tbody>')
    if not rows:
        # 空数据：跨列居中（主题半透灰）
        parts.append(f'<tr><td colspan="{len(headers)}" '
                     'style="padding:16px 8px;text-align:center;'
                     'color:rgba(var(--v-theme-on-surface),0.5)">'
                     '暂无数据</td></tr>')
    for row in rows:
        parts.append('<tr>')
        for idx, cell in enumerate(row):
            extra = (col_styles or {}).get(idx, "")
            style = td_style + (";" + extra if extra else "")
            if isinstance(cell, tuple) and len(cell) == 2 and cell[0] == "raw":
                parts.append(f'<td style="{style}">{cell[1]}</td>')
            else:
                parts.append(f'<td style="{style}">{_escape_html(str(cell))}</td>')
        parts.append('</tr>')
    parts.append('</tbody></table></div>')
    return "".join(parts)


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
        给AY机器人发关键词，分页收集缺集条目并逐集点击按钮拿 ed2k/115 链接。
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

    def my_id(self) -> Dict[str, Any]:
        """获取当前登录账号的 TG 用户 ID（v1.7.0，AY API 需要 tg_id）。
        复用现有客户端连接，不单独建连；返回 {ok, tg_id, error}"""
        if not _TG_LIB_OK:
            return {"ok": False, "error": "telethon 未安装"}
        try:
            return self.__run(self.__my_id_async(), timeout=60)
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

    async def __my_id_async(self) -> Dict[str, Any]:
        """在循环线程内取当前账号 get_me().id（复用已连接客户端）"""
        client = await self.__ensure_client()
        if not await client.is_user_authorized():
            return {"ok": False, "error": "TG 未登录"}
        me = await client.get_me()
        return {"ok": True, "tg_id": getattr(me, "id", None)}

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
                # 风控：每次点击后间隔，防点爆AY次数
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
                # 「任务已存在」和「你已经转存过该文件」都算成功（说明 115 已有该文件，无需再转 PT 重复下载）
                # v1.8.1：打包链接的回执按行分析，全部失败行都是"已存在"类才算成功；
                # 任何一行是「分享已取消」等真实失败则整条按失败处理（剩余集会转 PT 兜底）。
                _already = ("任务已存在", "已经转存过")
                if "失败" in text:
                    fail_lines = [ln for ln in text.splitlines() if "=>" in ln]
                    if fail_lines:
                        _ok = all(any(h in ln for h in _already) for ln in fail_lines)
                    else:
                        _ok = any(h in text for h in _already)
                    results[label] = {"ok": _ok, "msg": (text or "无回复")[:120]}
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
    plugin_version = "1.9.5"
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
    # Emby 就绪探测 TTL 缓存（v1.9.0）：{"ok": bool, "ts": 时间戳}，
    # 5 分钟内不重复探测；探测失败不写入，下条触发前重探
    _emby_ready_cache: Dict[str, Any] = {}

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

    # 【订阅闭环增强】（v1.5.0 新增；老配置缺字段时默认值兜底，向后兼容）
    _search_after_subscribe: bool = True   # 订阅成功后立即触发 MP 搜索该订阅
    _verify_auto_reset: bool = True        # 超时未入库自动重置订阅（让 MP 重新搜索）
    _verify_auto_resubscribe: bool = False # 订阅丢失自动补订（默认关，尊重手动退订）

    # 【增量扫描】（v1.6.0 新增；老配置缺字段时默认值兜底，向后兼容）
    _incremental_scan: bool = True         # 增量扫描：完结账本内的剧本轮跳过
    _full_scan_weekday: int = 6            # 每周全量扫描日（0=周一 ... 6=周日，默认周日）
    _full_scan_once: bool = False          # 本轮强制全量扫描（一次性，跑完自动关）

    # 【AY 115 通道】（v1.4.0 新增；默认全部兜底，Telethon 缺失时整体不启用）
    _aiying_enabled: bool = False        # AY115通道开关
    _tg_phone: str = ""                  # TG 手机号（登录你本人 TG 账号）
    _tg_code: str = ""                   # TG 验证码（一次性，保存后清空）
    _tg_password: str = ""               # TG 两步验证密码（可空）
    _tg_proxy: str = TG_DEFAULT_PROXY    # TG 代理地址
    _tg_send_code_once: bool = False     # 一次性开关：保存配置时发送验证码
    _tg_verify_once: bool = False        # 一次性开关：保存配置时完成登录
    _aiying_bot: str = "ayclub_bot"      # AY资源机器人用户名
    _sa_bot: str = ""                    # SA 转存机器人用户名（你的 Symedia 机器人）
    _aiying_interval: int = 3            # 每集点击/发送间隔秒数（风控）
    _aiying_max_eps: int = 30            # 每剧经此通道最多补集数（风控）

    # 【AY HTTP API 通道】（v1.7.0 新增；老配置缺字段时默认值兜底，向后兼容）
    _aiying_api_enabled: bool = True     # AY API 通道（主通道，异常时回退 TG 流程）
    # v1.9.2 脱敏：公开仓库不再内置真实服务地址与 token，请自行填写；
    # 留空时 API 通道优雅降级回退 TG 流程（用户在 MP 数据库的已存配置不受影响）
    _aiying_api_url: str = ""            # AY API 地址
    _aiying_api_token: str = ""          # API token（用户自己的）
    _aiying_api_max_links: int = 3       # 单剧最多提交分享链接数
    _tg_id: str = ""                     # TG 用户 ID（API 需要；留空则从 TG 会话自动获取并缓存）
    _ay_api_probe_once: bool = False     # 一次性开关：保存配置时测试 AY API 连通性

    # 【SA HTTP 直连转存】（v1.8.0 新增；v1.9.2 起降级为直连 API 的兜底；
    # 默认值已脱敏，请自行填写——用户在 MP 数据库的已存配置不受影响；
    # token/aeskey 为空时整条企微通道自动禁用）
    _sa_http_enabled: bool = True        # SA 企微通道总开关（False 或配置不齐则禁用）
    _sa_http_url: str = "http://127.0.0.1:8095/api/v1/message/"  # SA 消息 API（本地直连）
    _sa_http_token: str = ""             # 企业微信回调 token（msg_signature 签名用）
    _sa_http_aeskey: str = ""            # EncodingAESKey
    _sa_http_corpid: str = ""            # ToUserName/明文尾缀
    _sa_http_userid: str = ""            # FromUserName（SA 不校验来源）
    _sa_http_agentid: str = "1000003"    # AgentID（SA 不校验）

    # 【SA 直连 API】（v1.9.2 新增，最高优先级；三件套齐全才启用，
    # 老配置缺字段时默认值兜底，向后兼容）
    _sa_api_url: str = "http://127.0.0.1:8095"  # SA 服务地址（本地直连）
    _sa_api_user: str = ""               # SA 登录用户名（默认值已脱敏，请自行填写）
    _sa_api_password: str = ""           # SA 登录密码（同上）
    _sa_api_parent_id: str = ""          # 115 离线目标目录 cid（留空=自动取 folders 第一项）

    # 持久化数据的 key
    _DATA_PROCESSED = "processed"        # 已处理（已成功订阅）的剧 {tmdbid: {...}}
    _DATA_HISTORY = "history"            # 运行历史列表（最多保留 200 条）
    _DATA_DAILY = "daily"                # 当日配额计数 {"date": "YYYY-MM-DD", "count": n}
    _DATA_STATS = "stats"                # 累计统计
    _DATA_PENDING = "pending_verify"     # 已订阅未核销的剧 {tmdbid: 快照}
    _DATA_DEAD = "dead_tasks"            # 最近一轮疑似死任务快照（详情页展示用）
    _DATA_PROGRESS = "progress"          # 本轮实时进度快照（详情页进度条 / API 轮询用）
    _DATA_TG_LOGIN = "tg_login"          # TG 登录状态缓存 {logged_in, username, phone, ...}
    _DATA_AIYING = "aiying"              # AY通道状态 {quota_left: 本月剩余次数, updated: 时间}
    _DATA_DONELEDGER = "done_ledger"     # 完结账本（v1.6.0）：{tmdbid(str): {"title", "archived_at"}}

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
                # v1.6.0：完结账本一并清空，下一轮自然回到全量扫描
                self.save_data(self._DATA_DONELEDGER, {})
                self._clear_history = False
                logger.info(f"【{self.plugin_name}】历史记录、已处理清单与完结账本已清空")
                self.__update_config()

            # 处理「AY115通道」TG 登录一次性开关（发验证码/完成登录）
            if self._tg_send_code_once or self._tg_verify_once:
                try:
                    self.__handle_tg_login_actions()
                except Exception as e:
                    logger.error(f"【{self.plugin_name}】TG 登录动作处理失败（不影响主流程）: {e}")

            # 处理「测试 AY 连通性」一次性开关（保存即触发，通知后自动复位）
            if self._ay_api_probe_once:
                try:
                    probe = self.__ay_probe()
                    probe_text = {
                        "ok": "AY 连通性测试：✅ 通过",
                        "401": ("AY 连通性测试：❌ 鉴权失败 401（可能 IP/token 未授权，"
                                "需联系管理员加白名单）"),
                        "disabled": "AY 连通性测试：未启用或配置不完整",
                        "error": f"AY 连通性测试：异常（{probe.get('message') or '未知错误'}）",
                    }.get(probe.get("result"), "AY 连通性测试：异常（未知结果）")
                    logger.info(f"【{self.plugin_name}】{probe_text}")
                    self.post_message(
                        mtype=NotificationType.SiteMessage,
                        title=f"【{self.plugin_name}】AY 连通性测试",
                        text=probe_text,
                    )
                except Exception as e:
                    logger.error(f"【{self.plugin_name}】AY 连通性测试通知失败（不影响主流程）: {e}")
                finally:
                    self._ay_api_probe_once = False
                    self.__update_config()

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

        # 订阅闭环增强（v1.5.0 新增；老配置缺字段时默认值兜底，向后兼容）
        self._search_after_subscribe = bool(config.get("search_after_subscribe", True))
        self._verify_auto_reset = bool(config.get("verify_auto_reset", True))
        self._verify_auto_resubscribe = bool(config.get("verify_auto_resubscribe", False))

        # 增量扫描（v1.6.0 新增；老配置缺字段时默认值兜底，向后兼容）
        self._incremental_scan = bool(config.get("incremental_scan", True))
        # 每周全量扫描日：0=周一 ... 6=周日，非法值收敛到 0-6
        self._full_scan_weekday = min(6, max(0, self.__to_int(
            config.get("full_scan_weekday"), 6)))
        self._full_scan_once = bool(config.get("full_scan_once", False))

        # AY 115 通道（v1.4.0 新增；老配置缺字段时默认值兜底，向后兼容）
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

        # AY HTTP API 通道（v1.7.0 新增；老配置缺字段时默认值兜底，向后兼容）
        self._aiying_api_enabled = bool(config.get("aiying_api_enabled", True))
        # v1.9.2 脱敏：不再内置真实地址/token，缺省为空（空值时 API 通道优雅降级）
        self._aiying_api_url = str(config.get("aiying_api_url") or "").strip()
        self._aiying_api_token = str(config.get("aiying_api_token") or "").strip()
        self._aiying_api_max_links = max(
            1, self.__to_int(config.get("aiying_api_max_links"), 3))
        self._tg_id = str(config.get("tg_id") or "").strip()
        self._ay_api_probe_once = bool(config.get("ay_api_probe_once", False))

        # SA HTTP 企微通道（v1.8.0 新增；v1.9.2 起降级为直连 API 的兜底；
        # 默认值已脱敏，四件套缺省为空——空值时企微通道自动禁用）
        self._sa_http_enabled = bool(config.get("sa_http_enabled", True))
        self._sa_http_url = str(
            config.get("sa_http_url")
            or "http://127.0.0.1:8095/api/v1/message/").strip()
        self._sa_http_token = str(config.get("sa_http_token") or "").strip()
        self._sa_http_aeskey = str(config.get("sa_http_aeskey") or "").strip()
        self._sa_http_corpid = str(config.get("sa_http_corpid") or "").strip()
        self._sa_http_userid = str(config.get("sa_http_userid") or "").strip()
        self._sa_http_agentid = str(
            config.get("sa_http_agentid") or "1000003").strip()

        # SA 直连 API（v1.9.2 新增，最高优先级；三件套齐全才启用）
        self._sa_api_url = str(
            config.get("sa_api_url") or "http://127.0.0.1:8095").strip()
        self._sa_api_user = str(config.get("sa_api_user") or "").strip()
        self._sa_api_password = str(config.get("sa_api_password") or "")
        self._sa_api_parent_id = str(config.get("sa_api_parent_id") or "").strip()

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
            "search_after_subscribe": self._search_after_subscribe,
            "verify_auto_reset": self._verify_auto_reset,
            "verify_auto_resubscribe": self._verify_auto_resubscribe,
            "incremental_scan": self._incremental_scan,
            "full_scan_weekday": self._full_scan_weekday,
            "full_scan_once": self._full_scan_once,
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
            "aiying_api_enabled": self._aiying_api_enabled,
            "aiying_api_url": self._aiying_api_url,
            "aiying_api_token": self._aiying_api_token,
            "aiying_api_max_links": self._aiying_api_max_links,
            "tg_id": self._tg_id,
            "ay_api_probe_once": self._ay_api_probe_once,
            "sa_http_enabled": self._sa_http_enabled,
            "sa_http_url": self._sa_http_url,
            "sa_http_token": self._sa_http_token,
            "sa_http_aeskey": self._sa_http_aeskey,
            "sa_http_corpid": self._sa_http_corpid,
            "sa_http_userid": self._sa_http_userid,
            "sa_http_agentid": self._sa_http_agentid,
            "sa_api_url": self._sa_api_url,
            "sa_api_user": self._sa_api_user,
            "sa_api_password": self._sa_api_password,
            "sa_api_parent_id": self._sa_api_parent_id,
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
        同时优雅关闭AY TG 会话（下次使用自动重建连接）"""
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
          POST /tg_send_code AY115通道：发送 TG 登录验证码（body: {"phone": "+86..."}）
          POST /tg_verify     AY115通道：提交验证码完成登录（body: {"phone", "code", "password"?}）
          GET /tg_status      AY115通道：查询 TG 登录状态
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
                "description": "AY115通道登录第一步：body 传 {\"phone\": \"+86...\"}，"
                               "验证码发到 TG 内「Telegram」官方会话（不是短信）",
            },
            {
                "path": "/tg_verify",
                "endpoint": self.api_tg_verify,
                "methods": ["POST"],
                "auth": "apikey",
                "summary": "提交验证码完成 TG 登录",
                "description": "AY115通道登录第二步：body 传 {\"phone\", \"code\", \"password\"(可空)}；"
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
            {
                "path": "/ay_api_test",
                "endpoint": self.api_ay_api_test,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "AY HTTP API 联调测试",
                "description": "参数 tmdb_id（必填）+ save（0=只查询不提交，默认；1=查询并经 SA 提交第一条链接）"
                               "+ eps（可选，模拟缺集如 1:6-10;2:1-3，返回选包明细与理由）。"
                               "返回 http 状态/资源列表/选中链接/SA 回执/当前额度/tg_id 来源",
            },
            {
                "path": "/sa_http_test",
                "endpoint": self.api_sa_http_test,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "SA 企微通道联调测试",
                "description": "参数 content（默认「获取当前用户 ID」）。用企业微信回调协议向 SA 发一条文本；"
                               "返回「消息处理成功」即协议握手通过，命令执行结果看 SA 的通知渠道",
            },
            {
                "path": "/sa_api_test",
                "endpoint": self.api_sa_api_test,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "SA 直连 API 联调测试",
                "description": "执行登录+取离线目录并返回 {login_ok, folders, parent_id}；"
                               "带 &submit=链接 时真实提交一条（会产生真实离线任务，慎用）",
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
                    # AY API 本月剩余次数（最近一次响应的 times，v1.7.0）
                    "aiying_api_quota": (self.get_data(self._DATA_AIYING) or {})
                    .get("api_quota_left"),
                    # 当前生效的 SA 提交通道（v1.9.2）：api/http/tg/none
                    "sa_channel": ("api" if self.__sa_api_ready()
                                   else "http" if self.__sa_http_ready()
                                   else "tg" if (_TG_LIB_OK and self._sa_bot)
                                   else "none"),
                    "progress": self.get_data(self._DATA_PROGRESS) or {},  # 实时进度快照
                },
            }
        except Exception as e:
            logger.error(f"【{self.plugin_name}】API 查询状态失败: {e}")
            return {"success": False, "message": str(e), "data": None}

    # FastAPI Body 参数（导入失败时退化为普通默认参数，查询字符串也能传）
    _BODY_STR = Body(default="", embed=True) if Body else ""

    def api_tg_send_code(self, phone: str = _BODY_STR) -> Dict[str, Any]:
        """API 端点：AY115通道登录第一步，发送 TG 验证码"""
        try:
            phone = (phone or "").strip() or self._tg_phone
            if not phone:
                return {"success": False,
                        "message": "请提供手机号（body 传 phone，或在配置页填 TG 手机号）",
                        "data": None}
            mgr = self.__get_tg()
            if not mgr:
                return {"success": False,
                        "message": "telethon 未安装，AY通道不可用（PT 订阅不受影响）",
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
        """API 端点：AY115通道登录第二步，提交验证码（可选两步验证密码）"""
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
                        "message": "telethon 未安装，AY通道不可用（PT 订阅不受影响）",
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
                        "message": "telethon 未安装，AY通道不可用（PT 订阅不受影响）",
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

    @staticmethod
    def __parse_eps_param(eps: str) -> Tuple[Set[Tuple[int, int]], Dict[int, List[int]]]:
        """
        解析 /ay_api_test 的 eps 参数（v1.9.0，纯函数可独立测试）。
        格式：`1:6-10;2:1-3` 表示 S01E06-10 和 S02E01-03。
        返回 (缺集集合, {季: [集号,...]})；空/非法片段跳过。
        """
        lack: Set[Tuple[int, int]] = set()
        season_eps: Dict[int, List[int]] = {}
        for seg in (eps or "").split(";"):
            m = re.fullmatch(r"\s*(\d+)\s*:\s*(\d+)\s*(?:-\s*(\d+)\s*)?", seg or "")
            if not m:
                continue
            s, e1 = int(m.group(1)), int(m.group(2))
            e2 = int(m.group(3)) if m.group(3) else e1
            eps_set = set(range(min(e1, e2), max(e1, e2) + 1))
            lack |= {(s, e) for e in eps_set}
            season_eps[s] = sorted(set(season_eps.get(s) or []) | eps_set)
        return lack, season_eps

    def api_ay_api_test(self, tmdb_id: int = 0, save: int = 0,
                        eps: str = "") -> Dict[str, Any]:
        """
        API 端点：AY HTTP API 联调测试（v1.7.0）。
          save=0（默认）：只查 API，返回解析后的资源列表与额度，不提交 SA；
          save=1：查 API + 经 SA 通道提交第一条链接并返回 SA 回执（端到端联调；
            v1.9.4 起走 __sa_submit 三级回退：直连 API → 企微 → TG）；
          eps（可选，v1.9.0）：模拟缺集，格式 `1:6-10;2:1-3`（S01E06-10 与
            S02E01-03），传入时选包逻辑用该缺集跑，返回 covered/uncovered
            明细与选包理由；不传时按 v1.7.0 策略选包，行为不变。
        返回 JSON 含 http 状态/资源数/选中链接/SA 回执/当前额度/tg_id 来源。
        """
        logger.info(f"【{self.plugin_name}】收到AY API 联调测试请求 "
                    f"tmdb_id={tmdb_id} save={save} eps={eps or '-'}")
        try:
            tmdbid = int(tmdb_id)
        except (TypeError, ValueError):
            tmdbid = 0
        if not tmdbid:
            return {"success": False, "message": "参数 tmdb_id 必填（数字）", "data": None}
        # tg_id 来源：配置 / TG 会话（拿不到直接返回，不建额外连接）
        tg_id, tg_src = self.__resolve_tg_id()
        if not tg_id:
            return {"success": False,
                    "message": "无法获取 tg_id：配置未填且 TG 会话不可用",
                    "data": {"tg_id_source": "无"}}
        try:
            res = self.__ay_api_query(tmdbid, tg_id)
        except Exception as e:
            return {"success": False,
                    "message": f"AY API 请求异常: {e}",
                    "data": {"tg_id": tg_id, "tg_id_source": tg_src}}
        resources = res.get("resources") or []
        # v1.9.0：传入 eps 时按模拟缺集跑精确选包，否则退化为 v1.7.0 策略
        mock_lack, mock_season_eps = self.__parse_eps_param(eps)
        if mock_lack:
            picks, pick_reason = self.__ay_pick_links(
                resources, self._aiying_api_max_links, mock_lack, mock_season_eps)
        else:
            picks, pick_reason = self.__ay_pick_links(
                resources, self._aiying_api_max_links)
        data: Dict[str, Any] = {
            "http_status": res.get("http_status"),
            "api_message": res.get("message", ""),
            "resource_count": len(resources),
            "resources": [{
                "name": r.get("name", ""),
                "notes": r.get("notes", ""),
                "size": r.get("size", ""),
                "category": r.get("category", ""),
                "link": r.get("link", ""),
            } for r in resources],
            "selected_links": [p.get("link", "") for p in picks],
            "quota_left": res.get("quota_left"),
            "tg_id": tg_id,
            "tg_id_source": tg_src,
            "sa_result": None,
        }
        # eps 模拟选包明细：逐包覆盖、已覆盖/未覆盖集、选包理由
        if mock_lack:
            covered: Set[Tuple[int, int]] = set()
            for p in picks:
                p_text = f"{p.get('name') or ''} {p.get('notes') or ''}"
                covered |= self.__ay_resource_cover(p_text, mock_season_eps)
            covered &= mock_lack
            data["eps_test"] = {
                "lack": sorted(f"S{s:02d}E{e:02d}" for s, e in mock_lack),
                "pick_reason": pick_reason,
                "covered": sorted(f"S{s:02d}E{e:02d}" for s, e in covered),
                "uncovered": sorted(f"S{s:02d}E{e:02d}"
                                    for s, e in (mock_lack - covered)),
                "picks": [{"name": p.get("name", ""), "notes": p.get("notes", ""),
                           "size": p.get("size", ""), "link": p.get("link", "")}
                          for p in picks],
            }
        if res.get("quota_left") is not None:
            self.__save_api_quota(res["quota_left"])
        # save=1：端到端联调，提交第一条链接到 SA 并返回回执
        if int(save or 0) == 1:
            if not picks:
                data["sa_result"] = {"ok": False, "error": "无可提交的资源链接"}
            else:
                # v1.9.4：与主流程一致，走 __sa_submit 三级回退
                # （直连 API → 企微 → TG），返回形状对齐 mgr.submit 并带 via 通道
                sub = self.__sa_submit(
                    [("API测试", str(picks[0].get("link") or ""))], "联调测试")
                data["sa_result"] = sub
                logger.info(f"【{self.plugin_name}】联调测试 SA 提交结果"
                            f"（via={sub.get('via')}）: {sub}")
        return {"success": True, "message": "OK", "data": data}

    def api_sa_http_test(self, content: str = "") -> Dict[str, Any]:
        """
        API 端点：SA HTTP 直连联调测试（v1.8.0）。
        用 __sa_http_send 按企业微信回调协议向 SA 发送 content
        （默认「获取当前用户 ID」），返回 {http_status, sa_message, ok}。
        注意：返回「消息处理成功」仅表示协议握手通过（SA 收到并开始处理），
        命令的实际执行结果看 SA 自己的通知渠道。
        """
        text = (content or "").strip() or "获取当前用户 ID"
        logger.info(f"【{self.plugin_name}】收到 SA HTTP 联调测试请求: {text[:50]}")
        if not self.__sa_http_ready():
            return {"success": False,
                    "message": "SA HTTP 通道未启用或配置不齐（或 pycryptodome 未安装）",
                    "data": {"http_status": 0, "sa_message": "", "ok": False}}
        ok, msg = self.__sa_http_send(text)
        return {"success": ok,
                "message": msg if ok else f"提交失败: {msg}",
                "data": {
                    "http_status": getattr(self, "_last_sa_http_status", 0),
                    "sa_message": msg,
                    "ok": ok,
                }}

    def api_sa_api_test(self, submit: str = "") -> Dict[str, Any]:
        """
        API 端点：SA 直连 API 联调测试（v1.9.2）。
        执行登录 + 取离线目录，返回 {login_ok, folders, parent_id}；
        带 &submit=链接 时真实提交一条（会产生真实离线任务，慎用）。
        """
        logger.info(f"【{self.plugin_name}】收到 SA 直连联调测试请求"
                    f"{'（含真实提交）' if submit else ''}")
        if not self.__sa_api_ready():
            return {"success": False,
                    "message": "SA 直连三件套（地址/用户名/密码）未配置齐全",
                    "data": None}
        token = self.__sa_api_login()
        login_ok = bool(token)
        folders: List[Any] = []
        parent_id = None
        if login_ok:
            ok, data = self.__sa_api_request(
                "/api/v1/plugin/115/offline/folders", method="get")
            if ok and data:
                folders = data.get("data") or []
            parent_id = self.__sa_api_parent_id()
        result: Dict[str, Any] = {
            "login_ok": login_ok,
            "folders": folders,
            "parent_id": parent_id,
            "submit_result": None,
        }
        # 带 submit 参数：真实提交一条链接（会产生真实离线任务）
        if submit and parent_id:
            r = self.__sa_api_submit([submit], "联调测试")
            result["submit_result"] = r
            logger.info(f"【{self.plugin_name}】联调测试真实提交结果: {r}")
        return {"success": login_ok,
                "message": "" if login_ok else "登录失败（详见日志）",
                "data": result}

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
            self.__verify_pending(history, stats)
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
        ledger_skipped = 0  # 完结账本跳过部数（v1.6.0）
        subscribed = 0     # 本轮新增订阅数
        skipped = 0        # 跳过数（关键词/超限/已订阅/已处理）
        failed = 0         # 识别/请求/订阅失败数
        failed_tmdb = 0    # TMDB 识别/请求失败数
        failed_emby = 0    # Emby 季集读取失败数
        failed_sub = 0     # 订阅失败数
        failed_other = 0   # 单剧其他处理异常数
        subscribed_titles: List[str] = []   # 本轮订阅成功的剧名（通知用）
        timeout_hit = False      # 是否触发扫描超时收尾
        circuit_broken = False   # 是否触发连续失败熔断
        manual_stop = False      # 是否收到手动停止信号（stop_service 置位 _event）

        # 排除关键词列表
        exclude_words = [w.strip() for w in self._exclude_keywords.split(",")
                         if w.strip()] if self._exclude_keywords else []

        # 获取当日剩余配额
        remaining_quota = self.__get_remaining_quota()

        # ---------- 扫描模式判定（v1.6.0 增量扫描）----------
        # 「本轮强制全量」一次性开关：本轮生效后立即回写关闭，仿 _onlyonce 模式
        force_full = self._full_scan_once
        if force_full:
            self._full_scan_once = False
            self.__update_config()
            logger.info(f"【{self.plugin_name}】本轮强制全量扫描已生效"
                        f"（一次性开关已自动关闭）")
        # weekday(): 0=周一 ... 6=周日；到「每周全量扫描日」当天全量兜底重查
        weekly_full = (start_time.weekday() == self._full_scan_weekday)
        incremental = self._incremental_scan and not weekly_full and not force_full
        scan_mode = "incremental" if incremental else "full"
        # 完结账本：{tmdbid(str): {"title": 剧名, "archived_at": 时间字符串}}。
        # 在验证回环之后读取，本轮核销写入的账本立即可用；
        # 账本为空时第一轮自然等于全量，无需特判
        ledger: Dict[str, Any] = self.get_data(self._DATA_DONELEDGER) or {}
        # 待验证清单 key（str(tmdbid)，与 __register_pending 的写入格式一致）：
        # 还在盯入库的剧即使进了账本也不跳过
        pending_keys = set((self.get_data(self._DATA_PENDING) or {}).keys())
        if incremental:
            logger.info(f"【{self.plugin_name}】扫描模式：增量（完结账本 {len(ledger)} 部，"
                        f"账本内已完结且无缺集的剧本轮跳过；每周"
                        f"周{'一二三四五六日'[self._full_scan_weekday]}为全量日）")
        else:
            _full_reason = ("增量扫描未开启" if not self._incremental_scan
                            else "今天为每周全量扫描日" if weekly_full
                            else "本轮强制全量")
            logger.info(f"【{self.plugin_name}】扫描模式：全量（{_full_reason}）")

        # ---------- AY API 鉴权预检（v1.9.5；失败不阻断 PT/TG 兜底）----------
        ay_precheck = self.__ay_probe()
        ay_api_precheck_ok = ay_precheck.get("result") == "ok"
        if ay_precheck.get("result") == "ok":
            logger.info(f"【{self.plugin_name}】AY 鉴权预检通过")
        elif ay_precheck.get("result") == "401":
            logger.warning(f"【{self.plugin_name}】AY 鉴权预检失败：token/IP 授权失效(401)，"
                           f"本轮 AY 通道将不可用，自动回退")
        elif ay_precheck.get("result") == "disabled":
            logger.info(f"【{self.plugin_name}】AY 通道未启用，跳过预检")
        else:
            logger.warning(f"【{self.plugin_name}】AY API 预检异常："
                           f"{ay_precheck.get('message') or '未知错误'}")

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
            "failed_tmdb": 0,                # TMDB 失败部数（v1.9.5）
            "failed_emby": 0,                # Emby 失败部数（v1.9.5）
            "failed_sub": 0,                 # 订阅失败部数（v1.9.5）
            "failed_other": 0,               # 其他失败部数（v1.9.5）
            "sub_done": 0,                   # 订阅阶段已处理候选数
            "aiying": 0,                     # 本轮AY115通道补齐部数（v1.4.0）
            "ledger_skipped": 0,             # 本轮完结账本跳过部数（v1.6.0）
            "scan_mode": scan_mode,          # 本轮扫描模式 incremental/full（v1.6.0）
            "ay_precheck": ay_precheck.get("result", "error"),  # AY API 预检结果（v1.9.5）
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
            if timeout_hit or manual_stop:
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
                if timeout_hit or manual_stop:
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
                    # 【手动停止】v1.5.0 修复死代码：stop_service 置位 _event 后
                    # 此处能感知到，写一条「手动停止」历史记录并优雅收尾
                    if self._event.is_set():
                        if not manual_stop:
                            manual_stop = True
                            logger.warning(f"【{self.plugin_name}】收到手动停止信号，"
                                           f"扫描阶段优雅收尾（已扫 {scanned} 部）")
                            self.__append_history(
                                history, title="（系统）", year="", tmdbid=0,
                                lack_info={}, missing_count=0, result="手动停止",
                                message="收到停止信号，本轮扫描提前结束，已完成部分照常保存")
                        break

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
                    progress["failed_tmdb"] = failed_tmdb
                    progress["failed_emby"] = failed_emby
                    progress["failed_sub"] = failed_sub
                    progress["failed_other"] = failed_other
                    progress["ledger_skipped"] = ledger_skipped
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

                        # ---- 过滤 3.5：增量扫描·完结账本跳过（v1.6.0）----
                        # 账本内 = TMDB 已完结且当时无缺集的剧，集数不会再变，
                        # 本轮直接跳过（不调 Emby episodes / TMDB）；
                        # 但在待验证清单里的剧不跳过（还在盯入库）。
                        # tmdbid 用 Emby 条目自带的 provider ids，不为账本多调接口。
                        # 跳过量逐条只记 DEBUG，避免大库刷屏
                        if (incremental and str(tmdbid) in ledger
                                and str(tmdbid) not in pending_keys):
                            ledger_skipped += 1
                            logger.debug(f"【{title}】已在完结账本中，增量模式跳过")
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
                            failed_emby += 1
                            continue

                        # ---- 3.2 对比 TMDB 得出缺集（同时拿回 mediainfo 供优先级判定） ----
                        lack_info, tmdbinfo = self.__find_lack_episodes(
                            tmdbid=tmdbid, title=title, seasoninfo=seasoninfo)
                        if lack_info is None:
                            # 识别/TMDB 请求失败
                            failed += 1
                            failed_tmdb += 1
                            continue
                        if not lack_info:
                            # 不缺集
                            # v1.6.0：TMDB 已完结（Ended/Canceled）且无缺集 -> 写入完结账本，
                            # 之后增量扫描直接跳过。status 取自 __find_lack_episodes 同一次
                            # 识别返回的 tmdbinfo，不额外请求；调试模式不写账本
                            if (not self._dry_run and str(tmdbid) not in ledger
                                    and (getattr(tmdbinfo, "status", "") or "")
                                    in ("Ended", "Canceled")):
                                ledger[str(tmdbid)] = {
                                    "title": title,
                                    "archived_at": datetime.datetime.now(
                                        tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
                                }
                                logger.info(f"【{title}】已完结且无缺集，写入完结账本")
                            continue

                        # v1.6.0：发现有缺集 -> 若在账本中则移除（任何模式都执行；
                        # 完结剧出新季/用户删集都能自愈，下轮起重新参与扫描）
                        if str(tmdbid) in ledger:
                            del ledger[str(tmdbid)]
                            logger.info(f"【{title}】发现缺集，已从完结账本移除")

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
                        failed_other += 1
                        continue

        logger.info(f"【{self.plugin_name}】扫描完成：共扫描 {scanned} 部剧，"
                    f"发现缺集 {missing_shows} 部，进入候选 {len(candidates)} 部"
                    f"{'（超时提前收尾）' if timeout_hit else ''}")

        # ---------- 3.3 优先级排序 ----------
        candidates = self.__sort_candidates(candidates)

        # ---------- 3.3.1 AY115通道可用性检查（v1.4.0，每轮只查一次 TG 状态） ----------
        aiying_usable = False
        if not self._dry_run:
            try:
                aiying_usable = self.__aiying_ready()
            except Exception as e:
                logger.error(f"【{self.plugin_name}】AY通道检测异常（不影响 PT 订阅）: {e}")
                aiying_usable = False
        if aiying_usable:
            logger.info(f"【{self.plugin_name}】AY115通道已启用，缺集将优先尝试 115 离线")
        aiying_clicks = 0        # 本轮AY累计点击数（熔断用，上限 100）
        aiying_round = 0         # 本轮AY补齐剧数
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
            # 【手动停止】v1.5.0：订阅循环每轮开头也检查停止标志
            if self._event.is_set():
                if not manual_stop:
                    manual_stop = True
                    logger.warning(f"【{self.plugin_name}】收到手动停止信号，"
                                   f"订阅阶段优雅收尾（剩余 {len(candidates) - index} 部留待下轮）")
                    self.__append_history(
                        history, title="（系统）", year="", tmdbid=0, lack_info={},
                        missing_count=0, result="手动停止",
                        message=f"收到停止信号，订阅阶段提前结束，"
                                f"剩余 {len(candidates) - index} 部留待下轮")
                break

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
                    # 调试下不发任何 TG 消息，只记录AY将尝试的集数
                    dry_try = min(cand["missing"], self._aiying_max_eps)
                    logger.info(f"【{title}】[调试] AY将尝试 {dry_try} 集")
                    dry_msg += f"；AY将尝试 {dry_try} 集"
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

            # ---------- AY115通道：MP 订阅之前优先尝试 ----------
            # v1.7.0：查询主通道改为 HTTP API，TG 点按钮流程保留为兜底
            channel = "pt"   # pt / aiying_api / aiying_tg / mixed
            ok, msg = False, ""
            succ_seasons: List[int] = []   # 本轮 PT 订阅成功的季（v1.5.0）
            fail_seasons: List[int] = []   # 本轮 PT 订阅失败的季（v1.5.0）
            sid_map: Dict[int, int] = {}   # 成功季 -> sid（渠道明细用，v1.7.0）
            ay = None
            if aiying_usable:
                # 【风控】单轮AY总点击数熔断：超过 100 次本轮停止使用AY，剩余走 PT
                # （v1.7.0：API 查询与 SA 提交也计入该计数）
                if aiying_clicks >= 100:
                    if not aiying_fuse_logged:
                        aiying_fuse_logged = True
                        logger.warning(f"【{self.plugin_name}】AY本轮点击已达 100 次上限，"
                                       f"触发熔断，剩余候选全部转 PT 订阅")
                        self.__append_history(
                            history, title="（系统）", year="", tmdbid=0, lack_info={},
                            missing_count=0, result="AY熔断",
                            message="本轮AY点击超过 100 次，剩余候选转 PT")
                else:
                    # v1.7.0：先 HTTP API 后 TG——
                    # API 返回 None（请求异常/tg_id 不可得）才回退 TG 点按钮流程；
                    # API 明确「无资源」（status=none）时不再走 TG，直接转 PT
                    if self._aiying_api_enabled and ay_api_precheck_ok:
                        try:
                            ay = self.__aiying_api_fill(cand, 100 - aiying_clicks)
                            if ay:
                                aiying_clicks += ay.get("clicks", 0)
                        except Exception as e:
                            logger.error(f"【{title}】AY API 通道异常（回退 TG 流程）: {e}")
                            ay = None
                    if ay is None:
                        try:
                            ay = self.__aiying_fill(cand, 100 - aiying_clicks)
                            if ay:
                                aiying_clicks += ay.get("clicks", 0)
                        except Exception as e:
                            logger.error(f"【{title}】AY通道异常（静默转 PT 兜底）: {e}")
                            ay = None

            if ay and ay.get("status") == "all":
                # 全部缺集都经 115 拿到：不再调 __subscribe_show
                ok, channel = True, ay.get("channel", "aiying_tg")
                aiying_round += 1
                ay_detail = ay.get("detail") or f"已提交 {ay['got']} 集到 115 离线"
                msg = f"[AY115] {ay_detail}"
                if ay.get("quota_left") is not None:
                    msg += f"（本月剩余次数 {ay['quota_left']}）"
            elif ay and ay.get("status") == "partial":
                # 部分集拿到：拿不到的集仍走 MP 订阅（MP 会自己比对只补缺集）
                ok_pt, succ_seasons, fail_seasons, msg_pt, sid_map = \
                    self.__subscribe_show(
                        title, cand["year"], cand["tmdbid"], cand["lack_info"])
                # v1.5.0 修复：mixed 分支的成败以 PT 订阅的实际结果为准，
                # AY部分成功不再抵消 PT 失败（此前无条件 ok=True，PT 失败也销账，
                # 失败的季永不再补）
                ok, channel = ok_pt, "mixed"
                aiying_round += 1
                ay_detail = ay.get("detail") or f"{ay['got']} 集已提交 115"
                msg = (f"[AY115] {ay_detail}；"
                       f"剩余 {cand['missing'] - ay['got']} 集转 PT：{msg_pt}")
                if not ok_pt:
                    logger.warning(f"【{title}】AY已补 {ay['got']} 集，"
                                   f"剩余集 PT 订阅未全部成功: {msg_pt}")
            else:
                # AY完全没资源/超时/异常：静默落到 MP 订阅（PT 兜底）
                prefix = ""
                if aiying_usable and aiying_clicks < 100:
                    prefix = ("AY无资源，转 PT：" if ay is not None
                              else "AY通道异常，转 PT：")
                # 逐季添加订阅（MP 订阅后自己会比对媒体库只补缺集）
                ok, succ_seasons, fail_seasons, msg_pt, sid_map = \
                    self.__subscribe_show(
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
                # 通知里区分来源渠道（v1.7.0：前缀扩为 [PT]/[115·API]/[115·TG]/[混合]，115 附资源摘要）
                channel_tag = {"aiying_api": "[115·API]", "aiying_tg": "[115·TG]",
                               "aiying": "[115·TG]", "mixed": "[混合]"}.get(
                    channel, "[PT]")
                brief = ""
                if ay and channel in ("aiying_api", "aiying_tg", "aiying", "mixed"):
                    brief = (ay.get("notes_brief") or "")[:40]
                subscribed_titles.append(
                    f"{channel_tag} {title}（缺 {cand['missing']} 集）"
                    + (f"｜{brief}" if brief else ""))
                # v1.7.0：组织渠道人话明细，写进历史记录
                if channel == "pt":
                    channel_detail = self.__pt_detail(sid_map)
                else:
                    channel_detail = (ay or {}).get("detail") or ""
                    if channel == "mixed":
                        _pt_part = self.__pt_detail(sid_map)
                        if _pt_part:
                            channel_detail = (
                                (channel_detail + "；") if channel_detail else ""
                            ) + f"PT {_pt_part}"
                # 标记已处理，下一轮不再重复
                # v1.5.0：能走到这里说明该剧所有缺集季都订阅成功（或纯AY补齐），
                # 部分季失败的剧走 else 分支，不会写 processed，下轮重新评估
                processed[str(cand["tmdbid"])] = {
                    "title": title,
                    "time": datetime.datetime.now(
                        tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
                    "seasons": sorted(cand["lack_info"].keys()),
                }
                # 【验证回环】登记"已订阅未核销"快照，之后每轮复查入库情况
                # v1.9.0：传入本轮订阅成功的季（纯 115 补齐时 sid_map 为空，
                # None 表示全部缺集季）
                self.__register_pending(
                    cand, channel=channel,
                    seasons_done=sorted(sid_map.keys()) if sid_map else None)
                self.__append_history(
                    history, title=title, year=cand["year"], tmdbid=cand["tmdbid"],
                    lack_info=cand["lack_info"], missing_count=cand["missing"],
                    result="已订阅", message=msg,
                    channel=channel, channel_detail=channel_detail)
            else:
                failed += 1
                failed_sub += 1
                consecutive_failures += 1
                progress["failed"] = failed
                progress["failed_tmdb"] = failed_tmdb
                progress["failed_emby"] = failed_emby
                progress["failed_sub"] = failed_sub
                progress["failed_other"] = failed_other
                progress["sub_done"] = index + 1
                progress["percent"] = self.__calc_percent(progress)
                self.__save_progress(progress)
                # v1.5.0：区分「全部订阅失败」与「部分季失败」——部分失败的剧
                # 不写 processed，下轮重新评估；已成功的季下轮会被
                # 「该季已有订阅则跳过」逻辑挡住，不会重复订（见 __subscribe_show 注释）
                if succ_seasons:
                    # v1.9.0：部分季失败时，成功季也纳入验证回环（此前这些季的
                    # 订阅永远不被复查）；下轮剩余季成功后会合并进同一快照
                    cand_partial = dict(cand)
                    cand_partial["lack_info"] = {
                        s: cand["lack_info"][s] for s in succ_seasons
                        if s in cand["lack_info"]}
                    cand_partial["missing"] = sum(
                        len(v) for v in cand_partial["lack_info"].values())
                    if cand_partial["lack_info"]:
                        self.__register_pending(
                            cand_partial, channel=channel,
                            seasons_done=sorted(succ_seasons))
                    self.__append_history(
                        history, title=title, year=cand["year"], tmdbid=cand["tmdbid"],
                        lack_info=cand["lack_info"], missing_count=cand["missing"],
                        result="部分季订阅失败",
                        message=f"{msg}；该剧未销账，失败季 {fail_seasons} 下轮重试",
                        channel=channel,
                        channel_detail=(ay or {}).get("detail", ""))
                else:
                    self.__append_history(
                        history, title=title, year=cand["year"], tmdbid=cand["tmdbid"],
                        lack_info=cand["lack_info"], missing_count=cand["missing"],
                        result="订阅失败", message=msg,
                        channel=channel,
                        channel_detail=(ay or {}).get("detail", ""))

                # v1.6.0：订阅未全部成功（含部分季失败）需要下轮重试的剧，
                # 若在完结账本中则移除，保证下轮重新评估
                if str(cand["tmdbid"]) in ledger:
                    del ledger[str(cand["tmdbid"])]
                    logger.info(f"【{title}】订阅未全部成功，已从完结账本移除，"
                                f"下轮重新评估")

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
        stats["total_failed_tmdb"] = stats.get("total_failed_tmdb", 0) + failed_tmdb
        stats["total_failed_emby"] = stats.get("total_failed_emby", 0) + failed_emby
        stats["total_failed_sub"] = stats.get("total_failed_sub", 0) + failed_sub
        stats["total_failed_other"] = stats.get("total_failed_other", 0) + failed_other
        stats["total_aiying"] = stats.get("total_aiying", 0) + aiying_round  # 累计AY补齐
        stats["last_aiying"] = aiying_round                                  # 本轮AY补齐
        stats["last_run"] = start_time.strftime(TIME_FMT)

        self.save_data(self._DATA_PROCESSED, processed)
        self.save_data(self._DATA_HISTORY, history[-200:])  # 最多保留 200 条
        self.save_data(self._DATA_STATS, stats)
        # v1.6.0：完结账本随轮次落盘（写入/移除都在本轮内存副本上操作）
        self.save_data(self._DATA_DONELEDGER, ledger)

        elapsed = (datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                   - start_time).total_seconds()

        # 进度收尾：标记完成，页面进度条显示本轮最终结果
        progress.update({
            "running": False, "phase": "done", "phase_label": "本轮已完成",
            "scanned": scanned, "missing": missing_shows,
            "candidates": len(candidates), "subscribed": subscribed,
            "skipped": skipped, "failed": failed,
            "failed_tmdb": failed_tmdb, "failed_emby": failed_emby,
            "failed_sub": failed_sub, "failed_other": failed_other,
            "aiying": aiying_round,
            "ledger_skipped": ledger_skipped,
            "scan_mode": scan_mode,
            "current": "", "quota_left": remaining_quota,
            "percent": 100,
            "finished_at": datetime.datetime.now(
                tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
            "elapsed_sec": int(elapsed),
        })
        self.__save_progress(progress)

        logger.info(f"【{self.plugin_name}】===== 本轮结束，耗时 {elapsed:.0f} 秒："
                    f"新增订阅 {subscribed} 部，跳过 {skipped} 部，失败 {failed} 部，"
                    f"账本跳过 {ledger_skipped} 部"
                    f"（{'增量' if scan_mode == 'incremental' else '全量'}模式）"
                    f"{'，已熔断' if circuit_broken else ''}"
                    f"{'，超时收尾' if timeout_hit else ''}"
                    f"{'，手动停止' if manual_stop else ''} =====")

        # ---------- 5. 汇总通知 ----------
        if self._notify:
            self.__send_summary(scanned=scanned, missing_shows=missing_shows,
                                subscribed=subscribed, skipped=skipped, failed=failed,
                                subscribed_titles=subscribed_titles,
                                elapsed=elapsed,
                                circuit_broken=circuit_broken,
                                timeout_hit=timeout_hit,
                                scan_mode=scan_mode,
                                ledger_skipped=ledger_skipped)

    def __is_timeout(self, start_time: datetime.datetime) -> bool:
        """单轮任务是否已超过配置的超时分钟数"""
        elapsed = (datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                   - start_time).total_seconds()
        return elapsed > self._scan_timeout * 60

    # ==================================================================
    # 下载验证回环 0：订阅后入库验证
    # ==================================================================
    def __register_pending(self, cand: Dict[str, Any], channel: str = "pt",
                           seasons_done: Optional[List[int]] = None):
        """
        订阅成功后登记快照：之后每轮复查 Emby 是否真入库。
        channel：补齐渠道（pt=纯 PT 订阅 / aiying_api/aiying_tg=纯115 / mixed=混合）。
        v1.9.0：支持按成功季登记与合并——
          ①部分季订阅成功时也把成功季纳入快照（此前只有整部全部成功才登记，
            部分成功季的订阅永远不被复查）；
          ②下轮重评该剧剩余季成功后，合并进既有快照（seasons/remaining 取并集
            更新，同一部剧在待验证列表里只占一条，不新建不覆盖）；
          ③新增 done_seasons 字段记录已订阅成功的季；旧快照缺该字段时
            按全集缺集处理（核销逻辑不变：remaining 清零才核销）。
        seasons_done：本次订阅成功的季号列表；None 表示 cand 里全部缺集季。
        """
        pending: Dict[str, Any] = self.get_data(self._DATA_PENDING) or {}
        key = str(cand["tmdbid"])
        new_seasons = {str(s): list(eps) for s, eps in cand["lack_info"].items()}
        new_done = sorted(seasons_done if seasons_done is not None
                          else cand["lack_info"].keys())

        existing = pending.get(key)
        if existing:
            # ---- 合并进既有快照：缺集/剩余缺集取并集，保留最早订阅时间 ----
            merged_seasons = dict(existing.get("seasons") or {})
            merged_remaining = dict(existing.get("remaining") or {})
            for s, eps in new_seasons.items():
                merged_seasons[s] = sorted(set(merged_seasons.get(s, [])) | set(eps))
                merged_remaining[s] = sorted(
                    set(merged_remaining.get(s, [])) | set(eps))
            existing["seasons"] = merged_seasons
            existing["remaining"] = merged_remaining
            existing["done_seasons"] = sorted(
                set(existing.get("done_seasons") or []) | set(new_done))
            if channel:
                existing["channel"] = channel
            pending[key] = existing
            self.save_data(self._DATA_PENDING, pending)
            logger.info(f"【{cand['title']}】验证快照已合并：累计纳入 "
                        f"{len(merged_seasons)} 季，剩余缺集 "
                        f"{sum(len(v) for v in merged_remaining.values())} 集")
            return

        # ---- 新登记快照：tmdbid、剧名、年份、缺集列表、订阅时间、Emby 定位、渠道 ----
        pending[key] = {
            "title": cand["title"],
            "year": cand["year"],
            "tmdbid": cand["tmdbid"],
            "server": cand["server"],        # Emby 服务器名（复查时直接定位）
            "item_id": cand["item_id"],      # Emby 剧集 ID（复查时直接定位）
            "seasons": new_seasons,
            "remaining": {str(s): list(eps) for s, eps in new_seasons.items()},
            "done_seasons": new_done,        # 已订阅成功的季（v1.9.0）
            "channel": channel,              # 补齐渠道（v1.4.0）
            "subscribe_time": datetime.datetime.now(
                tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
            "alerted": False,                # 是否已发过"超时未补齐"告警（只告警一次）
        }
        self.save_data(self._DATA_PENDING, pending)
        logger.info(f"【{cand['title']}】已登记入库验证快照，"
                    f"缺集 {cand['missing']} 集，之后每轮复查")

    def __verify_pending(self, history: List[Dict[str, Any]],
                         stats: Dict[str, Any]):
        """
        复查所有「已订阅未核销」的剧：
          全部补齐 -> 核销 + 统计 +（可选）✅ 通知
          部分补齐 -> 更新剩余缺集，继续等
          超过 verify_alert_days 未补齐 -> 告警通知（只一次），历史标记「超时未补齐」，
            并按配置执行超时闭环动作（自动重置订阅 / 订阅丢失补订，v1.5.0）
        单部复查失败跳过该部，不中断整轮复查。
        """
        pending: Dict[str, Any] = self.get_data(self._DATA_PENDING) or {}
        if not pending:
            logger.debug(f"【{self.plugin_name}】无待验证的订阅，跳过入库复查")
            return

        # v1.5.0 修复：验证回环使用独立的计时起点与超时预算（复用「单轮超时保护」
        # 配置值，即验证最多再花同样时长），不再与扫描阶段共用同一个 start_time——
        # 此前 pending 积压时复查会吃光扫描阶段的 60 分钟预算，扫描开头即被判超时
        start_time = datetime.datetime.now(tz=pytz.timezone(settings.TZ))

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
                    message=f"订阅后第 {wait_days} 天确认全部入库",
                    channel=entry.get("channel", ""))
                # v1.4.0：115 渠道补齐的剧，退订本插件此前添加的 PT 订阅，避免重复下载
                try:
                    self.__unsub_pt_if_115(entry, title, history)
                except Exception as e:
                    logger.error(f"【{title}】退订 PT 订阅检查失败（不影响核销）: {e}")
                # v1.6.0：核销时若 TMDB 已完结（Ended/Canceled），写入完结账本，
                # 之后增量扫描直接跳过。核销事件量小，这里额外一次
                # recognize_media 换 status 可接受；调试模式不写账本
                if not self._dry_run:
                    try:
                        _tid = int(entry.get("tmdbid") or 0)
                        _mi = (self._mediaChain.recognize_media(
                            mtype=MediaType.TV, tmdbid=_tid)) if _tid else None
                        if _mi and (getattr(_mi, "status", "") or "") in ("Ended", "Canceled"):
                            ledger = self.get_data(self._DATA_DONELEDGER) or {}
                            if str(_tid) not in ledger:
                                ledger[str(_tid)] = {
                                    "title": title,
                                    "archived_at": datetime.datetime.now(
                                        tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
                                }
                                self.save_data(self._DATA_DONELEDGER, ledger)
                                logger.info(f"【{title}】核销且已完结，写入完结账本")
                    except Exception as e:
                        logger.error(f"【{title}】写入完结账本失败（不影响核销）: {e}")
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
                    message=f"订阅 {wait_days} 天未入库，请人工检查资源",
                    channel=entry.get("channel", ""))
                # v1.5.0：超时闭环动作——订阅还在则自动重置（默认开），
                # 订阅丢失则按配置决定是否补订（默认关），仅在首次告警这一轮执行一次
                try:
                    self.__handle_timeout_subscription(entry, title, history)
                except Exception as e:
                    logger.error(f"【{title}】超时订阅闭环处理失败（不影响告警）: {e}")
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

    def __handle_timeout_subscription(self, entry: Dict[str, Any], title: str,
                                      history: List[Dict[str, Any]]):
        """
        超时未入库时的闭环动作（v1.5.0 新增，仅在首次告警那一轮执行一次）：
          1. 「超时自动重置订阅」（默认开）：MP 里该订阅仍存在时，对它做重置
             （note 清空、lack_episode 重置为 total_episode），让 MP 下轮周期搜索
             重新搜整季——相当于自动帮用户点了「重新搜索」；
          2. 「订阅丢失自动补订」（默认关）：订阅已不存在时，按快照剩余缺集重新走
             __subscribe_show 补订。订阅消失多半是用户手动退订，所以默认关，
             尊重用户操作。
        两个动作都只处理 username=本插件名 的订阅（list_by_username 过滤），
        绝不动用户手动建的订阅；调试模式下都只记录日志不执行。
        """
        try:
            tmdbid = int(entry.get("tmdbid") or 0)
        except (TypeError, ValueError):
            return
        if not tmdbid:
            return
        # 快照里登记过的缺集季
        seasons: List[int] = []
        for s in (entry.get("seasons") or {}).keys():
            try:
                seasons.append(int(s))
            except (TypeError, ValueError):
                continue
        if not seasons:
            return

        # 查出本插件为该剧添加的订阅（username=插件名，用户手动订阅不在其中）
        try:
            my_subs = [sub for sub in
                       (self._subOper.list_by_username(self.plugin_name) or [])
                       if getattr(sub, "tmdbid", None) == tmdbid]
        except Exception as e:
            logger.error(f"【{title}】查询本插件订阅失败，跳过超时闭环处理: {e}")
            return
        sub_by_season = {getattr(sub, "season", None): sub for sub in my_subs}

        # ---- 动作 1：订阅还在 -> 重置 ----
        reset_seasons: List[int] = []
        missing_seasons: List[int] = []
        for season in seasons:
            sub = sub_by_season.get(season)
            if sub is None:
                missing_seasons.append(season)
                continue
            if not self._verify_auto_reset:
                continue
            total_ep = getattr(sub, "total_episode", 0) or 0
            if self._dry_run:
                logger.info(f"【{title}】[调试] 第 {season} 季订阅超时，将重置订阅 "
                            f"(sid={sub.id})：note 清空、lack_episode 重置为 {total_ep}")
                reset_seasons.append(season)
                continue
            try:
                payload: Dict[str, Any] = {"note": ""}
                if total_ep > 0:
                    payload["lack_episode"] = total_ep
                self._subOper.update(sub.id, payload)
                reset_seasons.append(season)
                logger.info(f"【{title}】第 {season} 季订阅超时未入库，已重置订阅 "
                            f"(sid={sub.id})，MP 下轮周期搜索将重新搜索整季")
            except Exception as e:
                logger.error(f"【{title}】第 {season} 季重置订阅失败: {e}")

        if reset_seasons:
            self.__append_history(
                history, title=title, year=str(entry.get("year", "")),
                tmdbid=tmdbid, lack_info={}, missing_count=0,
                result="超时重置订阅",
                message=f"已重置第 {reset_seasons} 季订阅，MP 下轮将重新搜索"
                        + ("（调试模式仅记录）" if self._dry_run else ""))

        # ---- 动作 2：订阅已不存在 -> 按剩余缺集补订（默认关） ----
        if not missing_seasons:
            return
        if not self._verify_auto_resubscribe:
            logger.info(f"【{title}】第 {missing_seasons} 季订阅已不存在"
                        f"（可能是手动退订），「订阅丢失自动补订」未开启，不补订")
            return
        # 只补仍有剩余缺集的季
        remaining = entry.get("remaining") or {}
        lack_info: Dict[int, List[int]] = {}
        for season in missing_seasons:
            eps = remaining.get(str(season)) or []
            if eps:
                lack_info[season] = list(eps)
        if not lack_info:
            logger.info(f"【{title}】丢失的订阅季已无剩余缺集，无需补订")
            return
        if self._dry_run:
            logger.info(f"【{title}】[调试] 订阅丢失，将按剩余缺集补订季: "
                        f"{sorted(lack_info.keys())}")
            return
        ok, succ, fail, msg, _sids = self.__subscribe_show(
            title, str(entry.get("year", "")), tmdbid, lack_info)
        logger.info(f"【{title}】订阅丢失自动补订结果: {msg}")
        self.__append_history(
            history, title=title, year=str(entry.get("year", "")),
            tmdbid=tmdbid, lack_info=lack_info,
            missing_count=sum(len(v) for v in lack_info.values()),
            result="超时补订" if ok else "超时补订失败",
            message=f"原订阅已丢失，按剩余缺集重新订阅：{msg}")

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
    ) -> Tuple[bool, List[int], List[int], str, Dict[int, int]]:
        """
        对一部剧的缺集季逐季添加 MP 订阅。
        v1.5.0 起返回结构化结果；v1.7.0 追加第五个返回值 sid_map：
        (是否全部成功, 成功季列表, 失败季列表, 汇总消息, {季号: sid})。
        只有所有缺集季都订阅成功才算成功，调用方才把该剧写入已处理清单；
        部分失败的剧不进已处理清单，下轮重新评估——已成功的季会被
        __find_lack_episodes 里「该季已有订阅则跳过」（subOper.exists 判断）挡住，
        且 SubscribeChain.add(exist_ok=True) 对已存在订阅也是复用而不报错，
        双重保障下轮不会重复订阅同一季。
        （MP 订阅后自行比对媒体库只下载缺集。）
        """
        success_seasons: List[int] = []
        failed_seasons: List[int] = []
        sid_map: Dict[int, int] = {}   # 订阅成功的季 -> sid（渠道明细记录用，v1.7.0）
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
                    sid_map[season] = sid
                    logger.info(f"【{title}】第 {season} 季订阅添加成功 (sid={sid})")
                    # v1.5.0：订阅成功后立即触发 MP 搜索该订阅（可选，默认开）
                    self.__trigger_subscribe_search(sid, title, season)
                else:
                    failed_seasons.append(season)
                    logger.warning(f"【{title}】第 {season} 季订阅添加失败: {msg}")
            except Exception as e:
                last_msg = str(e)
                failed_seasons.append(season)
                logger.error(f"【{title}】第 {season} 季订阅异常: {e}")

        if success_seasons and not failed_seasons:
            return (True, success_seasons, failed_seasons,
                    f"已订阅季: {success_seasons}", sid_map)
        if success_seasons:
            # 部分季成功：不算成功（调用方不销账），消息里带失败季号便于排查
            return (False, success_seasons, failed_seasons,
                    f"部分季订阅成功 {success_seasons}，失败季 {failed_seasons}，"
                    f"最后错误: {last_msg}", sid_map)
        return (False, success_seasons, failed_seasons,
                f"所有缺集季订阅失败，最后错误: {last_msg}", sid_map)

    def __trigger_subscribe_search(self, sid: int, title: str, season: int):
        """
        订阅添加成功后，立即触发 MP 对该订阅做一次搜索（v1.5.0 新增，
        配置项「订阅后立即搜索」，默认开）。
        解决「订阅了但半天没动静」：MP 周期搜索每个订阅之间随机休眠 60~300 秒，
        订阅多时新订阅要等很久才轮到；而 Scheduler().start("subscribe_search",
        sid=非空, state=None, manual=True) 指定单个订阅时不会随机休眠，直接搜这一部。
        失败只记日志，不影响订阅主流程；调试模式下不触发。
        """
        if not self._search_after_subscribe:
            return
        if self._dry_run:
            return
        # v1.8.1：异步触发——Scheduler().start 是同步执行一整次订阅搜索
        # （识别+站点查询+匹配，实测单部 1~13 分钟），直接在扫描循环里调用
        # 会把主流程卡死；改为 daemon 线程后台执行，扫描继续往下走。
        threading.Thread(target=self.__do_trigger_search,
                         args=(sid, title, season), daemon=True).start()
        # 同一部剧多季连续触发时每次至少间隔 1 秒，避免瞬间打爆搜索
        time.sleep(1)

    def __do_trigger_search(self, sid: int, title: str, season: int):
        """后台线程里真正执行的即时搜索触发（v1.8.1 拆出）"""
        # v1.9.0：触发前先探 Emby 就绪——Emby 重启/未就绪时 MP 的 media_exists
        # 查询失败会误判「整季缺失」拖整季大包（实测大明风华 131G 事故）；
        # 未就绪则跳过本次触发（订阅已建，MP 周期搜索会兜底），不影响主流程
        try:
            emby_ok = self.__emby_ready()
        except Exception:
            emby_ok = False   # 探测自身异常按「未就绪」处理
        if not emby_ok:
            logger.info(f"【{title}】Emby 未就绪，跳过第 {season} 季即时搜索触发 "
                        f"(sid={sid})，由 MP 周期搜索兜底")
            return
        try:
            # 惰性导入：与 MP 官方插件习惯一致，且极端环境下导入失败不影响插件加载
            from app.scheduler import Scheduler
            Scheduler().start("subscribe_search", sid=sid, state=None, manual=True)
            logger.info(f"【{title}】第 {season} 季已触发即时搜索 (sid={sid})")
        except Exception as e:
            logger.error(f"【{title}】第 {season} 季触发即时搜索失败"
                         f"（订阅本身已成功，不影响）: {e}")

    def __emby_ready(self) -> bool:
        """
        探测 Emby 是否就绪（v1.9.0）：GET {host}emby/System/Info/Public
        （公开接口无需鉴权），超时 5 秒；任一已配置媒体服务器就绪即算就绪。
        结果做 5 分钟 TTL 缓存（一轮内不重复探测；探测失败不写入缓存，
        下条触发前重探一次）。所有异常按「未就绪」处理。
        """
        now = time.time()
        cache = self._emby_ready_cache or {}
        if cache.get("ok") and now - float(cache.get("ts", 0)) < 300:
            return True
        try:
            services = self._msHelper.get_services() or {}
        except Exception as e:
            logger.info(f"【{self.plugin_name}】Emby 就绪探测：读取媒体服务器配置失败"
                        f"（按未就绪处理）: {e}")
            return False
        for name, conf in services.items():
            try:
                host = getattr(conf, "host", None)
                if not host and isinstance(conf, dict):
                    host = conf.get("host")
                host = str(host or "").strip()
                if not host:
                    continue
                resp = requests.get(host.rstrip("/") + "/emby/System/Info/Public",
                                    timeout=5)
                if resp.status_code == 200:
                    self._emby_ready_cache = {"ok": True, "ts": now}
                    logger.debug(f"【{self.plugin_name}】Emby 就绪探测通过: {name}")
                    return True
                logger.info(f"【{self.plugin_name}】Emby {name} 探测返回 "
                            f"HTTP {resp.status_code}（按未就绪处理）")
            except Exception as e:
                logger.info(f"【{self.plugin_name}】Emby {name} 就绪探测异常"
                            f"（按未就绪处理）: {e}")
        # 失败不缓存：下一条触发前会重探
        return False

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

    @staticmethod
    def __pt_detail(sid_map: Dict[int, int]) -> str:
        """PT 订阅的人话明细（v1.7.0）：订阅 S02/S03（sid=469,470）"""
        if not sid_map:
            return ""
        seasons = "/".join(f"S{s:02d}" for s in sorted(sid_map))
        sids = ",".join(str(sid_map[s]) for s in sorted(sid_map))
        return f"订阅 {seasons}（sid={sids}）"

    def __append_history(self, history: List[Dict[str, Any]], title: str, year: str,
                         tmdbid: int, lack_info: Dict[int, List[int]],
                         missing_count: int, result: str, message: str,
                         channel: str = "", channel_detail: str = ""):
        """追加一条历史记录并立即落盘（v1.3.0：原来整轮结束才保存，
        扫描中途打开详情页看不到任何记录，现在每产生一条就能在页面看到）。
        v1.7.0 新增 channel / channel_detail 两个可选字段（pt/aiying_api/aiying_tg/
        mixed + 人话明细），老记录没有这两个字段，读取侧一律 .get() 兜底，向后兼容"""
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
            "channel": channel or "",
            "channel_detail": channel_detail or "",
        })
        try:
            self.save_data(self._DATA_HISTORY, history[-200:])
        except Exception as e:
            logger.debug(f"【{self.plugin_name}】保存历史记录失败（不影响主流程）: {e}")

    def __send_summary(self, scanned: int, missing_shows: int, subscribed: int,
                       skipped: int, failed: int,
                       subscribed_titles: List[str], elapsed: float,
                       circuit_broken: bool, timeout_hit: bool,
                       scan_mode: str = "full", ledger_skipped: int = 0):
        """每轮结束发送汇总通知"""
        try:
            mode = "调试模式（未真正订阅）" if self._dry_run else "正式订阅"
            lines = [
                f"模式：{mode}",
                # v1.6.0：带上扫描模式与账本跳过数，一眼看出本轮是不是增量
                f"扫描：{'增量' if scan_mode == 'incremental' else '全量'}模式，"
                f"扫描剧集 {scanned} 部，增量跳过 {ledger_skipped} 部，"
                f"发现缺集 {missing_shows} 部",
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
    # AY 115 通道（v1.4.0 新增）：TG 会话 / 登录 / 缺集补齐 / PT 退订
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
        AY通道是否可用（每轮只查一次）。
        v1.8.0 起 TG 从必需降级为可选：TG 已登录时全功能（API/TG 查询 + TG 回执）；
        TG 不可用但 SA HTTP 通道齐全且开了 API 通道时，
        「API 查询 → HTTP 转存」链路仍可用（TG 点按钮查询兜底本轮不可用）。
        两边都不满足才返回 False，调用方静默落 PT 兜底。
        """
        if not self._aiying_enabled:
            return False
        # ---- TG 侧状态（可选，能登录则全功能）----
        tg_logged_in = False
        if not _TG_LIB_OK:
            logger.warning(f"【{self.plugin_name}】telethon 未安装，TG 侧功能不可用")
        elif not self._sa_bot:
            logger.warning(f"【{self.plugin_name}】未配置 SA 转存机器人，TG 提交不可用")
        else:
            mgr = self.__get_tg()
            if mgr:
                st = mgr.status()
                if st.get("logged_in"):
                    self.__persist_tg_login(st)
                    tg_logged_in = True
                else:
                    logger.warning(f"【{self.plugin_name}】TG 未登录"
                                   f"（{st.get('error') or '会话失效'}）")
                    self.__persist_tg_login({"logged_in": False})
        if tg_logged_in:
            return True
        # ---- v1.8.0：TG 不可用时，SA HTTP + AY API 双齐全仍可用 ----
        if self.__sa_http_ready() and self._aiying_api_enabled:
            logger.info(f"【{self.plugin_name}】TG 不可用，但 SA HTTP 通道已配置："
                        f"本轮AY走「API 查询 → HTTP 转存」（TG 兜底查询不可用）")
            return True
        logger.warning(f"【{self.plugin_name}】AY通道不可用：TG 未登录且 "
                       f"SA HTTP 通道未配置齐全（PT 订阅不受影响）")
        return False

    def __aiying_fill(self, cand: Dict[str, Any],
                      click_budget_left: int) -> Optional[Dict[str, Any]]:
        """
        AY115通道尝试补齐一部剧（同步方法，内部经 TG 管理器提交协程）。
        返回 None 表示通道异常（调用方静默落 PT）；否则返回：
          {"status": "all"/"partial"/"none", "got": 成功集数, "clicks": 点击数,
           "quota_left": AY本月剩余次数, "sa_failed": [失败集标签]}
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
            logger.info(f"【{title}】缺集 {len(lack_eps)} 集超过AY单剧上限 "
                        f"{self._aiying_max_eps}，仅尝试前 {self._aiying_max_eps} 集，"
                        f"剩余转 PT")
            lack_try = set(sorted(lack_eps)[:self._aiying_max_eps])
        else:
            lack_try = lack_eps

        # 搜索关键词：有年份发「剧名 年份」，否则发 tmdbid（机器人支持 tmdbid）
        keyword = f"{title} {cand['year']}".strip() if cand.get("year") else str(tmdbid)
        logger.info(f"【{title}】AY通道：向 @{self._aiying_bot} 查询「{keyword}」，"
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
            logger.info(f"【{title}】AY查询失败：{res.get('error')}（转 PT）")
            return {"status": "none", "got": 0, "clicks": int(res.get("clicks", 0)),
                    "quota_left": None, "sa_failed": [],
                    "channel": "aiying_tg", "detail": "", "notes_brief": ""}

        # 持久化AY本月剩余次数（详情页展示）
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
            logger.info(f"【{title}】AY无本剧缺集资源（转 PT）")
            return {"status": "none", "got": 0, "clicks": int(res.get("clicks", 0)),
                    "quota_left": res.get("quota_left"), "sa_failed": [],
                    "channel": "aiying_tg", "detail": "", "notes_brief": ""}

        # 把拿到的 ed2k/115 链接逐条发给 SA 转存机器人
        items = [(f"S{s:02d}E{e:02d}", url) for (s, e), url in sorted(links.items())]
        logger.info(f"【{title}】AY拿到 {len(items)} 集链接，逐条转发给 "
                    f"@{self._sa_bot} 离线到 115")
        # v1.8.0：提交走包装层（HTTP 直连优先，失败回退 TG）
        sub = self.__sa_submit(items, title)
        sa_via = sub.get("via", "tg")
        if not sub.get("ok"):
            logger.error(f"【{title}】SA 转存提交异常：{sub.get('error')}（已拿到的集按失败处理，转 PT）")
            return {"status": "none", "got": 0, "clicks": int(res.get("clicks", 0)),
                    "quota_left": res.get("quota_left"), "sa_failed": [],
                    "channel": "aiying_tg", "detail": "", "notes_brief": ""}

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
        logger.info(f"【{title}】AY通道结果：{status}（成功 {got} / 缺集 "
                    f"{len(lack_eps)} 集，点击 {res.get('clicks', 0)} 次）")
        # v1.7.0：TG 流程也产出人话明细（渠道详情记录），粒度保留并补充集数
        # v1.9.2：标明 SA 走的哪条路（API/企微提交无回执，措辞用「已提交」）
        sa_tag = {"api": "SA(API)已提交", "http": "SA(企微)已提交"}.get(
            sa_via, "SA(TG)转存")
        tg_detail = f"TG 点按钮拿到 {len(items)} 集链接，{sa_tag} {got}/{len(items)} 集"
        if sa_failed:
            tg_detail += f"，失败 {len(sa_failed)} 集"
        return {"status": status, "got": got, "clicks": int(res.get("clicks", 0)),
                "quota_left": res.get("quota_left"), "sa_failed": sa_failed,
                "channel": "aiying_tg", "detail": tg_detail,
                "notes_brief": f"TG 拿到 {len(items)} 集链接"}

    # ==================================================================
    # AY HTTP API 通道（v1.7.0 新增，主通道；TG 点按钮流程保留为兜底）
    # ==================================================================
    def __resolve_tg_id(self) -> Tuple[Optional[str], str]:
        """
        解析AY API 需要的 tg_id：优先读配置 tg_id；没有则从 TG 会话
        get_me 自动获取并回写配置缓存（复用 TG 管理器现有连接，不单独建连）；
        再失败返回 (None, "")，调用方回退 TG 流程。
        返回 (tg_id, 来源)，来源为 配置/会话（联调测试 API 展示用）。
        """
        if self._tg_id:
            return self._tg_id, "配置"
        mgr = self.__get_tg()
        if not mgr:
            return None, ""
        res = mgr.my_id()
        if res.get("ok") and res.get("tg_id"):
            self._tg_id = str(res["tg_id"])
            try:
                # 缓存回写，之后直接用配置值，不再每次调 get_me
                self.__update_config()
            except Exception:
                pass
            logger.info(f"【{self.plugin_name}】已从 TG 会话获取 tg_id={self._tg_id}"
                        f" 并缓存到配置")
            return self._tg_id, "会话"
        logger.warning(f"【{self.plugin_name}】获取 tg_id 失败: {res.get('error')}")
        return None, ""

    def __ay_probe(self) -> Dict[str, Any]:
        """探测 AY API 连通性与鉴权状态；任何异常都转换为结果字典。"""
        start = time.monotonic()
        try:
            if not (self._aiying_enabled and self._aiying_api_enabled
                    and self._aiying_api_url and self._aiying_api_token):
                return {"result": "disabled", "status": None,
                        "message": "AY API 未启用或配置不完整", "elapsed": 0.0}
            tg_id, _ = self.__resolve_tg_id()
            token = self._aiying_api_token or ""
            mask = token[:4] + "****" if len(token) > 4 else "****"
            target = urlparse(self._aiying_api_url).hostname or self._aiying_api_url
            logger.info(f"AY API 预检请求已发出 → 目标 {target}，tmdb_id=1")
            resp = requests.post(
                self._aiying_api_url,
                json={"tg_id": str(tg_id or "0"), "type": "tv",
                      "tmdb_id": "1", "token": self._aiying_api_token},
                timeout=10)
            elapsed = time.monotonic() - start
            logger.info(f"AY API 预检响应 ← 状态码 {resp.status_code}，"
                        f"耗时 {elapsed:.2f}s，token={mask}")
            if resp.status_code == 200:
                return {"result": "ok", "status": 200,
                        "message": "连接与鉴权正常", "elapsed": elapsed}
            if resp.status_code == 401:
                return {"result": "401", "status": 401,
                        "message": "token/IP 授权失效", "elapsed": elapsed}
            return {"result": "error", "status": resp.status_code,
                    "message": f"HTTP {resp.status_code}", "elapsed": elapsed}
        except Exception as e:
            elapsed = time.monotonic() - start
            logger.warning(f"AY API 预检异常：{e}，耗时 {elapsed:.2f}s")
            return {"result": "error", "status": None,
                    "message": str(e), "elapsed": elapsed}

    def __ay_api_query(self, tmdbid: int, tg_id: str) -> Dict[str, Any]:
        """
        调AY HTTP API 查询某部剧的资源（v1.7.0，协议已实测）。
        POST {tg_id, type: "tv", tmdb_id, token}，超时 15 秒；
        走 MP 全局代理（requests 默认读环境变量代理，与插件现有 HTTP 调用一致）。
        返回 {ok, resources, quota_left, http_status, message}；
        网络/超时/HTTP 错/JSON 解析错一律抛异常，由调用方决定回退 TG 流程。
        """
        # v1.9.2 脱敏：地址缺省为空，守卫防止空 URL 请求（联调测试直接调用时）
        if not self._aiying_api_url:
            raise ValueError("AY API 地址未配置")
        start = time.monotonic()
        target = urlparse(self._aiying_api_url).hostname or self._aiying_api_url
        logger.info(f"AY API 请求已发出 → 目标 {target}，tmdb_id={tmdbid}")
        resp = requests.post(
            self._aiying_api_url,
            json={"tg_id": str(tg_id), "type": "tv",
                  "tmdb_id": str(tmdbid), "token": self._aiying_api_token},
            timeout=15)
        elapsed = time.monotonic() - start
        token = self._aiying_api_token or ""
        mask = token[:4] + "****" if len(token) > 4 else "****"
        logger.info(f"AY API 响应 ← 状态码 {resp.status_code}，"
                    f"耗时 {elapsed:.2f}s，token={mask}")
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data")
        # 有资源时 data 是列表；无资源时是 {}（且无资源时返回字段拼写为 mesage）
        resources = ([r for r in data if isinstance(r, dict)]
                     if isinstance(data, list) else [])
        return {
            "ok": True,
            "resources": resources,
            "quota_left": payload.get("times"),   # 当月剩余额度，每查一次 -1
            "http_status": resp.status_code,
            "message": payload.get("message") or payload.get("mesage") or "",
        }

    @staticmethod
    def __ay_resource_cover(text: str,
                            season_eps: Dict[int, List[int]]) -> Set[Tuple[int, int]]:
        """
        解析单条资源的 name/notes 文本，计算它覆盖的 (季, 集) 集合（v1.9.0，
        纯函数可独立测试）。规则：
          S01E06-S01E10 / S1E6-E10 集范围 → 展开该范围（跨季时中间季取整季）；
          S01E101 单集 → 单集；
          S1-S5 / S01-S05 季范围 → 范围内各季全部集（按 season_eps 展开）；
          S01 / 第1季 单季 → 该季全部集；
          全集/全xx集/Complete → 全剧（season_eps 全部）；
          无任何标识 → 视为全剧覆盖（最大溢出，选包时吃亏）。
        season_eps 拿不到总集数的季展开为空（溢出会被低估，属可接受的近似）。
        """
        covered: Set[Tuple[int, int]] = set()

        def _whole() -> Set[Tuple[int, int]]:
            return {(s, e) for s, eps in season_eps.items() for e in eps}

        # 整剧：全集 / 全xx集 / Complete
        if re.search(r"全集|全\s*\d+\s*集|complete", text, re.I):
            return _whole()

        # 集范围 S01E06-S01E10 / S1E6-E10（先从文本摘出，避免被单集正则重复计）
        def _ep_range_sub(m) -> str:
            s1, e1 = int(m.group(1)), int(m.group(2))
            s2 = int(m.group(3)) if m.group(3) else s1
            e2 = int(m.group(4))
            if (s2, e2) < (s1, e1):
                s1, e1, s2, e2 = s2, e2, s1, e1
            for s in range(s1, s2 + 1):
                lo = e1 if s == s1 else 1
                hi = e2 if s == s2 else max(season_eps.get(s) or [e2])
                for e in range(lo, hi + 1):
                    covered.add((s, e))
            return " "

        text = re.sub(
            r"S(\d{1,2})E(\d{1,3})\s*[-~–]\s*(?:S(\d{1,2}))?E(\d{1,3})",
            _ep_range_sub, text, flags=re.I)

        # 单集 S01E101
        for m in re.finditer(r"S(\d{1,2})E(\d{1,3})", text, re.I):
            covered.add((int(m.group(1)), int(m.group(2))))

        # 季范围 S1-S5 / S01-S05（后面不带 E）
        for m in re.finditer(r"S(\d{1,2})\s*[-~–]\s*S?(\d{1,2})(?!\d*E)", text, re.I):
            s1, s2 = sorted((int(m.group(1)), int(m.group(2))))
            for s in range(s1, s2 + 1):
                covered |= {(s, e) for e in season_eps.get(s) or []}

        # 单季 S01 / 第1季（后面不带集号、不是范围起点）
        for m in re.finditer(
                r"S(\d{1,2})(?!\d*\s*[-~–E])|第\s*(\d{1,2})\s*季", text, re.I):
            s = int(m.group(1) or m.group(2))
            covered |= {(s, e) for e in season_eps.get(s) or []}

        # 无任何标识：视为全剧覆盖（最大溢出）
        if not covered:
            covered = _whole()
        return covered

    @staticmethod
    def __ay_pick_links(
        resources: List[Dict[str, Any]], max_links: int,
        lack_eps: Optional[Set[Tuple[int, int]]] = None,
        season_eps: Optional[Dict[int, List[int]]] = None,
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        从 API 返回的资源里挑选要提交的分享链接。
        v1.9.0 改为「按缺集精确选包」的贪心集合覆盖（缺 2 集不再拖回整季大包）：
          1. 每条资源用 __ay_resource_cover 算出覆盖集集合；
          2. 与缺集求交：有效覆盖 = covered ∩ lack；完全无交集的资源直接淘汰
             （对这部剧是无关包）；
          3. 贪心：每轮选「新增有效覆盖最大」的包；平局依次按
             ① 115 分享链接优先（http(s):// 开头 > ed2k://，实测 ed2k 离线
                失败率高、分享链接转存更稳，且「已转存过」兜底友好）
             ② 溢出更小（溢出 = covered − lack；溢不出未知时 size 小者优先）
             ③ size 小者优先；
          4. 数量上限 max_links；选满/覆盖完即停，剩余缺集由调用方转 PT。
        返回 (选中资源列表, 选包理由字符串)。
        兼容 v1.7.0 调用：lack_eps/season_eps 为空时退化为
        「整季整剧标识优先 + size 降序」，理由字符串为空。
        注意：ed2k 打包链接一条 link 字段里可能含多行 ed2k（一行一个文件），
        按资源条目整体算覆盖，不拆。
        """
        def _size_gb(r: Dict[str, Any]) -> float:
            try:
                return float(r.get("size") or 0)
            except (TypeError, ValueError):
                return 0.0

        # ---- 兼容旧调用：无缺集上下文时沿用 v1.7.0 策略 ----
        if not lack_eps:
            def _is_whole(r: Dict[str, Any]) -> bool:
                text = f"{r.get('notes') or ''} {r.get('name') or ''}"
                return bool(re.search(
                    r"全集|全\s*\d+\s*集|complete|S\d{1,2}\s*[-~–]\s*S?\d{1,2}|S\d{2}\b",
                    text, re.I))

            whole = sorted((r for r in resources if _is_whole(r)),
                           key=_size_gb, reverse=True)
            rest = sorted((r for r in resources if not _is_whole(r)),
                          key=_size_gb, reverse=True)
            return (whole + rest)[:max(1, max_links)], ""

        season_eps = season_eps or {}
        # ---- 1/2. 逐条算覆盖、求交、淘汰无关包 ----
        pool: List[Dict[str, Any]] = []
        for r in resources:
            text = f"{r.get('name') or ''} {r.get('notes') or ''}"
            covered = LackEpisodeAutoSub.__ay_resource_cover(text, season_eps)
            valid = covered & lack_eps
            if not valid:
                continue    # 完全无交集：无关包，直接淘汰
            pool.append({
                "res": r, "covered": covered, "valid": valid,
                "overflow": covered - lack_eps,
                "is_115": 0 if str(r.get("link") or "").lower().startswith("http") else 1,
                "size": _size_gb(r),
            })

        # ---- 3/4. 贪心集合覆盖 ----
        picks: List[Dict[str, Any]] = []
        pick_labels: List[str] = []
        covered_so_far: Set[Tuple[int, int]] = set()
        remaining = list(pool)
        while remaining and len(picks) < max(1, max_links) \
                and covered_so_far < lack_eps:
            # 每轮选「新增有效覆盖最大」；平局 ①115 优先 ②溢出小 ③size 小
            best = max(
                remaining,
                key=lambda p: (len(p["valid"] - covered_so_far),
                               -p["is_115"],
                               -len(p["overflow"]),
                               -p["size"]))
            new_cover = best["valid"] - covered_so_far
            if not new_cover:
                break    # 剩下的包都不带来新增覆盖，停
            picks.append(best["res"])
            covered_so_far |= new_cover
            # 选包标签：取文本里第一个集数范围/标识，取不到用 name 截断
            r_text = f"{best['res'].get('name') or ''} {best['res'].get('notes') or ''}"
            lm = re.search(
                r"S\d{1,2}E\d{1,3}\s*[-~–]\s*(?:S\d{1,2})?E\d{1,3}"
                r"|S\d{1,2}\s*[-~–]\s*S?\d{1,2}|S\d{1,2}E\d{1,3}"
                r"|全集|全\s*\d+\s*集|S\d{2}", r_text, re.I)
            pick_labels.append(lm.group(0) if lm else r_text.strip()[:16])
            remaining.remove(best)

        covered_n = len(covered_so_far & lack_eps)
        reason = (f"按缺集选包：缺 {len(lack_eps)} 集，"
                  f"选 {'+'.join(pick_labels) or '无'}，"
                  f"预计覆盖 {covered_n}/{len(lack_eps)}")
        return picks, reason

    def __save_api_quota(self, quota_left: Any):
        """持久化AY API 本月剩余次数（详情页统计卡展示用，与 TG 额度分开存）"""
        try:
            data = self.get_data(self._DATA_AIYING) or {}
            data["api_quota_left"] = quota_left
            data["api_updated"] = datetime.datetime.now(
                tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT)
            self.save_data(self._DATA_AIYING, data)
        except Exception:
            pass

    def __aiying_api_fill(self, cand: Dict[str, Any],
                          click_budget_left: int) -> Optional[Dict[str, Any]]:
        """
        AY HTTP API 通道尝试补齐一部剧（v1.7.0 主通道，同步方法）。
        返回 None 表示「API 请求异常 / tg_id 不可得」，调用方回退 TG 点按钮流程；
        否则返回 {"status": all/partial/none, "got", "clicks", "quota_left",
                  "sa_failed", "channel": "aiying_api", "detail", "notes_brief"}。
        clicks 计入口径：API 查询 1 次 + SA 提交每条 1 次（计入单轮AY熔断）。
        【风控】单剧缺集 >100 已在候选过滤阶段（max_missing）被跳过，与现有约定一致。
        """
        title = cand["title"]
        tmdbid = int(cand["tmdbid"])
        # v1.9.2 脱敏：地址/token 缺省为空——优雅降级回退 TG，不抛异常
        if not self._aiying_api_url or not self._aiying_api_token:
            logger.debug(f"【{title}】AY API 地址/token 未配置，回退 TG 流程")
            return None
        # tg_id：优先配置，其次 TG 会话自动获取；拿不到回退 TG 流程
        tg_id, _tg_src = self.__resolve_tg_id()
        if not tg_id:
            logger.warning(f"【{title}】AY API：无法获取 tg_id（配置与 TG 会话均无），"
                           f"回退 TG 流程")
            return None

        # ---- 1. 查询 API（异常回退 TG；明确无资源则直接转 PT，不再走 TG） ----
        try:
            res = self.__ay_api_query(tmdbid, tg_id)
        except Exception as e:
            logger.error(f"【{title}】AY API 请求异常（回退 TG 流程）: {e}")
            return None
        clicks = 1  # API 查询计 1 次
        if res.get("quota_left") is not None:
            self.__save_api_quota(res["quota_left"])
        resources = res.get("resources") or []
        none_result = {"status": "none", "got": 0, "clicks": clicks,
                       "quota_left": res.get("quota_left"), "sa_failed": [],
                       "channel": "aiying_api", "detail": "", "notes_brief": ""}
        if not resources:
            logger.info(f"【{title}】AY API 无本剧资源"
                        f"（{res.get('message') or '未查询到数据'}，转 PT，不再走 TG）")
            return none_result

        # ---- 2. 选包：按缺集精确选包（v1.9.0 贪心集合覆盖 + 115 链接优先）----
        lack_eps = {(int(s), int(e))
                    for s, eps in cand["lack_info"].items() for e in eps}
        # 展开「季范围/整剧」覆盖所需的季集信息：缺集季 + 资源文本里引用到的季，
        # 逐季取 TMDB 已播出集号（失败记空列表，溢出会被低估，属可接受近似；
        # 仅 API 查到资源的剧才会走到这里，调用量可控）
        season_eps: Dict[int, List[int]] = {}
        try:
            ref_seasons = {s for s, _ in lack_eps}
            for r in resources:
                r_text = f"{r.get('name') or ''} {r.get('notes') or ''}"
                ref_seasons |= {int(x) for x in
                                re.findall(r"S(\d{1,2})", r_text, re.I)}
            today = datetime.datetime.now(tz=pytz.timezone(settings.TZ)).date()
            for s in sorted(x for x in ref_seasons if 0 < x <= 40):
                season_eps[s] = self.__get_aired_episodes(tmdbid, s, title, today)
        except Exception as e:
            logger.error(f"【{title}】获取季集信息用于选包失败（按无总集数近似）: {e}")
        picks, pick_reason = self.__ay_pick_links(
            resources, self._aiying_api_max_links, lack_eps, season_eps)
        logger.info(f"【{title}】AY API 查到 {len(resources)} 条资源，"
                    f"{pick_reason}，选中 {len(picks)} 条提交 SA 离线")

        # ---- 3. 经 SA 通道提交（v1.8.0：HTTP 直连优先，失败回退 TG）----
        items = [(f"链接{i + 1}", str(r.get("link") or ""))
                 for i, r in enumerate(picks) if r.get("link")]
        if not items:
            logger.warning(f"【{title}】AY API 资源均无分享链接（转 PT）")
            return none_result
        sub = self.__sa_submit(items, title)
        sa_via = sub.get("via", "tg")
        clicks += len(items)  # SA 提交每条计 1 次
        none_result["clicks"] = clicks
        if not sub.get("ok"):
            logger.error(f"【{title}】SA 转存提交异常：{sub.get('error')}"
                         f"（按失败处理，转 PT）")
            return none_result
        results = sub.get("results") or {}
        ok_picks = [r for i, r in enumerate(picks) if r.get("link")
                    and (results.get(f"链接{i + 1}") or {}).get("ok")]
        fail_msgs = [(label, (r or {}).get("msg", ""))
                     for label, r in results.items() if not (r or {}).get("ok")]
        for label, fmsg in fail_msgs:
            logger.warning(f"【{title}】{label} SA 转存失败: {fmsg}")

        # ---- 4. 集数覆盖（v1.9.0：用选包同款解析精确计算成功包的覆盖；
        # 真实补齐仍以验证回环复查 Emby 入库为准） ----
        ok_covered: Set[Tuple[int, int]] = set()
        for r in ok_picks:
            r_text = f"{r.get('name') or ''} {r.get('notes') or ''}"
            ok_covered |= self.__ay_resource_cover(r_text, season_eps)
        cover = len(ok_covered & lack_eps)
        cover_text = f"预计覆盖 {cover}/{len(lack_eps)} 集"
        logger.info(f"【{title}】AY API 提交结果：{len(ok_picks)}/{len(items)} 条成功，"
                    f"{cover_text}")

        # ---- 5. 人话明细（渠道详情记录，写历史与汇总通知用） ----
        ok_n = len(ok_picks)
        # v1.9.2：明细里标明 SA 走了哪条路（API/企微提交无回执，措辞用「已提交」）
        sa_tag = {"api": "SA(API)已提交", "http": "SA(企微)已提交"}.get(
            sa_via, "SA(TG)已提交")
        if ok_picks:
            first = ok_picks[0]
            first_desc = (first.get("notes") or first.get("name") or "")[:60]
            detail = f"《{title}》{first_desc}（{first.get('size') or '?'}G）→ {sa_tag}"
            if ok_n < len(items):
                first_fail = fail_msgs[0][1] if fail_msgs else "未知原因"
                detail = (f"{ok_n}/{len(items)} 条成功，"
                          f"失败：{first_fail[:40]}；" + detail)
            # v1.9.0：选包理由（含预计覆盖）写入渠道明细
            detail += f"；{pick_reason}"
            notes_brief = (first.get("notes") or first.get("name") or "")[:40]
        else:
            first_fail = fail_msgs[0][1] if fail_msgs else "未知原因"
            detail = f"0/{len(items)} 条成功，失败：{first_fail[:60]}"
            notes_brief = ""

        # 状态判定：全部链接成功且精确覆盖全部缺集才敢记 all（不再走 PT）；
        # 有任何失败或未全覆盖一律 partial（PT 兜底，MP 只补缺集不会重复下载）
        if ok_n == len(items) and ok_picks and cover >= len(lack_eps):
            status, got = "all", len(lack_eps)
        elif ok_n > 0:
            status, got = "partial", cover
        else:
            status, got = "none", 0
        return {"status": status, "got": got, "clicks": clicks,
                "quota_left": res.get("quota_left"),
                "sa_failed": [label for label, _ in fail_msgs],
                "channel": "aiying_api", "detail": detail,
                "notes_brief": notes_brief}

    # ==================================================================
    # SA HTTP 直连转存（v1.8.0 新增）：企业微信回调协议（WXBizMsgCrypt）
    # ==================================================================
    def __sa_http_ready(self) -> bool:
        """SA HTTP 通道是否可用：总开关开 + pycryptodome 可用 + 关键配置齐全"""
        return bool(self._sa_http_enabled and _CRYPTO_OK
                    and self._sa_http_url and self._sa_http_token
                    and self._sa_http_aeskey and self._sa_http_corpid)

    def __sa_http_send(self, content: str) -> Tuple[bool, str]:
        """
        按企业微信回调协议向 SA 发送一条文本消息（v1.8.0，协议已实测跑通）。
        SA 的消息 API 唯一被处理的形态就是这种加密回调——明文 POST（form/json/
        纯文本）会被 SA 静默丢弃：返回 200 但 message 是「消息已接收」，只收不办。
        成功判据：HTTP 200 且 JSON message == 「消息处理成功」。
        返回 (ok, message)；网络/超时/非 200/「消息已接收」/JSON 解析异常都算失败。
        """
        if not _CRYPTO_OK:
            return False, "pycryptodome 未安装"
        try:
            ts = str(int(time.time()))
            nonce = "".join(random.choices("0123456789", k=10))
            # AES-256-CBC：key = base64decode(EncodingAESKey + "=")（43 字符补一个
            # "=" 成 32 字节），IV = key 前 16 字节
            key = base64.b64decode(self._sa_http_aeskey + "=")
            iv = key[:16]
            corpid_b = self._sa_http_corpid.encode("utf-8")
            msg_id = random.randint(10 ** 9, 10 ** 10 - 1)
            inner_xml = (
                f"<xml><ToUserName><![CDATA[{self._sa_http_corpid}]]></ToUserName>"
                f"<FromUserName><![CDATA[{self._sa_http_userid}]]></FromUserName>"
                f"<CreateTime>{ts}</CreateTime>"
                f"<MsgType><![CDATA[text]]></MsgType>"
                f"<Content><![CDATA[{content}]]></Content>"
                f"<MsgId>{msg_id}</MsgId>"
                f"<AgentID><![CDATA[{self._sa_http_agentid}]]></AgentID></xml>")
            inner_b = inner_xml.encode("utf-8")
            # 待加密明文 = random(16) + pack(">I", len) + inner_xml + corpid，PKCS7 补到 32 倍数
            plain = (os.urandom(16) + struct.pack(">I", len(inner_b))
                     + inner_b + corpid_b)
            pad_len = 32 - (len(plain) % 32)
            plain += bytes([pad_len]) * pad_len
            enc_b64 = base64.b64encode(
                _AES.new(key, _AES.MODE_CBC, iv).encrypt(plain)).decode("utf-8")
            # msg_signature = sha1(sort(token, timestamp, nonce, enc) 拼接)
            sig = hashlib.sha1("".join(sorted(
                [self._sa_http_token, ts, nonce, enc_b64])).encode("utf-8")).hexdigest()
            body_xml = (
                f"<xml><ToUserName><![CDATA[{self._sa_http_corpid}]]></ToUserName>"
                f"<Encrypt><![CDATA[{enc_b64}]]></Encrypt>"
                f"<AgentID><![CDATA[{self._sa_http_agentid}]]></AgentID></xml>")
            # 注意：URL query 里的 token=symedia 是 SA 路由标识（固定值），
            # 与签名用的 sa_http_token 是两回事
            resp = requests.post(
                self._sa_http_url,
                params={"token": "symedia", "msg_signature": sig,
                        "timestamp": ts, "nonce": nonce},
                data=body_xml.encode("utf-8"),
                headers={"Content-Type": "text/xml"},
                timeout=15)
            self._last_sa_http_status = resp.status_code  # 联调测试 API 展示用
            if resp.status_code != 200:
                return False, f"HTTP {resp.status_code}: {resp.text[:100]}"
            payload = resp.json()
            sa_msg = str(payload.get("message") or "")
            if payload.get("success") and sa_msg == "消息处理成功":
                return True, sa_msg
            # 「消息已接收」= 未被处理（明文/校验失败时 SA 也返回 200 但只收不办）
            return False, (sa_msg or f"未知返回: {resp.text[:100]}")
        except Exception as e:
            self._last_sa_http_status = 0
            return False, f"{type(e).__name__}: {e}"

    def __sa_http_submit(self, items: List[Tuple[str, str]],
                         title: str) -> Dict[str, Any]:
        """
        经 SA HTTP 通道提交一批链接（v1.8.0）。
        粒度与现有 TG 提交一致：多条 ed2k 合并为一条文本，115 分享链接逐条发送。
        返回 {"ok", "sent", "failed", "detail"}；任何一条失败整体 ok=False
        （调用方整体回退 TG 重提，SA 对重复任务会回「任务已存在」，无副作用）。
        不等离线结果（SA 异步处理）：统一视为「已提交待验证」，
        由入库验证回环兜底确认——这是设计意图。
        """
        links = [u for _, u in items if u]
        ed2k = [u for u in links if u.startswith("ed2k://")]
        others = [u for u in links if not u.startswith("ed2k://")]
        messages = others + (["\n".join(ed2k)] if ed2k else [])
        sent, failed, fail_msg = 0, 0, ""
        for m in messages:
            ok, rmsg = self.__sa_http_send(m)
            if ok:
                sent += 1
                logger.info(f"【{title}】SA(HTTP) 已提交 {len(m)} 字节（含 "
                            f"{m.count('://')} 个链接）")
            else:
                failed += 1
                fail_msg = rmsg
                logger.warning(f"【{title}】SA(HTTP) 提交失败: {rmsg}")
            time.sleep(1)   # 温和间隔，避免瞬间连发
        ok_all = failed == 0 and sent > 0
        detail = f"SA(HTTP)已提交 {sent} 条"
        if failed:
            detail += f"，失败 {failed} 条（{fail_msg[:60]}）"
        return {"ok": ok_all, "sent": sent, "failed": failed, "detail": detail}

    # ==================================================================
    # SA 直连 API（v1.9.2 新增，最高优先级）：JWT 登录 + 115 离线提交
    # ==================================================================
    def __sa_api_ready(self) -> bool:
        """SA 直连 API 是否可用：地址 + 用户名 + 密码三件套齐全"""
        return bool(self._sa_api_url and self._sa_api_user
                    and self._sa_api_password)

    def __sa_api_login(self, force: bool = False) -> Optional[str]:
        """
        登录 SA 取 JWT（v1.9.2，协议已实测）。
        POST {url}/api/v1/login/access-token，form 表单 username/password，
        返回 {"access_token": ...}，JWT 约 30 天有效。
        缓存到 plugindata（key sa_api_jwt，含 fetched_at），<25 天复用；
        force=True 强制重登（401 重试用）。失败返回 None（只记日志）。
        """
        try:
            if not force:
                cached = self.get_data("sa_api_jwt") or {}
                if cached.get("token") and cached.get("fetched_at"):
                    try:
                        fetched = datetime.datetime.strptime(
                            cached["fetched_at"], TIME_FMT)
                        age_days = (datetime.datetime.now() - fetched).days
                        if age_days < 25:
                            return cached["token"]
                    except (TypeError, ValueError):
                        pass
            resp = requests.post(
                self._sa_api_url.rstrip("/") + "/api/v1/login/access-token",
                data={"username": self._sa_api_user,
                      "password": self._sa_api_password},
                timeout=15)
            if resp.status_code != 200:
                logger.error(f"【{self.plugin_name}】SA 直连登录失败 "
                             f"HTTP {resp.status_code}")
                return None
            token = (resp.json() or {}).get("access_token")
            if not token:
                logger.error(f"【{self.plugin_name}】SA 直连登录返回无 access_token")
                return None
            self.save_data("sa_api_jwt", {
                "token": token,
                "fetched_at": datetime.datetime.now(
                    tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
            })
            logger.info(f"【{self.plugin_name}】SA 直连登录成功，"
                        f"JWT 已缓存（约 30 天有效）")
            return token
        except Exception as e:
            logger.error(f"【{self.plugin_name}】SA 直连登录异常: {e}")
            return None

    def __sa_api_request(self, path: str, json_body: Optional[dict] = None,
                         method: str = "post") -> Tuple[bool, Optional[dict]]:
        """
        带 JWT 调 SA API（v1.9.2）。
        注意：?token=/?apikey=/X-API-KEY 全部 401，必须用登录 JWT（已实测）。
        401 时强制重登一次并重试一次；网络异常/非 200 返回 (False, None)。
        """
        for attempt in range(2):
            token = self.__sa_api_login(force=(attempt == 1))
            if not token:
                return False, None
            try:
                resp = requests.request(
                    method, self._sa_api_url.rstrip("/") + path,
                    headers={"Authorization": f"Bearer {token}"},
                    json=json_body, timeout=30)
                if resp.status_code == 401 and attempt == 0:
                    logger.warning(f"【{self.plugin_name}】SA JWT 已失效（401），"
                                   f"强制重登并重试一次")
                    continue
                if resp.status_code != 200:
                    logger.error(f"【{self.plugin_name}】SA API {path} 返回 "
                                 f"HTTP {resp.status_code}: {resp.text[:100]}")
                    return False, None
                return True, resp.json()
            except Exception as e:
                logger.error(f"【{self.plugin_name}】SA API {path} 请求异常: {e}")
                return False, None
        return False, None

    def __sa_api_parent_id(self) -> Optional[str]:
        """
        解析 115 离线目标目录 cid（v1.9.2）：配置优先；留空则取 folders
        第一项并缓存到 plugindata（目录很少变，避免每次提交都查）。
        """
        if self._sa_api_parent_id:
            return self._sa_api_parent_id
        cached = self.get_data("sa_api_parent") or {}
        if cached.get("cid"):
            return cached["cid"]
        ok, data = self.__sa_api_request(
            "/api/v1/plugin/115/offline/folders", method="get")
        if ok and data:
            folders = data.get("data") or []
            if folders and isinstance(folders[0], (list, tuple)) \
                    and len(folders[0]) >= 2:
                cid = str(folders[0][1])
                try:
                    self.save_data("sa_api_parent",
                                   {"cid": cid, "name": str(folders[0][0])})
                except Exception:
                    pass
                logger.info(f"【{self.plugin_name}】SA 离线目标目录自动选用："
                            f"{folders[0][0]}（cid={cid}）")
                return cid
        logger.error(f"【{self.plugin_name}】SA 离线目标目录获取失败")
        return None

    def __sa_api_submit(self, links: List[str], title: str) -> Dict[str, Any]:
        """
        经 SA 直连 API 提交离线下载（v1.9.2，协议已实测）。
        POST /api/v1/plugin/115/offline，magnets 一次提交全部链接
        （ed2k 与 115 分享链接同接口都支持）。
        message 判定：含「任务已存在」/「已经转存过」→ 算成功（内容已在库）；
        含「离线成功」/「转存成功」→ 成功；其他（含「失败」字样）→ 失败。
        message 是汇总的：汇总成功则全部 ok；判定失败整体 not ok
        （调用方回退企微通道重提，SA 对已有任务返回「任务已存在」无副作用）。
        返回形状与 __sa_http_submit 对齐并附 via="api"。
        """
        parent_id = self.__sa_api_parent_id()
        if not parent_id:
            return {"ok": False, "sent": 0, "failed": len(links), "via": "api",
                    "detail": "SA(API) 目标目录获取失败"}
        ok, resp = self.__sa_api_request(
            "/api/v1/plugin/115/offline",
            {"magnets": links, "parent_id": parent_id})
        if not ok or resp is None:
            return {"ok": False, "sent": 0, "failed": len(links), "via": "api",
                    "detail": "SA(API) 提交请求失败（网络/鉴权）"}
        msg = str(resp.get("message") or "")
        # v1.9.2 修正：「任务已存在/已经转存过」优先于「失败」字样判定
        # （SA 的回执形如「115离线下载失败：任务已存在」，实为内容已在库的成功态）
        _already = ("任务已存在" in msg) or ("已经转存过" in msg)
        success = bool(resp.get("success")) and (
            _already
            or ("失败" not in msg and (
                ("离线成功" in msg) or ("转存成功" in msg))))
        if success:
            return {"ok": True, "sent": len(links), "failed": 0, "via": "api",
                    "detail": f"SA(API)已提交 {len(links)} 条（{msg[:40]}）"}
        return {"ok": False, "sent": 0, "failed": len(links), "via": "api",
                "detail": f"SA(API) 提交失败：{msg[:60] or '无响应 message'}"}

    def __sa_submit(self, items: List[Tuple[str, str]],
                    title: str) -> Dict[str, Any]:
        """
        SA 提交包装层：三级回退（v1.9.2）。
          ① sa_api 三件套齐全 → 直连 API（最高优先级）；
          ② 失败/未配 → 企微回调通道（v1.8.0）；
          ③ 再失败/未配 → TG 机器人提交（v1.4.0）。
        返回形状与 mgr.submit 对齐：{"ok", "results": {label: {ok, msg}}, "via"}，
        via = api / http / tg，调用方据此在人话明细里区分
        SA(API)/SA(企微)/SA(TG)。
        """
        # ---- ① 直连 API ----
        if self.__sa_api_ready():
            links = [u for _, u in items if u]
            r = self.__sa_api_submit(links, title)
            if r["ok"]:
                logger.info(f"【{title}】{r['detail']}（不等离线结果，"
                            f"交由入库验证回环确认）")
                return {"ok": True, "via": "api",
                        "results": {label: {"ok": True, "msg": "SA(API)已提交"}
                                    for label, _ in items}}
            logger.warning(f"【{title}】{r['detail']}，回退企微通道")
        # ---- ② 企微回调通道 ----
        if self.__sa_http_ready():
            r = self.__sa_http_submit(items, title)
            if r["ok"]:
                # 提交无回执可等（SA 异步离线）：标「已提交待验证」，
                # 由入库验证回环兜底确认；TG 会话在时用户也会收到 SA 自己的
                # TG 通知（带外渠道），插件不再监听
                logger.info(f"【{title}】{r['detail']}（不等离线结果，"
                            f"交由入库验证回环确认）")
                return {"ok": True, "via": "http",
                        "results": {label: {"ok": True, "msg": "SA(企微)已提交"}
                                    for label, _ in items}}
            logger.warning(f"【{title}】{r['detail']}，回退 TG 提交")
        # ---- ③ 回退/默认：TG 提交（Telethon 发 SA 机器人并等回执） ----
        mgr = self.__get_tg()
        if not mgr:
            return {"ok": False, "via": "tg", "error": "TG 管理器不可用",
                    "results": {}}
        sub = mgr.submit(self._sa_bot, items, interval=self._aiying_interval)
        sub["via"] = "tg"
        return sub

    def __unsub_pt_if_115(self, entry: Dict[str, Any], title: str,
                          history: List[Dict[str, Any]]):
        """
        核销时调用：channel 含 115 渠道的剧，若本插件此前给它加过 MP PT 订阅，
        核销后自动删除该订阅（115 已补齐，避免 PT 重复下载）。
        只删除本插件自己添加的订阅（username=插件名），用户手动订阅不动。
        """
        channel = entry.get("channel", "pt")
        # v1.7.0：渠道取值扩展为 aiying_api/aiying_tg（老快照里的 aiying 也兼容）
        if channel not in ("aiying", "aiying_api", "aiying_tg", "mixed"):
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
                    # ---- 第六行半：订阅闭环增强开关（v1.5.0 新增） ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'search_after_subscribe',
                                              'label': '订阅后立即搜索',
                                              'hint': '订阅成功后马上让 MP 搜这一部，不用等周期搜索慢慢轮',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'verify_auto_reset',
                                              'label': '超时自动重置订阅',
                                              'hint': '超过告警天数未入库时重置订阅，让 MP 下轮重新搜索',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'verify_auto_resubscribe',
                                              'label': '订阅丢失自动补订',
                                              'hint': '订阅不存在（多为手动退订）时按剩余缺集重新订阅，默认关',
                                              'persistent-hint': False}
                                }]
                            },
                        ]
                    },
                    # ---- 第六行又半：增量扫描（v1.6.0 新增） ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'incremental_scan',
                                              'label': '增量扫描（推荐开）',
                                              'hint': '已完结且不缺集的剧本轮跳过，大库一轮从一小时降到几分钟',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSelect',
                                    'props': {
                                        'model': 'full_scan_weekday',
                                        'label': '每周全量扫描日（当天忽略账本全库重查）',
                                        'items': [
                                            {'title': '周一', 'value': 0},
                                            {'title': '周二', 'value': 1},
                                            {'title': '周三', 'value': 2},
                                            {'title': '周四', 'value': 3},
                                            {'title': '周五', 'value': 4},
                                            {'title': '周六', 'value': 5},
                                            {'title': '周日', 'value': 6},
                                        ],
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'full_scan_once',
                                              'label': '本轮强制全量扫描',
                                              'hint': '一次性开关：保存后下一轮忽略完结账本，跑完自动关',
                                              'persistent-hint': False}
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
                    # ---- 第十行：AY115通道说明 ----
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
                                        'text': '【AY115通道】（实验功能）开启后，缺集会先问AY资源机器人'
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
                    # ---- 第十一行：AY开关 + 机器人配置 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'aiying_enabled',
                                              'label': '启用AY115通道',
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
                                              'label': 'AY资源机器人用户名',
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
                    # ---- 第十一行半：AY API 通道（v1.7.0，查询主通道） ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'aiying_api_enabled',
                                              'label': 'AY API 通道',
                                              'hint': '查询走 HTTP API（快），异常时自动回退 TG 点按钮流程',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 5},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'aiying_api_url',
                                              'label': 'AY API 地址',
                                              'placeholder': 'http://你的AY服务地址/api/user',
                                              'hint': 'v1.9.2 起默认值已脱敏，请自行填写；留空则自动回退 TG 流程',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'aiying_api_max_links',
                                              'label': '单剧最多提交分享链接数',
                                              'type': 'number', 'placeholder': '3',
                                              'hint': '整季整剧包优先，其次按大小降序',
                                              'persistent-hint': False}
                                }]
                            },
                        ]
                    },
                    # ---- 第十一行又半：API token 与 tg_id ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 8},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'aiying_api_token',
                                              'label': 'AY API token',
                                              'placeholder': 'AY_xxxxxxxx',
                                              'hint': '你的 AY API 令牌（v1.9.2 起默认值已脱敏，请自行填写）；每查一次扣一次当月额度',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'tg_id',
                                              'label': 'TG 用户 ID（可留空）',
                                              'placeholder': '1234567890',
                                              'hint': '留空则自动从 TG 会话获取并缓存到这里',
                                              'persistent-hint': False}
                                }]
                            },
                        ]
                    },
                    # ---- 第十一行又半1.5：AY API 连通性测试（v1.9.5） ----
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol',
                            'props': {'cols': 12},
                            'content': [{
                                'component': 'VSwitch',
                                'props': {'model': 'ay_api_probe_once',
                                          'label': '测试 AY 连通性（保存即触发）',
                                          'hint': '打开并保存后立即探测 AY API 是否连通，结果见通知与插件日志，随后自动复位',
                                          'persistent-hint': False}
                            }]
                        }]
                    },
                    # ---- 第十一行又半2：SA 直连 API（v1.9.2，最高优先级） ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'sa_api_url',
                                              'label': 'SA 服务地址（直连 API）',
                                              'placeholder': 'http://127.0.0.1:8095',
                                              'hint': '三件套齐全即启用直连，失败自动回退企微/TG',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'sa_api_user',
                                              'label': 'SA 登录用户名',
                                              'placeholder': '你的 SA 用户名',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'sa_api_password',
                                              'label': 'SA 登录密码',
                                              'type': 'password',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 2},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'sa_api_parent_id',
                                              'label': '115 目录 cid（可留空）',
                                              'placeholder': '留空自动取',
                                              'hint': '留空=自动取 folders 第一项',
                                              'persistent-hint': False}
                                }]
                            },
                        ]
                    },
                    # ---- 第十一行又半2：SA 企微通道（v1.8.0；v1.9.2 起为直连兜底） ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'sa_http_enabled',
                                              'label': 'SA 企微通道（兜底）',
                                              'hint': '企业微信回调协议提交；v1.9.2 起默认值已脱敏，请自行填写',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 5},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'sa_http_url',
                                              'label': 'SA 消息 API 地址',
                                              'placeholder': 'http://127.0.0.1:8095/api/v1/message/',
                                              'hint': '本地直连即可，也可填你的 SA 公网地址',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'sa_http_token',
                                              'label': 'SA 回调 token',
                                              'placeholder': '企业微信回调 token',
                                              'hint': 'msg_signature 签名用；留空则企微通道禁用',
                                              'persistent-hint': False}
                                }]
                            },
                        ]
                    },
                    # ---- 第十一行又半3：SA HTTP 加密参数 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 5},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'sa_http_aeskey',
                                              'label': 'EncodingAESKey',
                                              'placeholder': '43 位 AES Key',
                                              'hint': '企业微信回调加密密钥；留空则企微通道禁用',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'sa_http_corpid',
                                              'label': '企业 CorpID',
                                              'placeholder': 'ww 开头 CorpID',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 2},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'sa_http_userid',
                                              'label': '来源用户 ID',
                                              'placeholder': '你的用户 ID',
                                              'hint': 'SA 不校验来源',
                                              'persistent-hint': False}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 2},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'sa_http_agentid',
                                              'label': 'AgentID',
                                              'placeholder': '1000003',
                                              'hint': 'SA 不校验',
                                              'persistent-hint': False}
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
                    # ---- 第十四行：AY风控参数 ----
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {'model': 'aiying_interval',
                                              'label': 'AY每集间隔（秒）',
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
                                              'label': '每剧经AY最多补集数',
                                              'type': 'number', 'placeholder': '30',
                                              'hint': '防点爆AY次数；超出的集仍走 PT 订阅',
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
                                                '订阅后插件会每轮复查入库情况，超时未入库会告警'
                                                '（不自动退订；开了「超时自动重置订阅」会重置订阅让 MP 重新搜索）。'
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
            "search_after_subscribe": True,
            "verify_auto_reset": True,
            "verify_auto_resubscribe": False,
            "incremental_scan": True,
            "full_scan_weekday": 6,
            "full_scan_once": False,
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
            "aiying_api_enabled": True,
            "aiying_api_url": "",
            "aiying_api_token": "",
            "aiying_api_max_links": 3,
            "tg_id": "",
            "ay_api_probe_once": False,
            "sa_http_enabled": True,
            "sa_http_url": "http://127.0.0.1:8095/api/v1/message/",
            "sa_http_token": "",
            "sa_http_aeskey": "",
            "sa_http_corpid": "",
            "sa_http_userid": "",
            "sa_http_agentid": "1000003",
            "sa_api_url": "http://127.0.0.1:8095",
            "sa_api_user": "",
            "sa_api_password": "",
            "sa_api_parent_id": "",
        }

    def get_page(self) -> List[dict]:
        """详情页：统计卡片 + 补齐验证区块 + 疑似死任务区块 + 最近 50 条历史表格"""
        stats = self.get_data(self._DATA_STATS) or {}
        history = self.get_data(self._DATA_HISTORY) or []
        processed = self.get_data(self._DATA_PROCESSED) or {}
        pending = self.get_data(self._DATA_PENDING) or {}
        dead_snapshot = self.get_data(self._DATA_DEAD) or {}
        done_ledger = self.get_data(self._DATA_DONELEDGER) or {}  # 完结账本（v1.6.0）
        recent = list(reversed(history[-50:]))  # 最新在前
        # v1.7.0：历史表加「渠道」列（emoji 着色：蓝 PT / 绿 115 / 橙 混合）
        # 与「渠道详情」列（长文本截断显示；MP 页面 schema 不支持单元格 title 悬浮，
        # 完整明细同步写在「备注」列与日志里）
        _CHANNEL_LABEL = {"pt": "🔵 PT", "aiying_api": "🟢 115·API",
                          "aiying_tg": "🟢 115·TG", "aiying": "🟢 115·TG",
                          "mixed": "🟠 混合"}
        recent_view: List[dict] = []
        for _h in recent:
            _row = dict(_h)
            _row["channel_label"] = _CHANNEL_LABEL.get(_h.get("channel") or "", "")
            _detail = _h.get("channel_detail") or ""
            _row["detail_short"] = _detail[:50] + ("…" if len(_detail) > 50 else "")
            recent_view.append(_row)
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
        _ay_precheck_label = {
            "ok": "通过", "401": "401 失效", "disabled": "未启用", "error": "异常",
        }.get(progress_data.get("ay_precheck"), "未知")
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
                                              f"AY补齐 {progress_data.get('aiying', 0)} 部 · "
                                              f"AY预检：{_ay_precheck_label} · "
                                              f"跳过 {progress_data.get('skipped', 0)} 部 · "
                                              f"失败 {progress_data.get('failed', 0)} 部（"
                                              f"TMDB {progress_data.get('failed_tmdb', 0)} · "
                                              f"Emby {progress_data.get('failed_emby', 0)} · "
                                              f"订阅 {progress_data.get('failed_sub', 0)} · "
                                              f"其他 {progress_data.get('failed_other', 0)}） · "
                                              f"今日剩余配额 {progress_data.get('quota_left', 0)} 部")},
                                    {'component': 'div',
                                     'props': {'class': 'text-caption text-grey'},
                                     'text': (f"扫描模式：{'增量' if progress_data.get('scan_mode') == 'incremental' else '全量'}"
                                              f"（完结账本跳过 {progress_data.get('ledger_skipped', 0)} 部 · "
                                              f"账本总量 {len(done_ledger)} 部）"
                                              f"。开始于 {progress_data.get('started_at', '')}。"
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
                                     f"失败 {progress_data.get('failed', 0)} 部（"
                                     f"TMDB {progress_data.get('failed_tmdb', 0)} · "
                                     f"Emby {progress_data.get('failed_emby', 0)} · "
                                     f"订阅 {progress_data.get('failed_sub', 0)} · "
                                     f"其他 {progress_data.get('failed_other', 0)}） · "
                                     f"耗时 {_min} 分钟 · "
                                     f"{'增量' if progress_data.get('scan_mode') == 'incremental' else '全量'}模式"
                                     f"（账本跳过 {progress_data.get('ledger_skipped', 0)} 部）")
                        }
                    }]
                }]
            })

        # ---- TG 登录状态行（v1.4.0）：AY通道启用或登录过才显示 ----
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
                                'text': (f"AY115通道：TG 已登录（{_acc}）"
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
                                'text': ('AY115通道：TG 未登录。请到配置页填手机号，'
                                         '勾「发送验证码」保存，再到 TG 收码后填验证码、'
                                         '勾「完成登录」保存；登录成功后本行会显示账号名。')
                            }
                        }]
                    }]
                })

        page = tg_rows + progress_rows + [
            # ---- 静态快照提示（v1.5.0）：页面不会自动刷新 ----
            {
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [{
                        'component': 'div',
                        'props': {'class': 'text-caption text-grey'},
                        'text': '本页面为静态快照，执行操作或任务运行后请手动刷新查看最新状态。'
                    }]
                }]
            },
            # ---- 统计卡片（两行：扫描订阅类 + 验证回环类） ----
            {
                'component': 'VRow',
                'content': [
                    __stat_card("累计扫描", stats.get("total_scanned", 0), "blue-grey"),
                    # v1.5.0：该值每轮累加会膨胀，标题明确标注「累计」口径
                    __stat_card("累计发现缺集（含重复轮次）",
                                stats.get("total_missing", 0), "orange"),
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
                    # v1.6.0：完结账本总量（增量扫描每轮跳过的部数来源）
                    __stat_card("完结账本", len(done_ledger), "brown"),
                ]
            },
            # ---- 增量扫描规则说明（v1.6.0）----
            {
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [{
                        'component': 'div',
                        'props': {'class': 'text-caption text-grey'},
                        'text': ('增量扫描规则：TMDB 已完结且无缺集的剧记入「完结账本」，'
                                 '日常轮次直接跳过不重查；发现缺集或订阅未全部成功会自动移出账本；'
                                 '每周全量扫描日或勾「本轮强制全量扫描」时忽略账本全库重查。')
                    }]
                }]
            },
            # ---- AY统计卡片（v1.4.0，通道启用才显示）----
            *([{
                'component': 'VRow',
                'content': [
                    __stat_card("本轮AY补齐", stats.get("last_aiying", 0), "cyan"),
                    __stat_card("累计AY补齐", stats.get("total_aiying", 0), "teal"),
                    __stat_card(
                        "AY剩余次数",
                        (self.get_data(self._DATA_AIYING) or {}).get("quota_left", "未知"),
                        "indigo"),
                    # v1.7.0：API 通道额度（最近一次响应 times；为 0 或没查过时显示未知）
                    __stat_card(
                        "AYAPI剩余次数",
                        ((self.get_data(self._DATA_AIYING) or {}).get("api_quota_left")
                         or "未知"),
                        "deep-purple"),
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
                # v1.5.0：状态列按「已等天数 vs 告警天数阈值」实时计算，
                # 超期即显示超时，不再等下一轮复查把 alerted 置位才变红
                is_overdue = wait_days >= self._verify_alert_days
                verify_items.append({
                    "title": entry.get("title", ""),
                    "subscribe_time": entry.get("subscribe_time", ""),
                    "wait_days": wait_days,
                    "remaining": remaining_eps,
                    "status": "🔴 超时未补齐" if is_overdue else "等待入库",
                })
            # 等待天数长的排前面，最需要关注的在最上
            verify_items.sort(key=lambda x: -x["wait_days"])
            # v1.9.1：VDataTable 在 MP 前端 v2.15.6 渲染空白，改裸 HTML 表格
            verify_rows: List[List[Any]] = []
            for it in verify_items[:50]:
                if it["status"].startswith("🔴"):
                    _st = ("raw", '<span style="color:#ef5350;font-weight:600">'
                                  '🔴 超时未补齐</span>')
                else:
                    _st = ("raw", '<span style="color:#42a5f5">等待入库</span>')
                verify_rows.append([it["title"], it["subscribe_time"],
                                    it["wait_days"], it["remaining"], _st])
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
                                    'content': [
                                        # 数据超 50 时的小提示（无分页器）
                                        *([{
                                            'component': 'div',
                                            'props': {'class': 'text-caption text-grey mb-1'},
                                            'text': f'共 {len(verify_items)} 条，显示前 50 条'
                                        }] if len(verify_items) > 50 else []),
                                        {
                                            'component': 'div',
                                            'html': _html_table(
                                                ["剧名", "订阅日期", "已等天数",
                                                 "剩余缺集", "状态"],
                                                verify_rows)
                                        }
                                    ]
                                },
                            ]
                        }
                    ]
                }]
            })

        # ---- 疑似死任务区块 ----
        dead_tasks = (dead_snapshot or {}).get("tasks") or []
        if dead_tasks:
            # v1.9.1：裸 HTML 表格（VDataTable 渲染空白 bug 绕过）
            dead_rows: List[List[Any]] = [
                [t.get("name", ""), t.get("downloader", ""),
                 t.get("age_hours", ""), t.get("state", "")]
                for t in dead_tasks[:50]
            ]
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
                                    'content': [
                                        *([{
                                            'component': 'div',
                                            'props': {'class': 'text-caption text-grey mb-1'},
                                            'text': f'共 {len(dead_tasks)} 条，显示前 50 条'
                                        }] if len(dead_tasks) > 50 else []),
                                        {
                                            'component': 'div',
                                            'html': _html_table(
                                                ["任务名", "下载器",
                                                 "已挂时长(小时)", "状态"],
                                                dead_rows)
                                        }
                                    ]
                                },
                            ]
                        }
                    ]
                }]
            })

        # ---- 历史表格 ----
        if recent:
            # v1.9.1：裸 HTML 表格（VDataTable 渲染空白 bug 绕过）+ 渠道 chips 着色
            _CHIP_TEXT = {"pt": "PT", "aiying_api": "115·API",
                          "aiying_tg": "115·TG", "aiying": "115·TG", "mixed": "混合"}
            _CHIP_COLOR = {"pt": "#1e88e5", "aiying_api": "#2e7d32",
                           "aiying_tg": "#2e7d32", "aiying": "#2e7d32",
                           "mixed": "#ef6c00"}
            history_rows: List[List[Any]] = []
            for _h in recent_view:
                _ch = _h.get("channel") or ""
                if _ch in _CHIP_TEXT:
                    _chip = ("raw", f'<span style="padding:1px 8px;border-radius:8px;'
                                    f'font-size:11px;color:#fff;'
                                    f'background:{_CHIP_COLOR[_ch]}">'
                                    f'{_escape_html(_CHIP_TEXT[_ch])}</span>')
                elif _ch:
                    # 未知渠道值：灰色 chip 原样展示（转义后）
                    _chip = ("raw", f'<span style="padding:1px 8px;border-radius:8px;'
                                    f'font-size:11px;color:#fff;background:#9e9e9e">'
                                    f'{_escape_html(_ch)}</span>')
                else:
                    _chip = ""
                history_rows.append([
                    _h.get("time", ""), _h.get("title", ""), _h.get("year", ""),
                    _h.get("seasons", ""), _h.get("missing_count", ""),
                    _h.get("result", ""), _chip, _h.get("detail_short", ""),
                    _h.get("message", ""),
                ])
            page.append({
                'component': 'VRow',
                'content': [{
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        *([{
                            'component': 'div',
                            'props': {'class': 'text-caption text-grey mb-1'},
                            'text': f'共 {len(history)} 条，显示前 50 条'
                        }] if len(history) > 50 else []),
                        {
                            'component': 'div',
                            'html': _html_table(
                                ["时间", "剧名", "年份", "缺集季", "缺集数",
                                 "结果", "渠道", "渠道详情", "备注"],
                                history_rows,
                                # 渠道详情列允许换行并限宽
                                col_styles={7: "white-space:normal;max-width:320px"})
                        }
                    ]
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
