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
import time
import traceback
from threading import Event as ThreadEvent
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

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


class LackEpisodeAutoSub(_PluginBase):
    # 插件名称
    plugin_name = "缺集自动补齐"
    # 插件描述
    plugin_desc = "定时扫描 Emby 剧集库找出缺集的剧，按优先级与每日配额自动订阅，并验证下载入库闭环。"
    # 插件图标（本仓库 icons/ 目录）
    plugin_icon = "https://raw.githubusercontent.com/OneFlatWhite/MoviePilot-Plugins/main/icons/lackepisodeautosub.png"
    # 插件版本
    plugin_version = "1.2.2"
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

    # 持久化数据的 key
    _DATA_PROCESSED = "processed"        # 已处理（已成功订阅）的剧 {tmdbid: {...}}
    _DATA_HISTORY = "history"            # 运行历史列表（最多保留 200 条）
    _DATA_DAILY = "daily"                # 当日配额计数 {"date": "YYYY-MM-DD", "count": n}
    _DATA_STATS = "stats"                # 累计统计
    _DATA_PENDING = "pending_verify"     # 已订阅未核销的剧 {tmdbid: 快照}
    _DATA_DEAD = "dead_tasks"            # 最近一轮疑似死任务快照（详情页展示用）

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
                self._clear_history = False
                logger.info(f"【{self.plugin_name}】历史记录与已处理清单已清空")
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
        """停止一次性任务的本地调度器（cron 服务由 MP 托管，无需处理）"""
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

    # ==================================================================
    # 插件 API（_PluginBase 抽象方法，必须实现，否则插件加载失败）
    # ==================================================================
    def get_api(self) -> List[Dict[str, Any]]:
        """
        注册插件 API，挂载在 /api/v1/plugin/LackEpisodeAutoSub/ 下：
          GET /scan   手动触发一轮扫描（等价于「立即运行一次」），返回本轮摘要
          GET /status 查询当前统计（待验证/已核销/今日已订阅/今日配额等）
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
                },
            }
        except Exception as e:
            logger.error(f"【{self.plugin_name}】API 查询状态失败: {e}")
            return {"success": False, "message": str(e), "data": None}

    # ==================================================================
    # 核心主流程
    # ==================================================================
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

        # ---------- 3.4 按配额 + 风控订阅 ----------
        consecutive_failures = 0  # 连续失败计数（成功即清零）
        for index, cand in enumerate(candidates):
            # 【风控】超时保护：订阅阶段同样兜底
            if self.__is_timeout(start_time):
                timeout_hit = True
                logger.warning(f"【{self.plugin_name}】订阅阶段超过 "
                               f"{self._scan_timeout} 分钟，本轮提前收尾")
                self.__append_history(
                    history, title="（系统）", year="", tmdbid=0, lack_info={},
                    missing_count=0, result="超时收尾",
                    message=f"超过 {self._scan_timeout} 分钟，剩余 {len(candidates) - index} 部留待下轮")
                break

            title = cand["title"]

            # 调试模式：只记录，不订阅、不消耗配额、不标记已处理、不进入验证回环
            if self._dry_run:
                self.__append_history(
                    history, title=title, year=cand["year"], tmdbid=cand["tmdbid"],
                    lack_info=cand["lack_info"], missing_count=cand["missing"],
                    result="调试-待订阅", message="调试模式未真正订阅")
                continue

            # 【风控】每日配额控制：超出配额的剧不标记已处理，留到明天继续
            if remaining_quota <= 0:
                logger.info(f"【{self.plugin_name}】当日配额 {self._daily_quota} 已用完，"
                            f"【{title}】及之后候选留待下一轮")
                break

            # 逐季添加订阅（MP 订阅后自己会比对媒体库只补缺集）
            ok, msg = self.__subscribe_show(
                title, cand["year"], cand["tmdbid"], cand["lack_info"])

            # 【风控】订阅间隔：无论成败都 sleep，避免瞬间打爆 MP/TMDB/PT 站
            if self._subscribe_interval > 0:
                time.sleep(self._subscribe_interval)

            if ok:
                subscribed += 1
                consecutive_failures = 0  # 成功一次，连续失败清零
                remaining_quota -= 1
                self.__incr_daily_quota()
                subscribed_titles.append(f"{title}（缺 {cand['missing']} 集）")
                # 标记已处理，下一轮不再重复
                processed[str(cand["tmdbid"])] = {
                    "title": title,
                    "time": datetime.datetime.now(
                        tz=pytz.timezone(settings.TZ)).strftime(TIME_FMT),
                    "seasons": sorted(cand["lack_info"].keys()),
                }
                # 【验证回环】登记"已订阅未核销"快照，之后每轮复查入库情况
                self.__register_pending(cand)
                self.__append_history(
                    history, title=title, year=cand["year"], tmdbid=cand["tmdbid"],
                    lack_info=cand["lack_info"], missing_count=cand["missing"],
                    result="已订阅", message=msg)
            else:
                failed += 1
                consecutive_failures += 1
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
        stats["last_run"] = start_time.strftime(TIME_FMT)

        self.save_data(self._DATA_PROCESSED, processed)
        self.save_data(self._DATA_HISTORY, history[-200:])  # 最多保留 200 条
        self.save_data(self._DATA_STATS, stats)

        elapsed = (datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                   - start_time).total_seconds()
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
    def __register_pending(self, cand: Dict[str, Any]):
        """订阅成功后登记快照：之后每轮复查 Emby 是否真入库"""
        pending: Dict[str, Any] = self.get_data(self._DATA_PENDING) or {}
        # 快照内容：tmdbid、剧名、年份、缺集列表、订阅时间、Emby 定位信息
        pending[str(cand["tmdbid"])] = {
            "title": cand["title"],
            "year": cand["year"],
            "tmdbid": cand["tmdbid"],
            "server": cand["server"],        # Emby 服务器名（复查时直接定位）
            "item_id": cand["item_id"],      # Emby 剧集 ID（复查时直接定位）
            "seasons": {str(s): list(eps) for s, eps in cand["lack_info"].items()},
            "remaining": {str(s): list(eps) for s, eps in cand["lack_info"].items()},
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

    @staticmethod
    def __append_history(history: List[Dict[str, Any]], title: str, year: str,
                         tmdbid: int, lack_info: Dict[int, List[int]],
                         missing_count: int, result: str, message: str):
        """追加一条历史记录"""
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

        page = [
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
