# MoviePilot-Plugins（coldbrew 插件市场）

MoviePilot V2 自定义插件仓库。在 MP 的「插件 → 插件市场 → 添加仓库」中填入本仓库地址即可安装：

```
https://github.com/OneFlatWhite/MoviePilot-Plugins
```

## 插件列表

### 📺 缺集自动补齐（LackEpisodeAutoSub）v1.9.1

> **📖 完整使用 Wiki（小白向）：[docs/lackepisodeautosub](docs/lackepisodeautosub/README.md)**
> 安装准备 · 配置项全解 · 通道配置（爱影API/SA） · 日常使用 · 故障FAQ · 版本历史

自动找出 Emby 里缺集的电视剧，每天限量补齐——**115 网盘有资源就直接转存（不下载），没有就走 PT 站下载兜底**，最后自动刮削生成 STRM，Emby 直接能看。订完还盯着到底入没入库，闭环。

**核心能力**

- 🔍 **增量扫描**：完结账本机制，已完结已补齐的剧直接跳过，日常一轮几分钟
- ☁️ **115 优先通道**：爱影 API 按 TMDB ID 精确查资源（月额度 ~6500），按你缺的集**精确选包**，SA HTTP 直连离线转存——全程不经过本地下载
- 🌊 **PT 兜底**：逐季订阅 + 订阅后立即搜索 + Emby 就绪探测，四个 PT 站轮询
- 🔄 **验证回环**：每天复查是否真入库，超时自动重置重搜；115 补齐自动退 PT 订阅
- 🎯 **优先级策略 + 每日配额**：地区/动漫/纪录片分层，默认 200 部/天平滑补完
- 🛡️ **风控**：订阅间隔、连续失败熔断、单轮超时、死任务检测、磁盘告警
- 🧪 **调试模式**：先空跑验证识别，确认无误再放量
- 🔌 **远程 API**：`/scan` `/status` `/ay_api_test` `/sa_http_test`（apikey 鉴权）

**工作流**

```
增量扫描 Emby 库 → TMDB 对比找缺集 → 过滤去重（账本/已订阅/排除词）
→ 爱影 API 查 115 → 精确选包 → SA 离线转存（云端直达）
→ 无资源 → PT 逐季订阅 → qb 下载 → MP 整理 → 上传 115 → 删本地
→ Symedia 归档刮削 + STRM → Emby 可播 → 次日验证核销
```

**要求**：MoviePilot V2、已配置 Emby 媒体服务器、Emby 剧集已刮削（有 TMDB ID）；115 通道另需爱影 API Token 和 SA（如 Symedia），详见 [通道配置](docs/lackepisodeautosub/channels.md)。

## 许可证

MIT © coldbrew
