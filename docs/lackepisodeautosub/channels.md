# ③ 通道配置（爱影 API / SA 转存 / 影巢 / TG）

[← 返回首页](README.md)

插件的资源补齐有四级通道，按顺序自动回退：

```
① 爱影 API（HTTP 查 115 资源）→ ② SA HTTP 转存 → ③ TG 机器人查询（兜底）→ ④ PT 站下载
```

---

## 一、爱影 API 通道（主通道，v1.7.0+）

### 它是什么

爱影（ayclub）的资源查询 HTTP 接口：按 TMDB ID 精确查 115 资源（分享链接/ed2k），
返回后插件**按你实际缺的集精确选包**（只选覆盖缺集的分段包，不整季乱拖），交给 SA 离线到 115。

### 你需要准备两样东西

| 参数 | 怎么获取 |
|---|---|
| **API Token** | Telegram 里给 @ayclub_bot 发 `/token`，机器人回复 `AY_xxxxxxxx` |
| **tg_id** | 一般**留空**——插件会自动从 TG 会话读取；没有 TG 会话才需手填（你的 Telegram 数字 ID，可给 @userinfobot 发消息查） |

### 接口事实（已实测）

```
POST http://api.ayclub.vip:5050/api/user
Body(JSON): {"tg_id":"你的ID","type":"tv","tmdb_id":"44277","token":"AY_xxx"}
```

- 有资源：`state:0` + `data:[{name, notes, size, link, ...}]`
  - `link` 两种形态：115 分享链接（`https://115cdn.com/s/...`）或 ed2k 打包链接（常按 `S01E01-S01E05` 分段）
- 无资源：`state:0` + `data:{}`
- `times` 字段 = 当月剩余查询次数（约 6500/月，比 TG 机器人的 ~600 大 10 倍）

### 插件里的选包逻辑（v1.9.0）

1. 解析每条资源的覆盖集数（S01E06-S01E10 范围/单集/S1-S5 季/全集）；
2. **与你缺的集完全无交集的包直接淘汰**；
3. 贪心选「新增覆盖最大」，平局时：115 分享链接优先 → 溢出小优先 → 体积小优先；
4. 受「单剧最多提交链接数」约束，没覆盖的部分自动转 PT。

### 联调测试（配完必测）

```bash
# 只查询，不转存
curl "http://你的MP:3000/api/v1/plugin/LackEpisodeAutoSub/ay_api_test?tmdb_id=44277&save=0&apikey=你的MP_API_TOKEN"

# 查询+真实转存第一条（端到端）
curl "http://你的MP:3000/api/v1/plugin/LackEpisodeAutoSub/ay_api_test?tmdb_id=44277&save=1&apikey=你的MP_API_TOKEN"

# 模拟缺集验证选包（缺 S01E06-10）
curl "http://你的MP:3000/api/v1/plugin/LackEpisodeAutoSub/ay_api_test?tmdb_id=240993&save=0&eps=1:6-10&apikey=你的MP_API_TOKEN"
```

---

## 二、SA HTTP 转存通道（v1.8.0+）

### 它是什么

SA（如 Symedia）是负责把链接离线进 115 的执行者。以前插件通过 Telegram 机器人
@ColdSymMedia_bot 发链接（要维持 TG 登录态，易掉线）；v1.8.0 起改为
**直接调 SA 的 HTTP 消息接口**——TG 从必需降级为可选兜底。

### 你需要准备的参数（SA 的企业微信回调配置）

在 SA 的企业微信应用「接收消息服务器配置」页拿到：

| 配置项 | 说明 | 示例 |
|---|---|---|
| SA HTTP 地址 | SA 的消息 API 地址（本机直连优先） | `http://192.168.x.x:8095/api/v1/message/` |
| Token | 回调配置的 Token | `VAVTzTaj...` |
| EncodingAESKey | 回调配置的 EncodingAESKey | `pu5UO4eK...` |
| CorpID | 企业微信企业 ID | `ww11dd...` |
| UserID | 消息来源用户 ID（SA 不校验，任意字符串） | 默认用你的 tg_id |
| AgentID | 应用 ID（SA 不校验） | 默认 1000003 |

> **为什么报文是企业微信协议？** SA 的消息接口只处理企业微信标准加密消息——
> 明文 POST 会被静默丢弃（实测结论）。插件已内置完整的加密签名实现，你只填参数。

### 联调测试

```bash
curl "http://你的MP:3000/api/v1/plugin/LackEpisodeAutoSub/sa_http_test?content=获取当前用户 ID&apikey=你的MP_API_TOKEN"
```

返回 `消息处理成功` = 协议握手通过；命令执行结果看 SA 的通知渠道。

### 回执与"已转存过"

- HTTP 提交后**不等离线结果**（SA 异步处理）：最终是否入库由「验证回环」确认（Emby 里出现才算数）；
- TG 会话在时，插件会顺带监听 TG 回执拿到快速结果；
- SA 回「**你已经转存过该文件**」按成功处理（v1.8.1）——内容已在库里，不会再误转 PT 重复下载；
- SA 回「**分享已取消**」= 死链，自动换下一条或转 PT。

---

## 三、TG 机器人通道（兜底，可选）

v1.7.0 之前的主通道，现在是兜底：爱影 API 异常时自动回退到
「TG 问 bot → 模拟点按钮拿链接」的老流程。

- 需要插件里完成 TG 登录（Telethon 用户态，配置页按引导发验证码）；
- 日常不用管，掉线了重登即可；**不掉线也别折腾它**；
- 风控：单轮 100 次点击熔断、单剧 30 集上限。

---

## 四、影巢 HDHive（备选资源源）

如果你的 SA（Symedia）里已配置 HDHive（影巢）账号，它本身就是一路 115 资源源。
插件目前以爱影 API 为主；影巢接入在路线图上（双源互备）。

---

## 五、没有 115 通道也能跑

四级通道逐级回退意味着：**一个 115 相关的东西都不配，插件照样工作**——
所有缺集都走 PT 下载兜底。115 通道的价值是：省带宽、省时间、热门剧秒到。
