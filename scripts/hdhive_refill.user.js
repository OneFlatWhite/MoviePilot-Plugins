// ==UserScript==
// @name         影巢缺集回填助手（HDHive → MoviePilot 缺集自动补齐）
// @namespace    https://github.com/OneFlatWhite/MoviePilot-Plugins
// @version      1.0.0
// @description  自动拉取 MoviePilot 缺集队列，在影巢(hdhive.com)站内搜索剧名，抓取 115 分享链接回填给 MP 插件转存补齐。支持全自动巡队 / 单剧手动找 / 本页链接一键回填。
// @author       OneFlatWhite
// @match        *://hdhive.com/*
// @match        *://www.hdhive.com/*
// @connect      192.168.31.40
// @connect      localhost
// @grant        GM_xmlhttpRequest
// @grant        GM_setValue
// @grant        GM_getValue
// @grant        GM_addStyle
// @run-at       document-idle
// @noframes
// ==/UserScript==

(function () {
  'use strict';

  // ===================== 配置（按需修改） =====================
  const MP_BASE = 'http://192.168.31.40:3000';          // MoviePilot 地址
  const MP_APIKEY = 'WangMP2026WeChat88';               // MP 的 API_TOKEN
  const POLL_SEC = 60;                                  // 队列轮询间隔（秒）
  const TASK_GAP_MS = 9000;                             // 自动模式下两部剧之间间隔
  const SEARCH_WAIT_MS = 5000;                          // 触发搜索后等待出结果的时间
  const MAX_LINKS_PER_TASK = 3;                         // 每部剧最多回填的 115 链接数
  // ============================================================

  const API_QUEUE = MP_BASE + '/api/v1/plugin/LackEpisodeAutoSub/refill_queue?apikey=' + MP_APIKEY;
  const API_SUBMIT = MP_BASE + '/api/v1/plugin/LackEpisodeAutoSub/refill_submit?apikey=' + MP_APIKEY;

  let queue = [];
  let autoRunning = GM_getValue('autoRunning', false);
  let busy = false;
  let pollTimer = null;

  const RX_115 = /https?:\/\/(?:www\.)?(?:115|115cdn)\.com\/s\/[0-9A-Za-z]+[^"'\s<>\\]*/g;

  const CSS = `
  #mpx-panel{position:fixed;right:16px;bottom:16px;z-index:999999;width:330px;
    background:#11151c;color:#dfe6ee;border:1px solid #2b3442;border-radius:10px;
    font:12px/1.5 -apple-system,"PingFang SC","Microsoft Yahei",sans-serif;
    box-shadow:0 8px 30px rgba(0,0,0,.45);overflow:hidden}
  #mpx-panel .hd{display:flex;align-items:center;gap:8px;padding:8px 10px;
    background:#1a2230;cursor:pointer;user-select:none}
  #mpx-panel .hd b{flex:1;font-size:13px}
  #mpx-panel .bd{padding:10px;display:none}
  #mpx-panel.open .bd{display:block}
  #mpx-panel .row{display:flex;gap:8px;margin-bottom:8px;flex-wrap:wrap}
  #mpx-panel button{background:#2f6fed;border:0;color:#fff;border-radius:6px;
    padding:5px 10px;cursor:pointer;font-size:12px}
  #mpx-panel button.off{background:#3a4657}
  #mpx-panel button:disabled{opacity:.5;cursor:not-allowed}
  #mpx-panel .log{height:130px;overflow:auto;background:#0c1016;border-radius:6px;
    padding:6px 8px;white-space:pre-wrap;word-break:break-all;color:#9fb3c8}
  #mpx-panel .task{padding:4px 0;border-bottom:1px dashed #2b3442}
  #mpx-panel .task .go{color:#7cc4ff;cursor:pointer;text-decoration:underline}
  #mpx-panel .cnt{background:#e5484d;color:#fff;border-radius:9px;padding:0 7px;
    font-size:11px;font-weight:700}
  `;

  function log(msg) {
    const el = document.getElementById('mpx-log');
    if (!el) return;
    const t = new Date().toTimeString().slice(0, 8);
    el.textContent = (`[${t}] ${msg}\n` + el.textContent).split('\n').slice(0, 60).join('\n');
  }

  // ---------- MP API ----------
  function mpGet(url) {
    return new Promise((resolve, reject) => {
      GM_xmlhttpRequest({
        method: 'GET', url, timeout: 20000,
        onload: r => {
          try { resolve(JSON.parse(r.responseText)); }
          catch (e) { reject(new Error('响应非 JSON: ' + r.responseText.slice(0, 120))); }
        },
        onerror: () => reject(new Error('网络错误')),
        ontimeout: () => reject(new Error('超时')),
      });
    });
  }
  function mpPost(url, data) {
    return new Promise((resolve, reject) => {
      GM_xmlhttpRequest({
        method: 'POST', url, timeout: 60000,
        headers: { 'Content-Type': 'application/json' },
        data: JSON.stringify(data),
        onload: r => {
          try { resolve(JSON.parse(r.responseText)); }
          catch (e) { reject(new Error('响应非 JSON: ' + r.responseText.slice(0, 120))); }
        },
        onerror: () => reject(new Error('网络错误')),
        ontimeout: () => reject(new Error('超时')),
      });
    });
  }

  async function refreshQueue() {
    const d = await mpGet(API_QUEUE);
    if (!d.success) throw new Error(d.message || '队列接口失败');
    queue = (d.data && d.data.items) || [];
    updateBadge();
    return queue;
  }

  function doneKey(tmdbid) { return 'mpx_done_' + tmdbid; }
  function isDone(t) { return GM_getValue(doneKey(t.tmdbid), '') === todayStr(); }
  function markDone(t) { GM_getValue; GM_setValue(doneKey(t.tmdbid), todayStr()); }
  function todayStr() { return new Date().toISOString().slice(0, 10); }
  function failCnt(t) { return GM_getValue('mpx_fail_' + t.tmdbid, 0); }
  function addFail(t) { GM_setValue('mpx_fail_' + t.tmdbid, failCnt(t) + 1); }

  // ---------- 站内搜索 + 抓 115 链接 ----------
  function harvest115(scope) {
    const html = (scope || document).documentElement
      ? (scope || document).documentElement.innerHTML
      : '';
    const set = new Set();
    let m;
    const rx = new RegExp(RX_115.source, 'g');
    while ((m = rx.exec(html)) !== null) {
      let u = m[0].replace(/&amp;/g, '&');
      if (u.length > 120) u = u.slice(0, 120);
      set.add(u);
      if (set.size >= MAX_LINKS_PER_TASK) break;
    }
    return [...set];
  }

  function findSearchInput() {
    return document.querySelector(
      'input[type=search],input[placeholder*="搜索"],input[placeholder*="搜索"],' +
      'input[placeholder*="search" i],input[name="keyword"],input[name="q"]');
  }

  function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

  async function searchOnSite(keyword) {
    // 策略 1：页面自带搜索框 → 填词回车
    const input = findSearchInput();
    if (input) {
      input.focus();
      input.value = keyword;
      input.dispatchEvent(new Event('input', { bubbles: true }));
      input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', keyCode: 13, bubbles: true }));
      input.dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', keyCode: 13, bubbles: true }));
      await sleep(SEARCH_WAIT_MS);
      let links = harvest115(document);
      if (links.length) return links;
    }
    // 策略 2：常见搜索路由逐个试（SPA 路由切换）
    const kws = encodeURIComponent(keyword);
    const routes = [
      '/search?keyword=' + kws, '/search?kw=' + kws, '/search/' + kws,
      '/so/' + kws, '/s/' + kws, '/?s=' + kws,
    ];
    const savedHref = location.href;
    for (const rt of routes) {
      try { history.pushState({}, '', rt); window.dispatchEvent(new PopStateEvent('popstate')); } catch (e) {}
      await sleep(SEARCH_WAIT_MS);
      const links = harvest115(document);
      if (links.length) return links;
    }
    try { history.pushState({}, '', savedHref); window.dispatchEvent(new PopStateEvent('popstate')); } catch (e) {}
    return [];
  }

  async function processTask(t) {
    if (isDone(t)) { log(`⏭ ${t.title} 今天已处理过`); return; }
    if (failCnt(t) >= 2) { log(`⏭ ${t.title} 连续失败≥2 次，今天不再试`); return; }
    log(`🔍 ${t.title}（缺 ${t.missing} 集）站内搜索…`);
    const kw = t.year ? `${t.title} ${t.year}` : t.title;
    const links = await searchOnSite(kw);
    if (!links.length) {
      addFail(t);
      log(`❌ ${t.title} 没抓到 115 链接`);
      return;
    }
    log(`✅ ${t.title} 抓到 ${links.length} 条 115 链接，回填 MP…`);
    const r = await mpPost(API_SUBMIT, {
      tmdbid: t.tmdbid, title: t.title,
      links: links.join('\n'), source: 'hdhive_userscript',
    });
    if (r.success) {
      markDone(t);
      log(`📤 ${t.title} 回填成功（${r.data && r.data.via || ''}）→ SA 转存中`);
      queue = queue.filter(x => x.tmdbid !== t.tmdbid);
      updateBadge();
    } else {
      addFail(t);
      log(`⚠️ ${t.title} 回填失败：${r.message || '未知'}`);
    }
  }

  async function runAuto() {
    if (busy) return;
    busy = true;
    try {
      await refreshQueue();
      const pending = queue.filter(t => !isDone(t) && failCnt(t) < 2);
      log(`队列 ${queue.length} 部，待处理 ${pending.length} 部`);
      for (const t of pending) {
        if (!autoRunning) { log('已停止'); break; }
        await processTask(t);
        await sleep(TASK_GAP_MS);
      }
    } catch (e) {
      log('自动模式出错：' + e.message);
    } finally { busy = false; }
  }

  // ---------- 面板 UI ----------
  function taskListHtml() {
    return queue.slice(0, 8).map(t => {
      const sd = Object.entries(t.seasons || {})
        .map(([s, e]) => `S${s}×${(e || []).length}`).join(' ');
      return `<div class="task"><span class="go" data-kw="${t.title} ${t.year || ''}">${t.title}</span> ` +
        `<small>缺${t.missing}集(${sd}) 等${t.wait_days}天</small> ` +
        `<span class="go" data-one="${t.tmdbid}">[单找]</span></div>`;
    }).join('') || '<div class="task">队列空，等 MP 下轮扫描</div>';
  }

  function updateBadge() {
    const b = document.getElementById('mpx-cnt');
    if (b) b.textContent = queue.length;
    const tl = document.getElementById('mpx-tasks');
    if (tl) tl.innerHTML = taskListHtml();
    const st = document.getElementById('mpx-state');
    if (st) st.textContent = autoRunning ? '自动中' : '待机';
  }

  function buildPanel() {
    if (document.getElementById('mpx-panel')) return;
    GM_addStyle(CSS);
    const p = document.createElement('div');
    p.id = 'mpx-panel';
    p.innerHTML = `
      <div class="hd" id="mpx-toggle"><b>🧩 MP 缺集回填</b>
        <span class="cnt" id="mpx-cnt">0</span>
        <span id="mpx-state">待机</span></div>
      <div class="bd">
        <div class="row">
          <button id="mpx-auto">▶ 开始自动找</button>
          <button id="mpx-refresh">刷新队列</button>
          <button id="mpx-page">本页链接回填</button>
        </div>
        <div class="row"><small>最近任务：</small></div>
        <div id="mpx-tasks"></div>
        <div class="row" style="margin-top:6px"><small>日志：</small></div>
        <div class="log" id="mpx-log"></div>
      </div>`;
    document.body.appendChild(p);
    document.getElementById('mpx-toggle').onclick = () => p.classList.toggle('open');
    document.getElementById('mpx-refresh').onclick = async () => {
      try { await refreshQueue(); log(`队列已刷新：${queue.length} 部`); }
      catch (e) { log('刷新失败：' + e.message); }
    };
    document.getElementById('mpx-page').onclick = async () => {
      const links = harvest115(document);
      if (!links.length) { log('本页没发现 115 分享链接'); return; }
      const t = queue[0];
      if (!t) { log('队列为空，无法回填'); return; }
      const r = await mpPost(API_SUBMIT, {
        tmdbid: t.tmdbid, title: t.title,
        links: links.join('\n'), source: 'hdhive_manual',
      });
      log(r.success ? `📤 本页 ${links.length} 条已回填给【${t.title}】`
        : '回填失败：' + (r.message || ''));
    };
    document.getElementById('mpx-auto').onclick = (ev) => {
      autoRunning = !autoRunning;
      GM_setValue('autoRunning', autoRunning);
      ev.target.textContent = autoRunning ? '⏸ 停止' : '▶ 开始自动找';
      ev.target.classList.toggle('off', !autoRunning);
      if (autoRunning) runAuto();
    };
    p.addEventListener('click', async (ev) => {
      const one = ev.target.getAttribute && ev.target.getAttribute('data-one');
      if (one) {
        const t = queue.find(x => String(x.tmdbid) === one);
        if (t && !busy) { busy = true; await processTask(t); busy = false; }
      }
      const kw = ev.target.getAttribute && ev.target.getAttribute('data-kw');
      if (kw) {
        const input = findSearchInput();
        if (input) { input.focus(); input.value = kw.trim();
          input.dispatchEvent(new KeyboardEvent('keydown', {key:'Enter',keyCode:13,bubbles:true})); }
        else { location.href = '/search?keyword=' + encodeURIComponent(kw.trim()); }
      }
    });
    // 恢复按钮状态
    const btn = document.getElementById('mpx-auto');
    btn.textContent = autoRunning ? '⏸ 停止' : '▶ 开始自动找';
    btn.classList.toggle('off', !autoRunning);
  }

  function main() {
    buildPanel();
    refreshQueue().then(q => log(`已连接 MP，当前缺集队列 ${q.length} 部`))
      .catch(e => log('MP 连接失败：' + e.message));
    pollTimer = setInterval(() => {
      refreshQueue().catch(e => log('轮询失败：' + e.message));
      if (autoRunning && !busy) runAuto();
    }, POLL_SEC * 1000);
    if (autoRunning) runAuto();
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') main();
  else window.addEventListener('DOMContentLoaded', main);
})();
