// ==UserScript==
// @name         RE0影巢缺集回填助手（re0.me → MoviePilot 缺集自动补齐）
// @namespace    https://github.com/OneFlatWhite/MoviePilot-Plugins
// @version      1.2.0
// @description  自动拉取 MoviePilot 缺集队列，在 RE0影巢(re0.me) 站内搜索剧名，抓取 115 分享链接回填给 MP 插件转存补齐。支持全自动巡队 / 单剧手动找 / 本页链接回填（可选剧）。
// @author       OneFlatWhite
// @match        *://re0.me/*
// @match        *://www.re0.me/*
// @match        *://hdhive.com/*
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
  const TASK_GAP_MS = 9000;                             // 自动模式两部剧之间间隔
  const SEARCH_WAIT_MS = 3000;                          // 搜索路由切换后单次等待
  const SEARCH_RETRY = 3;                               // 搜索后抓取重试轮数
  const MAX_LINKS_PER_TASK = 3;                         // 每部剧最多回填的 115 链接数
  // ============================================================

  const API_QUEUE = MP_BASE + '/api/v1/plugin/LackEpisodeAutoSub/refill_queue?apikey=' + MP_APIKEY;
  const API_SUBMIT = MP_BASE + '/api/v1/plugin/LackEpisodeAutoSub/refill_submit?apikey=' + MP_APIKEY;

  let queue = [];
  let autoRunning = GM_getValue('autoRunning', false);
  let busy = false;

  const CSS = `
  #mpx-panel{position:fixed;right:16px;bottom:16px;z-index:999999;width:340px;
    background:#11151c;color:#dfe6ee;border:1px solid #2b3442;border-radius:10px;
    font:12px/1.5 -apple-system,"PingFang SC","Microsoft Yahei",sans-serif;
    box-shadow:0 8px 30px rgba(0,0,0,.45);overflow:hidden}
  #mpx-panel .hd{display:flex;align-items:center;gap:8px;padding:8px 10px;
    background:#1a2230;cursor:pointer;user-select:none}
  #mpx-panel .hd b{flex:1;font-size:13px}
  #mpx-panel .bd{padding:10px;display:none;max-height:70vh;overflow:auto}
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
  #mpx-chooser{position:fixed;inset:0;z-index:1000000;background:rgba(0,0,0,.55);
    display:flex;align-items:center;justify-content:center}
  #mpx-chooser .box{width:420px;max-height:70vh;overflow:auto;background:#151a23;
    border:1px solid #2b3442;border-radius:10px;padding:14px;color:#dfe6ee;
    font:13px/1.6 -apple-system,"PingFang SC",sans-serif}
  #mpx-chooser .it{padding:7px 10px;border:1px solid #2b3442;border-radius:7px;
    margin-bottom:7px;cursor:pointer}
  #mpx-chooser .it:hover{border-color:#2f6fed;background:#1a2230}
  #mpx-banner{position:fixed;left:50%;transform:translateX(-50%);bottom:24px;
    z-index:999998;background:#1a2230;color:#fff;border:1px solid #2f6fed;
    border-radius:10px;padding:10px 16px;font:13px/1.5 -apple-system,"PingFang SC",sans-serif;
    box-shadow:0 8px 30px rgba(0,0,0,.5)}
  #mpx-banner button{margin-left:10px;background:#2f6fed;border:0;color:#fff;
    border-radius:6px;padding:4px 10px;cursor:pointer}
  `;

  function log(msg) {
    const el = document.getElementById('mpx-log');
    if (!el) return;
    const t = new Date().toTimeString().slice(0, 8);
    el.textContent = (`[${t}] ${msg}\n` + el.textContent).split('\n').slice(0, 60).join('\n');
  }
  function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
  function todayStr() { return new Date().toISOString().slice(0, 10); }
  function doneKey(t) { return 'mpx_done_' + t.tmdbid; }
  function isDone(t) { return GM_getValue(doneKey(t), '') === todayStr(); }
  function markDone(t) { GM_setValue(doneKey(t), todayStr()); }
  function failCnt(t) { return GM_getValue('mpx_fail_' + t.tmdbid, 0); }
  function addFail(t) { GM_setValue('mpx_fail_' + t.tmdbid, failCnt(t) + 1); }

  // ---------- MP API ----------
  function mpGet(url) {
    return new Promise((resolve, reject) => {
      GM_xmlhttpRequest({
        method: 'GET', url, timeout: 20000,
        onload: r => { try { resolve(JSON.parse(r.responseText)); } catch (e) { reject(new Error('响应非 JSON')); } },
        onerror: () => reject(new Error('网络错误')), ontimeout: () => reject(new Error('超时')),
      });
    });
  }
  function mpPost(url, data) {
    return new Promise((resolve, reject) => {
      GM_xmlhttpRequest({
        method: 'POST', url, timeout: 60000,
        headers: { 'Content-Type': 'application/json' },
        data: JSON.stringify(data),
        onload: r => { try { resolve(JSON.parse(r.responseText)); } catch (e) { reject(new Error('响应非 JSON')); } },
        onerror: () => reject(new Error('网络错误')), ontimeout: () => reject(new Error('超时')),
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

  // ---------- 抓 115 链接 ----------
  function harvest115() {
    const html = document.documentElement.innerHTML || '';
    const rx = /https?:\/\/(?:www\.)?(?:115|115cdn)\.com\/s\/[0-9A-Za-z]+[^"'\s<>\\]*/g;
    const set = new Set();
    let m;
    while ((m = rx.exec(html)) !== null) {
      let u = m[0].replace(/&amp;/g, '&');
      if (u.length > 150) u = u.slice(0, 150);
      set.add(u);
      if (set.size >= MAX_LINKS_PER_TASK) break;
    }
    return [...set];
  }

  async function harvestWithRetry() {
    for (let i = 0; i < SEARCH_RETRY; i++) {
      const links = harvest115();
      if (links.length) return links;
      await sleep(SEARCH_WAIT_MS);
    }
    return [];
  }

  function navTo(path) {
    try {
      history.pushState({}, '', path);
      window.dispatchEvent(new PopStateEvent('popstate'));
    } catch (e) { location.href = path; }
  }

  function findSearchInput() {
    return document.querySelector(
      'input[type=search],input[placeholder*="搜索"],input[placeholder*="搜 索"],' +
      'input[placeholder*="search" i],input[name="keyword"],input[name="q"]');
  }
  function findTmdbInput() {
    // RE0 搜索页的 TMDB ID 输入框（placeholder 形如「输入剧集 TMDB ID...」）
    return document.querySelector(
      'input[placeholder*="TMDB" i],input[placeholder*="tmdb"]');
  }
  async function harvestOnResults(t) {
    // 结果页直接抓 115；抓不到再看是否有指向该 tmdbid 的详情入口，点进去再抓
    let links = await harvestWithRetry();
    if (links.length) return links;
    const detail = document.querySelector(`a[href*="${t.tmdbid}"]`);
    if (detail) {
      const back = location.href;
      detail.click();
      await sleep(SEARCH_WAIT_MS);
      links = await harvestWithRetry();
      if (links.length) return links;
      navTo(back);
      await sleep(1500);
    }
    return [];
  }

  async function searchOnSite(t) {
    const id = String(t.tmdbid);
    // ---- 策略 0（最准）：剧集 TMDB ID 精确查找 ----
    // RE0 搜索页结构（用户实测）：/search?tab=media&type=tv_tmdb_id，输入框直填 ID
    for (const rt of [
      `/search?tab=media&type=tv_tmdb_id&keyword=${id}`,
      `/search?tab=media&type=tv_tmdb_id&q=${id}`,
      `/search?tab=media&type=tv_tmdb_id&kw=${id}`,
      `/search?tab=media&type=tv_tmdb_id&word=${id}`,
    ]) {
      navTo(rt);
      await sleep(SEARCH_WAIT_MS);
      const links = await harvestOnResults(t);
      if (links.length) return links;
    }
    // ---- 策略 0.5：到 TMDB ID 页直接驱动输入框回车 ----
    navTo('/search?tab=media&type=tv_tmdb_id');
    await sleep(SEARCH_WAIT_MS);
    const tin = findTmdbInput();
    if (tin) {
      tin.focus();
      tin.value = id;
      tin.dispatchEvent(new Event('input', { bubbles: true }));
      tin.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', keyCode: 13, bubbles: true }));
      const links = await harvestOnResults(t);
      if (links.length) return links;
    }
    // ---- 策略 1：页面自带搜索框 → 片名回车（兜底） ----
    const kw = t.year ? `${t.title} ${t.year}` : t.title;
    const input = findSearchInput();
    if (input) {
      input.focus();
      input.value = kw;
      input.dispatchEvent(new Event('input', { bubbles: true }));
      input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', keyCode: 13, bubbles: true }));
      await sleep(600);
      const links = await harvestOnResults(t);
      if (links.length) return links;
    }
    // ---- 策略 2：片名搜索路由（兜底） ----
    for (const rt of ['/search?keyword=' + encodeURIComponent(kw),
                      '/search?q=' + encodeURIComponent(kw),
                      '/search/' + encodeURIComponent(kw)]) {
      navTo(rt);
      await sleep(SEARCH_WAIT_MS);
      const links = await harvestOnResults(t);
      if (links.length) return links;
    }
    return [];
  }

  async function submitLinks(t, links, source) {
    log(`✅ 抓到 ${links.length} 条 115 链接，回填【${t.title}】…`);
    const r = await mpPost(API_SUBMIT, {
      tmdbid: t.tmdbid, title: t.title,
      links: links.join('\n'), source: source || 're0_userscript',
    });
    if (r.success) {
      markDone(t);
      queue = queue.filter(x => x.tmdbid !== t.tmdbid);
      updateBadge();
      log(`📤 ${t.title} 回填成功 → SA 转存中`);
      return true;
    }
    addFail(t);
    log(`⚠️ ${t.title} 回填失败：${r.message || '未知'}`);
    return false;
  }

  async function processTask(t) {
    if (isDone(t)) { log(`⏭ ${t.title} 今天已处理过`); return; }
    if (failCnt(t) >= 2) { log(`⏭ ${t.title} 连续失败≥2 次，今天不再试`); return; }
    log(`🔍 ${t.title}（缺 ${t.missing} 集）站内搜索…`);
    const kw = t.year ? `${t.title} ${t.year}` : t.title;
    const links = await searchOnSite(t);
    if (!links.length) {
      addFail(t);
      log(`❌ ${t.title} 没抓到 115 链接`);
      return;
    }
    await submitLinks(t, links, 're0_auto');
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

  // ---------- 面板 ----------
  function taskListHtml() {
    return queue.slice(0, 8).map(t => {
      const sd = Object.entries(t.seasons || {})
        .map(([s, e]) => `S${s}×${(e || []).length}`).join(' ');
      return `<div class="task"><span class="go" data-search="${t.title} ${t.year || ''}">${t.title}</span> ` +
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

  function chooserPick(links) {
    // 本页链接回填：让用户选给哪部剧（默认队列第一部）
    return new Promise((resolve) => {
      const ov = document.createElement('div');
      ov.id = 'mpx-chooser';
      const items = queue.slice(0, 12).map(t =>
        `<div class="it" data-tid="${t.tmdbid}"><b>${t.title}</b>（${t.year || ''}） ` +
        `缺${t.missing}集 / 等${t.wait_days}天</div>`).join('') ||
        '<div class="it">队列为空</div>';
      ov.innerHTML = `<div class="box"><b>本页抓到 ${links.length} 条 115 链接，转给哪部剧？</b>` +
        `<div style="margin:10px 0">${items}</div>` +
        `<button id="mpx-cc">取消</button></div>`;
      document.body.appendChild(ov);
      ov.addEventListener('click', (ev) => {
        const tid = ev.target.getAttribute && ev.target.getAttribute('data-tid');
        if (tid) {
          const t = queue.find(x => String(x.tmdbid) === tid);
          ov.remove(); resolve(t || null);
        } else if (ev.target.id === 'mpx-cc' || ev.target === ov) {
          ov.remove(); resolve(null);
        }
      });
    });
  }

  function buildPanel() {
    if (document.getElementById('mpx-panel')) return;
    GM_addStyle(CSS);
    const p = document.createElement('div');
    p.id = 'mpx-panel';
    p.innerHTML = `
      <div class="hd" id="mpx-toggle"><b>🧩 MP 缺集回填（RE0影巢）</b>
        <span class="cnt" id="mpx-cnt">0</span>
        <span id="mpx-state">待机</span></div>
      <div class="bd">
        <div class="row">
          <button id="mpx-auto">▶ 开始自动找</button>
          <button id="mpx-refresh">刷新队列</button>
          <button id="mpx-page">本页链接回填</button>
        </div>
        <div class="row"><small>队列前 8 部（点剧名去搜索，[单找]自动抓）：</small></div>
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
      const links = harvest115();
      if (!links.length) { log('本页没发现 115 分享链接'); return; }
      const t = await chooserPick(links);
      if (!t) { log('已取消'); return; }
      await submitLinks(t, links, 're0_manual');
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
      const kw = ev.target.getAttribute && ev.target.getAttribute('data-search');
      if (kw) {
        const input = findSearchInput();
        if (input) { input.focus(); input.value = kw.trim();
          input.dispatchEvent(new KeyboardEvent('keydown', {key:'Enter',keyCode:13,bubbles:true})); }
        else { navTo('/search?keyword=' + encodeURIComponent(kw.trim())); }
      }
    });
    const btn = document.getElementById('mpx-auto');
    btn.textContent = autoRunning ? '⏸ 停止' : '▶ 开始自动找';
    btn.classList.toggle('off', !autoRunning);
  }

  // 详情页彩蛋：页面同时出现队列剧名 + 115 链接时弹一键回填条
  let bannerShown = false;
  function maybeBanner() {
    if (bannerShown || document.getElementById('mpx-banner')) return;
    const links = harvest115();
    if (!links.length || !queue.length) return;
    const html = document.documentElement.innerHTML;
    const hit = queue.find(t => t.title && html.includes(t.title));
    if (!hit) return;
    bannerShown = true;
    const b = document.createElement('div');
    b.id = 'mpx-banner';
    b.innerHTML = `发现队列资源【${hit.title}】+ ${links.length} 条 115 链接` +
      `<button id="mpx-go">一键回填</button><button id="mpx-no">忽略</button>`;
    document.body.appendChild(b);
    document.getElementById('mpx-go').onclick = async () => {
      b.remove(); busy = true; await submitLinks(hit, links, 're0_banner'); busy = false;
    };
    document.getElementById('mpx-no').onclick = () => b.remove();
    setTimeout(() => { const x = document.getElementById('mpx-banner'); if (x) x.remove(); }, 30000);
  }

  function main() {
    buildPanel();
    refreshQueue().then(q => log(`已连接 MP，当前缺集队列 ${q.length} 部`))
      .catch(e => log('MP 连接失败：' + e.message));
    setInterval(() => {
      refreshQueue().catch(e => log('轮询失败：' + e.message));
      maybeBanner();
      if (autoRunning && !busy) runAuto();
    }, POLL_SEC * 1000);
    if (autoRunning) runAuto();
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') main();
  else window.addEventListener('DOMContentLoaded', main);
})();
