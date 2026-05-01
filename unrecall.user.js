// ==UserScript==
// @name         UnRecall – Chatbot NoTakebacks
// @namespace    https://github.com/Achillesy/JavaScript-UnRecall
// @version      1.1.0
// @description  Captures DeepSeek replies before content-filter erasure
// @author       Achillesy
// @match        https://chat.deepseek.com/*
// @run-at       document-start
// @grant        none
// @updateURL    https://raw.githubusercontent.com/Achillesy/JavaScript-UnRecall/master/unrecall.user.js
// @downloadURL  https://raw.githubusercontent.com/Achillesy/JavaScript-UnRecall/master/unrecall.user.js
// @homepageURL  https://github.com/Achillesy/JavaScript-UnRecall
// ==/UserScript==

// Confirm the actual SSE endpoint in DevTools → Network tab (filter: chat/completion).
// Update ENDPOINT_RE below if the path differs.

(function () {
  'use strict';

  // ── config ────────────────────────────────────────────────────────────────

  const ENDPOINT_RE = /\/chat\/completions?\b/i;

  // ── state ─────────────────────────────────────────────────────────────────

  const sessions = []; // captured messages (newest last)
  let ui = null;       // { panel, body, cnt, arr }

  // ── path helpers ──────────────────────────────────────────────────────────

  function nav(root, parts, depth) {
    let cur = root;
    for (let i = 0; i < depth; i++) {
      if (cur == null) return undefined;
      const p = parts[i];
      cur = p === '-1' ? cur[cur.length - 1] : cur[p];
    }
    return cur;
  }

  function applySet(root, path, value) {
    const parts = path.split('/');
    const parent = nav(root, parts, parts.length - 1);
    if (parent == null) return;
    const k = parts[parts.length - 1];
    parent[k === '-1' ? parent.length - 1 : k] = value;
  }

  function applyAppend(root, path, value) {
    const parts = path.split('/');
    const parent = nav(root, parts, parts.length - 1);
    if (parent == null) return;
    const rawK = parts[parts.length - 1];
    const k = rawK === '-1' ? parent.length - 1 : rawK;
    const target = parent[k];
    if (Array.isArray(target)) {
      Array.isArray(value) ? target.push(...value) : target.push(value);
    } else {
      parent[k] = (target == null ? '' : String(target)) + value;
    }
  }

  // ── per-stream processor ──────────────────────────────────────────────────

  function createProcessor() {
    const state = {};
    let lastPath = null;
    let censored = false;

    function handlePacket(pkt) {
      // Continuation token: {"v":"..."} — only v field, no p or o
      if (pkt.p === undefined && pkt.o === undefined && pkt.v !== undefined) {
        if (pkt.v && typeof pkt.v === 'object' && pkt.v.response) {
          // Initialization packet: {"v":{"response":{...}}}
          state.response = JSON.parse(JSON.stringify(pkt.v.response));
          lastPath = null;
        } else if (typeof pkt.v === 'string' && lastPath) {
          applyAppend(state, lastPath, pkt.v);
        }
        return;
      }

      if (pkt.p === undefined) return;
      lastPath = pkt.p;

      if (pkt.o === 'BATCH') {
        handleBatch(pkt.p, pkt.v || []);
        return;
      }
      if (pkt.o === 'SET') {
        applySet(state, pkt.p, pkt.v);
        return;
      }
      // APPEND — explicit (o:"APPEND") or implicit (p present, o absent)
      applyAppend(state, pkt.p, pkt.v);
    }

    function handleBatch(base, subs) {
      // Detect CONTENT_FILTER: look for {"p":"status","v":"CONTENT_FILTER"}
      if (subs.some(s => s.p === 'status' && s.v === 'CONTENT_FILTER')) {
        censored = true;
        // Skip sub-patches that would destroy accumulated content or hide the censor
        const skip = new Set(['fragments', 'status', 'ban_regenerate', 'quasi_status']);
        for (const s of subs) {
          if (!skip.has(s.p)) applySubPatch(base, s);
        }
        return;
      }
      for (const s of subs) applySubPatch(base, s);
    }

    function applySubPatch(base, sp) {
      const full = `${base}/${sp.p}`;
      // Sub-patches inside BATCH default to SET when o is absent
      const op = sp.o || 'SET';
      if (op === 'SET') applySet(state, full, sp.v);
      else if (op === 'APPEND') applyAppend(state, full, sp.v);
    }

    function finish() {
      const r = state.response;
      if (!r) return;
      const frags = r.fragments || [];
      if (!frags.some(f => f.content)) return; // nothing captured
      sessions.push({
        id: r.message_id,
        censored,
        fragments: JSON.parse(JSON.stringify(frags)),
        time: Date.now(),
      });
      renderPanel();
    }

    return { handlePacket, finish };
  }

  // ── SSE consumer ─────────────────────────────────────────────────────────

  async function consumeSSE(stream) {
    const proc = createProcessor();
    const decoder = new TextDecoder();
    const reader = stream.getReader();
    let buf = '';
    let evt = null;

    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const lines = buf.split('\n');
        buf = lines.pop(); // keep trailing incomplete line
        for (const raw of lines) {
          const line = raw.trimEnd();
          if (line.startsWith('event:')) {
            evt = line.slice(6).trim();
          } else if (line.startsWith('data:')) {
            if (evt === 'close') { proc.finish(); return; }
            try { proc.handlePacket(JSON.parse(line.slice(5).trim())); } catch { /* ignore */ }
          } else if (line === '') {
            evt = null; // blank line resets event name per SSE spec
          }
        }
      }
    } finally {
      proc.finish();
    }
  }

  // ── fetch intercept ───────────────────────────────────────────────────────

  const _fetch = window.fetch;
  window.fetch = async function (...args) {
    const url = args[0] instanceof Request ? args[0].url : String(args[0]);
    const res = await _fetch.apply(this, args);
    if (!ENDPOINT_RE.test(url)) return res;
    if (!(res.headers.get('content-type') || '').includes('text/event-stream')) return res;
    const [s1, s2] = res.body.tee();
    consumeSSE(s1);
    return new Response(s2, {
      status: res.status,
      statusText: res.statusText,
      headers: res.headers,
    });
  };

  // ── UI ────────────────────────────────────────────────────────────────────

  const CSS = `
    /* ── always-visible tab on the right edge ── */
    #ur-tab {
      all: initial;
      position: fixed;
      top: 120px;
      right: 0;
      width: 40px;
      height: 68px;
      background: linear-gradient(170deg, #3535bb, #6040cc);
      border: 2px solid #7766ff;
      border-right: none;
      border-radius: 10px 0 0 10px;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 5px;
      cursor: pointer;
      z-index: 2147483647;
      box-shadow: -4px 2px 18px rgba(80,60,230,.5);
      user-select: none;
      transition: filter .15s;
    }
    #ur-tab:hover { filter: brightness(1.25); }
    #ur-tab-icon { font-size: 18px; line-height: 1; }
    #ur-tab-cnt {
      background: #ee2222;
      color: #fff;
      border-radius: 99px;
      font: 700 10px/18px -apple-system,BlinkMacSystemFont,sans-serif;
      min-width: 18px;
      height: 18px;
      text-align: center;
      padding: 0 4px;
      display: none;
    }
    #ur-tab-cnt.ur-show { display: block; }
    @keyframes ur-flash {
      0%,100% { filter: brightness(1); }
      50%      { filter: brightness(1.8) saturate(1.4); }
    }
    #ur-tab.ur-flash { animation: ur-flash .45s ease 3; }

    /* ── slide-in panel (sits left of the tab) ── */
    #ur-panel {
      all: initial;
      position: fixed;
      top: 120px;
      right: 40px;
      width: 360px;
      max-height: calc(100vh - 140px);
      background: #12121f;
      border: 1px solid #2a2a55;
      border-right: none;
      border-radius: 10px 0 0 10px;
      box-shadow: -6px 0 24px rgba(20,20,100,.55);
      z-index: 2147483646;
      display: flex;
      flex-direction: column;
      font: 13px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
      color: #d0d0e8;
      transform: translateX(calc(100% + 40px));
      transition: transform .25s ease;
      pointer-events: none;
    }
    #ur-panel.ur-open {
      transform: translateX(0);
      pointer-events: auto;
    }

    #ur-head {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 9px 12px;
      background: #0c0c1a;
      border-bottom: 1px solid #2a2a55;
      border-radius: 10px 0 0 0;
      flex-shrink: 0;
    }
    #ur-head .ur-name {
      flex: 1;
      font-weight: 700;
      font-size: 11px;
      letter-spacing: 1px;
      color: #8888cc;
    }
    #ur-head .ur-close {
      color: #5050a0;
      cursor: pointer;
      font-size: 16px;
      line-height: 1;
      user-select: none;
      padding: 0 2px;
    }
    #ur-head .ur-close:hover { color: #aaaadd; }

    #ur-body { overflow-y: auto; flex: 1; }
    .ur-empty {
      color: #383860;
      text-align: center;
      padding: 36px 16px;
      font-size: 12px;
      line-height: 1.6;
    }

    .ur-card { border-bottom: 1px solid #1c1c30; padding: 10px 12px; }
    .ur-card:last-child { border-bottom: none; }

    .ur-card-head {
      display: flex;
      align-items: center;
      gap: 6px;
      margin-bottom: 8px;
    }
    .ur-badge {
      font-size: 10px;
      font-weight: 700;
      letter-spacing: .5px;
      padding: 2px 8px;
      border-radius: 99px;
    }
    .ur-ok  { background: #0c2a10; color: #5db86a; border: 1px solid #1a4a22; }
    .ur-cut { background: #2a0c0c; color: #cc5555; border: 1px solid #4a1818; }
    .ur-meta { font-size: 10px; color: #34345a; margin-left: auto; }

    .ur-frag { margin-bottom: 5px; }
    .ur-flabel {
      display: flex;
      align-items: center;
      gap: 5px;
      font-size: 10px;
      font-weight: 700;
      letter-spacing: .7px;
      padding: 3px 2px;
      cursor: pointer;
      user-select: none;
    }
    .ur-think    { color: #5a68b8; }
    .ur-response { color: #4a9858; }
    .ur-other    { color: #985050; }
    .ur-chevron {
      display: inline-block;
      font-size: 8px;
      transition: transform .15s;
      width: 10px;
      text-align: center;
    }
    .ur-chevron.open { transform: rotate(90deg); }

    .ur-fcontent {
      background: #0a0a18;
      border: 1px solid #1c1c34;
      border-radius: 4px;
      padding: 8px 10px;
      max-height: 200px;
      overflow-y: auto;
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 12px;
      line-height: 1.55;
      color: #b0b0cc;
    }
    .ur-fcontent.ur-hidden { display: none; }
  `;

  function ensureUI() {
    if (ui || !document.body) return;

    const style = document.createElement('style');
    style.textContent = CSS;
    (document.head || document.documentElement).appendChild(style);

    // Always-visible tab button
    const tab = document.createElement('div');
    tab.id = 'ur-tab';
    tab.innerHTML = `<span id="ur-tab-icon">🔍</span><span id="ur-tab-cnt"></span>`;
    document.body.appendChild(tab);

    // Slide-in panel
    const panel = document.createElement('div');
    panel.id = 'ur-panel';
    panel.innerHTML =
      `<div id="ur-head">` +
        `<span class="ur-name">UNRECALL</span>` +
        `<span class="ur-close" id="ur-close">✕</span>` +
      `</div>` +
      `<div id="ur-body">` +
        `<div class="ur-empty">Watching for responses…<br>Nothing captured yet.</div>` +
      `</div>`;
    document.body.appendChild(panel);

    tab.addEventListener('click', () => panel.classList.toggle('ur-open'));
    document.getElementById('ur-close').addEventListener('click', () => panel.classList.remove('ur-open'));

    document.getElementById('ur-body').addEventListener('click', e => {
      const label = e.target.closest('.ur-flabel');
      if (!label) return;
      const chevron = label.querySelector('.ur-chevron');
      const content = label.nextElementSibling;
      if (chevron) chevron.classList.toggle('open');
      if (content) content.classList.toggle('ur-hidden');
    });

    ui = {
      panel,
      tab,
      body:   document.getElementById('ur-body'),
      tabCnt: document.getElementById('ur-tab-cnt'),
    };
  }

  function esc(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  function renderPanel() {
    if (!document.body) {
      document.addEventListener('DOMContentLoaded', renderPanel, { once: true });
      return;
    }
    ensureUI();
    if (!ui) return;

    const { panel, tab, body, tabCnt } = ui;

    // Update tab badge
    if (sessions.length > 0) {
      tabCnt.textContent = sessions.length;
      tabCnt.classList.add('ur-show');
    }

    // Flash tab to signal new capture
    tab.classList.remove('ur-flash');
    void tab.offsetWidth; // reflow to restart animation
    tab.classList.add('ur-flash');

    if (!sessions.length) return;

    body.innerHTML = sessions.slice().reverse().map(s => {
      const fragsHTML = s.fragments.map(f => {
        const kind =
          f.type === 'THINK'    ? 'think'    :
          f.type === 'RESPONSE' ? 'response' : 'other';
        const label =
          f.type === 'THINK'    ? '◆ THINK'    :
          f.type === 'RESPONSE' ? '◆ RESPONSE' : `◆ ${esc(f.type)}`;
        // THINK collapsed by default; RESPONSE and others open
        const open = f.type !== 'THINK';
        return (
          `<div class="ur-frag">` +
            `<div class="ur-flabel ur-${kind}">` +
              `<span class="ur-chevron${open ? ' open' : ''}">▶</span>${label}` +
            `</div>` +
            `<div class="ur-fcontent${open ? '' : ' ur-hidden'}">${esc(f.content || '(empty)')}</div>` +
          `</div>`
        );
      }).join('');

      const badgeCls = s.censored ? 'ur-cut' : 'ur-ok';
      const badgeTxt = s.censored ? '✂ CENSORED' : '✓ FINISHED';
      const time = new Date(s.time).toLocaleTimeString();

      return (
        `<div class="ur-card">` +
          `<div class="ur-card-head">` +
            `<span class="ur-badge ${badgeCls}">${badgeTxt}</span>` +
            `<span class="ur-meta">msg #${esc(s.id)} · ${esc(time)}</span>` +
          `</div>` +
          fragsHTML +
        `</div>`
      );
    }).join('');

    // Auto-open on first capture and on every censored response
    const last = sessions[sessions.length - 1];
    if (sessions.length === 1 || last.censored) {
      panel.classList.add('ur-open');
    }
  }

})();
