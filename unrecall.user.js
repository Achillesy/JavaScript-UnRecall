// ==UserScript==
// @name         UnRecall – Chatbot NoTakebacks
// @namespace    https://github.com/Achillesy/JavaScript-UnRecall
// @version      1.6.0
// @description  Captures chatbot replies before content-filter erasure
// @author       Achillesy
// @match        https://chat.deepseek.com/*
// @match        https://www.doubao.com/*
// @match        https://chat2.qianwen.com/*
// @run-at       document-start
// @grant        none
// @updateURL    https://raw.githubusercontent.com/Achillesy/JavaScript-UnRecall/master/unrecall.user.js
// @downloadURL  https://raw.githubusercontent.com/Achillesy/JavaScript-UnRecall/master/unrecall.user.js
// @homepageURL  https://github.com/Achillesy/JavaScript-UnRecall
// ==/UserScript==

// ───────────────────────────────────────────────────────────────────────────
// Architecture
// ───────────────────────────────────────────────────────────────────────────
// Chrome MV3 isolates Tampermonkey's JS context from the page's real JS
// environment.  Even @grant unsafeWindow does not reliably let us replace
// the fetch the page actually calls, and CustomEvents on `window` do not
// cross the isolation boundary.
//
// Solution: stringify ONE function (pageWorld) containing everything — the
// fetch intercept, SSE parser, AND the UI — then inject it via a <script>
// tag.  A <script> element runs in the page's native JS context, with full
// DOM access and the real window.fetch, so no bridging is needed.
//
// Diagnostic side-effect: the purple tab is created by pageWorld itself.
// If you see the tab, injection succeeded.  If you don't, injection was
// blocked (CSP, etc.) and we need a different strategy.
// ───────────────────────────────────────────────────────────────────────────

(function () {
  'use strict';

  function pageWorld() {
    'use strict';

    const ENDPOINT_RE = /(?:\/chat\/completions?\b|\/api\/v2\/chat\b)/i;
    const QIANWEN_RE = /qianwen\.com/i;
    const sessions = [];
    let ui = null;

    // ── path helpers ────────────────────────────────────────────────────────

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

    // ── per-stream processor ────────────────────────────────────────────────

    function createProcessor() {
      const state = {};
      let lastPath = null;
      let lastOp = null;   // 'APPEND' | 'SET' | 'BATCH' — for continuation packets
      let censored = false;
      let done = false;

      function handlePacket(pkt) {
        // Continuation / init packet (no p, no o, just v).
        if (pkt.p === undefined && pkt.o === undefined && pkt.v !== undefined) {
          // Init: {"v": {"response": {...}}}
          if (pkt.v && typeof pkt.v === 'object' && !Array.isArray(pkt.v) && pkt.v.response) {
            state.response = JSON.parse(JSON.stringify(pkt.v.response));
            lastPath = null;
            lastOp = null;
            return;
          }
          if (!lastPath) return;
          // Continuation of a BATCH on the same path: {"v": [ ...sub-patches ]}.
          // This is how DeepSeek delivers post-hoc CONTENT_FILTER replacements
          // after a fully-generated answer — without it the censorship signal
          // is silently dropped.
          if (lastOp === 'BATCH' && Array.isArray(pkt.v)) {
            handleBatch(lastPath, pkt.v);
            return;
          }
          // Continuation of an APPEND: {"v": "next token"}.
          applyAppend(state, lastPath, pkt.v);
          return;
        }
        if (pkt.p === undefined) return;
        lastPath = pkt.p;

        if (pkt.o === 'BATCH') { lastOp = 'BATCH'; handleBatch(pkt.p, pkt.v || []); return; }
        if (pkt.o === 'SET')   { lastOp = 'SET';   applySet(state, pkt.p, pkt.v); return; }
        // APPEND — explicit or implicit (p present, o absent)
        lastOp = 'APPEND';
        applyAppend(state, pkt.p, pkt.v);
      }

      function handleBatch(base, subs) {
        if (subs.some(s => s.p === 'status' && s.v === 'CONTENT_FILTER')) {
          censored = true;
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
        const op = sp.o || 'SET';
        if (op === 'SET') applySet(state, full, sp.v);
        else if (op === 'APPEND') applyAppend(state, full, sp.v);
      }

      function finish() {
        // SSE close event AND XHR readyState=4 (or fetch try/finally) both
        // trigger finish for the same stream — guard against double push.
        if (done) return;
        done = true;
        // Only retain censored responses; normal completions are dropped.
        if (!censored) return;
        const r = state.response;
        if (!r) return;
        const frags = (r.fragments || []).filter(
          f => (f.type === 'THINK' || f.type === 'RESPONSE') && f.content
        );
        if (!frags.length) return;
        sessions.push({
          id: r.message_id,
          fragments: frags,
          time: Date.now(),
        });
        renderPanel();
      }

      return { handlePacket, finish };
    }

    // ── Qianwen processor ───────────────────────────────────────────────────
    // Qianwen streams full accumulated text in each SSE packet under
    // data.messages[].content (mime_type "multi_load/iframe").  Retraction
    // arrives as a separate "event:audit" with {"code":"AU001"}.

    function createQianwenProcessor() {
      let lastContent = '';
      let reqId = null;
      let censored = false;
      let done = false;

      function handlePacket(pkt, eventType) {
        if (eventType === 'audit') {
          if (pkt && pkt.code === 'AU001') censored = true;
          return;
        }
        if (!pkt || !pkt.data) return;
        if (pkt.communication && pkt.communication.reqid) reqId = pkt.communication.reqid;
        const msgs = pkt.data.messages;
        if (!Array.isArray(msgs)) return;
        for (const msg of msgs) {
          if (msg.mime_type === 'multi_load/iframe' && typeof msg.content === 'string') {
            lastContent = msg.content;
          }
        }
      }

      function finish() {
        if (done) return;
        done = true;
        if (!censored || !lastContent) return;
        sessions.push({
          id: reqId || String(Date.now()),
          fragments: [{ type: 'RESPONSE', content: lastContent }],
          time: Date.now(),
        });
        renderPanel();
      }

      return { handlePacket, finish };
    }

    // ── SSE consumer ────────────────────────────────────────────────────────

    async function consumeSSE(stream, url) {
      const proc = QIANWEN_RE.test(url) ? createQianwenProcessor() : createProcessor();
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
          buf = lines.pop();
          for (const raw of lines) {
            const line = raw.trimEnd();
            if (line.startsWith('event:')) {
              evt = line.slice(6).trim();
            } else if (line.startsWith('data:')) {
              if (evt === 'close') { proc.finish(); return; }
              if (evt === 'audit') {
                try { proc.handlePacket(JSON.parse(line.slice(5).trim()), 'audit'); } catch { /* skip */ }
                proc.finish(); return;
              }
              try { proc.handlePacket(JSON.parse(line.slice(5).trim())); } catch { /* skip */ }
            } else if (line === '') {
              evt = null;
            }
          }
        }
      } finally {
        proc.finish();
      }
    }

    // ── network interception (fetch / XHR / EventSource) ───────────────────
    // All three are hooked so SSE can be captured regardless of which API the
    // chat app uses (DeepSeek uses XHR; other chatbots may differ).

    // Process an SSE text stream chunked line-by-line (used by XHR path).
    function feedSSEChunk(state, chunk) {
      state.buf += chunk;
      const lines = state.buf.split('\n');
      state.buf = lines.pop();
      for (const raw of lines) {
        const line = raw.trimEnd();
        if (line.startsWith('event:')) {
          state.evt = line.slice(6).trim();
        } else if (line.startsWith('data:')) {
          if (state.evt === 'close') { state.proc.finish(); return; }
          if (state.evt === 'audit') {
            try { state.proc.handlePacket(JSON.parse(line.slice(5).trim()), 'audit'); } catch { /* skip */ }
            state.proc.finish(); return;
          }
          try { state.proc.handlePacket(JSON.parse(line.slice(5).trim())); } catch { /* skip */ }
        } else if (line === '') {
          state.evt = null;
        }
      }
    }

    // ── fetch ──────────────────────────────────────────────────────────────
    const _fetch = window.fetch.bind(window);
    window.fetch = async function (...args) {
      const url = args[0] instanceof Request ? args[0].url : String(args[0]);
      const res = await _fetch.apply(this, args);
      if (!ENDPOINT_RE.test(url)) return res;
      if (!(res.headers.get('content-type') || '').includes('text/event-stream')) return res;
      const [s1, s2] = res.body.tee();
      consumeSSE(s1, url);
      return new Response(s2, {
        status: res.status,
        statusText: res.statusText,
        headers: res.headers,
      });
    };

    // ── XMLHttpRequest ─────────────────────────────────────────────────────
    const _xhrOpen = XMLHttpRequest.prototype.open;
    const _xhrSend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.open = function (method, url, ...rest) {
      const u = String(url);
      this.__urMatch = ENDPOINT_RE.test(u);
      this.__urUrl = this.__urMatch ? u : null;
      return _xhrOpen.apply(this, [method, url, ...rest]);
    };
    XMLHttpRequest.prototype.send = function (body) {
      if (this.__urMatch) {
        const proc = QIANWEN_RE.test(this.__urUrl) ? createQianwenProcessor() : createProcessor();
        const sse = { proc, buf: '', evt: null, processed: 0 };
        this.addEventListener('readystatechange', () => {
          // readyState 3 = LOADING (chunks arriving), 4 = DONE
          if (this.readyState >= 3) {
            const text = this.responseText || '';
            if (text.length > sse.processed) {
              feedSSEChunk(sse, text.slice(sse.processed));
              sse.processed = text.length;
            }
          }
          if (this.readyState === 4) sse.proc.finish();
        });
      }
      return _xhrSend.call(this, body);
    };

    // ── EventSource ────────────────────────────────────────────────────────
    const _ES = window.EventSource;
    if (_ES) {
      const WrappedES = function (url, init) {
        const es = new _ES(url, init);
        if (ENDPOINT_RE.test(String(url))) {
          const proc = createProcessor();
          es.addEventListener('message', (e) => {
            try { proc.handlePacket(JSON.parse(e.data)); } catch { /* skip */ }
          });
          es.addEventListener('close', () => proc.finish());
          es.addEventListener('error', () => proc.finish());
        }
        return es;
      };
      WrappedES.prototype = _ES.prototype;
      Object.assign(WrappedES, _ES);
      window.EventSource = WrappedES;
    }

    // ── UI ──────────────────────────────────────────────────────────────────

    const CSS = `
      /* ── Recycle-bin tab on right edge ────────────────────────────────── */
      #ur-tab {
        all: initial;
        position: fixed;
        top: 120px;
        right: 0;
        width: 44px;
        height: 56px;
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-right: none;
        border-radius: 10px 0 0 10px;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
        z-index: 2147483647;
        box-shadow: -3px 0 12px rgba(0,0,0,.08);
        user-select: none;
        color: #6b7280;
        transition: color .15s, background .15s;
      }
      #ur-tab:hover {
        background: #f9fafb;
        color: #4d6bfe;
      }
      #ur-tab[data-full="1"] {
        color: #dc2626;
        background: #fef2f2;
        border-color: #fecaca;
      }
      #ur-tab[data-full="1"]:hover {
        background: #fee2e2;
      }
      #ur-tab svg { display: block; }
      #ur-tab .ur-icon-full { display: none; }
      #ur-tab[data-full="1"] .ur-icon-empty { display: none; }
      #ur-tab[data-full="1"] .ur-icon-full { display: block; }
      #ur-tab-cnt {
        position: absolute;
        top: -6px;
        right: -6px;
        background: #dc2626;
        color: #fff;
        border-radius: 99px;
        font: 700 11px/18px -apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif;
        min-width: 20px;
        height: 20px;
        text-align: center;
        padding: 0 5px;
        box-shadow: 0 2px 6px rgba(220,38,38,.35);
        display: none;
      }
      #ur-tab-cnt.ur-show { display: block; }

      /* ── Slide-in panel ───────────────────────────────────────────────── */
      #ur-panel {
        all: initial;
        position: fixed;
        top: 120px;
        right: 44px;
        width: 420px;
        max-height: calc(100vh - 140px);
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-right: none;
        border-radius: 10px 0 0 10px;
        box-shadow: -8px 0 32px rgba(0,0,0,.12);
        z-index: 2147483646;
        display: flex;
        flex-direction: column;
        font: 15px/1.65 -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;
        color: #1f2937;
        transform: translateX(calc(100% + 44px));
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
        gap: 10px;
        padding: 12px 16px;
        background: #f9fafb;
        border-bottom: 1px solid #e5e7eb;
        border-radius: 10px 0 0 0;
        flex-shrink: 0;
      }
      #ur-head .ur-name {
        flex: 1;
        font-weight: 600;
        font-size: 14px;
        color: #4d6bfe;
        letter-spacing: .5px;
      }
      #ur-head .ur-close {
        color: #9ca3af;
        cursor: pointer;
        font-size: 18px;
        line-height: 1;
        user-select: none;
        padding: 0 4px;
      }
      #ur-head .ur-close:hover { color: #1f2937; }

      #ur-body { overflow-y: auto; flex: 1; }
      .ur-empty {
        color: #9ca3af;
        text-align: center;
        padding: 48px 20px;
        font-size: 14px;
        line-height: 1.7;
      }

      /* ── Capture cards ───────────────────────────────────────────────── */
      .ur-card { border-bottom: 1px solid #f3f4f6; padding: 14px 16px; }
      .ur-card:last-child { border-bottom: none; }
      .ur-card-head {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 12px;
      }
      .ur-badge {
        font-size: 12px;
        font-weight: 600;
        letter-spacing: .3px;
        padding: 3px 10px;
        border-radius: 99px;
        background: #fee2e2;
        color: #dc2626;
        border: 1px solid #fecaca;
      }
      .ur-meta { font-size: 12px; color: #9ca3af; margin-left: auto; }

      .ur-frag { margin-bottom: 10px; }
      .ur-flabel {
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: .6px;
        padding: 4px 0 6px;
        cursor: pointer;
        user-select: none;
      }
      .ur-think    { color: #6366f1; }
      .ur-response { color: #4d6bfe; }
      .ur-chevron {
        display: inline-block;
        font-size: 9px;
        transition: transform .15s;
        width: 10px;
        text-align: center;
      }
      .ur-chevron.open { transform: rotate(90deg); }
      .ur-fcontent {
        background: #f7f7f9;
        border: 1px solid #e5e7eb;
        border-radius: 6px;
        padding: 12px 14px;
        max-height: 320px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-break: break-word;
        font-size: 14px;
        line-height: 1.7;
        color: #1f2937;
      }
      .ur-fcontent.ur-hidden { display: none; }
    `;

    function ensureUI() {
      // SPA navigation (DeepSeek left-sidebar switch / new chat) re-renders
      // <body>'s subtree and React strips out our tab/panel since it doesn't
      // own them. Detect detachment and rebuild from scratch.
      if (ui && document.body && !document.body.contains(ui.tab)) {
        ui = null;
      }
      if (ui || !document.body) return;

      const style = document.createElement('style');
      style.textContent = CSS;
      (document.head || document.documentElement).appendChild(style);

      const tab = document.createElement('div');
      tab.id = 'ur-tab';
      tab.title = 'UnRecall';
      tab.innerHTML =
        // Empty bin (default)
        '<svg class="ur-icon-empty" width="22" height="22" viewBox="0 0 24 24" fill="none" ' +
          'stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">' +
          '<path d="M3 6h18"/>' +
          '<path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/>' +
          '<path d="M9 6V4a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2v2"/>' +
          '<path d="M10 11v6M14 11v6"/>' +
        '</svg>' +
        // Full bin (with crumpled paper inside)
        '<svg class="ur-icon-full" width="22" height="22" viewBox="0 0 24 24" fill="none" ' +
          'stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">' +
          '<path d="M3 6h18"/>' +
          '<path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/>' +
          '<path d="M9 6V4a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2v2"/>' +
          // Crumpled papers
          '<circle cx="9.5" cy="13" r="1.3" fill="currentColor" stroke="none"/>' +
          '<circle cx="14.2" cy="14" r="1.6" fill="currentColor" stroke="none"/>' +
          '<circle cx="11.5" cy="17" r="1.2" fill="currentColor" stroke="none"/>' +
        '</svg>' +
        '<span id="ur-tab-cnt"></span>';
      document.body.appendChild(tab);

      const panel = document.createElement('div');
      panel.id = 'ur-panel';
      panel.innerHTML =
        '<div id="ur-head">' +
          '<span class="ur-name">UnRecall · 回收站</span>' +
          '<span class="ur-close" id="ur-close">✕</span>' +
        '</div>' +
        '<div id="ur-body">' +
          '<div class="ur-empty">回收站是空的<br>当对话被撤销时，撤回的内容会出现在这里</div>' +
        '</div>';
      document.body.appendChild(panel);

      tab.addEventListener('click', () => panel.classList.toggle('ur-open'));
      document.getElementById('ur-close').addEventListener('click', () => panel.classList.remove('ur-open'));
      document.getElementById('ur-body').addEventListener('click', e => {
        const label = e.target.closest('.ur-flabel');
        if (!label) return;
        label.querySelector('.ur-chevron').classList.toggle('open');
        label.nextElementSibling.classList.toggle('ur-hidden');
      });

      ui = {
        panel,
        tab,
        body: document.getElementById('ur-body'),
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

      // Switch trash bin between empty and full state
      if (sessions.length > 0) {
        tab.dataset.full = '1';
        tabCnt.textContent = sessions.length;
        tabCnt.classList.add('ur-show');
      } else {
        delete tab.dataset.full;
        tabCnt.classList.remove('ur-show');
      }

      if (!sessions.length) return;

      // Newest first
      body.innerHTML = sessions.slice().reverse().map(s => {
        const fragsHTML = s.fragments.map(f => {
          const kind = f.type === 'THINK' ? 'think' : 'response';
          const label = f.type === 'THINK' ? '◆ THINK · 思考过程' : '◆ RESPONSE · 回复内容';
          // Both expanded by default
          return (
            '<div class="ur-frag">' +
              '<div class="ur-flabel ur-' + kind + '">' +
                '<span class="ur-chevron open">▶</span>' + label +
              '</div>' +
              '<div class="ur-fcontent">' + esc(f.content || '(empty)') + '</div>' +
            '</div>'
          );
        }).join('');

        const time = new Date(s.time).toLocaleTimeString();

        return (
          '<div class="ur-card">' +
            '<div class="ur-card-head">' +
              '<span class="ur-badge">✂ 已撤回</span>' +
              '<span class="ur-meta">msg #' + esc(s.id) + ' · ' + esc(time) + '</span>' +
            '</div>' +
            fragsHTML +
          '</div>'
        );
      }).join('');

      // Auto-open the panel whenever a new censored response arrives
      panel.classList.add('ur-open');
    }

    // ── boot + SPA-resilient UI mounting ────────────────────────────────────
    // The XHR / fetch / EventSource hooks are installed on prototypes once and
    // survive SPA navigation forever. The UI elements, however, get torn down
    // by React when the user switches conversations. Three layers of defense:
    //   1. ensureUI auto-detects detached tab and rebuilds.
    //   2. MutationObserver watches <body> and re-mounts on demand.
    //   3. history.pushState / replaceState / popstate hooks force a check
    //      right after every SPA navigation.
    // The captured `sessions` array lives in this closure and persists across
    // navigations, so re-mounting just re-renders the existing list.

    function mountOrRemount() {
      if (!document.body) return;
      ensureUI();
      // Re-render content whenever we (re)mount so the bin shows the right
      // empty/full state and any previously captured sessions reappear.
      renderPanel();
    }

    function watchBody() {
      if (!document.body) return;
      const obs = new MutationObserver(() => {
        if (ui && !document.body.contains(ui.tab)) {
          ui = null;
          mountOrRemount();
        }
      });
      obs.observe(document.body, { childList: true });
    }

    function watchNavigation() {
      const wrap = (key) => {
        const orig = history[key];
        if (!orig) return;
        history[key] = function () {
          const result = orig.apply(this, arguments);
          // React updates the tree synchronously; wait one frame to let it
          // settle before we re-mount.
          setTimeout(mountOrRemount, 50);
          return result;
        };
      };
      wrap('pushState');
      wrap('replaceState');
      window.addEventListener('popstate', () => setTimeout(mountOrRemount, 50));
    }

    function boot() {
      mountOrRemount();
      watchBody();
      watchNavigation();
    }

    if (document.body) {
      boot();
    } else {
      document.addEventListener('DOMContentLoaded', boot, { once: true });
    }
  }

  // ── inject pageWorld() into the page's native JS context ─────────────────
  // <script> elements run in the page world regardless of MV2/MV3 isolation.

  const injected = document.createElement('script');
  injected.textContent = '(' + pageWorld.toString() + ')()';
  (document.documentElement || document.head || document.body).prepend(injected);
  injected.remove();

})();
