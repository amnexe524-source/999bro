// =========================================================
//  INVENTORY WORKER — Cloudflare Workers + KV
//  ✅ Rate-limit safe (batch writes ทุก 30s)
//  ✅ SSE polling แทน long-connection
//  ✅ Telegram Bot + Mini App
//  ✅ Item Alert System
//  ✅ Dungeon runs, Farm session, Config, Icons
// =========================================================

const BOT_TOKEN     = "8746970065:AAGwhDSRhbxPlz-eVqzBUkxAMUz9u-IVn9k";
const TELEGRAM_API  = `https://api.telegram.org/bot${BOT_TOKEN}`;

// ── KV Write Batcher ─────────────────────────────────────
// KV free = 1,000 writes/day → batch เขียนทุก 30s
let _kvPending   = {};   // { key: value }
let _kvLastFlush = 0;
const KV_BATCH_MS = 30_000;

async function kvSet(env, key, value) {
  _kvPending[key] = value;
  const now = Date.now();
  if (now - _kvLastFlush >= KV_BATCH_MS) {
    await kvFlush(env);
  }
}

async function kvFlush(env) {
  const keys = Object.keys(_kvPending);
  if (!keys.length) return;
  await Promise.all(keys.map(k => env.KV.put(k, _kvPending[k])));
  _kvPending   = {};
  _kvLastFlush = Date.now();
}

async function kvGet(env, key) {
  // ถ้ามีใน pending buffer ให้ใช้ค่านั้นก่อน (สด)
  if (_kvPending[key] !== undefined) return _kvPending[key];
  return env.KV.get(key);
}

// ── CORS headers ─────────────────────────────────────────
const CORS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
};
function cors(body, status = 200, extra = {}) {
  return new Response(body, { status, headers: { ...CORS, "Content-Type": "application/json", ...extra } });
}
function html(body) {
  return new Response(body, { headers: { ...CORS, "Content-Type": "text/html;charset=UTF-8" } });
}

// ── Main fetch ───────────────────────────────────────────
export default {
  async fetch(request, env) {
    const url    = new URL(request.url);
    const path   = url.pathname;
    const method = request.method;

    if (method === "OPTIONS") return new Response(null, { headers: CORS });

    // ════════════════════════════════════════════════════
    // POST /  ← Inventory จาก Lua
    // ════════════════════════════════════════════════════
    if (method === "POST" && path === "/") {
      let body;
      try { body = await request.json(); } catch { return cors('{"error":"invalid json"}', 400); }

      await kvSet(env, "inventory", JSON.stringify(body));
      await kvSet(env, "inv_updated", new Date().toISOString());
      await kvFlush(env); // flush ทันทีสำหรับ inventory (สำคัญ)

      // ── Item Alert Check ──────────────────────────────
      const alertsRaw = await kvGet(env, "alerts");
      if (alertsRaw) {
        const alerts  = JSON.parse(alertsRaw);
        const chatId  = await kvGet(env, "chat_id");
        const firedRaw = await kvGet(env, "alerts_fired") || "{}";
        const fired    = JSON.parse(firedRaw);
        const flat     = flattenInv(body);

        for (const alert of alerts) {
          if (!alert.item || !alert.qty || !alert.enabled) continue;
          const current = flat[alert.item] || 0;
          const fireKey = `${alert.item}__${alert.qty}__${alert.mode}`;

          if (alert.mode === "above" && current >= Number(alert.qty)) {
            if (!fired[fireKey] && chatId) {
              await sendTelegram(chatId,
                `🚨 *ITEM ALERT*\n` +
                `📦 *${alert.item}* ถึง ${current.toLocaleString()} แล้ว!\n` +
                `_(กำหนดไว้ที่ ≥ ${Number(alert.qty).toLocaleString()})_`
              );
              fired[fireKey] = Date.now();
            }
          } else if (alert.mode === "below" && current <= Number(alert.qty)) {
            if (!fired[fireKey] && chatId) {
              await sendTelegram(chatId,
                `⚠️ *ITEM LOW ALERT*\n` +
                `📦 *${alert.item}* เหลือแค่ ${current.toLocaleString()}!\n` +
                `_(กำหนดไว้ที่ ≤ ${Number(alert.qty).toLocaleString()})_`
              );
              fired[fireKey] = Date.now();
            }
          } else {
            // reset fired ถ้าออกจาก threshold แล้ว
            if (fired[fireKey]) {
              delete fired[fireKey];
            }
          }
        }
        await env.KV.put("alerts_fired", JSON.stringify(fired));
      }

      return cors('{"ok":true}');
    }

    // ════════════════════════════════════════════════════
    // POST /stats ← Stats+Wave จาก Lua
    // ════════════════════════════════════════════════════
    if (method === "POST" && path === "/stats") {
      let body;
      try { body = await request.json(); } catch { return cors('{"error":"invalid json"}', 400); }

      await kvSet(env, "stats",         JSON.stringify(body));
      await kvSet(env, "stats_updated", new Date().toISOString());

      // Wave change notification
      const prevWave = await kvGet(env, "last_wave") || "";
      const newWave  = String(body.wave || "").trim();
      if (newWave && newWave !== prevWave && newWave !== "Not in Dungeon") {
        await env.KV.put("last_wave", newWave);
        const chatId = await kvGet(env, "chat_id");
        if (chatId) {
          await sendTelegram(chatId, `🌊 Wave เปลี่ยนเป็น: *${newWave}*`);
        }
      }

      await kvFlush(env);
      return cors('{"ok":true}');
    }

    // ════════════════════════════════════════════════════
    // GET /events ← SSE polling (Cloudflare ไม่รองรับ SSE จริง)
    // HTML จะ poll ทุก 3s แทน
    // ════════════════════════════════════════════════════
    if (method === "GET" && path === "/events") {
      const inv     = await kvGet(env, "inventory");
      const stats   = await kvGet(env, "stats");
      const dungeon = await kvGet(env, "dungeon_runs");
      const icons   = await kvGet(env, "custom_icons");
      const updated = await kvGet(env, "inv_updated");

      const payload = {
        type:        "all",
        inventory:   inv    ? JSON.parse(inv)    : null,
        stats:       stats  ? JSON.parse(stats)  : null,
        dungeonRuns: dungeon ? JSON.parse(dungeon): null,
        icons:       icons  ? JSON.parse(icons)  : {},
        updated,
      };
      return cors(JSON.stringify(payload));
    }

    // ════════════════════════════════════════════════════
    // POST /dungeon-runs
    // ════════════════════════════════════════════════════
    if (method === "POST" && path === "/dungeon-runs") {
      let body;
      try { body = await request.json(); } catch { return cors('{"error":"invalid json"}', 400); }
      await kvSet(env, "dungeon_runs", JSON.stringify(body));
      await kvFlush(env);
      return cors('{"ok":true}');
    }

    // ════════════════════════════════════════════════════
    // POST /farm-session
    // ════════════════════════════════════════════════════
    if (method === "POST" && path === "/farm-session") {
      let body;
      try { body = await request.json(); } catch { return cors('{"error":"invalid json"}', 400); }

      if (body.action === "stop") {
        await kvSet(env, "farm_session_active", "false");
      } else if (body.action === "start") {
        await kvSet(env, "farm_session_active", "true");
        await kvSet(env, "farm_session_start",  new Date().toISOString());
        if (body.snapshot) await kvSet(env, "farm_snapshot", JSON.stringify(body.snapshot));
      } else {
        // save diff
        await kvSet(env, "farm_session", JSON.stringify(body));
      }
      await kvFlush(env);
      return cors('{"ok":true}');
    }

    // GET /farm-session
    if (method === "GET" && path === "/farm-session") {
      const session = await kvGet(env, "farm_session");
      const active  = await kvGet(env, "farm_session_active");
      const start   = await kvGet(env, "farm_session_start");
      return cors(JSON.stringify({ session: session ? JSON.parse(session) : null, active: active === "true", startTime: start }));
    }

    // ════════════════════════════════════════════════════
    // GET|POST /config ← UI Config
    // ════════════════════════════════════════════════════
    if (path === "/config") {
      if (method === "GET") {
        const cfg = await kvGet(env, "ui_config");
        return cors(cfg || "{}");
      }
      if (method === "POST") {
        let body;
        try { body = await request.json(); } catch { return cors('{"error":"invalid json"}', 400); }
        await env.KV.put("ui_config", JSON.stringify(body)); // config เขียนทันที
        return cors('{"ok":true}');
      }
    }

    // ════════════════════════════════════════════════════
    // GET|POST /discord-config
    // ════════════════════════════════════════════════════
    if (path === "/discord-config") {
      if (method === "GET") {
        const cfg = await kvGet(env, "discord_config");
        return cors(cfg || "{}");
      }
      if (method === "POST") {
        let body;
        try { body = await request.json(); } catch { return cors('{"error":"invalid json"}', 400); }
        await env.KV.put("discord_config", JSON.stringify(body));
        return cors('{"ok":true}');
      }
    }

    // ════════════════════════════════════════════════════
    // GET|POST|DELETE /icons ← Custom Icons
    // ════════════════════════════════════════════════════
    if (path === "/icons") {
      if (method === "GET") {
        const icons = await kvGet(env, "custom_icons");
        return cors(icons || "{}");
      }
      if (method === "POST") {
        let body;
        try { body = await request.json(); } catch { return cors('{"error":"invalid json"}', 400); }
        const existing = JSON.parse(await kvGet(env, "custom_icons") || "{}");
        if (body.name && body.url) existing[body.name] = body.url;
        await env.KV.put("custom_icons", JSON.stringify(existing));
        return cors('{"ok":true}');
      }
      if (method === "DELETE") {
        let body;
        try { body = await request.json(); } catch { return cors('{"error":"invalid json"}', 400); }
        const existing = JSON.parse(await kvGet(env, "custom_icons") || "{}");
        if (body.name) delete existing[body.name];
        await env.KV.put("custom_icons", JSON.stringify(existing));
        return cors('{"ok":true}');
      }
    }

    // ════════════════════════════════════════════════════
    // GET /db-stats ← Usage stats
    // ════════════════════════════════════════════════════
    if (method === "GET" && path === "/db-stats") {
      const inv     = await kvGet(env, "inventory");
      const updated = await kvGet(env, "inv_updated");
      const stats   = { updated, inventorySize: inv ? inv.length : 0, pendingWrites: Object.keys(_kvPending).length };
      return cors(JSON.stringify(stats));
    }

    // ════════════════════════════════════════════════════
    // GET|POST /alerts ← Item Alert System
    // ════════════════════════════════════════════════════
    if (path === "/alerts") {
      if (method === "GET") {
        const alerts = await kvGet(env, "alerts");
        return cors(alerts || "[]");
      }
      if (method === "POST") {
        let body;
        try { body = await request.json(); } catch { return cors('{"error":"invalid json"}', 400); }
        // body = array of alert objects: [{ item, qty, mode, enabled }]
        await env.KV.put("alerts", JSON.stringify(body));
        await env.KV.put("alerts_fired", "{}"); // reset fired state
        return cors('{"ok":true}');
      }
    }

    // ════════════════════════════════════════════════════
    // POST /webhook ← Telegram Bot
    // ════════════════════════════════════════════════════
    if (method === "POST" && path === "/webhook") {
      let body;
      try { body = await request.json(); } catch { return new Response("OK"); }

      const msg = body.message || body.callback_query?.message;
      if (!msg) return new Response("OK");

      const chatId = String(msg.chat.id);
      await env.KV.put("chat_id", chatId);

      const text = (body.message?.text || "").trim();

      if (text === "/start") {
        await sendTelegramButtons(chatId,
          `👋 *Inventory Tracker* พร้อมใช้งานแล้ว!\n\n` +
          `📦 ระบบติดตาม Inventory realtime จากเกม\n` +
          `🔔 แจ้งเตือนเมื่อ item ถึงจำนวนที่กำหนด\n\n` +
          `เลือกเมนูด้านล่างได้เลยครับ`,
          [
            [{ text: "📦 เปิด Dashboard", web_app: { url: `https://${url.hostname}/app` } }],
            [{ text: "📊 Inventory", callback_data: "cmd_inv" }, { text: "🌊 Wave", callback_data: "cmd_wave" }],
            [{ text: "🔔 จัดการ Alert", callback_data: "cmd_alerts" }, { text: "📈 Farm Session", callback_data: "cmd_farm" }],
          ]
        );

      } else if (text === "/inv" || body.callback_query?.data === "cmd_inv") {
        if (body.callback_query) await answerCallback(body.callback_query.id);
        const invRaw = await kvGet(env, "inventory");
        const invData = invRaw ? JSON.parse(invRaw) : {};
        const updated = await kvGet(env, "inv_updated");
        const flat    = flattenInv(invData);
        const total   = Object.values(flat).reduce((s, v) => s + v, 0);

        if (!Object.keys(flat).length) {
          await sendTelegram(chatId, "📦 ยังไม่มีข้อมูล Inventory ครับ");
        } else {
          let txt = `📦 *Inventory* (${Object.keys(flat).length} items · ${total.toLocaleString()} qty)\n`;
          if (updated) txt += `_อัปเดต: ${new Date(updated).toLocaleString("th-TH", { timeZone: "Asia/Bangkok" })}_\n\n`;
          for (const [cat, items] of Object.entries(invData)) {
            txt += `*── ${cat} ──*\n`;
            for (const [name, qty] of Object.entries(items)) {
              txt += `  • ${name}: \`${qty.toLocaleString()}\`\n`;
            }
          }
          await sendTelegram(chatId, txt.slice(0, 4096));
        }

      } else if (text === "/wave" || body.callback_query?.data === "cmd_wave") {
        if (body.callback_query) await answerCallback(body.callback_query.id);
        const statsRaw = await kvGet(env, "stats");
        const statsData = statsRaw ? JSON.parse(statsRaw) : {};
        const wave = statsData.wave || "ไม่ทราบ";
        const updated = await kvGet(env, "stats_updated");
        let txt = `🌊 *Wave ปัจจุบัน:* \`${wave}\``;
        if (updated) txt += `\n_อัปเดต: ${new Date(updated).toLocaleString("th-TH", { timeZone: "Asia/Bangkok" })}_`;
        await sendTelegram(chatId, txt);

      } else if (text === "/farm" || body.callback_query?.data === "cmd_farm") {
        if (body.callback_query) await answerCallback(body.callback_query.id);
        const farmRaw  = await kvGet(env, "farm_session");
        const farmData = farmRaw ? JSON.parse(farmRaw) : null;
        const start    = await kvGet(env, "farm_session_start");
        const active   = await kvGet(env, "farm_session_active");

        if (!farmData || !farmData.diff) {
          await sendTelegram(chatId, "📈 ยังไม่มีข้อมูล Farm Session ครับ");
        } else {
          const elapsed = start
            ? (() => { const s = Math.floor((Date.now() - new Date(start)) / 1000); return `${String(Math.floor(s/3600)).padStart(2,"0")}:${String(Math.floor(s%3600/60)).padStart(2,"0")}:${String(s%60).padStart(2,"0")}`; })()
            : "--:--:--";
          const drops = Object.entries(farmData.diff || {}).filter(([,v]) => v > 0).sort(([,a],[,b]) => b-a);
          const total  = drops.reduce((s,[,v]) => s+v, 0);
          let txt = `📈 *Farm Session*\n`;
          txt += `⏱ เวลา: \`${elapsed}\` · ${active === "true" ? "🟢 Active" : "🔴 Stopped"}\n`;
          txt += `📦 Total drops: \`${total.toLocaleString()}\`\n\n`;
          for (const [name, qty] of drops.slice(0, 20)) {
            txt += `  • ${name}: \`+${qty.toLocaleString()}\`\n`;
          }
          await sendTelegram(chatId, txt.slice(0, 4096));
        }

      } else if (text === "/alerts" || body.callback_query?.data === "cmd_alerts") {
        if (body.callback_query) await answerCallback(body.callback_query.id);
        const alertsRaw = await kvGet(env, "alerts");
        const alerts = alertsRaw ? JSON.parse(alertsRaw) : [];
        if (!alerts.length) {
          await sendTelegram(chatId,
            `🔔 *Item Alert*\n\nยังไม่มี alert ตั้งไว้ครับ\n\n` +
            `วิธีตั้ง alert ผ่าน Dashboard:\n` +
            `1. เปิด Dashboard → แท็บ ⚙️ Settings\n` +
            `2. กด "เพิ่ม Alert"\n` +
            `3. ใส่ชื่อ item + จำนวน + เงื่อนไข`
          );
        } else {
          let txt = `🔔 *Item Alerts* (${alerts.length} รายการ)\n\n`;
          for (const a of alerts) {
            const icon = a.mode === "above" ? "📈" : "📉";
            const status = a.enabled ? "✅" : "❌";
            txt += `${status} ${icon} *${a.item}* ${a.mode === "above" ? "≥" : "≤"} \`${Number(a.qty).toLocaleString()}\`\n`;
          }
          await sendTelegram(chatId, txt);
        }

      } else if (text === "/help") {
        await sendTelegram(chatId,
          `📖 *คำสั่งทั้งหมด*\n\n` +
          `/start — เมนูหลัก\n` +
          `/inv — ดู Inventory ปัจจุบัน\n` +
          `/wave — ดู Wave ปัจจุบัน\n` +
          `/farm — ดู Farm Session\n` +
          `/alerts — ดู Item Alerts\n` +
          `/help — แสดงคำสั่งทั้งหมด`
        );
      }

      return new Response("OK");
    }

    // ════════════════════════════════════════════════════
    // GET /app ← Telegram Mini App + Web Dashboard
    // ════════════════════════════════════════════════════
    if (method === "GET" && path === "/app") {
      return html(getDashboardHTML(url.hostname));
    }

    // ════════════════════════════════════════════════════
    // GET / ← redirect to /app
    // ════════════════════════════════════════════════════
    if (method === "GET" && path === "/") {
      return Response.redirect(`https://${url.hostname}/app`, 302);
    }

    return cors('{"error":"not found"}', 404);
  }
};

// ── Helpers ──────────────────────────────────────────────
function flattenInv(inv) {
  const flat = {};
  for (const items of Object.values(inv || {})) {
    if (items && typeof items === "object") {
      for (const [name, qty] of Object.entries(items)) {
        flat[name] = (flat[name] || 0) + (Number(qty) || 0);
      }
    }
  }
  return flat;
}

async function sendTelegram(chatId, text) {
  return fetch(`${TELEGRAM_API}/sendMessage`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ chat_id: chatId, text, parse_mode: "Markdown" }),
  });
}

async function sendTelegramButtons(chatId, text, keyboard) {
  return fetch(`${TELEGRAM_API}/sendMessage`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({
      chat_id:      chatId,
      text,
      parse_mode:   "Markdown",
      reply_markup: { inline_keyboard: keyboard },
    }),
  });
}

async function answerCallback(callbackQueryId) {
  return fetch(`${TELEGRAM_API}/answerCallbackQuery`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ callback_query_id: callbackQueryId }),
  });
}

// ── Dashboard HTML ────────────────────────────────────────
function getDashboardHTML(hostname) {
  const API = `https://${hostname}`;
  return `<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>INVENTORY VIEWER</title>
<link rel="icon" type="image/jpeg" href="https://i.postimg.cc/kg8SjvdT/Screenshot-20260330-011203.jpg">
<link href="https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;500;600;700&family=Share+Tech+Mono&family=Teko:wght@400;500;600&display=swap" rel="stylesheet">
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
:root {
  --bg:#080809; --bg2:#0f0f12; --panel:#111114; --panel2:#18181e;
  --border:#1e1e26; --border2:#2e1520; --border3:#4a1a28;
  --red:#b91c1c; --red2:#ef4444; --red3:#ff6b6b;
  --redglow:rgba(185,28,28,0.45); --redglow2:rgba(239,68,68,0.10);
  --text:#f0e8e0; --text2:#8a7a6a; --text3:#504030;
  --gold:#d4a843; --green:#22c55e; --radius:0px;
}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Rajdhani',sans-serif;min-height:100vh;overflow-x:hidden}
body::after{content:'';position:fixed;inset:0;pointer-events:none;z-index:9999;background:repeating-linear-gradient(0deg,transparent,transparent 3px,rgba(185,28,28,.015) 3px,rgba(185,28,28,.015) 4px)}

/* ── Header ── */
header{position:sticky;top:0;z-index:50;background:rgba(8,8,9,.95);backdrop-filter:blur(12px);border-bottom:1px solid var(--border3);box-shadow:0 1px 30px var(--redglow2);padding:10px 16px;display:flex;align-items:center;justify-content:space-between;gap:12px}
.logo-text{font-family:'Teko',sans-serif;font-size:22px;font-weight:600;letter-spacing:4px}.logo-text em{color:var(--red2);font-style:normal}
.status-pill{display:flex;align-items:center;gap:6px;font-family:'Share Tech Mono',monospace;font-size:10px;color:var(--text2)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 10px var(--green);animation:blink 2s infinite}
.dot.off{background:var(--red);box-shadow:0 0 10px var(--red);animation:none}
.dot.warn{background:#f59e0b;box-shadow:0 0 10px #f59e0b}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}
.ts{font-family:'Share Tech Mono',monospace;font-size:10px;color:var(--red2)}

/* ── Nav Tabs ── */
.nav-tabs{display:flex;background:var(--panel);border-bottom:1px solid var(--border);overflow-x:auto;scrollbar-width:none}
.nav-tabs::-webkit-scrollbar{display:none}
.ntab{flex:0 0 auto;padding:10px 16px;font-family:'Teko',sans-serif;font-size:15px;letter-spacing:2px;color:var(--text3);cursor:pointer;border-bottom:2px solid transparent;transition:all .2s;white-space:nowrap}
.ntab.active{color:var(--red2);border-bottom-color:var(--red2)}
.ntab:hover{color:var(--text2)}

/* ── Content ── */
.page{display:none;padding:12px;max-width:1200px;margin:0 auto}
.page.active{display:block}

/* ── Wave Card ── */
.wave-card{background:linear-gradient(135deg,#1a0a0a,#2a0f0f);border:1px solid var(--border3);padding:14px;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center}
.wave-num{font-size:48px;font-weight:900;color:var(--red2);font-family:'Teko',sans-serif;text-shadow:0 0 20px rgba(239,68,68,.5);line-height:1}
.wave-lbl{font-size:10px;color:var(--text3);letter-spacing:3px;margin-bottom:2px}
.wave-sub{font-size:11px;color:var(--text2);font-family:'Share Tech Mono',monospace}

/* ── Statsbar ── */
.statsbar{display:flex;gap:8px;overflow-x:auto;margin-bottom:12px;padding-bottom:4px;scrollbar-width:none}
.statsbar::-webkit-scrollbar{display:none}
.stat-item{flex:0 0 auto;background:var(--panel);border:1px solid var(--border);padding:6px 12px;text-align:center}
.stat-lbl{font-size:9px;color:var(--text3);letter-spacing:2px}
.stat-val{font-size:16px;font-weight:700;color:var(--text);font-family:'Teko',sans-serif}
.stat-item.total .stat-val{color:var(--red2)}

/* ── Filter ── */
.filter-row{display:flex;gap:6px;margin-bottom:10px;overflow-x:auto;scrollbar-width:none;padding-bottom:2px}
.filter-row::-webkit-scrollbar{display:none}
.ftab{flex:0 0 auto;padding:5px 12px;background:var(--panel);border:1px solid var(--border);color:var(--text3);font-family:'Rajdhani',sans-serif;font-size:12px;letter-spacing:1px;cursor:pointer;transition:all .15s}
.ftab.active{background:var(--red);border-color:var(--red);color:#fff}
.ftab:hover:not(.active){border-color:var(--red2);color:var(--text2)}

/* ── Search ── */
.search-wrap{position:relative;margin-bottom:10px}
.search-input{width:100%;background:var(--panel);border:1px solid var(--border);color:var(--text);font-family:'Rajdhani',sans-serif;font-size:14px;padding:8px 12px;outline:none;transition:border-color .2s}
.search-input::placeholder{color:var(--text3)}
.search-input:focus{border-color:var(--red);box-shadow:0 0 0 1px var(--red)}

/* ── Item Grid ── */
.cat-sec{margin-bottom:16px}
.cat-hdr{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid var(--border3)}
.cat-name{font-family:'Teko',sans-serif;font-size:16px;letter-spacing:3px;color:var(--gold)}
.cat-count{font-size:10px;color:var(--text3);font-family:'Share Tech Mono',monospace}
.item-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(100px,1fr));gap:6px}
.card{background:var(--panel);border:1px solid var(--border);padding:8px;cursor:default;transition:border-color .15s,box-shadow .15s;animation:fadeUp .3s ease both}
.card:hover{border-color:var(--red2);box-shadow:0 0 12px var(--redglow2)}
@keyframes fadeUp{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
.img-wrap{width:100%;aspect-ratio:1;display:flex;align-items:center;justify-content:center;margin-bottom:6px;background:var(--panel2);overflow:hidden}
.item-img{width:100%;height:100%;object-fit:contain}
.no-img{width:100%;height:100%;display:flex;align-items:center;justify-content:center;color:var(--text3);font-size:18px}
.iname{font-size:11px;color:var(--text);line-height:1.2;margin-bottom:3px;word-break:break-word}
.iextra{font-size:9px;color:var(--gold);font-family:'Share Tech Mono',monospace}
.iqty{font-size:14px;font-weight:700;color:var(--red2);font-family:'Teko',sans-serif}
.empty{text-align:center;padding:48px 16px;color:var(--text3)}
.empty-title{font-family:'Teko',sans-serif;font-size:22px;letter-spacing:3px;margin-bottom:6px}
.empty-sub{font-size:12px;color:var(--text3)}

/* ── Dungeon Panels ── */
.dungeon-row{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}
.run-panel{background:var(--panel);border:1px solid var(--border);padding:10px}
.run-panel-header{display:flex;align-items:center;gap:6px;margin-bottom:8px;font-family:'Share Tech Mono',monospace;font-size:10px;color:var(--text3);letter-spacing:2px}
.run-live-dot{width:6px;height:6px;border-radius:50%;background:var(--green);box-shadow:0 0 6px var(--green);animation:blink 1s infinite;flex-shrink:0}
.run-chips{display:flex;flex-wrap:wrap;gap:4px;min-height:32px}
.diff-chip{display:inline-flex;align-items:center;gap:4px;padding:3px 7px;font-size:11px;font-family:'Share Tech Mono',monospace;animation:chipPop .3s ease both}
.diff-chip.pos{background:rgba(34,197,94,.12);border:1px solid rgba(34,197,94,.3);color:#22c55e}
.diff-chip.neg{background:rgba(239,68,68,.10);border:1px solid rgba(239,68,68,.25);color:var(--red2)}
@keyframes chipPop{from{opacity:0;transform:scale(.8)}to{opacity:1;transform:none}}
.run-empty{font-size:11px;color:var(--text3);font-family:'Share Tech Mono',monospace}

/* ── Farm Mode ── */
.farm-block{background:var(--panel);border:1px solid var(--border2);padding:12px;margin-bottom:12px}
.farm-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.farm-title{font-family:'Teko',sans-serif;font-size:16px;letter-spacing:3px;color:var(--gold)}
.farm-timer{font-family:'Share Tech Mono',monospace;font-size:13px;color:var(--red2)}
.farm-chips{display:flex;flex-wrap:wrap;gap:4px;min-height:28px}

/* ── Alert Panel ── */
.alert-panel{background:var(--panel);border:1px solid var(--border);padding:12px;margin-bottom:12px}
.alert-title{font-family:'Teko',sans-serif;font-size:16px;letter-spacing:3px;color:var(--red2);margin-bottom:10px;display:flex;align-items:center;justify-content:space-between}
.alert-list{display:flex;flex-direction:column;gap:6px;margin-bottom:10px}
.alert-row{display:flex;align-items:center;gap:8px;background:var(--panel2);border:1px solid var(--border);padding:8px 10px}
.alert-row-item{flex:1;font-size:13px;font-family:'Rajdhani',sans-serif;color:var(--text)}
.alert-row-qty{font-size:12px;font-family:'Share Tech Mono',monospace;color:var(--red2)}
.alert-row-mode{font-size:10px;color:var(--text3);letter-spacing:1px}
.alert-toggle{margin-left:auto;cursor:pointer;width:32px;height:18px;border-radius:9px;background:var(--border);border:none;position:relative;transition:background .2s;flex-shrink:0}
.alert-toggle.on{background:var(--green)}
.alert-toggle::after{content:'';position:absolute;top:2px;left:2px;width:14px;height:14px;border-radius:50%;background:#fff;transition:left .2s}
.alert-toggle.on::after{left:16px}
.alert-del{background:none;border:none;color:var(--red2);cursor:pointer;font-size:14px;padding:0 4px;opacity:.6}
.alert-del:hover{opacity:1}
.add-alert-form{display:flex;flex-direction:column;gap:8px;border-top:1px solid var(--border);padding-top:10px}
.form-row{display:flex;gap:6px}
.form-input{flex:1;background:var(--panel2);border:1px solid var(--border);color:var(--text);font-family:'Rajdhani',sans-serif;font-size:13px;padding:7px 10px;outline:none}
.form-input:focus{border-color:var(--red)}
.form-select{background:var(--panel2);border:1px solid var(--border);color:var(--text);font-family:'Rajdhani',sans-serif;font-size:13px;padding:7px 8px;outline:none;cursor:pointer}
.btn{padding:8px 16px;background:var(--red);border:none;color:#fff;font-family:'Teko',sans-serif;font-size:15px;letter-spacing:2px;cursor:pointer;transition:background .15s}
.btn:hover{background:var(--red2)}
.btn-outline{background:transparent;border:1px solid var(--border);color:var(--text2)}
.btn-outline:hover{border-color:var(--red2);color:var(--text)}
.btn-sm{padding:5px 10px;font-size:13px}

/* ── Stats Panel ── */
.stats-panel{padding:12px}
.sp-card{background:var(--panel);border:1px solid var(--border);padding:10px;margin-bottom:8px}
.sp-cat{font-size:10px;color:var(--text3);letter-spacing:2px;margin-bottom:4px}
.sp-name{font-size:15px;font-family:'Teko',sans-serif;letter-spacing:1px}
.sp-tags{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px}
.sp-tag{font-size:10px;padding:2px 7px;font-family:'Share Tech Mono',monospace;background:rgba(239,68,68,.08);border:1px solid rgba(239,68,68,.2);color:var(--red3)}

/* ── Toast ── */
#toastContainer{position:fixed;bottom:80px;left:50%;transform:translateX(-50%);z-index:9998;display:flex;flex-direction:column;gap:6px;pointer-events:none;width:90%;max-width:320px}
.toast{background:var(--panel2);border:1px solid var(--border3);color:var(--text);font-family:'Share Tech Mono',monospace;font-size:11px;padding:8px 14px;animation:toastIn .25s ease both}
.toast.ok{border-color:var(--green);color:var(--green)}
.toast.err{border-color:var(--red2);color:var(--red2)}
@keyframes toastIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:none}}

/* ── Offline overlay ── */
#offlineOverlay{display:none;position:fixed;inset:0;background:rgba(8,8,9,.92);z-index:9000;align-items:center;justify-content:center;flex-direction:column;gap:8px;text-align:center}
#offlineOverlay.visible{display:flex}
.offline-title{font-family:'Teko',sans-serif;font-size:28px;letter-spacing:4px;color:var(--red2)}
.offline-sub{font-size:12px;color:var(--text3);font-family:'Share Tech Mono',monospace}
</style>
</head>
<body>

<header>
  <div class="logo-text">INV<em>IEWƏR</em></div>
  <div style="display:flex;align-items:center;gap:10px">
    <div class="status-pill"><div class="dot off" id="statusDot"></div><span id="statusTxt">NOT CONNECTED</span></div>
    <div class="ts" id="tsClock"></div>
  </div>
</header>

<div class="nav-tabs">
  <div class="ntab active" onclick="switchTab('inventory',this)">INVENTORY</div>
  <div class="ntab" onclick="switchTab('dungeon',this)">DUNGEON</div>
  <div class="ntab" onclick="switchTab('farm',this)">FARM</div>
  <div class="ntab" onclick="switchTab('stats',this)">STATS</div>
  <div class="ntab" onclick="switchTab('alerts',this)">🔔 ALERTS</div>
</div>

<!-- ════ INVENTORY ════ -->
<div class="page active" id="tab-inventory">
  <div style="padding:12px;padding-bottom:0">
    <div class="statsbar">
      <div class="stat-item total"><div class="stat-lbl">ITEMS</div><div class="stat-val" id="sItems">0</div></div>
      <div class="stat-item total"><div class="stat-lbl">QTY</div><div class="stat-val" id="sQty">0</div></div>
    </div>
    <div class="filter-row" id="filterTabs">
      <button class="ftab active" onclick="setFilter('all',this)">ALL</button>
    </div>
    <div class="search-wrap">
      <input class="search-input" id="searchInput" placeholder="ค้นหา item..." oninput="render()">
    </div>
  </div>
  <div style="padding:0 12px 80px" id="main">
    <div class="empty"><div class="empty-title">รอข้อมูลจาก Lua</div><div class="empty-sub">Run Lua script ในเกมก่อนนะ</div></div>
  </div>
</div>

<!-- ════ DUNGEON ════ -->
<div class="page" id="tab-dungeon">
  <div style="padding:12px 12px 80px">
    <div style="margin-bottom:10px;display:flex;align-items:center;justify-content:space-between">
      <div class="wave-card" style="flex:1;margin:0">
        <div><div class="wave-lbl">CURRENT WAVE</div><div class="wave-num" id="waveDisplay">—</div><div class="wave-sub" id="waveSub">NOT IN DUNGEON</div></div>
        <div style="text-align:right;font-size:11px;color:var(--text3);font-family:'Share Tech Mono',monospace" id="statsUpdated"></div>
      </div>
    </div>
    <div class="dungeon-row">
      <div class="run-panel">
        <div class="run-panel-header">LAST RUN</div>
        <div class="run-chips" id="dungeonPrevChips"><span class="run-empty">—</span></div>
      </div>
      <div class="run-panel" id="dungeonCurrPanel">
        <div class="run-panel-header">THIS RUN</div>
        <div class="run-chips" id="dungeonCurrChips"><span class="run-empty">NOT IN DUNGEON</span></div>
      </div>
    </div>
  </div>
</div>

<!-- ════ FARM ════ -->
<div class="page" id="tab-farm">
  <div style="padding:12px 12px 80px">
    <div class="farm-block">
      <div class="farm-header">
        <div class="farm-title">FARM SESSION</div>
        <div class="farm-timer" id="farmTimerVal">00:00:00</div>
      </div>
      <div style="font-size:10px;color:var(--text3);font-family:'Share Tech Mono',monospace;margin-bottom:6px">DROPS THIS SESSION</div>
      <div class="farm-chips" id="farmLootChips"><span class="run-empty">No drops recorded yet...</span></div>
    </div>
    <div style="display:flex;gap:8px">
      <button class="btn btn-outline btn-sm" onclick="resetFarm()">RESET SESSION</button>
    </div>
  </div>
</div>

<!-- ════ STATS ════ -->
<div class="page" id="tab-stats">
  <div class="stats-panel" id="statsPanel">
    <div class="empty"><div class="empty-title">รอข้อมูล Stats</div><div class="empty-sub">ข้อมูลจะปรากฏเมื่อ Lua ส่งมา</div></div>
  </div>
</div>

<!-- ════ ALERTS ════ -->
<div class="page" id="tab-alerts">
  <div style="padding:12px 12px 80px">
    <div class="alert-panel">
      <div class="alert-title">
        🔔 ITEM ALERTS
        <span style="font-size:11px;color:var(--text3);font-family:'Share Tech Mono',monospace">แจ้งเตือนผ่าน Telegram</span>
      </div>
      <div class="alert-list" id="alertList"></div>
      <div class="add-alert-form">
        <div style="font-size:11px;color:var(--text3);letter-spacing:2px;font-family:'Share Tech Mono',monospace">เพิ่ม ALERT ใหม่</div>
        <input class="form-input" id="alertItem" placeholder="ชื่อ item (เช่น Health Potion)">
        <div class="form-row">
          <input class="form-input" id="alertQty" type="number" placeholder="จำนวน" style="max-width:120px">
          <select class="form-select" id="alertMode">
            <option value="above">≥ มากกว่าเท่ากับ</option>
            <option value="below">≤ น้อยกว่าเท่ากับ</option>
          </select>
        </div>
        <button class="btn btn-sm" onclick="addAlert()">+ เพิ่ม ALERT</button>
      </div>
    </div>
    <div style="font-size:10px;color:var(--text3);font-family:'Share Tech Mono',monospace;line-height:1.8">
      💡 Alert จะส่งแจ้งเตือนผ่าน Telegram ทันทีที่ item ถึงจำนวนที่กำหนด<br>
      🔄 Reset fired state อัตโนมัติเมื่อ item กลับมาอยู่นอก threshold
    </div>
  </div>
</div>

<!-- Offline -->
<div id="offlineOverlay">
  <div class="offline-title">LUA OFFLINE</div>
  <div class="offline-sub">ไม่ได้รับข้อมูลมากกว่า 90 วินาที</div>
  <div class="offline-sub" id="offlineElapsed" style="color:var(--red2)">00:00</div>
</div>
<div id="toastContainer"></div>

<script>
// ════════════════════════════════════════════════
// CONFIG
// ════════════════════════════════════════════════
const API_BASE = "${API}";
const POLL_MS  = 3000; // poll ทุก 3 วินาที

// ════════════════════════════════════════════════
// STATE
// ════════════════════════════════════════════════
let data            = {};
let prevData        = {};
let activeFilter    = 'all';
let currentTab      = 'inventory';
let lastUpdated     = null;
let offlineTimer    = null;
let offlineElapsed  = 0;
let offlineTick     = null;
let farmStartTime   = null;
let farmDiffAccum   = {};
let farmTimerIntvl  = null;
let dungeonPrevDiff = {};
let dungeonDiffAcc  = {};
let dungeonSnap     = null;
let lastWaveInDun   = false;
let isInfTower      = false;
let lastTowerWave   = 0;
let alerts          = [];
let prevInvFlat     = null;

// ════════════════════════════════════════════════
// CLOCK
// ════════════════════════════════════════════════
setInterval(() => {
  document.getElementById('tsClock').textContent =
    new Date().toLocaleTimeString('th-TH', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}, 1000);

// ════════════════════════════════════════════════
// POLLING (แทน SSE เพราะ Cloudflare Workers ไม่รองรับ)
// ════════════════════════════════════════════════
async function poll() {
  try {
    const res  = await fetch(API_BASE + '/events');
    const json = await res.json();

    if (json.inventory && Object.keys(json.inventory).length) {
      const newData = parseRaw(json.inventory);
      if (JSON.stringify(newData) !== JSON.stringify(prevData)) {
        data     = newData;
        prevData = newData;
        render();
        checkAlertsTrigger(newData);
      }
      lastUpdated = json.updated;
      if (json.updated) {
        const d = new Date(json.updated);
        document.getElementById('statsUpdated').textContent = 'LAST: ' + d.toLocaleTimeString('th-TH');
      }
      setStatus('ok', 'ONLINE');
      markReceived();
    }

    if (json.stats) {
      processStats(json.stats);
    }

    if (json.dungeonRuns && json.dungeonRuns.thisRun) {
      restoreDungeon(json.dungeonRuns);
    }

    if (json.icons && Object.keys(json.icons).length) {
      Object.assign(customIcons, json.icons);
    }

  } catch (e) {
    setStatus('warn', 'POLLING...');
  }
}

setInterval(poll, POLL_MS);
poll();

// ════════════════════════════════════════════════
// STATUS
// ════════════════════════════════════════════════
function setStatus(state, txt) {
  const dot = document.getElementById('statusDot');
  dot.className = 'dot' + (state === 'ok' ? '' : state === 'warn' ? ' warn' : ' off');
  document.getElementById('statusTxt').textContent = txt;
}

function markReceived() {
  clearTimeout(offlineTimer);
  clearInterval(offlineTick);
  offlineElapsed = 0;
  document.getElementById('offlineOverlay').classList.remove('visible');
  offlineTimer = setTimeout(showOffline, 90000);
}

function showOffline() {
  document.getElementById('offlineOverlay').classList.add('visible');
  setStatus('off', 'LUA OFFLINE');
  offlineElapsed = 0;
  offlineTick = setInterval(() => {
    offlineElapsed++;
    const m = String(Math.floor(offlineElapsed/60)).padStart(2,'0');
    const s = String(offlineElapsed%60).padStart(2,'0');
    document.getElementById('offlineElapsed').textContent = m + ':' + s;
  }, 1000);
}

// ════════════════════════════════════════════════
// TAB SWITCHER
// ════════════════════════════════════════════════
function switchTab(name, el) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.ntab').forEach(t => t.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  el.classList.add('active');
  currentTab = name;
}

// ════════════════════════════════════════════════
// PARSE RAW
// ════════════════════════════════════════════════
function parseRaw(json) {
  const out = {};
  for (const [cat, val] of Object.entries(json)) {
    if (Array.isArray(val)) {
      out[cat] = val;
    } else if (val && typeof val === 'object') {
      out[cat] = Object.entries(val).map(([name, qty]) => ({
        Name: name, Quantity: typeof qty === 'number' ? qty : 1, Extra: ''
      }));
    }
  }
  return out;
}

function flattenData(d) {
  const out = {};
  for (const items of Object.values(d)) {
    (items || []).forEach(i => { out[i.Name] = (out[i.Name] || 0) + i.Quantity; });
  }
  return out;
}

// ════════════════════════════════════════════════
// RENDER INVENTORY
// ════════════════════════════════════════════════
let customIcons = {};

function resolveIcon(name) {
  if (customIcons[name]) return customIcons[name];
  return null;
}

function rebuildFilters() {
  const tabs = document.getElementById('filterTabs');
  tabs.innerHTML = '<button class="ftab ' + (activeFilter==='all'?'active':'') + '" onclick="setFilter(\'all\',this)">ALL</button>';
  Object.keys(data).forEach(cat => {
    const b = document.createElement('button');
    b.className = 'ftab' + (activeFilter===cat?' active':'');
    b.textContent = cat.toUpperCase();
    b.onclick = function(){ setFilter(cat, this); };
    tabs.appendChild(b);
  });
}

function setFilter(cat, el) {
  activeFilter = cat;
  document.querySelectorAll('.ftab').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  render();
}

function render() {
  rebuildFilters();
  const search = document.getElementById('searchInput').value.toLowerCase();
  const main   = document.getElementById('main');
  main.innerHTML = '';

  const cats = activeFilter === 'all' ? Object.keys(data) : [activeFilter];
  let totalItems = 0, totalQty = 0, hasAny = false;

  cats.forEach(cat => {
    let items = (data[cat] || []).filter(i => !search || i.Name.toLowerCase().includes(search));
    if (!items.length) return;
    items.sort((a,b) => b.Quantity - a.Quantity);
    hasAny = true;
    totalItems += items.length;
    totalQty   += items.reduce((s,i) => s+i.Quantity, 0);

    const sec = document.createElement('div');
    sec.className = 'cat-sec';
    sec.innerHTML = '<div class="cat-hdr"><span class="cat-name">' + cat + '</span><span class="cat-count">' + items.length + ' items</span></div><div class="item-grid" id="g' + cat.replace(/\\W/g,'_') + '"></div>';
    main.appendChild(sec);

    const grid = sec.querySelector('.item-grid');
    items.forEach((item, i) => {
      const card = document.createElement('div');
      card.className = 'card';
      const url = resolveIcon(item.Name);
      const imgHtml = url
        ? '<img class="item-img" src="' + url + '" alt="' + item.Name + '" loading="lazy" onerror="this.parentElement.innerHTML=\'<div class=\\\'no-img\\\'>?</div>\'">'
        : '<div class="no-img">?</div>';
      card.innerHTML = '<div class="img-wrap">' + imgHtml + '</div><div class="iname">' + item.Name + '</div>' +
        (item.Extra ? '<div class="iextra">' + item.Extra + '</div>' : '') +
        '<div class="iqty">×' + item.Quantity.toLocaleString() + '</div>';
      card.style.animationDelay = (i * 0.02) + 's';
      grid.appendChild(card);
    });
  });

  if (!hasAny) {
    main.innerHTML = '<div class="empty"><div class="empty-title">' +
      (Object.keys(data).length ? 'ไม่พบไอเทม' : 'รอข้อมูลจาก Lua') + '</div><div class="empty-sub">' +
      (Object.keys(data).length ? 'ลองค้นหาคำอื่น' : 'Run Lua script ในเกมก่อนนะ') + '</div></div>';
  }

  document.getElementById('sItems').textContent = totalItems;
  document.getElementById('sQty').textContent   = totalQty.toLocaleString();
}

// ════════════════════════════════════════════════
// FARM SESSION
// ════════════════════════════════════════════════
function startFarmTimer(startTime) {
  clearInterval(farmTimerIntvl);
  farmTimerIntvl = setInterval(() => {
    const s = Math.floor((Date.now() - new Date(startTime)) / 1000);
    const h = String(Math.floor(s/3600)).padStart(2,'0');
    const m = String(Math.floor(s%3600/60)).padStart(2,'0');
    const sc = String(s%60).padStart(2,'0');
    document.getElementById('farmTimerVal').textContent = h + ':' + m + ':' + sc;
  }, 1000);
}

function updateFarmLoot(diff) {
  const drops = Object.entries(diff).filter(([,v]) => v > 0).sort(([,a],[,b]) => b-a);
  const el = document.getElementById('farmLootChips');
  if (!drops.length) { el.innerHTML = '<span class="run-empty">No drops recorded yet...</span>'; return; }
  el.innerHTML = drops.map(([name, qty]) =>
    '<span class="diff-chip pos">' + name + ' +' + qty.toLocaleString() + '</span>'
  ).join('');
}

function resetFarm() {
  farmDiffAccum = {};
  farmStartTime = new Date().toISOString();
  prevInvFlat   = flattenData(data);
  startFarmTimer(farmStartTime);
  updateFarmLoot({});
  fetch(API_BASE + '/farm-session', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'start', snapshot: prevInvFlat })
  }).catch(() => {});
  showToast('Farm session reset!', 'ok');
}

// ════════════════════════════════════════════════
// DUNGEON
// ════════════════════════════════════════════════
function restoreDungeon(runs) {
  if (runs.thisRun) {
    document.getElementById('dungeonCurrChips').innerHTML = buildChips(runs.thisRun);
  }
  if (runs.lastRun) {
    document.getElementById('dungeonPrevChips').innerHTML = buildChips(runs.lastRun);
  }
}

function buildChips(diff) {
  const entries = Object.entries(diff).filter(([,v]) => v !== 0);
  if (!entries.length) return '<span class="run-empty">—</span>';
  return entries.sort((a,b) => b[1]-a[1]).map(([name, delta]) => {
    const sign = delta > 0 ? '+' : '';
    const cls  = delta > 0 ? 'pos' : 'neg';
    return '<span class="diff-chip ' + cls + '">' + name + ' ' + sign + delta.toLocaleString() + '</span>';
  }).join('');
}

// ════════════════════════════════════════════════
// STATS
// ════════════════════════════════════════════════
function processStats(statsJson) {
  const wave    = String(statsJson.wave || '').trim();
  const rawText = statsJson.stats || '';

  const wm  = wave.match(/(\\d+)\\s*\\/\\s*(\\d+)/);
  const wm1 = !wm && /\\d/.test(wave) ? wave.match(/(\\d+)/) : null;
  const isInDun = !!(wm || wm1);
  const isInfTw = !!wm1;

  const waveEl  = document.getElementById('waveDisplay');
  const waveSub = document.getElementById('waveSub');

  if (wm) {
    waveEl.textContent  = wm[1] + ' / ' + wm[2];
    waveSub.textContent = 'IN DUNGEON';
  } else if (wm1) {
    waveEl.textContent  = wm1[1];
    waveSub.textContent = 'INFINITE TOWER';
  } else {
    waveEl.textContent  = wave || '—';
    waveSub.textContent = 'NOT IN DUNGEON';
  }

  // Stats cards
  if (rawText) {
    const panel = document.getElementById('statsPanel');
    const lines = rawText.split('\\n').map(l => l.trim()).filter(Boolean);
    const sections = [];
    let cur = null;
    for (const line of lines) {
      const ci = line.indexOf(':');
      if (ci < 0) continue;
      const key = line.slice(0, ci).trim();
      const val = line.slice(ci+1).trim();
      if (key === 'StatName') {
        cur = { name: val, stats: [] };
        sections.push(cur);
      } else if (/^Stat\\d+$/.test(key) && cur && val && val !== 'Stat Text') {
        cur.stats.push(val);
      }
    }
    if (sections.length) {
      panel.innerHTML = sections.map(s =>
        '<div class="sp-card"><div class="sp-cat">' + (s.name.split(':')[0] || '').toUpperCase() + '</div>' +
        '<div class="sp-name">' + (s.name.split(':')[1] || s.name).trim() + '</div>' +
        (s.stats.length ? '<div class="sp-tags">' + s.stats.map(t => '<span class="sp-tag">' + t + '</span>').join('') + '</div>' : '') +
        '</div>'
      ).join('');
    }
  }
}

// ════════════════════════════════════════════════
// ALERTS
// ════════════════════════════════════════════════
async function loadAlerts() {
  try {
    const res = await fetch(API_BASE + '/alerts');
    alerts = await res.json();
    renderAlerts();
  } catch { alerts = []; }
}

function renderAlerts() {
  const list = document.getElementById('alertList');
  if (!alerts.length) {
    list.innerHTML = '<div style="color:var(--text3);font-size:12px;font-family:Share Tech Mono,monospace;padding:8px 0">ยังไม่มี Alert ครับ เพิ่มได้ด้านล่าง</div>';
    return;
  }
  list.innerHTML = alerts.map((a, i) =>
    '<div class="alert-row">' +
    '<div>' +
      '<div class="alert-row-item">' + a.item + '</div>' +
      '<div style="display:flex;gap:6px;align-items:center">' +
        '<span class="alert-row-mode">' + (a.mode === 'above' ? '≥' : '≤') + '</span>' +
        '<span class="alert-row-qty">' + Number(a.qty).toLocaleString() + '</span>' +
      '</div>' +
    '</div>' +
    '<button class="alert-toggle' + (a.enabled ? ' on' : '') + '" onclick="toggleAlert(' + i + ')" title="เปิด/ปิด"></button>' +
    '<button class="alert-del" onclick="deleteAlert(' + i + ')" title="ลบ">✕</button>' +
    '</div>'
  ).join('');
}

async function saveAlerts() {
  await fetch(API_BASE + '/alerts', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(alerts)
  });
}

function addAlert() {
  const item = document.getElementById('alertItem').value.trim();
  const qty  = document.getElementById('alertQty').value.trim();
  const mode = document.getElementById('alertMode').value;
  if (!item || !qty) { showToast('กรุณาใส่ชื่อ item และจำนวน', 'err'); return; }
  alerts.push({ item, qty: Number(qty), mode, enabled: true });
  renderAlerts();
  saveAlerts();
  document.getElementById('alertItem').value = '';
  document.getElementById('alertQty').value  = '';
  showToast('เพิ่ม Alert สำเร็จ!', 'ok');
}

function toggleAlert(i) {
  alerts[i].enabled = !alerts[i].enabled;
  renderAlerts();
  saveAlerts();
}

function deleteAlert(i) {
  alerts.splice(i, 1);
  renderAlerts();
  saveAlerts();
  showToast('ลบ Alert แล้ว', 'ok');
}

// Client-side alert check (แสดง toast)
function checkAlertsTrigger(newData) {
  if (!alerts.length) return;
  const flat = flattenData(newData);
  for (const a of alerts) {
    if (!a.enabled) continue;
    const qty = flat[a.item] || 0;
    if (a.mode === 'above' && qty >= a.qty) {
      showToast('🚨 ' + a.item + ' ถึง ' + qty.toLocaleString() + ' แล้ว!', 'ok');
    } else if (a.mode === 'below' && qty <= a.qty) {
      showToast('⚠️ ' + a.item + ' เหลือแค่ ' + qty.toLocaleString(), 'err');
    }
  }
}

// ════════════════════════════════════════════════
// TOAST
// ════════════════════════════════════════════════
function showToast(msg, type='ok') {
  const c = document.getElementById('toastContainer');
  const t = document.createElement('div');
  t.className = 'toast ' + type;
  t.textContent = msg;
  c.appendChild(t);
  setTimeout(() => t.remove(), 3000);
}

// ════════════════════════════════════════════════
// TELEGRAM MINI APP
// ════════════════════════════════════════════════
if (window.Telegram?.WebApp) {
  Telegram.WebApp.ready();
  Telegram.WebApp.expand();
}

// ════════════════════════════════════════════════
// INIT
// ════════════════════════════════════════════════
loadAlerts();
setStatus('off', 'NOT CONNECTED');

// Farm session restore
fetch(API_BASE + '/farm-session').then(r => r.json()).then(d => {
  if (d.active && d.startTime) {
    farmStartTime = d.startTime;
    startFarmTimer(farmStartTime);
  }
  if (d.session?.diff) {
    farmDiffAccum = d.session.diff;
    updateFarmLoot(farmDiffAccum);
  }
}).catch(() => {});
</script>
</body>
</html>`;
}
