/**
 * Tracker Merokok — Server Online (shared realtime)
 * Data disimpan terpusat di server, jadi semua orang yang buka link
 * melihat daftar yang SAMA secara real-time.
 */
const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, "data.json");

let state = { active: [], history: [] };

/* ---------- persistence ---------- */
function loadData() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const raw = fs.readFileSync(DATA_FILE, "utf8");
      const parsed = JSON.parse(raw);
      state.active = Array.isArray(parsed.active) ? parsed.active : [];
      state.history = Array.isArray(parsed.history) ? parsed.history : [];
    }
  } catch (e) {
    console.error("Gagal load data:", e.message);
    state = { active: [], history: [] };
  }
}

let saveTimer = null;
function saveData() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    try {
      fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2));
    } catch (e) {
      console.error("Gagal simpan data:", e.message);
    }
  }, 150);
}

/* ---------- SSE clients ---------- */
const clients = new Set();

function broadcast() {
  const payload = `data: ${JSON.stringify(state)}\n\n`;
  for (const res of clients) {
    try { res.write(payload); } catch (e) { clients.delete(res); }
  }
}

/* ---------- helpers ---------- */
function sendJSON(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (c) => {
      data += c;
      if (data.length > 1e6) { reject(new Error("Body terlalu besar")); req.destroy(); }
    });
    req.on("end", () => {
      try { resolve(data ? JSON.parse(data) : {}); }
      catch (e) { reject(new Error("JSON tidak valid")); }
    });
    req.on("error", reject);
  });
}

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
};

/* ---------- server ---------- */
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const pathname = url.pathname;

  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    });
    return res.end();
  }

  /* ---- API ---- */
  if (pathname === "/api/state" && req.method === "GET") {
    return sendJSON(res, 200, state);
  }

  if (pathname === "/api/stream" && req.method === "GET") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "Access-Control-Allow-Origin": "*",
      "X-Accel-Buffering": "no",
    });
    res.write(`retry: 3000\n\n`);
    res.write(`data: ${JSON.stringify(state)}\n\n`);
    clients.add(res);

    const ping = setInterval(() => {
      try { res.write(`: ping\n\n`); } catch (e) { /* ignore */ }
    }, 20000);

    req.on("close", () => {
      clearInterval(ping);
      clients.delete(res);
    });
    return;
  }

  if (pathname === "/api/out" && req.method === "POST") {
    try {
      const body = await readBody(req);
      let name = String(body.name || "").trim().replace(/\s+/g, " ").toUpperCase();
      if (!name) return sendJSON(res, 400, { ok: false, error: "Nama wajib diisi." });
      if (name.length > 60) name = name.slice(0, 60);

      const existing = state.active.find((a) => a.name === name);
      if (existing) {
        return sendJSON(res, 409, {
          ok: false,
          error: `${name} sudah tercatat sedang merokok sejak ${new Date(existing.outAt).toISOString()}.`,
        });
      }

      const entry = { name, outAt: Date.now(), id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}` };
      state.active.push(entry);
      saveData();
      broadcast();
      return sendJSON(res, 200, { ok: true, entry, state });
    } catch (e) {
      return sendJSON(res, 400, { ok: false, error: e.message });
    }
  }

  if (pathname === "/api/in" && req.method === "POST") {
    try {
      const body = await readBody(req);
      const name = String(body.name || "").trim().replace(/\s+/g, " ").toUpperCase();
      const idx = state.active.findIndex((a) => a.name === name);
      if (idx === -1) return sendJSON(res, 404, { ok: false, error: "Nama tidak ditemukan di daftar keluar." });

      const item = state.active[idx];
      const inAt = Date.now();
      const durationMs = inAt - item.outAt;
      const rec = { name: item.name, outAt: item.outAt, inAt, durationMs };
      state.history.unshift(rec);
      if (state.history.length > 2000) state.history.length = 2000;
      state.active.splice(idx, 1);
      saveData();
      broadcast();
      return sendJSON(res, 200, { ok: true, record: rec, state });
    } catch (e) {
      return sendJSON(res, 400, { ok: false, error: e.message });
    }
  }

  /* ---- static files ---- */
  let filePath = pathname === "/" ? "/index.html" : pathname;
  filePath = path.normalize(filePath).replace(/^(\.\.[/\\])+/, "");
  const abs = path.join(__dirname, filePath);

  if (!abs.startsWith(__dirname)) {
    res.writeHead(403); return res.end("Forbidden");
  }

  fs.readFile(abs, (err, data) => {
    if (err) {
      res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      return res.end("404 Not Found");
    }
    const ext = path.extname(abs).toLowerCase();
    res.writeHead(200, {
      "Content-Type": MIME[ext] || "application/octet-stream",
      "Cache-Control": ext === ".html" ? "no-store" : "public, max-age=60",
    });
    res.end(data);
  });
});

loadData();
server.listen(PORT, "0.0.0.0", () => {
  console.log(`Tracker Merokok online di port ${PORT}`);
});
