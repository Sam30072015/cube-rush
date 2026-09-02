const path = require("path");
const http = require("http");
const express = require("express");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 3000);
const ADMIN_KEY = process.env.ADMIN_KEY || "603781";

const app = express();
app.use(express.json({ limit: "50kb" }));
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Spielername -> WebSocket
const players = new Map();

function cleanName(name) {
  return String(name || "").trim().slice(0, 40);
}

function send(ws, payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function broadcast(payload) {
  const data = JSON.stringify(payload);

  for (const ws of wss.clients) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(data);
    }
  }
}

function adminOK(req) {
  return req.headers["x-admin-key"] === ADMIN_KEY;
}

function amountFrom(body) {
  return Math.max(0, Math.floor(Number(body?.coins) || 0));
}

wss.on("connection", (ws) => {
  ws.playerName = "";
  ws.lastHeartbeat = Date.now();

  ws.on("message", (raw) => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    ws.lastHeartbeat = Date.now();

    if (msg.type === "identify") {
      let name = cleanName(msg.name);

      if (!name) {
        name =
          "Spieler-" +
          Math.random().toString(36).slice(2, 8).toUpperCase();
      }

      // Verhindert zwei aktive Verbindungen mit gleichem Namen.
      if (players.has(name) && players.get(name) !== ws) {
        name =
          name +
          "-" +
          Math.random().toString(36).slice(2, 5).toUpperCase();
      }

      if (
        ws.playerName &&
        players.get(ws.playerName) === ws
      ) {
        players.delete(ws.playerName);
      }

      ws.playerName = name;
      players.set(name, ws);

      send(ws, {
        type: "connected",
        name
      });

      return;
    }

    if (msg.type === "heartbeat") {
      send(ws, {
        type: "heartbeatAck"
      });
    }
  });

  ws.on("close", () => {
    if (
      ws.playerName &&
      players.get(ws.playerName) === ws
    ) {
      players.delete(ws.playerName);
    }
  });

  ws.on("error", () => {
    if (
      ws.playerName &&
      players.get(ws.playerName) === ws
    ) {
      players.delete(ws.playerName);
    }
  });
});

// Entfernt Spieler, die nicht mehr erreichbar sind.
setInterval(() => {
  const now = Date.now();

  for (const [name, ws] of players) {
    if (
      ws.readyState !== WebSocket.OPEN ||
      now - (ws.lastHeartbeat || 0) > 30000
    ) {
      players.delete(name);

      try {
        ws.terminate();
      } catch {}
    }
  }
}, 15000);


/* =========================================================
   ADMIN: 2x MÜNZEN EVENT
   ========================================================= */

app.post("/api/admin/coins-event", (req, res) => {
  if (!adminOK(req)) {
    return res.status(401).json({
      error: "Unauthorized"
    });
  }

  const action = String(req.body?.action || "");

  if (action === "start") {
    const durationMs = Math.max(
      1000,
      Math.min(
        10080 * 60 * 1000,
        Math.floor(Number(req.body?.durationMs) || 0)
      )
    );

    const until = Date.now() + durationMs;

    const message = {
      type: "coinEvent",
      until
    };

    broadcast(message);

    return res.json({
      ok: true,
      ...message
    });
  }

  if (action === "stop") {
    const message = {
      type: "coinEvent",
      until: 0
    };

    broadcast(message);

    return res.json({
      ok: true,
      ...message
    });
  }

  return res.status(400).json({
    error: "Invalid action"
  });
});


/* =========================================================
   ADMIN: MÜNZEN AN EINEN SPIELER
   ========================================================= */

app.post("/api/admin/give", (req, res) => {
  if (!adminOK(req)) {
    return res.status(401).json({
      error: "Unauthorized"
    });
  }

  const player = cleanName(req.body?.player);
  const coins = amountFrom(req.body);

  if (!player || coins <= 0) {
    return res.status(400).json({
      error: "Player and a coin amount are required"
    });
  }

  const target = players.get(player);

  if (
    !target ||
    target.readyState !== WebSocket.OPEN
  ) {
    if (players.get(player) === target) {
      players.delete(player);
    }

    return res.status(404).json({
      error: "Player is not online"
    });
  }

  // Der Spieler bekommt EXAKT die eingegebene Anzahl Münzen.
  send(target, {
    type: "gift",
    target: player,
    coins: coins
  });

  return res.json({
    ok: true,
    player: player,
    coins: coins
  });
});


/* =========================================================
   ADMIN: MÜNZEN AN ALLE SPIELER
   ========================================================= */

app.post("/api/admin/give-all", (req, res) => {
  if (!adminOK(req)) {
    return res.status(401).json({
      error: "Unauthorized"
    });
  }

  const coins = amountFrom(req.body);

  if (coins <= 0) {
    return res.status(400).json({
      error: "A coin amount greater than 0 is required"
    });
  }

  let count = 0;

  const message = JSON.stringify({
    type: "giftAll",
    coins: coins
  });

  for (const [name, ws] of players) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(message);
      count += 1;
    } else {
      players.delete(name);
    }
  }

  return res.json({
    ok: true,
    coins: coins,
    count: count
  });
});


/* =========================================================
   ADMIN: SERVER-NACHRICHT
   ========================================================= */

app.post("/api/admin/message", (req, res) => {
  if (!adminOK(req)) {
    return res.status(401).json({
      error: "Unauthorized"
    });
  }

  const text = String(
    req.body?.text || ""
  )
    .trim()
    .slice(0, 120);

  const minutes = Math.max(
    0,
    Math.floor(Number(req.body?.minutes) || 0)
  );

  const seconds = Math.max(
    0,
    Math.min(
      59,
      Math.floor(Number(req.body?.seconds) || 0)
    )
  );

  const duration =
    (minutes * 60 + seconds) * 1000;

  if (!text || duration <= 0) {
    return res.status(400).json({
      error: "Text and duration required"
    });
  }

  const message = {
    type: "serverMessage",
    text: text,
    endsAt: Date.now() + duration
  };

  broadcast(message);

  return res.json({
    ok: true,
    ...message
  });
});


/* =========================================================
   ADMIN: ONLINE SPIELER
   ========================================================= */

app.get("/api/status", (req, res) => {
  if (!adminOK(req)) {
    return res.status(401).json({
      error: "Unauthorized"
    });
  }

  res.json({
    onlinePlayers: [...players.keys()]
  });
});


/* =========================================================
   RENDER / HTML FALLBACK
   ========================================================= */

app.use((req, res) => {
  res.sendFile(
    path.join(
      __dirname,
      "public",
      "index.html"
    )
  );
});


/* =========================================================
   SERVER START
   ========================================================= */

server.listen(PORT, () => {
  console.log(
    `Cube Rush server running on http://localhost:${PORT}`
  );
});
