const path = require("path");
const http = require("http");
const express = require("express");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 3000);

const ADMIN_KEY = process.env.ADMIN_KEY || "603781";
const SECOND_ADMIN_KEY = "6301";

const app = express();

app.use(express.json({ limit: "50kb" }));
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const players = new Map();

let poopEventUntil = 0;
let coinEventUntil = 0;

let serverMessages = [];


/* =========================================================
   HILFSFUNKTIONEN
   ========================================================= */

function cleanName(name) {
  return String(name || "")
    .trim()
    .slice(0, 40);
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

function secondAdminOK(req) {
  return req.headers["x-admin-key"] === SECOND_ADMIN_KEY;
}

function anyAdminOK(req) {
  return adminOK(req) || secondAdminOK(req);
}

function amountFrom(body) {
  return Math.max(
    0,
    Math.floor(Number(body?.coins) || 0)
  );
}


/* =========================================================
   WEBSOCKET
   ========================================================= */

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
          Math.random()
            .toString(36)
            .slice(2, 8)
            .toUpperCase();
      }

      // Nur eine Verbindung pro Spielername
      const oldSocket = players.get(name);

      if (oldSocket && oldSocket !== ws) {
        try {
          oldSocket.terminate();
        } catch {}

        players.delete(name);
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
        name,

        poopEventUntil:
          poopEventUntil > Date.now()
            ? poopEventUntil
            : 0,

        coinEventUntil:
          coinEventUntil > Date.now()
            ? coinEventUntil
            : 0,

        serverMessages
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


/* =========================================================
   OFFLINE SPIELER ENTFERNEN
   ========================================================= */

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
   NORMALES ADMIN-PANEL
   CODE: 603781
   ========================================================= */


/* ---------- 2x-Münzen-Event ---------- */

app.post(
  "/api/admin/coins-event",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const action = String(
      req.body?.action || ""
    );

    if (action === "start") {
      const durationMs = Math.max(
        1000,
        Math.min(
          10080 * 60 * 1000,
          Math.floor(
            Number(req.body?.durationMs) || 0
          )
        )
      );

      coinEventUntil =
        Date.now() + durationMs;

      broadcast({
        type: "coinEvent",
        until: coinEventUntil
      });

      return res.json({
        ok: true,
        until: coinEventUntil
      });
    }

    if (action === "stop") {
      coinEventUntil = 0;

      broadcast({
        type: "coinEvent",
        until: 0
      });

      return res.json({
        ok: true,
        until: 0
      });
    }

    return res.status(400).json({
      error: "Invalid action"
    });
  }
);


/* ---------- Münzen geben ---------- */

app.post(
  "/api/admin/give",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const player = cleanName(
      req.body?.player
    );

    const coins = amountFrom(req.body);

    if (!player || coins <= 0) {
      return res.status(400).json({
        error:
          "Player and a coin amount are required"
      });
    }

    const target = players.get(player);

    if (
      !target ||
      target.readyState !== WebSocket.OPEN
    ) {
      return res.status(404).json({
        error: "Player is not online"
      });
    }

    send(target, {
      type: "gift",
      target: player,
      coins
    });

    return res.json({
      ok: true,
      player,
      coins
    });
  }
);


/* ---------- Münzen an alle ---------- */

app.post(
  "/api/admin/give-all",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const coins = amountFrom(req.body);

    if (coins <= 0) {
      return res.status(400).json({
        error:
          "A coin amount greater than 0 is required"
      });
    }

    let count = 0;

    const message = JSON.stringify({
      type: "giftAll",
      coins
    });

    for (const [name, ws] of players) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(message);
        count++;
      } else {
        players.delete(name);
      }
    }

    return res.json({
      ok: true,
      coins,
      count
    });
  }
);


/* ---------- Münzen abziehen ---------- */

app.post(
  "/api/admin/take",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const player = cleanName(
      req.body?.player
    );

    const coins = amountFrom(req.body);

    if (!player || coins <= 0) {
      return res.status(400).json({
        error:
          "Player and a coin amount are required"
      });
    }

    const target = players.get(player);

    if (
      !target ||
      target.readyState !== WebSocket.OPEN
    ) {
      return res.status(404).json({
        error: "Player is not online"
      });
    }

    send(target, {
      type: "giftSubtract",
      target: player,
      coins
    });

    return res.json({
      ok: true,
      player,
      coins
    });
  }
);


/* ---------- Kackhaufen-Event ---------- */

app.post(
  "/api/admin/poop-event",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const action = String(
      req.body?.action || ""
    );

    if (action === "start") {
      const durationMs = Math.max(
        60000,
        Math.min(
          10080 * 60 * 1000,
          Math.floor(
            Number(req.body?.durationMs) || 0
          )
        )
      );

      poopEventUntil =
        Date.now() + durationMs;

      broadcast({
        type: "poopEvent",
        until: poopEventUntil
      });

      return res.json({
        ok: true,
        until: poopEventUntil
      });
    }

    if (action === "stop") {
      poopEventUntil = 0;

      broadcast({
        type: "poopEvent",
        until: 0
      });

      return res.json({
        ok: true,
        until: 0
      });
    }

    return res.status(400).json({
      error: "Invalid action"
    });
  }
);


/* =========================================================
   SERVER-NACHRICHTEN
   ========================================================= */


/* ---------- Nachricht senden ---------- */

app.post(
  "/api/admin/message",
  (req, res) => {
    if (!anyAdminOK(req)) {
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
      Math.floor(
        Number(req.body?.minutes) || 0
      )
    );

    const seconds = Math.max(
      0,
      Math.min(
        59,
        Math.floor(
          Number(req.body?.seconds) || 0
        )
      )
    );

    const duration =
      (minutes * 60 + seconds) * 1000;

    if (!text || duration <= 0) {
      return res.status(400).json({
        error:
          "Text and duration required"
      });
    }

    const message = {
      id:
        Date.now().toString() +
        Math.random()
          .toString(36)
          .slice(2, 8),

      type: "serverMessage",

      text,

      endsAt:
        Date.now() + duration
    };

    serverMessages.push(message);

    // Alte abgelaufene Nachrichten entfernen
    serverMessages =
      serverMessages.filter(
        (m) => m.endsAt > Date.now()
      );

    broadcast(message);

    return res.json({
      ok: true,
      message
    });
  }
);


/* ---------- Alle Servernachrichten löschen ---------- */

app.post(
  "/api/admin/message/clear",
  (req, res) => {
    if (!anyAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    serverMessages = [];

    broadcast({
      type: "serverMessagesClear"
    });

    return res.json({
      ok: true
    });
  }
);


/* ---------- Eine Nachricht löschen ---------- */

app.post(
  "/api/admin/message/delete",
  (req, res) => {
    if (!anyAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const id = String(
      req.body?.id || ""
    );

    if (!id) {
      return res.status(400).json({
        error: "Message id required"
      });
    }

    serverMessages =
      serverMessages.filter(
        (m) => String(m.id) !== id
      );

    broadcast({
      type: "serverMessageDelete",
      id
    });

    return res.json({
      ok: true
    });
  }
);


/* =========================================================
   ZWEITES ADMIN-PANEL
   CODE: 6301
   ========================================================= */


/* ---------- 2x-Münzen-Event starten/stoppen ---------- */

app.post(
  "/api/second-admin/coins-event",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const action = String(
      req.body?.action || ""
    );

    if (action === "start") {
      const durationMs = Math.max(
        1000,
        Math.min(
          10080 * 60 * 1000,
          Math.floor(
            Number(req.body?.durationMs) || 0
          )
        )
      );

      coinEventUntil =
        Date.now() + durationMs;

      broadcast({
        type: "coinEvent",
        until: coinEventUntil
      });

      return res.json({
        ok: true,
        until: coinEventUntil
      });
    }

    if (action === "stop") {
      coinEventUntil = 0;

      broadcast({
        type: "coinEvent",
        until: 0
      });

      return res.json({
        ok: true,
        until: 0
      });
    }

    return res.status(400).json({
      error: "Invalid action"
    });
  }
);


/* ---------- Kackhaufen-Event starten/stoppen ---------- */

app.post(
  "/api/second-admin/poop-event",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const action = String(
      req.body?.action || ""
    );

    if (action === "start") {
      const durationMs = Math.max(
        60000,
        Math.min(
          10080 * 60 * 1000,
          Math.floor(
            Number(req.body?.durationMs) || 0
          )
        )
      );

      poopEventUntil =
        Date.now() + durationMs;

      broadcast({
        type: "poopEvent",
        until: poopEventUntil
      });

      return res.json({
        ok: true,
        until: poopEventUntil
      });
    }

    if (action === "stop") {
      poopEventUntil = 0;

      broadcast({
        type: "poopEvent",
        until: 0
      });

      return res.json({
        ok: true,
        until: 0
      });
    }

    return res.status(400).json({
      error: "Invalid action"
    });
  }
);


/* ---------- Status des zweiten Admins ---------- */

app.get(
  "/api/second-admin/status",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    res.json({
      ok: true,

      coinEventUntil:
        coinEventUntil > Date.now()
          ? coinEventUntil
          : 0,

      poopEventUntil:
        poopEventUntil > Date.now()
          ? poopEventUntil
          : 0,

      messages:
        serverMessages
    });
  }
);


/* =========================================================
   ONLINE SPIELER
   ========================================================= */

app.get(
  "/api/status",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    res.json({
      onlinePlayers: [
        ...players.keys()
      ]
    });
  }
);


/* =========================================================
   SERVER-NACHRICHTEN AUTOMATISCH AUFRÄUMEN
   ========================================================= */

setInterval(() => {
  const now = Date.now();

  const oldLength =
    serverMessages.length;

  serverMessages =
    serverMessages.filter(
      (message) =>
        Number(message.endsAt || 0) > now
    );

  if (
    serverMessages.length !== oldLength
  ) {
    broadcast({
      type: "serverMessagesUpdate",
      messages: serverMessages
    });
  }
}, 1000);


/* =========================================================
   RENDER FALLBACK
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

  console.log(
    "Haupt-Admin aktiv."
  );

  console.log(
    "Zweites Admin-Panel aktiv."
  );
});
