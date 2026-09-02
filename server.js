const path = require("path");
const http = require("http");
const express = require("express");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 3000);
const ADMIN_KEY = process.env.ADMIN_KEY || "603781";
const SECOND_ADMIN_KEY = process.env.SECOND_ADMIN_KEY || "6301";

const app = express();
app.use(express.json({ limit: "50kb" }));
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const players = new Map();
let poopEventUntil = 0;
let coinEventUntil = 0;
let serverMessages = [];
let eventRequests = [];

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
    if (ws.readyState === WebSocket.OPEN) ws.send(data);
  }
}

function adminOK(req) {
  return req.headers["x-admin-key"] === ADMIN_KEY;
}

function secondAdminOK(req) {
  return req.headers["x-admin-key"] === SECOND_ADMIN_KEY;
}

function adminMessageEventOK(req) {
  const key = req.headers["x-admin-key"];
  return key === ADMIN_KEY || key === SECOND_ADMIN_KEY;
}

function amountFrom(body) {
  return Math.max(0, Math.floor(Number(body?.coins) || 0));
}

function makeRequestId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
}

function publicRequests() {
  return eventRequests.map(r => ({
    id: r.id,
    event: r.event,
    action: r.action,
    durationMs: r.durationMs,
    createdAt: r.createdAt
  }));
}

function executeEvent(event, action, durationMs) {
  if (event === "coins") {
    if (action === "start") {
      coinEventUntil = Date.now() + durationMs;
      broadcast({ type: "coinEvent", until: coinEventUntil });
      return { until: coinEventUntil };
    }

    if (action === "stop") {
      coinEventUntil = 0;
      broadcast({ type: "coinEvent", until: 0 });
      return { until: 0 };
    }
  }

  if (event === "poop") {
    if (action === "start") {
      poopEventUntil = Date.now() + durationMs;
      broadcast({ type: "poopEvent", until: poopEventUntil });
      return { until: poopEventUntil };
    }

    if (action === "stop") {
      poopEventUntil = 0;
      broadcast({ type: "poopEvent", until: 0 });
      return { until: 0 };
    }
  }

  throw new Error("Ungültiges Event");
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
          Math.random()
            .toString(36)
            .slice(2, 8)
            .toUpperCase();
      }

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
   HAUPT-ADMIN 603781
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
        Math.floor(
          Number(req.body?.durationMs) || 0
        )
      )
    );

    const result = executeEvent(
      "coins",
      "start",
      durationMs
    );

    return res.json({
      ok: true,
      ...result
    });
  }

  if (action === "stop") {
    const result = executeEvent(
      "coins",
      "stop",
      0
    );

    return res.json({
      ok: true,
      ...result
    });
  }

  return res.status(400).json({
    error: "Invalid action"
  });
});


app.post("/api/admin/poop-event", (req, res) => {
  if (!adminOK(req)) {
    return res.status(401).json({
      error: "Unauthorized"
    });
  }

  const action = String(req.body?.action || "");

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

    const result = executeEvent(
      "poop",
      "start",
      durationMs
    );

    return res.json({
      ok: true,
      ...result
    });
  }

  if (action === "stop") {
    const result = executeEvent(
      "poop",
      "stop",
      0
    );

    return res.json({
      ok: true,
      ...result
    });
  }

  return res.status(400).json({
    error: "Invalid action"
  });
});


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
      error:
        "Player and a coin amount are required"
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
});


app.post("/api/admin/give-all", (req, res) => {
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
});


app.post("/api/admin/take", (req, res) => {
  if (!adminOK(req)) {
    return res.status(401).json({
      error: "Unauthorized"
    });
  }

  const player = cleanName(req.body?.player);
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
    if (players.get(player) === target) {
      players.delete(player);
    }

    return res.status(404).json({
      error: "Player is not online"
    });
  }

  send(target, {
    type: "takeCoins",
    target: player,
    coins
  });

  return res.json({
    ok: true,
    player,
    coins
  });
});


/* =========================================================
   SERVER-NACHRICHTEN — BEIDE ADMINS
   ========================================================= */

app.post("/api/admin/message", (req, res) => {
  if (!adminMessageEventOK(req)) {
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
      Date.now().toString(36) +
      Math.random()
        .toString(36)
        .slice(2, 7),

    type: "serverMessage",

    text,

    endsAt:
      Date.now() + duration
  };

  // Neue Nachricht hinzufügen,
  // bestehende Nachrichten bleiben erhalten.
  serverMessages.push(message);

  broadcast(message);

  return res.json({
    ok: true,
    message
  });
});


/* ---------- Alle Nachrichten löschen ---------- */

app.post(
  "/api/admin/message/delete",
  (req, res) => {
    if (!adminMessageEventOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    serverMessages = [];

    broadcast({
      type: "serverMessagesClear"
    });

    broadcast({
      type: "serverMessageDelete"
    });

    return res.json({
      ok: true
    });
  }
);


/* =========================================================
   ZWEITES ADMIN-PANEL 6301
   ========================================================= */


/* ---------- Event-Anfrage ---------- */

app.post(
  "/api/second-admin/event-request",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const event = String(
      req.body?.event || ""
    );

    const action = String(
      req.body?.action || ""
    );

    if (
      !["coins", "poop"].includes(event)
    ) {
      return res.status(400).json({
        error: "Ungültiges Event"
      });
    }

    if (
      !["start", "stop"].includes(action)
    ) {
      return res.status(400).json({
        error: "Ungültige Aktion"
      });
    }

    let durationMs = 0;

    if (action === "start") {
      const value =
        Number(req.body?.durationMs) || 0;

      durationMs =
        event === "poop"
          ? Math.max(
              60000,
              Math.min(
                10080 * 60 * 1000,
                Math.floor(value)
              )
            )
          : Math.max(
              1000,
              Math.min(
                10080 * 60 * 1000,
                Math.floor(value)
              )
            );
    }

    // Doppelanfragen verhindern
    const duplicate =
      eventRequests.find(
        (r) =>
          r.event === event &&
          r.action === action
      );

    if (duplicate) {
      return res.json({
        ok: true,
        pending: true,
        id: duplicate.id,
        duplicate: true
      });
    }

    const request = {
      id: makeRequestId(),
      event,
      action,
      durationMs,
      createdAt: Date.now()
    };

    eventRequests.push(request);

    return res.json({
      ok: true,
      pending: true,
      id: request.id
    });
  }
);


/* =========================================================
   ADMIN 1: OFFENE EVENT-ANFRAGEN ABRUFEN
   ========================================================= */

app.get(
  "/api/admin/event-requests",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const now = Date.now();

    eventRequests =
      eventRequests.filter(
        (request) =>
          now -
            Number(
              request.createdAt || 0
            ) <
          10 * 60 * 1000
      );

    return res.json({
      ok: true,
      requests: publicRequests()
    });
  }
);


/* =========================================================
   ADMIN 1: EVENT ANNEHMEN
   ========================================================= */

app.post(
  [
    "/api/admin/event-requests/:id/approve",
    "/api/admin/event-requests/approve"
  ],
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const id = String(
      req.params.id ||
      req.body?.id ||
      ""
    );

    const index =
      eventRequests.findIndex(
        (request) =>
          request.id === id
      );

    if (index < 0) {
      return res.status(404).json({
        error:
          "Anfrage nicht gefunden"
      });
    }

    const request =
      eventRequests[index];

    eventRequests.splice(index, 1);

    try {
      const result =
        executeEvent(
          request.event,
          request.action,
          request.durationMs
        );

      return res.json({
        ok: true,
        approved: true,
        request,
        ...result
      });
    } catch (error) {
      return res.status(400).json({
        error:
          error.message ||
          "Event konnte nicht gestartet werden"
      });
    }
  }
);


/* =========================================================
   ADMIN 1: EVENT ABLEHNEN
   ========================================================= */

app.post(
  [
    "/api/admin/event-requests/:id/deny",
    "/api/admin/event-requests/reject"
  ],
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const id = String(
      req.params.id ||
      req.body?.id ||
      ""
    );

    const index =
      eventRequests.findIndex(
        (request) =>
          request.id === id
      );

    if (index < 0) {
      return res.status(404).json({
        error:
          "Anfrage nicht gefunden"
      });
    }

    const request =
      eventRequests[index];

    eventRequests.splice(index, 1);

    return res.json({
      ok: true,
      denied: true,
      request
    });
  }
);


/* =========================================================
   ZWEITER ADMIN — KOMPATIBILITÄT
   ========================================================= */

// Start: nur Anfrage.
// Stop: direkt stoppen.

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

    if (action === "start") {
      const durationMs = Math.max(
        1000,
        Math.min(
          10080 * 60 * 1000,
          Math.floor(
            Number(
              req.body?.durationMs
            ) || 0
          )
        )
      );

      const duplicate =
        eventRequests.find(
          (request) =>
            request.event === "coins" &&
            request.action === "start"
        );

      if (duplicate) {
        return res.json({
          ok: true,
          pending: true,
          id: duplicate.id
        });
      }

      const request = {
        id: makeRequestId(),
        event: "coins",
        action: "start",
        durationMs,
        createdAt: Date.now()
      };

      eventRequests.push(request);

      return res.json({
        ok: true,
        pending: true,
        id: request.id
      });
    }

    return res.status(400).json({
      error: "Invalid action"
    });
  }
);


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

    if (action === "start") {
      const durationMs = Math.max(
        60000,
        Math.min(
          10080 * 60 * 1000,
          Math.floor(
            Number(
              req.body?.durationMs
            ) || 0
          )
        )
      );

      const duplicate =
        eventRequests.find(
          (request) =>
            request.event === "poop" &&
            request.action === "start"
        );

      if (duplicate) {
        return res.json({
          ok: true,
          pending: true,
          id: duplicate.id
        });
      }

      const request = {
        id: makeRequestId(),
        event: "poop",
        action: "start",
        durationMs,
        createdAt: Date.now()
      };

      eventRequests.push(request);

      return res.json({
        ok: true,
        pending: true,
        id: request.id
      });
    }

    return res.status(400).json({
      error: "Invalid action"
    });
  }
);


/* =========================================================
   STATUS
   ========================================================= */

app.get(
  "/api/second-admin/status",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const now = Date.now();

    eventRequests =
      eventRequests.filter(
        (request) =>
          now -
            Number(
              request.createdAt || 0
            ) <
          10 * 60 * 1000
      );

    res.json({
      ok: true,

      coinEventUntil:
        coinEventUntil > now
          ? coinEventUntil
          : 0,

      poopEventUntil:
        poopEventUntil > now
          ? poopEventUntil
          : 0,

      messages:
        serverMessages,

      pendingRequests:
        publicRequests()
    });
  }
);


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
   AUTOMATISCHES AUFRÄUMEN
   ========================================================= */

setInterval(() => {
  const now = Date.now();

  serverMessages =
    serverMessages.filter(
      (message) =>
        Number(
          message.endsAt || 0
        ) > now
    );

  eventRequests =
    eventRequests.filter(
      (request) =>
        now -
          Number(
            request.createdAt || 0
          ) <
        10 * 60 * 1000
    );
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
    "Haupt-Admin: 603781"
  );

  console.log(
    "Zweiter Admin: 6301"
  );
});
