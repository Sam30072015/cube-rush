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

let coinEventUntil = 0;
let tenCoinEventUntil = 0;
let galaxyEventUntil = 0;

let serverMessages = [];
let eventRequests = [];

// =========================================================
// TRADE
// =========================================================

let tradeRequests = [];
const activeTrades = new Map();
const completedTrades = new Map();

// =========================================================
// FRAGE / UMFRAGE
// =========================================================

let activePoll = null;

// =========================================================
// HILFSFUNKTIONEN
// =========================================================

function now() {
  return Date.now();
}

function json(res, status, data) {
  return res.status(status).json(data);
}

function broadcast(data) {
  const message = JSON.stringify(data);

  for (const ws of wss.clients) {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(message);
      } catch (err) {
        console.error("WebSocket broadcast error:", err);
      }
    }
  }
}

function send(ws, data) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;

  try {
    ws.send(JSON.stringify(data));
  } catch (err) {
    console.error("WebSocket send error:", err);
  }
}

function isAdmin(req) {
  const key =
    req.headers["x-admin-key"] ||
    req.query.key ||
    req.body?.key;

  return String(key || "") === String(ADMIN_KEY);
}

function isSecondAdmin(req) {
  const key =
    req.headers["x-admin-key"] ||
    req.query.key ||
    req.body?.key;

  return String(key || "") === String(SECOND_ADMIN_KEY);
}

function requireAdmin(req, res, next) {
  if (!isAdmin(req)) {
    return json(res, 403, {
      ok: false,
      error: "Nicht autorisiert"
    });
  }

  next();
}

function requireSecondAdmin(req, res, next) {
  if (!isSecondAdmin(req)) {
    return json(res, 403, {
      ok: false,
      error: "Nicht autorisiert"
    });
  }

  next();
}

function cleanText(value, max = 500) {
  return String(value ?? "")
    .replace(/\0/g, "")
    .trim()
    .slice(0, max);
}

function randomId(prefix = "id") {
  return (
    prefix +
    "_" +
    Date.now().toString(36) +
    "_" +
    Math.random().toString(36).slice(2, 10)
  );
}

// =========================================================
// EVENTS
// =========================================================

function eventState() {
  const t = Date.now();

  return {
    coinEventUntil:
      coinEventUntil > t ? coinEventUntil : 0,

    tenCoinEventUntil:
      tenCoinEventUntil > t ? tenCoinEventUntil : 0,

    galaxyEventUntil:
      galaxyEventUntil > t ? galaxyEventUntil : 0
  };
}

function broadcastEventState() {
  const state = eventState();

  broadcast({
    type: "coinEvent",
    until: state.coinEventUntil
  });

  broadcast({
    type: "tenCoinEvent",
    until: state.tenCoinEventUntil
  });

  broadcast({
    type: "galaxyEvent",
    until: state.galaxyEventUntil
  });
}

function executeEvent(event, durationMs = 60000) {
  const duration = Math.max(
    0,
    Number(durationMs) || 0
  );

  const until = Date.now() + duration;

  // =======================================================
  // MÜNZEN-EVENT
  //
  // Das Haupt-Münzen-Event ist das 10×-Event.
  //
  // Die eigentliche Level-Belohnung kommt aus der
  // index.html:
  //
  // Normale Welt:
  //   500 × Spur
  //
  // Galaxy:
  //   1000 × Spur
  //
  // Dadurch:
  //
  // Normal + normale Spur = 500
  // Normal + Doppelspur   = 1000
  // Galaxy + normale Spur = 1000
  // Galaxy + Doppelspur   = 2000
  //
  // Das 10×-Event wird separat über tenCoinEventUntil
  // übertragen.
  // =======================================================

  if (event === "coins") {
    // Altes 2×-Event nicht parallel aktiv lassen.
    coinEventUntil = 0;

    tenCoinEventUntil = until;

    broadcast({
      type: "coinEvent",
      until: 0
    });

    broadcast({
      type: "tenCoinEvent",
      until: tenCoinEventUntil
    });

    return {
      ok: true,
      event: "coins",
      until: tenCoinEventUntil
    };
  }

  // =======================================================
  // LEGACY 10× EVENT
  // =======================================================

  if (event === "tenCoins") {
    tenCoinEventUntil = until;

    broadcast({
      type: "tenCoinEvent",
      until: tenCoinEventUntil
    });

    return {
      ok: true,
      event: "tenCoins",
      until: tenCoinEventUntil
    };
  }

  // =======================================================
  // GALAXY EVENT
  // =======================================================

  if (event === "galaxy") {
    galaxyEventUntil = until;

    broadcast({
      type: "galaxyEvent",
      until: galaxyEventUntil
    });

    return {
      ok: true,
      event: "galaxy",
      until: galaxyEventUntil
    };
  }

  return {
    ok: false,
    error: "Unbekanntes Event"
  };
}

function stopEvent(event) {
  if (event === "coins") {
    coinEventUntil = 0;
    tenCoinEventUntil = 0;

    broadcast({
      type: "coinEvent",
      until: 0
    });

    broadcast({
      type: "tenCoinEvent",
      until: 0
    });

    return {
      ok: true,
      event: "coins",
      until: 0
    };
  }

  if (event === "tenCoins") {
    tenCoinEventUntil = 0;

    broadcast({
      type: "tenCoinEvent",
      until: 0
    });

    return {
      ok: true,
      event: "tenCoins",
      until: 0
    };
  }

  if (event === "galaxy") {
    galaxyEventUntil = 0;

    broadcast({
      type: "galaxyEvent",
      until: 0
    });

    return {
      ok: true,
      event: "galaxy",
      until: 0
    };
  }

  return {
    ok: false,
    error: "Unbekanntes Event"
  };
}

// =========================================================
// STATUS
// =========================================================

app.get("/api/status", (req, res) => {
  return json(res, 200, {
    ok: true,
    players: players.size,
    ...eventState(),
    serverMessages,
    eventRequests,
    activePoll,
    tradeRequests
  });
});

// =========================================================
// PLAYER STATUS
// =========================================================

app.get("/api/players", requireAdmin, (req, res) => {
  const result = [];

  for (const [id, player] of players.entries()) {
    result.push({
      id,
      name: player.name,
      connectedAt: player.connectedAt,
      level: player.level,
      world: player.world,
      coins: player.coins
    });
  }

  return json(res, 200, {
    ok: true,
    players: result
  });
});

// =========================================================
// SERVER MESSAGES
// =========================================================

app.post(
  "/api/admin/message",
  requireAdmin,
  (req, res) => {
    const text = cleanText(req.body?.message, 500);

    if (!text) {
      return json(res, 400, {
        ok: false,
        error: "Nachricht fehlt"
      });
    }

    const message = {
      id: randomId("msg"),
      text,
      createdAt: Date.now()
    };

    serverMessages.push(message);

    if (serverMessages.length > 100) {
      serverMessages =
        serverMessages.slice(-100);
    }

    broadcast({
      type: "serverMessage",
      message
    });

    return json(res, 200, {
      ok: true,
      message
    });
  }
);

app.get(
  "/api/admin/messages",
  requireAdmin,
  (req, res) => {
    return json(res, 200, {
      ok: true,
      messages: serverMessages
    });
  }
);

app.delete(
  "/api/admin/messages",
  requireAdmin,
  (req, res) => {
    serverMessages = [];

    broadcast({
      type: "serverMessagesClear"
    });

    return json(res, 200, {
      ok: true
    });
  }
);

// =========================================================
// MÜNZEN-EVENT
// =========================================================

app.post(
  "/api/admin/coins-event",
  requireAdmin,
  (req, res) => {
    const minutes =
      Number(req.body?.minutes);

    const durationMs =
      Number.isFinite(minutes) && minutes > 0
        ? minutes * 60 * 1000
        : Number(req.body?.durationMs) || 60000;

    return json(
      res,
      200,
      executeEvent("coins", durationMs)
    );
  }
);

app.post(
  "/api/admin/coins-event/stop",
  requireAdmin,
  (req, res) => {
    return json(
      res,
      200,
      stopEvent("coins")
    );
  }
);

// =========================================================
// LEGACY 10× MÜNZEN-EVENT
// =========================================================

app.post(
  "/api/admin/ten-coins-event",
  requireAdmin,
  (req, res) => {
    const minutes =
      Number(req.body?.minutes);

    const durationMs =
      Number.isFinite(minutes) && minutes > 0
        ? minutes * 60 * 1000
        : Number(req.body?.durationMs) || 60000;

    return json(
      res,
      200,
      executeEvent("tenCoins", durationMs)
    );
  }
);

app.post(
  "/api/admin/ten-coins-event/stop",
  requireAdmin,
  (req, res) => {
    return json(
      res,
      200,
      stopEvent("tenCoins")
    );
  }
);

// =========================================================
// GALAXY EVENT
// =========================================================

app.post(
  "/api/admin/galaxy-event",
  requireAdmin,
  (req, res) => {
    const minutes =
      Number(req.body?.minutes);

    const durationMs =
      Number.isFinite(minutes) && minutes > 0
        ? minutes * 60 * 1000
        : Number(req.body?.durationMs) || 60000;

    return json(
      res,
      200,
      executeEvent("galaxy", durationMs)
    );
  }
);

app.post(
  "/api/admin/galaxy-event/stop",
  requireAdmin,
  (req, res) => {
    return json(
      res,
      200,
      stopEvent("galaxy")
    );
  }
);

// =========================================================
// SECOND ADMIN – EVENT REQUESTS
// =========================================================

app.post(
  "/api/second-admin/event-request",
  requireSecondAdmin,
  (req, res) => {
    const event = cleanText(
      req.body?.event,
      50
    );

    const allowed = [
      "coins",
      "tenCoins",
      "galaxy"
    ];

    if (!allowed.includes(event)) {
      return json(res, 400, {
        ok: false,
        error: "Ungültiges Event"
      });
    }

    const request = {
      id: randomId("event"),
      event,
      createdAt: Date.now()
    };

    eventRequests.push(request);

    if (eventRequests.length > 100) {
      eventRequests =
        eventRequests.slice(-100);
    }

    broadcast({
      type: "eventRequest",
      request
    });

    return json(res, 200, {
      ok: true,
      request
    });
  }
);

app.get(
  "/api/second-admin/event-requests",
  requireSecondAdmin,
  (req, res) => {
    return json(res, 200, {
      ok: true,
      requests: eventRequests
    });
  }
);

app.post(
  "/api/admin/event-request/:id/approve",
  requireAdmin,
  (req, res) => {
    const index =
      eventRequests.findIndex(
        x => x.id === req.params.id
      );

    if (index < 0) {
      return json(res, 404, {
        ok: false,
        error: "Anfrage nicht gefunden"
      });
    }

    const request =
      eventRequests[index];

    eventRequests.splice(index, 1);

    const result =
      executeEvent(request.event, 10 * 60 * 1000);

    broadcast({
      type: "eventRequestApproved",
      request,
      result
    });

    return json(res, 200, {
      ok: true,
      request,
      result
    });
  }
);

app.post(
  "/api/admin/event-request/:id/deny",
  requireAdmin,
  (req, res) => {
    const index =
      eventRequests.findIndex(
        x => x.id === req.params.id
      );

    if (index < 0) {
      return json(res, 404, {
        ok: false,
        error: "Anfrage nicht gefunden"
      });
    }

    const request =
      eventRequests[index];

    eventRequests.splice(index, 1);

    broadcast({
      type: "eventRequestDenied",
      request
    });

    return json(res, 200, {
      ok: true,
      request
    });
  }
);

// =========================================================
// POLL / UMFRAGE
// =========================================================

app.get(
  "/api/poll",
  (req, res) => {
    return json(res, 200, {
      ok: true,
      poll: activePoll
    });
  }
);

app.post(
  "/api/admin/poll",
  requireAdmin,
  (req, res) => {
    const question =
      cleanText(req.body?.question, 300);

    if (!question) {
      return json(res, 400, {
        ok: false,
        error: "Frage fehlt"
      });
    }

    activePoll = {
      id: randomId("poll"),
      question,
      yes: 0,
      no: 0,
      createdAt: Date.now()
    };

    broadcast({
      type: "poll",
      poll: activePoll
    });

    return json(res, 200, {
      ok: true,
      poll: activePoll
    });
  }
);

app.post(
  "/api/admin/poll/stop",
  requireAdmin,
  (req, res) => {
    activePoll = null;

    broadcast({
      type: "poll",
      poll: null
    });

    return json(res, 200, {
      ok: true
    });
  }
);

// =========================================================
// TRADE
// =========================================================

app.get(
  "/api/trades",
  (req, res) => {
    return json(res, 200, {
      ok: true,
      requests: tradeRequests
    });
  }
);

app.get(
  "/api/admin/trades",
  requireAdmin,
  (req, res) => {
    return json(res, 200, {
      ok: true,
      requests: tradeRequests,
      activeTrades:
        Array.from(activeTrades.entries()),
      completedTrades:
        Array.from(completedTrades.entries())
    });
  }
);

app.post(
  "/api/trade/request",
  (req, res) => {
    const from = cleanText(
      req.body?.from,
      100
    );

    const to = cleanText(
      req.body?.to,
      100
    );

    const offer =
      req.body?.offer || {};

    const request = {
      id: randomId("trade"),
      from,
      to,
      offer,
      createdAt: Date.now(),
      status: "pending"
    };

    tradeRequests.push(request);

    broadcast({
      type: "tradeRequest",
      request
    });

    return json(res, 200, {
      ok: true,
      request
    });
  }
);

app.post(
  "/api/trade/:id/accept",
  (req, res) => {
    const request =
      tradeRequests.find(
        x => x.id === req.params.id
      );

    if (!request) {
      return json(res, 404, {
        ok: false,
        error: "Trade nicht gefunden"
      });
    }

    request.status = "accepted";
    request.acceptedAt = Date.now();

    activeTrades.set(
      request.id,
      request
    );

    broadcast({
      type: "tradeAccepted",
      request
    });

    return json(res, 200, {
      ok: true,
      request
    });
  }
);

app.post(
  "/api/trade/:id/decline",
  (req, res) => {
    const request =
      tradeRequests.find(
        x => x.id === req.params.id
      );

    if (!request) {
      return json(res, 404, {
        ok: false,
        error: "Trade nicht gefunden"
      });
    }

    request.status = "declined";
    request.declinedAt = Date.now();

    broadcast({
      type: "tradeDeclined",
      request
    });

    return json(res, 200, {
      ok: true,
      request
    });
  }
);

// =========================================================
// WEBSOCKET
// =========================================================

wss.on("connection", (ws, req) => {
  const id = randomId("player");

  const player = {
    id,
    name: "Spieler",
    connectedAt: Date.now(),
    level: 0,
    world: "normal",
    coins: 0
  };

  players.set(id, player);

  send(ws, {
    type: "connected",
    playerId: id,
    ...eventState(),
    poll: activePoll,
    messages: serverMessages
  });

  send(ws, {
    type: "serverMessages",
    messages: serverMessages
  });

  ws.on("message", raw => {
    let data;

    try {
      data = JSON.parse(
        raw.toString()
      );
    } catch (err) {
      send(ws, {
        type: "error",
        error: "Ungültige Nachricht"
      });

      return;
    }

    if (!data || typeof data !== "object") {
      return;
    }

    // =====================================================
    // PLAYER REGISTER
    // =====================================================

    if (data.type === "register") {
      player.name =
        cleanText(
          data.name || "Spieler",
          100
        ) || "Spieler";

      if (
        Number.isFinite(
          Number(data.level)
        )
      ) {
        player.level =
          Number(data.level);
      }

      if (data.world) {
        player.world =
          cleanText(
            data.world,
            100
          );
      }

      if (
        Number.isFinite(
          Number(data.coins)
        )
      ) {
        player.coins =
          Math.max(
            0,
            Math.floor(
              Number(data.coins)
            )
          );
      }

      broadcast({
        type: "playerUpdate",
        player: {
          id: player.id,
          name: player.name,
          level: player.level,
          world: player.world,
          coins: player.coins
        }
      });

      return;
    }

    // =====================================================
    // PLAYER LEVEL UPDATE
    // =====================================================

    if (data.type === "levelUpdate") {
      const level =
        Number(data.level);

      if (Number.isFinite(level)) {
        player.level = level;
      }

      if (data.world) {
        player.world =
          cleanText(
            data.world,
            100
          );
      }

      if (
        Number.isFinite(
          Number(data.coins)
        )
      ) {
        player.coins =
          Math.max(
            0,
            Math.floor(
              Number(data.coins)
            )
          );
      }

      broadcast({
        type: "playerUpdate",
        player: {
          id: player.id,
          name: player.name,
          level: player.level,
          world: player.world,
          coins: player.coins
        }
      });

      return;
    }

    // =====================================================
    // PLAYER POLL ANSWER
    // =====================================================

    if (data.type === "pollAnswer") {
      if (!activePoll) {
        send(ws, {
          type: "pollAnswerResult",
          ok: false,
          error: "Keine aktive Umfrage"
        });

        return;
      }

      const choice =
        String(
          data.choice || ""
        ).toLowerCase();

      if (
        choice !== "yes" &&
        choice !== "no"
      ) {
        send(ws, {
          type: "pollAnswerResult",
          ok: false,
          error: "Ungültige Antwort"
        });

        return;
      }

      if (choice === "yes") {
        activePoll.yes++;
      } else {
        activePoll.no++;
      }

      broadcast({
        type: "pollUpdate",
        poll: activePoll
      });

      send(ws, {
        type: "pollAnswerResult",
        ok: true,
        choice
      });

      return;
    }

    // =====================================================
    // PING
    // =====================================================

    if (data.type === "ping") {
      send(ws, {
        type: "pong",
        time: Date.now()
      });

      return;
    }

    // =====================================================
    // ADMIN MESSAGE VIA WS
    // =====================================================

    if (data.type === "serverMessage") {
      const text =
        cleanText(
          data.message,
          500
        );

      if (!text) return;

      const message = {
        id: randomId("msg"),
        text,
        createdAt: Date.now()
      };

      serverMessages.push(message);

      if (serverMessages.length > 100) {
        serverMessages =
          serverMessages.slice(-100);
      }

      broadcast({
        type: "serverMessage",
        message
      });

      return;
    }
  });

  ws.on("close", () => {
    players.delete(id);

    broadcast({
      type: "playerDisconnected",
      playerId: id
    });
  });

  ws.on("error", err => {
    console.error(
      "WebSocket error:",
      err
    );
  });
});

// =========================================================
// AUTO STOP EVENTS
// =========================================================

setInterval(() => {
  const t = Date.now();

  let changed = false;

  if (
    coinEventUntil > 0 &&
    coinEventUntil <= t
  ) {
    coinEventUntil = 0;

    broadcast({
      type: "coinEvent",
      until: 0
    });

    changed = true;
  }

  if (
    tenCoinEventUntil > 0 &&
    tenCoinEventUntil <= t
  ) {
    tenCoinEventUntil = 0;

    broadcast({
      type: "tenCoinEvent",
      until: 0
    });

    changed = true;
  }

  if (
    galaxyEventUntil > 0 &&
    galaxyEventUntil <= t
  ) {
    galaxyEventUntil = 0;

    broadcast({
      type: "galaxyEvent",
      until: 0
    });

    changed = true;
  }

  if (changed) {
    broadcast({
      type: "eventState",
      ...eventState()
    });
  }
}, 1000);

// =========================================================
// ERROR HANDLER
// =========================================================

app.use(
  (
    err,
    req,
    res,
    next
  ) => {
    console.error(
      "Server error:",
      err
    );

    if (res.headersSent) {
      return next(err);
    }

    return json(res, 500, {
      ok: false,
      error: "Interner Serverfehler"
    });
  }
);

// =========================================================
// START
// =========================================================

server.listen(
  PORT,
  () => {
    console.log(
      `Cube Rush server running on http://localhost:${PORT}`
    );

    console.log(
      "Haupt-Admin: 603781"
    );

    console.log(
      "Zweites Admin-Panel: 6301"
    );
  }
);
