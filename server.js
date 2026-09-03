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

function makeRequestId() {
  return (
    Date.now().toString(36) +
    "-" +
    Math.random()
      .toString(36)
      .slice(2, 10)
  );
}

function publicRequests() {
  return eventRequests.map((request) => ({
    id: request.id,
    event: request.event,
    action: request.action,
    durationMs: request.durationMs,
    createdAt: request.createdAt
  }));
}

function cleanupRequests() {
  const now = Date.now();

  eventRequests = eventRequests.filter(
    (request) =>
      now -
        Number(request.createdAt || 0) <
      10 * 60 * 1000
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

      // Pro Spielername nur eine Verbindung
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

        coinEventUntil:
          coinEventUntil > Date.now()
            ? coinEventUntil
            : 0,

        tenCoinEventUntil:
          tenCoinEventUntil > Date.now()
            ? tenCoinEventUntil
            : 0,

        galaxyEventUntil:
          galaxyEventUntil > Date.now()
            ? galaxyEventUntil
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
   EVENT-FUNKTIONEN
   ========================================================= */

function startCoinEvent(durationMs) {
  coinEventUntil =
    Date.now() + durationMs;

  broadcast({
    type: "coinEvent",
    until: coinEventUntil
  });

  return coinEventUntil;
}

function stopCoinEvent() {
  coinEventUntil = 0;

  broadcast({
    type: "coinEvent",
    until: 0
  });

  return 0;
}

function startTenCoinEvent(durationMs) {
  tenCoinEventUntil =
    Date.now() + durationMs;

  broadcast({
    type: "tenCoinEvent",
    until: tenCoinEventUntil
  });

  return tenCoinEventUntil;
}

function stopTenCoinEvent() {
  tenCoinEventUntil = 0;

  broadcast({
    type: "tenCoinEvent",
    until: 0
  });

  return 0;
}

function startGalaxyEvent(durationMs) {
  galaxyEventUntil =
    Date.now() + durationMs;

  broadcast({
    type: "galaxyEvent",
    until: galaxyEventUntil
  });

  return galaxyEventUntil;
}

function stopGalaxyEvent() {
  galaxyEventUntil = 0;

  broadcast({
    type: "galaxyEvent",
    until: 0
  });

  return 0;
}


/* =========================================================
   HAUPT-ADMIN 603781
   ========================================================= */


/* ---------- 2x-MÜNZEN-EVENT ---------- */

app.post("/api/admin/coins-event", (req, res) => {
  if (!adminOK(req)) {
    return res.status(401).json({
      error: "Unauthorized"
    });
  }

  const action =
    String(req.body?.action || "");

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

    const until =
      startCoinEvent(durationMs);

    return res.json({
      ok: true,
      until
    });
  }

  if (action === "stop") {
    const until =
      stopCoinEvent();

    return res.json({
      ok: true,
      until
    });
  }

  return res.status(400).json({
    error: "Invalid action"
  });
});


/* ---------- 10x-MÜNZEN-EVENT ---------- */

app.post(
  "/api/admin/ten-coin-event",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const action =
      String(req.body?.action || "");

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

      const until =
        startTenCoinEvent(
          durationMs
        );

      return res.json({
        ok: true,
        until
      });
    }

    if (action === "stop") {
      const until =
        stopTenCoinEvent();

      return res.json({
        ok: true,
        until
      });
    }

    return res.status(400).json({
      error: "Invalid action"
    });
  }
);


/* ---------- GALAXY-EVENT ---------- */

app.post(
  "/api/admin/galaxy-event",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const action =
      String(req.body?.action || "");

    if (action === "start") {
      const durationMs =
        Math.max(
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

      const until =
        startGalaxyEvent(
          durationMs
        );

      return res.json({
        ok: true,
        until
      });
    }

    if (action === "stop") {
      const until =
        stopGalaxyEvent();

      return res.json({
        ok: true,
        until
      });
    }

    return res.status(400).json({
      error: "Invalid action"
    });
  }
);


/* ---------- MÜNZEN GEBEN ---------- */

app.post("/api/admin/give", (req, res) => {
  if (!adminOK(req)) {
    return res.status(401).json({
      error: "Unauthorized"
    });
  }

  const player =
    cleanName(req.body?.player);

  const coins =
    amountFrom(req.body);

  if (!player || coins <= 0) {
    return res.status(400).json({
      error:
        "Player and a coin amount are required"
    });
  }

  const target =
    players.get(player);

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
});


/* ---------- MÜNZEN AN ALLE ---------- */

app.post(
  "/api/admin/give-all",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const coins =
      amountFrom(req.body);

    if (coins <= 0) {
      return res.status(400).json({
        error:
          "A coin amount greater than 0 is required"
      });
    }

    let count = 0;

    const message =
      JSON.stringify({
        type: "giftAll",
        coins
      });

    for (const [name, ws] of players) {
      if (
        ws.readyState ===
        WebSocket.OPEN
      ) {
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


/* ---------- MÜNZEN ABZIEHEN ---------- */

app.post(
  "/api/admin/take",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const player =
      cleanName(req.body?.player);

    const coins =
      amountFrom(req.body);

    if (!player || coins <= 0) {
      return res.status(400).json({
        error:
          "Player and a coin amount are required"
      });
    }

    const target =
      players.get(player);

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


/* =========================================================
   SERVERNACHRICHTEN
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

    const text =
      String(req.body?.text || "")
        .trim()
        .slice(0, 120);

    const minutes =
      Math.max(
        0,
        Math.floor(
          Number(
            req.body?.minutes
          ) || 0
        )
      );

    const seconds =
      Math.max(
        0,
        Math.min(
          59,
          Math.floor(
            Number(
              req.body?.seconds
            ) || 0
          )
        )
      );

    const duration =
      (minutes * 60 + seconds) *
      1000;

    if (
      !text ||
      duration <= 0
    ) {
      return res.status(400).json({
        error:
          "Text and duration required"
      });
    }

    const now = Date.now();

    const message = {
      id:
        now.toString(36) +
        "-" +
        Math.random()
          .toString(36)
          .slice(2, 10),

      type:
        "serverMessage",

      text,

      createdAt: now,

      endsAt:
        now + duration
    };

    // Mehrere Nachrichten gleichzeitig
    serverMessages.push(message);

    broadcast(message);

    return res.json({
      ok: true,
      message
    });
  }
);


/* ---------- Alle Nachrichten löschen ---------- */

app.post(
  "/api/admin/message/delete",
  (req, res) => {
    if (!anyAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    serverMessages = [];

    broadcast({
      type:
        "serverMessagesClear"
    });

    return res.json({
      ok: true
    });
  }
);


/* =========================================================
   ZWEITER ADMIN 6301
   ========================================================= */


/*
 * Admin 2 darf Events nur anfragen.
 * Der Haupt-Admin muss sie genehmigen.
 */

app.post(
  "/api/second-admin/event-request",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const event =
      String(
        req.body?.event || ""
      );

    const action =
      String(
        req.body?.action || "start"
      );

    const allowedEvents = [
      "coins",
      "tenCoins",
      "galaxy"
    ];

    if (
      !allowedEvents.includes(event)
    ) {
      return res.status(400).json({
        error:
          "Unbekanntes Event"
      });
    }

    if (action !== "start") {
      return res.status(400).json({
        error:
          "Nur Start-Anfragen sind erlaubt"
      });
    }

    let minDuration = 1000;

    if (event === "galaxy") {
      minDuration = 60000;
    }

    const value =
      Number(
        req.body?.durationMs
      ) || 0;

    const durationMs =
      Math.max(
        minDuration,
        Math.min(
          10080 * 60 * 1000,
          Math.floor(value)
        )
      );

    // Keine doppelten offenen Anfragen
    const existing =
      eventRequests.find(
        (request) =>
          request.event === event &&
          request.action === "start"
      );

    if (existing) {
      return res.json({
        ok: true,
        pending: true,
        id: existing.id,
        duplicate: true
      });
    }

    const request = {
      id:
        makeRequestId(),

      event,

      action:
        "start",

      durationMs,

      createdAt:
        Date.now()
    };

    eventRequests.push(
      request
    );

    return res.json({
      ok: true,
      pending: true,
      id: request.id
    });
  }
);


/* =========================================================
   ADMIN 1: ANFRAGEN ABRUFEN
   ========================================================= */

app.get(
  "/api/admin/event-requests",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    cleanupRequests();

    return res.json({
      ok: true,
      requests:
        publicRequests()
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

    cleanupRequests();

    const id =
      String(
        req.params.id ||
        req.body?.id ||
        req.body?.requestId ||
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

    eventRequests.splice(
      index,
      1
    );

    if (
      request.event === "coins"
    ) {
      startCoinEvent(
        request.durationMs
      );
    }

    if (
      request.event === "tenCoins"
    ) {
      startTenCoinEvent(
        request.durationMs
      );
    }

    if (
      request.event === "galaxy"
    ) {
      startGalaxyEvent(
        request.durationMs
      );
    }

    return res.json({
      ok: true,
      approved: true,
      request
    });
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

    cleanupRequests();

    const id =
      String(
        req.params.id ||
        req.body?.id ||
        req.body?.requestId ||
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

    eventRequests.splice(
      index,
      1
    );

    return res.json({
      ok: true,
      denied: true,
      request
    });
  }
);


/* =========================================================
   ZWEITER ADMIN: EVENTS STOPPEN
   ========================================================= */

app.post(
  "/api/second-admin/event-stop",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const event =
      String(
        req.body?.event || ""
      );

    if (event === "coins") {
      const until =
        stopCoinEvent();

      return res.json({
        ok: true,
        until
      });
    }

    if (event === "tenCoins") {
      const until =
        stopTenCoinEvent();

      return res.json({
        ok: true,
        until
      });
    }

    if (event === "galaxy") {
      const until =
        stopGalaxyEvent();

      return res.json({
        ok: true,
        until
      });
    }

    return res.status(400).json({
      error:
        "Unbekanntes Event"
    });
  }
);


/* =========================================================
   ZWEITER ADMIN: KOMPATIBILITÄTS-ENDPOINTS
   ========================================================= */


/* 2x Start = Anfrage */
app.post(
  "/api/second-admin/coins-event",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const action =
      String(
        req.body?.action || ""
      );

    if (action === "stop") {
      const until =
        stopCoinEvent();

      return res.json({
        ok: true,
        until
      });
    }

    if (action === "start") {
      const durationMs =
        Math.max(
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

      const existing =
        eventRequests.find(
          (request) =>
            request.event ===
              "coins" &&
            request.action ===
              "start"
        );

      if (existing) {
        return res.json({
          ok: true,
          pending: true,
          id: existing.id
        });
      }

      const request = {
        id: makeRequestId(),
        event: "coins",
        action: "start",
        durationMs,
        createdAt: Date.now()
      };

      eventRequests.push(
        request
      );

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


/* 10x Start = Anfrage */
app.post(
  "/api/second-admin/ten-coin-event",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const action =
      String(
        req.body?.action || ""
      );

    if (action === "stop") {
      const until =
        stopTenCoinEvent();

      return res.json({
        ok: true,
        until
      });
    }

    if (action === "start") {
      const durationMs =
        Math.max(
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

      const existing =
        eventRequests.find(
          (request) =>
            request.event ===
              "tenCoins" &&
            request.action ===
              "start"
        );

      if (existing) {
        return res.json({
          ok: true,
          pending: true,
          id: existing.id
        });
      }

      const request = {
        id: makeRequestId(),
        event: "tenCoins",
        action: "start",
        durationMs,
        createdAt: Date.now()
      };

      eventRequests.push(
        request
      );

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


/* Galaxy Start = Anfrage */
app.post(
  "/api/second-admin/galaxy-event",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const action =
      String(
        req.body?.action || ""
      );

    if (action === "stop") {
      const until =
        stopGalaxyEvent();

      return res.json({
        ok: true,
        until
      });
    }

    if (action === "start") {
      const durationMs =
        Math.max(
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

      const existing =
        eventRequests.find(
          (request) =>
            request.event ===
              "galaxy" &&
            request.action ===
              "start"
        );

      if (existing) {
        return res.json({
          ok: true,
          pending: true,
          id: existing.id
        });
      }

      const request = {
        id: makeRequestId(),
        event: "galaxy",
        action: "start",
        durationMs,
        createdAt: Date.now()
      };

      eventRequests.push(
        request
      );

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
   ADMIN STATUS
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
   SECOND ADMIN STATUS
   ========================================================= */

app.get(
  "/api/second-admin/status",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    cleanupRequests();

    const now = Date.now();

    res.json({
      ok: true,

      coinEventUntil:
        coinEventUntil > now
          ? coinEventUntil
          : 0,

      tenCoinEventUntil:
        tenCoinEventUntil > now
          ? tenCoinEventUntil
          : 0,

      galaxyEventUntil:
        galaxyEventUntil > now
          ? galaxyEventUntil
          : 0,

      messages:
        serverMessages,

      pendingRequests:
        publicRequests()
    });
  }
);


/* =========================================================
   ABGELAUFENE NACHRICHTEN UND ANFRAGEN AUFRÄUMEN
   ========================================================= */

setInterval(() => {
  const now = Date.now();

  serverMessages =
    serverMessages.filter(
      (message) =>
        Number(message.endsAt || 0) >
        now
    );

  cleanupRequests();
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
    `Cube Rush server running on port ${PORT}`
  );

  console.log(
    "Haupt-Admin: 603781"
  );

  console.log(
    "Zweiter Admin: 6301"
  );
});
