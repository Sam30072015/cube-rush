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

      const oldSocket = players.get(name);

      // Pro Spielername nur eine aktive Verbindung
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
   HAUPT-ADMIN 603781
   ========================================================= */


/* ---------- 2x-MÜNZEN-EVENT ---------- */

app.post("/api/admin/coins-event", (req, res) => {
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
});


/* ---------- MÜNZEN GEBEN ---------- */

app.post("/api/admin/give", (req, res) => {
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
});


/* ---------- MÜNZEN AN ALLE ---------- */

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

  // Jeder Spielername nur einmal
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


/* ---------- MÜNZEN ABZIEHEN ---------- */

app.post("/api/admin/take", (req, res) => {
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
});


/* =========================================================
   GALAXY-EVENT
   ========================================================= */


/* ---------- Galaxy starten ---------- */

app.post("/api/admin/galaxy-event", (req, res) => {
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

    galaxyEventUntil =
      Date.now() + durationMs;

    broadcast({
      type: "galaxyEvent",
      until: galaxyEventUntil
    });

    return res.json({
      ok: true,
      until: galaxyEventUntil
    });
  }

  if (action === "stop") {
    galaxyEventUntil = 0;

    broadcast({
      type: "galaxyEvent",
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
});


/* =========================================================
   SERVERNACHRICHTEN
   ========================================================= */


/* ---------- Nachricht senden ---------- */

app.post("/api/admin/message", (req, res) => {
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
      Date.now().toString(36) +
      "-" +
      Math.random()
        .toString(36)
        .slice(2, 10),

    type: "serverMessage",

    text,

    createdAt: Date.now(),

    endsAt:
      Date.now() + duration
  };

  // Mehrere Nachrichten gleichzeitig möglich
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


/* =========================================================
   ZWEITER ADMIN 6301
   ========================================================= */


/*
 * Der zweite Admin darf Galaxy und 2x-Münzen
 * NICHT direkt starten.
 * Er muss eine Anfrage stellen.
 */

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
      req.body?.action || "start"
    );

    if (
      event !== "coins" &&
      event !== "galaxy"
    ) {
      return res.status(400).json({
        error: "Unbekanntes Event"
      });
    }

    if (action !== "start") {
      return res.status(400).json({
        error:
          "Nur Start-Anfragen sind erlaubt"
      });
    }

    let durationMs;

    if (event === "galaxy") {
      durationMs = Math.max(
        60000,
        Math.min(
          10080 * 60 * 1000,
          Math.floor(
            Number(req.body?.durationMs) || 0
          )
        )
      );
    } else {
      durationMs = Math.max(
        1000,
        Math.min(
          10080 * 60 * 1000,
          Math.floor(
            Number(req.body?.durationMs) || 0
          )
        )
      );
    }

    // Doppelte offene Anfrage verhindern
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
      id: makeRequestId(),

      event,

      action: "start",

      durationMs,

      createdAt: Date.now()
    };

    eventRequests.push(request);

    /*
     * Keine Broadcast-Spam-Nachrichten.
     * Das erste Admin-Panel fragt die offenen
     * Anfragen regelmäßig ab.
     */
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
      requests: publicRequests()
    });
  }
);


/* =========================================================
   ADMIN 1: ANNEHMEN
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

    const id = String(
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

    // Aus der offenen Liste entfernen
    eventRequests.splice(
      index,
      1
    );

    if (
      request.event === "coins"
    ) {
      coinEventUntil =
        Date.now() +
        request.durationMs;

      broadcast({
        type: "coinEvent",
        until: coinEventUntil
      });
    }

    if (
      request.event === "galaxy"
    ) {
      galaxyEventUntil =
        Date.now() +
        request.durationMs;

      broadcast({
        type: "galaxyEvent",
        until: galaxyEventUntil
      });
    }

    return res.json({
      ok: true,
      approved: true,
      request
    });
  }
);


/* =========================================================
   ADMIN 1: ABLEHNEN
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

    const id = String(
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
   ZWEITER ADMIN: 2x-MÜNZEN STOPPEN
   ========================================================= */

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

      const existing =
        eventRequests.find(
          (request) =>
            request.event === "coins" &&
            request.action === "start"
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
   ZWEITER ADMIN: GALAXY STOPPEN
   ========================================================= */

app.post(
  "/api/second-admin/galaxy-event",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const action = String(
      req.body?.action || ""
    );

    // Stoppen darf direkt gemacht werden
    if (action === "stop") {
      galaxyEventUntil = 0;

      broadcast({
        type: "galaxyEvent",
        until: 0
      });

      return res.json({
        ok: true,
        until: 0
      });
    }

    // Starten geht nur über Anfrage
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

      const existing =
        eventRequests.find(
          (request) =>
            request.event === "galaxy" &&
            request.action === "start"
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
   ZWEITER ADMIN: STATUS
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
   AUTOMATISCHES AUFRÄUMEN
   ========================================================= */

setInterval(() => {
  const now = Date.now();

  // Abgelaufene Nachrichten
  serverMessages =
    serverMessages.filter(
      (message) =>
        Number(
          message.endsAt || 0
        ) > now
    );

  // Abgelaufene Event-Anfragen
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

  console.log(
    "Galaxy-Event aktiviert."
  );
});
