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
let serverMessages = [];

// Event-Anfragen vom zweiten Admin
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

function executeCoinEvent(action, durationMs) {
  if (action === "start") {
    coinEventUntil =
      Date.now() + durationMs;

    broadcast({
      type: "coinEvent",
      until: coinEventUntil
    });

    return {
      until: coinEventUntil
    };
  }

  if (action === "stop") {
    coinEventUntil = 0;

    broadcast({
      type: "coinEvent",
      until: 0
    });

    return {
      until: 0
    };
  }

  throw new Error("Ungültige Event-Aktion");
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

      // Nur eine aktive Verbindung pro Spieler
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


/* ---------- 2x-Münzen-Event ---------- */

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

    const result =
      executeCoinEvent(
        "start",
        durationMs
      );

    return res.json({
      ok: true,
      ...result
    });
  }

  if (action === "stop") {
    const result =
      executeCoinEvent(
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


/* ---------- Münzen an Spieler ---------- */

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


/* ---------- Münzen an alle ---------- */

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

  // Jeder Spielername existiert nur einmal.
  for (const [name, ws] of players) {
    if (
      ws.readyState === WebSocket.OPEN
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
});


/* ---------- Münzen abziehen ---------- */

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
   SERVERNACHRICHTEN
   ========================================================= */


/* ---------- Nachricht erstellen ---------- */

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
    (minutes * 60 + seconds) *
    1000;

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

  // Mehrere Nachrichten dürfen gleichzeitig existieren.
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
 * Admin 2 darf ein 2x-Münzen-Event NICHT direkt starten.
 * Er erstellt eine Anfrage.
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

    // Beim jetzigen System gibt es nur
    // das 2x-Münzen-Event.
    if (event !== "coins") {
      return res.status(400).json({
        error:
          "Dieses Event ist nicht verfügbar"
      });
    }

    if (action !== "start") {
      return res.status(400).json({
        error:
          "Nur Event-Start benötigt eine Anfrage"
      });
    }

    const value =
      Number(
        req.body?.durationMs
      ) || 0;

    const durationMs = Math.max(
      1000,
      Math.min(
        10080 * 60 * 1000,
        Math.floor(value)
      )
    );

    // Doppelte offene Anfrage vermeiden
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
);


/* =========================================================
   OFFENE ANFRAGEN FÜR ADMIN 1
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

    // Anfragen maximal 10 Minuten offen lassen
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
      requests:
        publicRequests()
    });
  }
);


/* =========================================================
   ADMIN 1: ANFRAGE ANNEHMEN
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

    // Anfrage aus der Liste entfernen
    eventRequests.splice(
      index,
      1
    );

    if (
      request.event === "coins" &&
      request.action === "start"
    ) {
      const result =
        executeCoinEvent(
          "start",
          request.durationMs
        );

      return res.json({
        ok: true,
        approved: true,
        request,
        ...result
      });
    }

    return res.status(400).json({
      error: "Ungültige Anfrage"
    });
  }
);


/* =========================================================
   ADMIN 1: ANFRAGE ABLEHNEN
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
   ZWEITER ADMIN: EVENT STOPPEN
   ========================================================= */

app.post(
  "/api/second-admin/event-stop",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const event = String(
      req.body?.event || ""
    );

    if (event !== "coins") {
      return res.status(400).json({
        error:
          "Dieses Event ist nicht verfügbar"
      });
    }

    const result =
      executeCoinEvent(
        "stop",
        0
      );

    return res.json({
      ok: true,
      ...result
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
   AUFRÄUMEN
   ========================================================= */

setInterval(() => {
  const now = Date.now();

  // Abgelaufene Nachrichten entfernen
  serverMessages =
    serverMessages.filter(
      (message) =>
        Number(
          message.endsAt || 0
        ) > now
    );

  // Alte Event-Anfragen entfernen
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
    `Cube Rush server running on port ${PORT}`
  );

  console.log(
    "Haupt-Admin: 603781"
  );

  console.log(
    "Zweiter Admin: 6301"
  );

  console.log(
    "Kackhaufen-Event ist deaktiviert."
  );
});
