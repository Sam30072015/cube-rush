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

let activePoll = null;


/* =========================================================
   HILFSFUNKTIONEN
   ========================================================= */

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

function secondAdminOK(req) {
  return req.headers["x-admin-key"] === SECOND_ADMIN_KEY;
}

function adminMessageEventOK(req) {
  const key = req.headers["x-admin-key"];
  return key === ADMIN_KEY || key === SECOND_ADMIN_KEY;
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
    Math.random().toString(36).slice(2, 9)
  );
}

function publicRequests() {
  return eventRequests.map((r) => ({
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
  }

  if (event === "galaxy") {
    if (action === "start") {
      galaxyEventUntil = Date.now() + durationMs;

      broadcast({
        type: "galaxyEvent",
        until: galaxyEventUntil
      });

      return {
        until: galaxyEventUntil
      };
    }

    if (action === "stop") {
      galaxyEventUntil = 0;

      broadcast({
        type: "galaxyEvent",
        until: 0
      });

      return {
        until: 0
      };
    }
  }

  if (event === "tenCoins") {
    if (action === "start") {
      tenCoinEventUntil = Date.now() + durationMs;

      broadcast({
        type: "tenCoinEvent",
        until: tenCoinEventUntil
      });

      return {
        until: tenCoinEventUntil
      };
    }

    if (action === "stop") {
      tenCoinEventUntil = 0;

      broadcast({
        type: "tenCoinEvent",
        until: 0
      });

      return {
        until: 0
      };
    }
  }

  throw new Error("Ungültiges Event");
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

        serverMessages,

        poll:
          activePoll &&
          activePoll.endsAt > Date.now()
            ? {
                id: activePoll.id,
                question: activePoll.question,
                yesLabel: activePoll.yesLabel,
                noLabel: activePoll.noLabel,
                createdAt: activePoll.createdAt,
                endsAt: activePoll.endsAt
              }
            : null
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
   SPIELER AUFRÄUMEN
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


/* ---------- 2x MÜNZEN ---------- */

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

    return res.json({
      ok: true,
      ...executeEvent(
        "coins",
        "start",
        durationMs
      )
    });
  }

  if (action === "stop") {
    return res.json({
      ok: true,
      ...executeEvent(
        "coins",
        "stop",
        0
      )
    });
  }

  return res.status(400).json({
    error: "Invalid action"
  });
});


/* ---------- 10x MÜNZEN ---------- */

app.post(
  "/api/admin/ten-coin-event",
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

      return res.json({
        ok: true,
        ...executeEvent(
          "tenCoins",
          "start",
          durationMs
        )
      });
    }

    if (action === "stop") {
      return res.json({
        ok: true,
        ...executeEvent(
          "tenCoins",
          "stop",
          0
        )
      });
    }

    return res.status(400).json({
      error: "Invalid action"
    });
  }
);


/* ---------- GALAXY ---------- */

app.post(
  "/api/admin/galaxy-event",
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

      return res.json({
        ok: true,
        ...executeEvent(
          "galaxy",
          "start",
          durationMs
        )
      });
    }

    if (action === "stop") {
      return res.json({
        ok: true,
        ...executeEvent(
          "galaxy",
          "stop",
          0
        )
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


/* ---------- MÜNZEN AN ALLE ---------- */

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
      type: "takeCoins",
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

app.post(
  "/api/admin/message",
  (req, res) => {
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
        Number(
          req.body?.minutes
        ) || 0
      )
    );

    const seconds = Math.max(
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
          .slice(2, 8),

      type: "serverMessage",

      text,

      createdAt:
        Date.now(),

      endsAt:
        Date.now() + duration
    };

    serverMessages.push(message);

    broadcast(message);

    return res.json({
      ok: true,
      message
    });
  }
);


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


/* ---------- EVENT-ANFRAGE ---------- */

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

    const minDuration =
      event === "galaxy"
        ? 60000
        : 1000;

    const durationMs =
      Math.max(
        minDuration,
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
          request.event === event &&
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
      id:
        makeRequestId(),

      event,

      action:
        "start",

      durationMs,

      createdAt:
        Date.now()
    };

    eventRequests.push(request);

    return res.json({
      ok: true,
      pending: true,
      id: request.id
    });
  }
);


/* ---------- OFFENE ANFRAGEN ---------- */

app.get(
  "/api/admin/event-requests",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    cleanupEventRequests();

    return res.json({
      ok: true,
      requests:
        publicRequests()
    });
  }
);


/* ---------- ANFRAGE ANNEHMEN ---------- */

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

    cleanupEventRequests();

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


/* ---------- ANFRAGE ABLEHNEN ---------- */

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

    cleanupEventRequests();

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
      return res.json({
        ok: true,
        ...executeEvent(
          "coins",
          "stop",
          0
        )
      });
    }

    if (event === "tenCoins") {
      return res.json({
        ok: true,
        ...executeEvent(
          "tenCoins",
          "stop",
          0
        )
      });
    }

    if (event === "galaxy") {
      return res.json({
        ok: true,
        ...executeEvent(
          "galaxy",
          "stop",
          0
        )
      });
    }

    return res.status(400).json({
      error:
        "Unbekanntes Event"
    });
  }
);


/* =========================================================
   ZWEITER ADMIN: KOMPATIBILITÄTS-ENDPUNKTE
   ========================================================= */


/* ---------- 2x Münzen ---------- */

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
      return res.json({
        ok: true,
        ...executeEvent(
          "coins",
          "stop",
          0
        )
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


/* ---------- 10x Münzen ---------- */

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
      return res.json({
        ok: true,
        ...executeEvent(
          "tenCoins",
          "stop",
          0
        )
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


/* ---------- Galaxy ---------- */

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
      return res.json({
        ok: true,
        ...executeEvent(
          "galaxy",
          "stop",
          0
        )
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
      ],

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
          : 0
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

    cleanupEventRequests();

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
   SPIELER-UMFRAGE
   ========================================================= */

app.post(
  "/api/admin/poll",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const question =
      String(
        req.body?.question || ""
      )
        .trim()
        .slice(0, 140);

    const yesLabel =
      String(
        req.body?.yesLabel ||
          "Ja"
      )
        .trim()
        .slice(0, 40) ||
        "Ja";

    const noLabel =
      String(
        req.body?.noLabel ||
          "Nein"
      )
        .trim()
        .slice(0, 40) ||
        "Nein";

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

    if (!question) {
      return res.status(400).json({
        error:
          "Frage erforderlich"
      });
    }

    const now = Date.now();

    activePoll = {
      id: makeRequestId(),

      question,

      yesLabel,

      noLabel,

      createdAt: now,

      endsAt:
        now + durationMs,

      answers: {
        yes: 0,
        no: 0
      },

      voters:
        new Set()
    };

    broadcast({
      type: "playerPoll",

      poll: {
        id: activePoll.id,
        question:
          activePoll.question,
        yesLabel:
          activePoll.yesLabel,
        noLabel:
          activePoll.noLabel,
        createdAt:
          activePoll.createdAt,
        endsAt:
          activePoll.endsAt
      }
    });

    return res.json({
      ok: true,

      poll: {
        id: activePoll.id,
        question:
          activePoll.question,
        yesLabel:
          activePoll.yesLabel,
        noLabel:
          activePoll.noLabel,
        createdAt:
          activePoll.createdAt,
        endsAt:
          activePoll.endsAt,
        answers:
          activePoll.answers
      }
    });
  }
);


/* ---------- Umfrage stoppen ---------- */

app.post(
  "/api/admin/poll/stop",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    activePoll = null;

    broadcast({
      type:
        "playerPollClear"
    });

    return res.json({
      ok: true
    });
  }
);


/* ---------- Umfrage-Status ---------- */

app.get(
  "/api/admin/poll/status",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    if (
      !activePoll ||
      activePoll.endsAt <= Date.now()
    ) {
      activePoll = null;

      return res.json({
        ok: true,
        poll: null
      });
    }

    return res.json({
      ok: true,

      poll: {
        id:
          activePoll.id,

        question:
          activePoll.question,

        yesLabel:
          activePoll.yesLabel,

        noLabel:
          activePoll.noLabel,

        createdAt:
          activePoll.createdAt,

        endsAt:
          activePoll.endsAt,

        answers:
          activePoll.answers
      }
    });
  }
);


/* ---------- Spieler antwortet ---------- */

app.post(
  "/api/poll/answer",
  (req, res) => {
    if (
      !activePoll ||
      activePoll.endsAt <= Date.now()
    ) {
      activePoll = null;

      return res.status(410).json({
        error:
          "Die Umfrage ist beendet"
      });
    }

    const pollId =
      String(
        req.body?.pollId || ""
      );

    const answer =
      String(
        req.body?.answer || ""
      );

    if (
      pollId !==
      activePoll.id
    ) {
      return res.status(409).json({
        error:
          "Umfrage nicht mehr aktuell"
      });
    }

    if (
      answer !== "yes" &&
      answer !== "no"
    ) {
      return res.status(400).json({
        error:
          "Ungültige Antwort"
      });
    }

    const voterId =
      String(
        req.body?.voterId || ""
      ).slice(0, 80);

    /*
     * Mit voterId nur eine Abstimmung
     * pro Spieler.
     */
    if (
      voterId &&
      activePoll.voters.has(voterId)
    ) {
      return res.json({
        ok: true,
        alreadyAnswered: true,
        answers:
          activePoll.answers
      });
    }

    if (voterId) {
      activePoll.voters.add(
        voterId
      );
    }

    activePoll.answers[
      answer
    ]++;

    return res.json({
      ok: true,
      answers:
        activePoll.answers
    });
  }
);


/* =========================================================
   AUFRÄUMEN
   ========================================================= */

setInterval(() => {
  const now = Date.now();

  // Nachrichten ablaufen lassen
  serverMessages =
    serverMessages.filter(
      (message) =>
        Number(
          message.endsAt || 0
        ) > now
    );

  // Event-Anfragen nach 10 Minuten löschen
  cleanupEventRequests();

  // Umfrage automatisch beenden
  if (
    activePoll &&
    activePoll.endsAt <= now
  ) {
    activePoll = null;

    broadcast({
      type:
        "playerPollClear"
    });
  }

  // 2x Event automatisch beenden
  if (
    coinEventUntil > 0 &&
    coinEventUntil <= now
  ) {
    coinEventUntil = 0;

    broadcast({
      type: "coinEvent",
      until: 0
    });
  }

  // 10x Event automatisch beenden
  if (
    tenCoinEventUntil > 0 &&
    tenCoinEventUntil <= now
  ) {
    tenCoinEventUntil = 0;

    broadcast({
      type: "tenCoinEvent",
      until: 0
    });
  }

  // Galaxy automatisch beenden
  if (
    galaxyEventUntil > 0 &&
    galaxyEventUntil <= now
  ) {
    galaxyEventUntil = 0;

    broadcast({
      type: "galaxyEvent",
      until: 0
    });
  }
}, 1000);


/* =========================================================
   HILFSFUNKTION FÜR ANFRAGEN
   ========================================================= */

function cleanupEventRequests() {
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
}


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
    "Events: 2x / 10x / Galaxy"
  );

  console.log(
    "Spieler-Umfragen aktiviert."
  );
});
