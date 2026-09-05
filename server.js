const path = require("path");
const http = require("http");
const express = require("express");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 3000);

const ADMIN_KEY = process.env.ADMIN_KEY || "603781";
const SECOND_ADMIN_KEY =
  process.env.SECOND_ADMIN_KEY || "6301";

const app = express();

app.use(express.json({ limit: "50kb" }));
app.use(
  express.static(path.join(__dirname, "public"))
);

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const players = new Map();

/* =========================================================
   EVENTS
   ========================================================= */

let coinEventUntil = 0;       // 2x
let tenCoinEventUntil = 0;    // 10x
let galaxyEventUntil = 0;     // Galaxy

/* =========================================================
   NACHRICHTEN / ANFRAGEN
   ========================================================= */

let serverMessages = [];
let eventRequests = [];

/* =========================================================
   TRADE
   ========================================================= */

const tradeRequests = [];
const activeTrades = new Map();
const completedTrades = new Map();

const VALID_TRADE_SKINS = new Set([
  "rainbow",
  "fire",
  "ocean",
  "shadow",
  "sunset",
  "cat",
  "magma",
  "poop",
  "plane:rainbow",
  "plane:magma",
  "plane:galaxy"
]);

/* =========================================================
   HILFSFUNKTIONEN
   ========================================================= */

function cleanName(name) {
  return String(name || "")
    .trim()
    .slice(0, 40);
}

function send(ws, payload) {
  if (
    ws &&
    ws.readyState === WebSocket.OPEN
  ) {
    ws.send(JSON.stringify(payload));
  }
}

function broadcast(payload) {
  const data = JSON.stringify(payload);

  for (const ws of wss.clients) {
    if (
      ws.readyState === WebSocket.OPEN
    ) {
      ws.send(data);
    }
  }
}

function adminOK(req) {
  return (
    req.headers["x-admin-key"] ===
    ADMIN_KEY
  );
}

function secondAdminOK(req) {
  return (
    req.headers["x-admin-key"] ===
    SECOND_ADMIN_KEY
  );
}

function anyAdminOK(req) {
  return (
    adminOK(req) ||
    secondAdminOK(req)
  );
}

function amountFrom(body) {
  return Math.max(
    0,
    Math.floor(
      Number(body?.coins) || 0
    )
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
  return eventRequests.map(
    (request) => ({
      id: request.id,
      event: request.event,
      action: request.action,
      durationMs: request.durationMs,
      createdAt: request.createdAt
    })
  );
}

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
   EVENT FUNKTIONEN
   ========================================================= */

function startCoinEvent(
  durationMs
) {
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

function startTenCoinEvent(
  durationMs
) {
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

function startGalaxyEvent(
  durationMs
) {
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

function executeEvent(
  event,
  action,
  durationMs
) {
  if (event === "coins") {
    if (action === "start") {
      return {
        until:
          startCoinEvent(
            durationMs
          )
      };
    }

    if (action === "stop") {
      return {
        until:
          stopCoinEvent()
      };
    }
  }

  if (event === "tenCoins") {
    if (action === "start") {
      return {
        until:
          startTenCoinEvent(
            durationMs
          )
      };
    }

    if (action === "stop") {
      return {
        until:
          stopTenCoinEvent()
      };
    }
  }

  if (event === "galaxy") {
    if (action === "start") {
      return {
        until:
          startGalaxyEvent(
            durationMs
          )
      };
    }

    if (action === "stop") {
      return {
        until:
          stopGalaxyEvent()
      };
    }
  }

  throw new Error(
    "Ungültiges Event"
  );
}

/* =========================================================
   WEBSOCKET
   ========================================================= */

wss.on(
  "connection",
  (ws) => {
    ws.playerName = "";
    ws.lastHeartbeat =
      Date.now();

    ws.on(
      "message",
      (raw) => {
        let msg;

        try {
          msg = JSON.parse(
            raw.toString()
          );
        } catch {
          return;
        }

        ws.lastHeartbeat =
          Date.now();

        if (
          msg.type ===
          "identify"
        ) {
          let name =
            cleanName(
              msg.name
            );

          if (!name) {
            name =
              "Spieler-" +
              Math.random()
                .toString(
                  36
                )
                .slice(
                  2,
                  8
                )
                .toUpperCase();
          }

          const oldSocket =
            players.get(name);

          if (
            oldSocket &&
            oldSocket !== ws
          ) {
            try {
              oldSocket.terminate();
            } catch {}

            players.delete(
              name
            );
          }

          if (
            ws.playerName &&
            players.get(
              ws.playerName
            ) === ws
          ) {
            players.delete(
              ws.playerName
            );
          }

          ws.playerName =
            name;

          players.set(
            name,
            ws
          );

          send(ws, {
            type:
              "connected",

            name,

            coinEventUntil:
              coinEventUntil >
              Date.now()
                ? coinEventUntil
                : 0,

            tenCoinEventUntil:
              tenCoinEventUntil >
              Date.now()
                ? tenCoinEventUntil
                : 0,

            galaxyEventUntil:
              galaxyEventUntil >
              Date.now()
                ? galaxyEventUntil
                : 0,

            serverMessages
          });

          return;
        }

        if (
          msg.type ===
          "heartbeat"
        ) {
          send(ws, {
            type:
              "heartbeatAck"
          });
        }
      }
    );

    ws.on(
      "close",
      () => {
        if (
          ws.playerName &&
          players.get(
            ws.playerName
          ) === ws
        ) {
          players.delete(
            ws.playerName
          );
        }
      }
    );

    ws.on(
      "error",
      () => {
        if (
          ws.playerName &&
          players.get(
            ws.playerName
          ) === ws
        ) {
          players.delete(
            ws.playerName
          );
        }
      }
    );
  }
);

/* =========================================================
   OFFLINE SPIELER ENTFERNEN
   ========================================================= */

setInterval(
  () => {
    const now =
      Date.now();

    for (
      const [name, ws]
      of players
    ) {
      if (
        ws.readyState !==
          WebSocket.OPEN ||
        now -
          (ws.lastHeartbeat ||
            0) >
          30000
      ) {
        players.delete(
          name
        );

        try {
          ws.terminate();
        } catch {}
      }
    }
  },
  15000
);

/* =========================================================
   HAUPT-ADMIN: 2x
   ========================================================= */

app.post(
  "/api/admin/coins-event",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const action =
      String(
        req.body?.action ||
          ""
      );

    if (
      action === "start"
    ) {
      const durationMs =
        Math.max(
          1000,
          Math.min(
            10080 *
              60 *
              1000,
            Math.floor(
              Number(
                req.body
                  ?.durationMs
              ) || 0
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

    if (
      action === "stop"
    ) {
      return res.json({
        ok: true,
        ...executeEvent(
          "coins",
          "stop",
          0
        )
      });
    }

    return res.status(
      400
    ).json({
      error:
        "Invalid action"
    });
  }
);

/* =========================================================
   HAUPT-ADMIN: 10x
   ========================================================= */

app.post(
  "/api/admin/ten-coin-event",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const action =
      String(
        req.body?.action ||
          ""
      );

    if (
      action === "start"
    ) {
      const durationMs =
        Math.max(
          1000,
          Math.min(
            10080 *
              60 *
              1000,
            Math.floor(
              Number(
                req.body
                  ?.durationMs
              ) || 0
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

    if (
      action === "stop"
    ) {
      return res.json({
        ok: true,
        ...executeEvent(
          "tenCoins",
          "stop",
          0
        )
      });
    }

    return res.status(
      400
    ).json({
      error:
        "Invalid action"
    });
  }
);

/* =========================================================
   HAUPT-ADMIN: GALAXY
   ========================================================= */

app.post(
  "/api/admin/galaxy-event",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const action =
      String(
        req.body?.action ||
          ""
      );

    if (
      action === "start"
    ) {
      const durationMs =
        Math.max(
          60000,
          Math.min(
            10080 *
              60 *
              1000,
            Math.floor(
              Number(
                req.body
                  ?.durationMs
              ) || 0
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

    if (
      action === "stop"
    ) {
      return res.json({
        ok: true,
        ...executeEvent(
          "galaxy",
          "stop",
          0
        )
      });
    }

    return res.status(
      400
    ).json({
      error:
        "Invalid action"
    });
  }
);

/* =========================================================
   MÜNZEN GEBEN
   ========================================================= */

app.post(
  "/api/admin/give",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const player =
      cleanName(
        req.body?.player
      );

    const coins =
      amountFrom(
        req.body
      );

    if (
      !player ||
      coins <= 0
    ) {
      return res.status(
        400
      ).json({
        error:
          "Player and a coin amount are required"
      });
    }

    const target =
      players.get(
        player
      );

    if (
      !target ||
      target.readyState !==
        WebSocket.OPEN
    ) {
      return res.status(
        404
      ).json({
        error:
          "Player is not online"
      });
    }

    send(target, {
      type:
        "gift",

      target:
        player,

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
   MÜNZEN AN ALLE
   ========================================================= */

app.post(
  "/api/admin/give-all",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const coins =
      amountFrom(
        req.body
      );

    if (
      coins <= 0
    ) {
      return res.status(
        400
      ).json({
        error:
          "A coin amount greater than 0 is required"
      });
    }

    let count = 0;

    const message =
      JSON.stringify({
        type:
          "giftAll",
        coins
      });

    for (
      const [name, ws]
      of players
    ) {
      if (
        ws.readyState ===
        WebSocket.OPEN
      ) {
        ws.send(
          message
        );

        count++;
      } else {
        players.delete(
          name
        );
      }
    }

    return res.json({
      ok: true,
      coins,
      count
    });
  }
);

/* =========================================================
   MÜNZEN ABZIEHEN
   ========================================================= */

app.post(
  "/api/admin/take",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const player =
      cleanName(
        req.body?.player
      );

    const coins =
      amountFrom(
        req.body
      );

    if (
      !player ||
      coins <= 0
    ) {
      return res.status(
        400
      ).json({
        error:
          "Player and a coin amount are required"
      });
    }

    const target =
      players.get(
        player
      );

    if (
      !target ||
      target.readyState !==
        WebSocket.OPEN
    ) {
      return res.status(
        404
      ).json({
        error:
          "Player is not online"
      });
    }

    send(target, {
      type:
        "takeCoins",

      target:
        player,

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
    if (!anyAdminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const text =
      String(
        req.body?.text ||
          ""
      )
        .trim()
        .slice(
          0,
          120
        );

    const minutes =
      Math.max(
        0,
        Math.floor(
          Number(
            req.body
              ?.minutes
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
              req.body
                ?.seconds
            ) || 0
          )
        )
      );

    const duration =
      (minutes * 60 +
        seconds) *
      1000;

    if (
      !text ||
      duration <= 0
    ) {
      return res.status(
        400
      ).json({
        error:
          "Text and duration required"
      });
    }

    const now =
      Date.now();

    const message = {
      id:
        now.toString(36) +
        "-" +
        Math.random()
          .toString(
            36
          )
          .slice(
            2,
            9
          ),

      type:
        "serverMessage",

      text,

      createdAt:
        now,

      endsAt:
        now + duration
    };

    serverMessages.push(
      message
    );

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
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    serverMessages =
      [];

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
   EVENT-ANFRAGE VON ADMIN 2
   ========================================================= */

app.post(
  "/api/second-admin/event-request",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const event =
      String(
        req.body?.event ||
          ""
      );

    const action =
      String(
        req.body?.action ||
          "start"
      );

    const allowedEvents = [
      "coins",
      "tenCoins",
      "galaxy"
    ];

    if (
      !allowedEvents.includes(
        event
      )
    ) {
      return res.status(
        400
      ).json({
        error:
          "Unbekanntes Event"
      });
    }

    if (
      action !== "start"
    ) {
      return res.status(
        400
      ).json({
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
          10080 *
            60 *
            1000,
          Math.floor(
            Number(
              req.body
                ?.durationMs
            ) || 0
          )
        )
      );

    const existing =
      eventRequests.find(
        (request) =>
          request.event ===
            event &&
          request.action ===
            "start"
      );

    if (existing) {
      return res.json({
        ok: true,
        pending: true,
        id:
          existing.id,
        duplicate:
          true
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
      id:
        request.id
    });
  }
);


/* =========================================================
   EVENT-ANFRAGEN ABRUFEN
   ========================================================= */

app.get(
  "/api/admin/event-requests",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
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


/* =========================================================
   EVENT-ANFRAGE ANNEHMEN
   ========================================================= */

app.post(
  [
    "/api/admin/event-requests/:id/approve",
    "/api/admin/event-requests/approve"
  ],
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    cleanupEventRequests();

    const id =
      String(
        req.params.id ||
        req.body?.id ||
        req.body
          ?.requestId ||
        ""
      );

    const index =
      eventRequests.findIndex(
        (request) =>
          request.id ===
          id
      );

    if (
      index < 0
    ) {
      return res.status(
        404
      ).json({
        error:
          "Anfrage nicht gefunden"
      });
    }

    const request =
      eventRequests[
        index
      ];

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
      return res.status(
        400
      ).json({
        error:
          error.message ||
          "Event konnte nicht gestartet werden"
      });
    }
  }
);


/* =========================================================
   EVENT-ANFRAGE ABLEHNEN
   ========================================================= */

app.post(
  [
    "/api/admin/event-requests/:id/deny",
    "/api/admin/event-requests/reject"
  ],
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    cleanupEventRequests();

    const id =
      String(
        req.params.id ||
        req.body?.id ||
        req.body
          ?.requestId ||
        ""
      );

    const index =
      eventRequests.findIndex(
        (request) =>
          request.id ===
          id
      );

    if (
      index < 0
    ) {
      return res.status(
        404
      ).json({
        error:
          "Anfrage nicht gefunden"
      });
    }

    const request =
      eventRequests[
        index
      ];

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
   ADMIN 2: EVENTS STOPPEN
   ========================================================= */

app.post(
  "/api/second-admin/event-stop",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const event =
      String(
        req.body?.event ||
          ""
      );

    if (
      event === "coins"
    ) {
      return res.json({
        ok: true,
        until:
          stopCoinEvent()
      });
    }

    if (
      event === "tenCoins"
    ) {
      return res.json({
        ok: true,
        until:
          stopTenCoinEvent()
      });
    }

    if (
      event === "galaxy"
    ) {
      return res.json({
        ok: true,
        until:
          stopGalaxyEvent()
      });
    }

    return res.status(
      400
    ).json({
      error:
        "Unbekanntes Event"
    });
  }
);


/* =========================================================
   ZWEITER ADMIN: DIREKTE KOMPATIBILITÄT
   ========================================================= */

app.post(
  "/api/second-admin/coins-event",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const action =
      String(
        req.body?.action ||
          ""
      );

    if (
      action === "stop"
    ) {
      return res.json({
        ok: true,
        until:
          stopCoinEvent()
      });
    }

    if (
      action === "start"
    ) {
      const durationMs =
        Math.max(
          1000,
          Math.min(
            10080 *
              60 *
              1000,
            Math.floor(
              Number(
                req.body
                  ?.durationMs
              ) || 0
            )
          )
        );

      return createSecondAdminRequest(
        res,
        "coins",
        durationMs
      );
    }

    return res.status(
      400
    ).json({
      error:
        "Invalid action"
    });
  }
);


app.post(
  "/api/second-admin/ten-coin-event",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const action =
      String(
        req.body?.action ||
          ""
      );

    if (
      action === "stop"
    ) {
      return res.json({
        ok: true,
        until:
          stopTenCoinEvent()
      });
    }

    if (
      action === "start"
    ) {
      const durationMs =
        Math.max(
          1000,
          Math.min(
            10080 *
              60 *
              1000,
            Math.floor(
              Number(
                req.body
                  ?.durationMs
              ) || 0
            )
          )
        );

      return createSecondAdminRequest(
        res,
        "tenCoins",
        durationMs
      );
    }

    return res.status(
      400
    ).json({
      error:
        "Invalid action"
    });
  }
);


app.post(
  "/api/second-admin/galaxy-event",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const action =
      String(
        req.body?.action ||
          ""
      );

    if (
      action === "stop"
    ) {
      return res.json({
        ok: true,
        until:
          stopGalaxyEvent()
      });
    }

    if (
      action === "start"
    ) {
      const durationMs =
        Math.max(
          60000,
          Math.min(
            10080 *
              60 *
              1000,
            Math.floor(
              Number(
                req.body
                  ?.durationMs
              ) || 0
            )
          )
        );

      return createSecondAdminRequest(
        res,
        "galaxy",
        durationMs
      );
    }

    return res.status(
      400
    ).json({
      error:
        "Invalid action"
    });
  }
);


/* =========================================================
   HELFER FÜR ADMIN-2-ANFRAGEN
   ========================================================= */

function createSecondAdminRequest(
  res,
  event,
  durationMs
) {
  const existing =
    eventRequests.find(
      (request) =>
        request.event ===
          event &&
        request.action ===
          "start"
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
    id:
      request.id
  });
}


/* =========================================================
   ONLINE-SPIELER
   ========================================================= */

app.get(
  "/api/admin/online-players",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const onlinePlayers =
      [...players.entries()]
        .filter(
          ([, ws]) =>
            ws.readyState ===
            WebSocket.OPEN
        )
        .map(
          ([name]) =>
            name
        );

    return res.json({
      ok: true,
      onlinePlayers
    });
  }
);


/* =========================================================
   ALLGEMEINER STATUS
   ========================================================= */

app.get(
  "/api/status",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(
        401
      ).json({
        error:
          "Unauthorized"
      });
    }

    const now =
      Date.now();

    res.json({
      onlinePlayers:
        [...players.keys()],

      coinEventUntil:
        coinEventUntil >
        now
          ? coinEventUntil
          : 0,

      tenCoinEventUntil:
        tenCoinEventUntil >
        now
          ? tenCoinEventUntil
          : 0,

      galaxyEventUntil:
        galaxyEventUntil >
        now
          ? galaxyEventUntil
          : 0
    });
  }
);


/* =========================================================
   TRADE
   ========================================================= */

function getTradeForPlayer(
  player
) {
  for (
    const trade
    of activeTrades.values()
  ) {
    if (
      trade.from === player ||
      trade.to === player
    ) {
      return {
        id:
          trade.id,

        other:
          trade.from === player
            ? trade.to
            : trade.from,

        myOffer:
          trade.from === player
            ? trade.fromOffer
            : trade.toOffer,

        otherOffer:
          trade.from === player
            ? trade.toOffer
            : trade.fromOffer,

        myConfirmed:
          trade.from === player
            ? trade.fromConfirmed
            : trade.toConfirmed,

        otherConfirmed:
          trade.from === player
            ? trade.toConfirmed
            : trade.fromConfirmed
      };
    }
  }

  return null;
}


/* ---------- Trade-Status ---------- */

app.get(
  "/api/trade/status",
  (req, res) => {
    const player =
      cleanName(
        req.query.player
      );

    if (!player) {
      return res.status(
        400
      ).json({
        error:
          "Spielername fehlt"
      });
    }

    const onlinePlayers =
      [...players.keys()]
        .filter(
          name =>
            name !== player
        );

    const incomingRequests =
      tradeRequests.filter(
        request =>
          request.to ===
          player
      );

    const trade =
      getTradeForPlayer(
        player
      );

    const completed =
      completedTrades.get(
        player
      ) || null;

    if (completed) {
      completedTrades.delete(
        player
      );
    }

    return res.json({
      ok: true,
      onlinePlayers,
      incomingRequests,
      trade,
      completed
    });
  }
);


/* ---------- Trade-Anfrage ---------- */

app.post(
  "/api/trade/request",
  (req, res) => {
    const from =
      cleanName(
        req.body?.from
      );

    const to =
      cleanName(
        req.body?.to
      );

    if (
      !from ||
      !to ||
      from === to
    ) {
      return res.status(
        400
      ).json({
        error:
          "Ungültiger Spieler"
      });
    }

    if (!players.has(from)) {
      return res.status(
        400
      ).json({
        error:
          "Du bist nicht online"
      });
    }

    if (!players.has(to)) {
      return res.status(
        404
      ).json({
        error:
          "Spieler ist nicht online"
      });
    }

    if (
      getTradeForPlayer(
        from
      )
    ) {
      return res.status(
        409
      ).json({
        error:
          "Du bist bereits in einem Trade"
      });
    }

    if (
      tradeRequests.some(
        request =>
          request.from ===
            from &&
          request.to ===
            to
      )
    ) {
      return res.status(
        409
      ).json({
        error:
          "Anfrage wurde bereits gesendet"
      });
    }

    const request = {
      id:
        makeRequestId(),

      from,

      to,

      createdAt:
        Date.now()
    };

    tradeRequests.push(
      request
    );

    const target =
      players.get(to);

    if (target) {
      send(target, {
        type:
          "tradeRequest",

        request
      });
    }

    return res.json({
      ok: true,
      request
    });
  }
);


/* ---------- Trade annehmen/ablehnen ---------- */

app.post(
  "/api/trade/respond",
  (req, res) => {
    const player =
      cleanName(
        req.body?.player
      );

    const requestId =
      String(
        req.body
          ?.requestId ||
        ""
      );

    const accept =
      req.body?.accept ===
      true;

    const index =
      tradeRequests.findIndex(
        request =>
          request.id ===
            requestId &&
          request.to ===
            player
      );

    if (index < 0) {
      return res.status(
        404
      ).json({
        error:
          "Trade-Anfrage nicht gefunden"
      });
    }

    const request =
      tradeRequests[
        index
      ];

    tradeRequests.splice(
      index,
      1
    );

    if (!accept) {
      const fromWs =
        players.get(
          request.from
        );

      if (fromWs) {
        send(fromWs, {
          type:
            "tradeRequestResult",

          requestId:
            request.id,

          accepted:
            false
        });
      }

      return res.json({
        ok: true,
        accepted:
          false
      });
    }

    if (
      !players.has(
        request.from
      ) ||
      !players.has(
        request.to
      )
    ) {
      return res.status(
        409
      ).json({
        error:
          "Spieler ist nicht mehr online"
      });
    }

    if (
      getTradeForPlayer(
        request.from
      ) ||
      getTradeForPlayer(
        request.to
      )
    ) {
      return res.status(
        409
      ).json({
        error:
          "Ein Spieler ist bereits in einem Trade"
      });
    }

    const trade = {
      id:
        makeRequestId(),

      from:
        request.from,

      to:
        request.to,

      fromOffer:
        null,

      toOffer:
        null,

      fromConfirmed:
        false,

      toConfirmed:
        false,

      createdAt:
        Date.now()
    };

    activeTrades.set(
      trade.id,
      trade
    );

    const fromWs =
      players.get(
        request.from
      );

    if (fromWs) {
      send(fromWs, {
        type:
          "tradeAccepted",

        tradeId:
          trade.id
      });
    }

    return res.json({
      ok: true,
      accepted:
        true,

      trade:
        getTradeForPlayer(
          player
        )
    });
  }
);


/* ---------- Skin anbieten ---------- */

app.post(
  "/api/trade/offer",
  (req, res) => {
    const player =
      cleanName(
        req.body?.player
      );

    const tradeId =
      String(
        req.body
          ?.tradeId ||
        ""
      );

    const skinId =
      String(
        req.body?.skinId ||
        ""
      );

    if (
      !VALID_TRADE_SKINS.has(
        skinId
      )
    ) {
      return res.status(
        400
      ).json({
        error:
          "Dieser Skin kann nicht getauscht werden"
      });
    }

    const trade =
      activeTrades.get(
        tradeId
      );

    if (
      !trade ||
      (
        trade.from !==
          player &&
        trade.to !==
          player
      )
    ) {
      return res.status(
        404
      ).json({
        error:
          "Trade nicht gefunden"
      });
    }

    if (
      trade.from ===
      player
    ) {
      if (
        trade.fromConfirmed
      ) {
        return res.status(
          409
        ).json({
          error:
            "Bereits bestätigt"
        });
      }

      trade.fromOffer =
        skinId;
    } else {
      if (
        trade.toConfirmed
      ) {
        return res.status(
          409
        ).json({
          error:
            "Bereits bestätigt"
        });
      }

      trade.toOffer =
        skinId;
    }

    return res.json({
      ok: true,
      trade:
        getTradeForPlayer(
          player
        )
    });
  }
);


/* ---------- Trade bestätigen ---------- */

app.post(
  "/api/trade/confirm",
  (req, res) => {
    const player =
      cleanName(
        req.body?.player
      );

    const tradeId =
      String(
        req.body
          ?.tradeId ||
        ""
      );

    const trade =
      activeTrades.get(
        tradeId
      );

    if (
      !trade ||
      (
        trade.from !==
          player &&
        trade.to !==
          player
      )
    ) {
      return res.status(
        404
      ).json({
        error:
          "Trade nicht gefunden"
      });
    }

    if (
      trade.from ===
      player
    ) {
      if (
        !trade.fromOffer
      ) {
        return res.status(
          400
        ).json({
          error:
            "Bitte zuerst einen Skin auswählen"
        });
      }

      trade.fromConfirmed =
        true;
    } else {
      if (
        !trade.toOffer
      ) {
        return res.status(
          400
        ).json({
          error:
            "Bitte zuerst einen Skin auswählen"
        });
      }

      trade.toConfirmed =
        true;
    }

    if (
      trade.fromConfirmed &&
      trade.toConfirmed
    ) {
      const fromPlayer =
        trade.from;

      const toPlayer =
        trade.to;

      const fromSkin =
        trade.fromOffer;

      const toSkin =
        trade.toOffer;

      activeTrades.delete(
        trade.id
      );

      completedTrades.set(
        fromPlayer,
        {
          id:
            trade.id,

          sentSkin:
            fromSkin,

          receivedSkin:
            toSkin
        }
      );

      completedTrades.set(
        toPlayer,
        {
          id:
            trade.id,

          sentSkin:
            toSkin,

          receivedSkin:
            fromSkin
        }
      );

      const fromWs =
        players.get(
          fromPlayer
        );

      const toWs =
        players.get(
          toPlayer
        );

      if (fromWs) {
        send(fromWs, {
          type:
            "tradeComplete",

          tradeId:
            trade.id
        });
      }

      if (toWs) {
        send(toWs, {
          type:
            "tradeComplete",

          tradeId:
            trade.id
        });
      }

      return res.json({
        ok: true,
        completed:
          true
      });
    }

    return res.json({
      ok: true,
      completed:
        false,

      trade:
        getTradeForPlayer(
          player
        )
    });
  }
);


/* ---------- Trade abbrechen ---------- */

app.post(
  "/api/trade/cancel",
  (req, res) => {
    const player =
      cleanName(
        req.body?.player
      );

    const tradeId =
      String(
        req.body
          ?.tradeId ||
        ""
      );

    const trade =
      activeTrades.get(
        tradeId
      );

    if (
      !trade ||
      (
        trade.from !==
          player &&
        trade.to !==
          player
      )
    ) {
      return res.status(
        404
      ).json({
        error:
          "Trade nicht gefunden"
      });
    }

    activeTrades.delete(
      tradeId
    );

    const other =
      trade.from ===
      player
        ? trade.to
        : trade.from;

    const otherWs =
      players.get(
        other
      );

    if (otherWs) {
      send(otherWs, {
        type:
          "tradeCancelled",

        tradeId:
          tradeId
      });
    }

    return res.json({
      ok: true
    });
  }
);


/* =========================================================
   TRADE AUFRÄUMEN
   ========================================================= */

setInterval(
  () => {
    const now =
      Date.now();

    for (
      let i =
        tradeRequests.length -
        1;
      i >= 0;
      i--
    ) {
      if (
        now -
          Number(
            tradeRequests[
              i
            ].createdAt ||
              0
          ) >
        5 * 60 * 1000
      ) {
        tradeRequests.splice(
          i,
          1
        );
      }
    }

    for (
      const [
        id,
        trade
      ] of activeTrades
    ) {
      if (
        now -
          Number(
            trade.createdAt ||
              0
          ) >
        10 * 60 * 1000
      ) {
        activeTrades.delete(
          id
        );
      }
    }
  },
  5000
);


/* =========================================================
   AUTOMATISCHES AUFRÄUMEN
   ========================================================= */

setInterval(
  () => {
    const now =
      Date.now();

    serverMessages =
      serverMessages.filter(
        message =>
          Number(
            message.endsAt ||
              0
          ) > now
      );

    cleanupEventRequests();

    // 2x beenden
    if (
      coinEventUntil >
        0 &&
      coinEventUntil <=
        now
    ) {
      stopCoinEvent();
    }

    // 10x beenden
    if (
      tenCoinEventUntil >
        0 &&
      tenCoinEventUntil <=
        now
    ) {
      stopTenCoinEvent();
    }

    // Galaxy beenden
    if (
      galaxyEventUntil >
        0 &&
      galaxyEventUntil <=
        now
    ) {
      stopGalaxyEvent();
    }
  },
  1000
);


/* =========================================================
   RENDER FALLBACK
   ========================================================= */

app.use(
  (req, res) => {
    res.sendFile(
      path.join(
        __dirname,
        "public",
        "index.html"
      )
    );
  }
);


/* =========================================================
   SERVER START
   ========================================================= */

server.listen(
  PORT,
  () => {
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
      "Trade-System aktiviert."
    );
  }
);
