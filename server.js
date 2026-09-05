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
const leaderboard = new Map();

let coinEventUntil = 0;
let tenCoinEventUntil = 0;
let galaxyEventUntil = 0;

let serverMessages = [];
let eventRequests = [];

let tradeRequests = [];
const activeTrades = new Map();
const completedTrades = new Map();

let activePoll = null;

function cleanName(name) {
  return String(name || "").trim().slice(0, 40);
}

function normalizeCoinValue(value) {
  const n = Number(value);

  if (!Number.isFinite(n)) return 0;

  return Math.max(0, n);
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
  return Math.max(0, Math.floor(Number(body?.coins) || 0));
}

function makeRequestId() {
  return (
    Date.now().toString(36) +
    Math.random().toString(36).slice(2, 9)
  );
}

/* =========================================================
   RANGLISTE
   ========================================================= */

function leaderboardRows(limit = 50) {
  return [...leaderboard.entries()]
    .map(([name, coins]) => ({
      name,
      coins: normalizeCoinValue(coins)
    }))
    .sort((a, b) => {
      if (b.coins !== a.coins) {
        return b.coins - a.coins;
      }

      return a.name.localeCompare(
        b.name,
        "de",
        { sensitivity: "base" }
      );
    })
    .slice(0, limit)
    .map((entry, index) => ({
      rank: index + 1,
      name: entry.name,
      coins: entry.coins
    }));
}

function updateLeaderboard(name, coins) {
  name = cleanName(name);

  if (!name) return;

  leaderboard.set(
    name,
    normalizeCoinValue(coins)
  );

  broadcast({
    type: "leaderboardUpdate"
  });
}

/* =========================================================
   POLL
   ========================================================= */

function publicPoll() {
  if (!activePoll) return null;

  if (
    Number(activePoll.endsAt || 0) <=
    Date.now()
  ) {
    return null;
  }

  return {
    id: activePoll.id,
    question: activePoll.question,
    yesLabel: activePoll.yesLabel,
    noLabel: activePoll.noLabel,
    endsAt: activePoll.endsAt,
    answers: {
      yes: Number(
        activePoll.answers?.yes || 0
      ),
      no: Number(
        activePoll.answers?.no || 0
      )
    }
  };
}

/* =========================================================
   EVENTS
   ========================================================= */

function executeEvent(
  event,
  action,
  durationMs
) {
  if (event === "coins") {
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
  }

  if (event === "tenCoins") {
    if (action === "start") {
      tenCoinEventUntil =
        Date.now() + durationMs;

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

  if (event === "galaxy") {
    if (action === "start") {
      galaxyEventUntil =
        Date.now() + durationMs;

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

      const oldSocket =
        players.get(name);

      if (
        oldSocket &&
        oldSocket !== ws
      ) {
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

      if (!leaderboard.has(name)) {
        leaderboard.set(name, "0");
      }

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

        poll: publicPoll()
      });

      return;
    }

    /*
      Die HTML sendet die aktuellen Münzen.
      Dadurch wird die Rangliste auf dem Server
      aktuell gehalten.
    */
    if (msg.type === "coinsUpdate") {
      const name =
        ws.playerName ||
        cleanName(msg.name);

      if (!name) return;

      const coins =
        normalizeCoinValue(msg.coins);

      updateLeaderboard(
        name,
        coins
      );

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
   RANGLISTE API
   ========================================================= */

app.get(
  "/api/leaderboard",
  (req, res) => {
    return res.json({
      ok: true,
      leaderboard: leaderboardRows(50)
    });
  }
);

/* =========================================================
   HAUPT-ADMIN
   ========================================================= */

app.post(
  "/api/admin/coins-event",
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
  }
);

app.post(
  "/api/admin/ten-coins-event",
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

app.post(
  "/api/admin/give",
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

    for (
      const [name, ws] of players
    ) {
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

/* =========================================================
   SERVER-NACHRICHTEN
   ========================================================= */

app.post(
  "/api/admin/message",
  (req, res) => {
    if (!adminMessageEventOK(req)) {
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
   ZWEITES ADMIN-PANEL
   ========================================================= */

app.post(
  "/api/second-admin/event-request",
  (req, res) => {
    if (!secondAdminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const event =
      String(req.body?.event || "");

    const action =
      String(req.body?.action || "");

    if (
      ![
        "coins",
        "tenCoins",
        "galaxy"
      ].includes(event)
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
        Math.floor(
          Number(
            req.body?.durationMs
          ) || 0
        );

      if (event === "galaxy") {
        durationMs =
          Math.max(
            60000,
            Math.min(
              10080 * 60 * 1000,
              value
            )
          );
      } else {
        durationMs =
          Math.max(
            1000,
            Math.min(
              10080 * 60 * 1000,
              value
            )
          );
      }
    }

    const duplicate =
      eventRequests.find(
        r =>
          r.event === event &&
          r.action === action
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

app.get(
  "/api/admin/event-requests",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    return res.json({
      ok: true,
      requests: eventRequests.map(
        r => ({
          id: r.id,
          event: r.event,
          action: r.action,
          durationMs: r.durationMs,
          createdAt: r.createdAt
        })
      )
    });
  }
);

app.post(
  "/api/admin/event-request/respond",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
      });
    }

    const requestId =
      String(
        req.body?.requestId || ""
      );

    const approve =
      req.body?.approve === true;

    const index =
      eventRequests.findIndex(
        r => r.id === requestId
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

    if (!approve) {
      return res.json({
        ok: true,
        approved: false
      });
    }

    const result =
      executeEvent(
        request.event,
        request.action,
        request.durationMs
      );

    return res.json({
      ok: true,
      approved: true,
      ...result
    });
  }
);

/* =========================================================
   ONLINE
   ========================================================= */

app.get(
  "/api/admin/online-players",
  (req, res) => {
    if (!adminOK(req)) {
      return res.status(401).json({
        error: "Unauthorized"
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
          ([name]) => name
        );

    return res.json({
      ok: true,
      onlinePlayers
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

    return res.json({
      onlinePlayers:
        [...players.keys()],

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

      poll: publicPoll()
    });
  }
);

/* =========================================================
   FRAGE
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
        .slice(0, 200);

    const yesLabel =
      String(
        req.body?.yesLabel || "Ja"
      )
        .trim()
        .slice(0, 60);

    const noLabel =
      String(
        req.body?.noLabel || "Nein"
      )
        .trim()
        .slice(0, 60);

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

    const durationMs =
      (minutes * 60 + seconds) *
      1000;

    if (!question || durationMs <= 0) {
      return res.status(400).json({
        error:
          "Frage und Dauer erforderlich"
      });
    }

    activePoll = {
      id: makeRequestId(),
      question,
      yesLabel: yesLabel || "Ja",
      noLabel: noLabel || "Nein",
      endsAt:
        Date.now() + durationMs,

      answers: {
        yes: 0,
        no: 0
      },

      voters: new Set()
    };

    broadcast({
      type: "playerPoll",
      poll: publicPoll()
    });

    return res.json({
      ok: true,
      poll: publicPoll()
    });
  }
);

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
      type: "playerPollClear"
    });

    return res.json({
      ok: true
    });
  }
);

app.post(
  "/api/poll/answer",
  (req, res) => {
    if (
      !activePoll ||
      !publicPoll()
    ) {
      return res.status(404).json({
        error:
          "Keine aktive Frage"
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

    const voterId =
      String(
        req.body?.voterId || ""
      )
        .trim()
        .slice(0, 100);

    if (
      pollId !==
      activePoll.id
    ) {
      return res.status(409).json({
        error:
          "Frage ist nicht mehr aktiv"
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

    if (!voterId) {
      return res.status(400).json({
        error:
          "Voter-ID fehlt"
      });
    }

    if (
      activePoll.voters.has(
        voterId
      )
    ) {
      return res.status(409).json({
        error:
          "Du hast schon abgestimmt"
      });
    }

    activePoll.voters.add(
      voterId
    );

    activePoll.answers[
      answer
    ]++;

    broadcast({
      type: "pollUpdated",
      poll: publicPoll()
    });

    return res.json({
      ok: true,
      answers: {
        ...activePoll.answers
      }
    });
  }
);

/* =========================================================
   TRADE
   ========================================================= */

function getTradeForPlayer(player) {
  for (
    const trade of
      activeTrades.values()
  ) {
    if (
      trade.from === player ||
      trade.to === player
    ) {
      const mine =
        trade.from === player
          ? trade.fromOffer
          : trade.toOffer;

      const theirs =
        trade.from === player
          ? trade.toOffer
          : trade.fromOffer;

      const mineConfirmed =
        trade.from === player
          ? trade.fromConfirmed
          : trade.toConfirmed;

      const theirsConfirmed =
        trade.from === player
          ? trade.toConfirmed
          : trade.fromConfirmed;

      return {
        id: trade.id,

        other:
          trade.from === player
            ? trade.to
            : trade.from,

        myOffer: mine,
        otherOffer: theirs,

        myConfirmed:
          mineConfirmed,

        otherConfirmed:
          theirsConfirmed
      };
    }
  }

  return null;
}

app.get(
  "/api/trade/status",
  (req, res) => {
    const player =
      cleanName(
        req.query.player
      );

    if (!player) {
      return res.status(400).json({
        error:
          "Spielername fehlt"
      });
    }

    const onlinePlayers =
      [...players.keys()].filter(
        name =>
          name !== player
      );

    const incomingRequests =
      tradeRequests.filter(
        request =>
          request.to === player
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
      return res.status(400).json({
        error:
          "Ungültiger Spieler"
      });
    }

    if (!players.has(from)) {
      return res.status(400).json({
        error:
          "Du bist nicht online"
      });
    }

    if (!players.has(to)) {
      return res.status(404).json({
        error:
          "Spieler ist nicht online"
      });
    }

    if (
      getTradeForPlayer(from)
    ) {
      return res.status(409).json({
        error:
          "Du bist bereits in einem Trade"
      });
    }

    if (
      tradeRequests.some(
        r =>
          r.from === from &&
          r.to === to
      )
    ) {
      return res.status(409).json({
        error:
          "Anfrage wurde bereits gesendet"
      });
    }

    const request = {
      id: makeRequestId(),
      from,
      to,
      createdAt: Date.now()
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

app.post(
  "/api/trade/respond",
  (req, res) => {
    const player =
      cleanName(
        req.body?.player
      );

    const requestId =
      String(
        req.body?.requestId || ""
      );

    const accept =
      req.body?.accept === true;

    const index =
      tradeRequests.findIndex(
        request =>
          request.id ===
            requestId &&
          request.to ===
            player
      );

    if (index < 0) {
      return res.status(404).json({
        error:
          "Trade-Anfrage nicht gefunden"
      });
    }

    const request =
      tradeRequests[index];

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

          accepted: false
        });
      }

      return res.json({
        ok: true,
        accepted: false
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
      return res.status(409).json({
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
      return res.status(409).json({
        error:
          "Ein Spieler ist bereits in einem Trade"
      });
    }

    const trade = {
      id: makeRequestId(),

      from: request.from,
      to: request.to,

      fromOffer: null,
      toOffer: null,

      fromConfirmed: false,
      toConfirmed: false,

      createdAt: Date.now()
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
      accepted: true,

      trade:
        getTradeForPlayer(
          player
        )
    });
  }
);

app.post(
  "/api/trade/offer",
  (req, res) => {
    const player =
      cleanName(
        req.body?.player
      );

    const tradeId =
      String(
        req.body?.tradeId || ""
      );

    const skinId =
      String(
        req.body?.skinId || ""
      ).trim();

    if (!skinId) {
      return res.status(400).json({
        error:
          "Ungültiger Skin"
      });
    }

    const trade =
      activeTrades.get(
        tradeId
      );

    if (
      !trade ||
      (
        trade.from !== player &&
        trade.to !== player
      )
    ) {
      return res.status(404).json({
        error:
          "Trade nicht gefunden"
      });
    }

    if (trade.from === player) {
      if (trade.fromConfirmed) {
        return res.status(409).json({
          error:
            "Bereits bestätigt"
        });
      }

      trade.fromOffer =
        skinId;
    } else {
      if (trade.toConfirmed) {
        return res.status(409).json({
          error:
            "Bereits bestätigt"
        });
      }

      trade.toOffer =
        skinId;
    }

    const fromWs =
      players.get(
        trade.from
      );

    const toWs =
      players.get(
        trade.to
      );

    if (fromWs) {
      send(fromWs, {
        type:
          "tradeUpdated",
        trade:
          getTradeForPlayer(
            trade.from
          )
      });
    }

    if (toWs) {
      send(toWs, {
        type:
          "tradeUpdated",
        trade:
          getTradeForPlayer(
            trade.to
          )
      });
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

app.post(
  "/api/trade/confirm",
  (req, res) => {
    const player =
      cleanName(
        req.body?.player
      );

    const tradeId =
      String(
        req.body?.tradeId || ""
      );

    const trade =
      activeTrades.get(
        tradeId
      );

    if (
      !trade ||
      (
        trade.from !== player &&
        trade.to !== player
      )
    ) {
      return res.status(404).json({
        error:
          "Trade nicht gefunden"
      });
    }

    if (trade.from === player) {
      if (!trade.fromOffer) {
        return res.status(400).json({
          error:
            "Bitte zuerst einen Skin auswählen"
        });
      }

      trade.fromConfirmed = true;
    } else {
      if (!trade.toOffer) {
        return res.status(400).json({
          error:
            "Bitte zuerst einen Skin auswählen"
        });
      }

      trade.toConfirmed = true;
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
          id: trade.id,

          sentSkin:
            fromSkin,

          receivedSkin:
            toSkin,

          removeSkin:
            fromSkin,

          addSkin:
            toSkin
        }
      );

      completedTrades.set(
        toPlayer,
        {
          id: trade.id,

          sentSkin:
            toSkin,

          receivedSkin:
            fromSkin,

          removeSkin:
            toSkin,

          addSkin:
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
            trade.id,

          sentSkin:
            fromSkin,

          receivedSkin:
            toSkin,

          removeSkin:
            fromSkin,

          addSkin:
            toSkin
        });
      }

      if (toWs) {
        send(toWs, {
          type:
            "tradeComplete",

          tradeId:
            trade.id,

          sentSkin:
            toSkin,

          receivedSkin:
            fromSkin,

          removeSkin:
            toSkin,

          addSkin:
            fromSkin
        });
      }

      return res.json({
        ok: true,

        completed: {
          id: trade.id,

          sentSkin:
            player === fromPlayer
              ? fromSkin
              : toSkin,

          receivedSkin:
            player === fromPlayer
              ? toSkin
              : fromSkin,

          removeSkin:
            player === fromPlayer
              ? fromSkin
              : toSkin,

          addSkin:
            player === fromPlayer
              ? toSkin
              : fromSkin
        }
      });
    }

    return res.json({
      ok: true,
      completed: false,

      trade:
        getTradeForPlayer(
          player
        )
    });
  }
);

app.post(
  "/api/trade/cancel",
  (req, res) => {
    const player =
      cleanName(
        req.body?.player
      );

    const tradeId =
      String(
        req.body?.tradeId || ""
      );

    const trade =
      activeTrades.get(
        tradeId
      );

    if (
      !trade ||
      (
        trade.from !== player &&
        trade.to !== player
      )
    ) {
      return res.status(404).json({
        error:
          "Trade nicht gefunden"
      });
    }

    activeTrades.delete(
      tradeId
    );

    const other =
      trade.from === player
        ? trade.to
        : trade.from;

    const otherWs =
      players.get(other);

    if (otherWs) {
      send(otherWs, {
        type:
          "tradeCancelled",
        tradeId
      });
    }

    return res.json({
      ok: true
    });
  }
);

/* =========================================================
   AUFRÄUMEN
   ========================================================= */

setInterval(() => {
  const now = Date.now();

  serverMessages =
    serverMessages.filter(
      m =>
        Number(m.endsAt || 0) > now
    );

  eventRequests =
    eventRequests.filter(
      r =>
        now -
          Number(
            r.createdAt || 0
          ) <
        10 * 60 * 1000
    );

  if (
    activePoll &&
    Number(activePoll.endsAt || 0) <= now
  ) {
    activePoll = null;

    broadcast({
      type:
        "playerPollClear"
    });
  }
}, 1000);

setInterval(() => {
  if (
    coinEventUntil > 0 &&
    Date.now() >= coinEventUntil
  ) {
    executeEvent(
      "coins",
      "stop",
      0
    );
  }

  if (
    tenCoinEventUntil > 0 &&
    Date.now() >= tenCoinEventUntil
  ) {
    executeEvent(
      "tenCoins",
      "stop",
      0
    );
  }

  if (
    galaxyEventUntil > 0 &&
    Date.now() >= galaxyEventUntil
  ) {
    executeEvent(
      "galaxy",
      "stop",
      0
    );
  }
}, 1000);

setInterval(() => {
  const now = Date.now();

  tradeRequests =
    tradeRequests.filter(
      request =>
        now -
          Number(
            request.createdAt || 0
          ) <
        5 * 60 * 1000
    );

  for (
    const [
      id,
      trade
    ] of activeTrades
  ) {
    if (
      now -
        Number(
          trade.createdAt || 0
        ) >
      10 * 60 * 1000
    ) {
      activeTrades.delete(id);
    }
  }
}, 5000);

/* =========================================================
   ROUTING
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
   START
   ========================================================= */

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
