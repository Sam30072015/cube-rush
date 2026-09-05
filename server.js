// server.js

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;

// ============================================================
// SPIELER
// ============================================================

const players = new Map();

// Online-Leaderboard.
// Die Daten liegen nur im Arbeitsspeicher des Servers.
const leaderboard = new Map();


// ============================================================
// HILFSFUNKTIONEN
// ============================================================

function cleanName(name) {
  return String(name || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 24);
}


// ============================================================
// LEADERBOARD
// ============================================================

function leaderboardRows(limit = 50) {
  return [...leaderboard.entries()]
    .map(([name, score]) => ({
      name,
      score: Math.max(0, Math.floor(Number(score) || 0))
    }))
    .sort((a, b) => {
      if (b.score !== a.score) {
        return b.score - a.score;
      }

      return a.name.localeCompare(
        b.name,
        "de",
        {
          sensitivity: "base"
        }
      );
    })
    .slice(0, limit)
    .map((entry, index) => ({
      rank: index + 1,
      name: entry.name,
      score: entry.score
    }));
}


function updateLeaderboard(name, score) {
  name = cleanName(name);

  score = Math.max(
    0,
    Math.floor(Number(score) || 0)
  );

  if (!name || score <= 0) {
    return false;
  }

  const oldScore = Math.max(
    0,
    Math.floor(Number(leaderboard.get(name)) || 0)
  );

  // Nur ein neuer persönlicher Bestwert wird gespeichert.
  if (score <= oldScore) {
    return false;
  }

  leaderboard.set(name, score);

  broadcast({
    type: "leaderboardUpdate",
    leaderboard: leaderboardRows(50)
  });

  return true;
}


// ============================================================
// WEBSOCKET BROADCAST
// ============================================================

function broadcast(data) {
  const message = JSON.stringify(data);

  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      try {
        client.send(message);
      } catch (err) {
        console.error(
          "WebSocket senden fehlgeschlagen:",
          err.message
        );
      }
    }
  }
}


// ============================================================
// HTTP / API
// ============================================================

app.use(express.json());

app.use(
  express.static(
    path.join(__dirname, "public")
  )
);


// Leaderboard API
app.get("/api/leaderboard", (req, res) => {
  res.json({
    ok: true,
    leaderboard: leaderboardRows(50)
  });
});


// ============================================================
// WEBSOCKET
// ============================================================

wss.on("connection", (ws) => {

  ws.playerName = null;


  ws.on("message", (raw) => {

    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch (err) {
      return;
    }

    if (!msg || typeof msg !== "object") {
      return;
    }


    // ----------------------------------------------------------
    // SPIELER IDENTIFIZIEREN
    // ----------------------------------------------------------

    if (msg.type === "identify") {

      const name = cleanName(msg.name);

      if (!name) {
        return;
      }

      ws.playerName = name;

      players.set(
        name,
        ws
      );


      // Falls der Spieler noch keinen Score hat,
      // wird ein Startwert von 0 angelegt.
      if (!leaderboard.has(name)) {
        leaderboard.set(name, 0);
      }


      // Verbindung bestätigen
      try {
        ws.send(
          JSON.stringify({
            type: "connected",
            name: name,

            leaderboard:
              leaderboardRows(50)
          })
        );
      } catch (err) {
        console.error(
          "Connected-Nachricht fehlgeschlagen:",
          err.message
        );
      }

      return;
    }


    // ----------------------------------------------------------
    // SCORE UPDATE
    // ----------------------------------------------------------

    if (
      msg.type === "scoreUpdate" ||
      msg.type === "leaderboardScore"
    ) {

      const name =
        ws.playerName ||
        cleanName(msg.name);

      if (!name) {
        return;
      }

      updateLeaderboard(
        name,
        msg.score
      );

      return;
    }


    // ----------------------------------------------------------
    // HEARTBEAT
    // ----------------------------------------------------------

    if (msg.type === "heartbeat") {

      try {
        ws.send(
          JSON.stringify({
            type: "heartbeat"
          })
        );
      } catch (err) {
        // Verbindung vermutlich geschlossen
      }

      return;
    }

  });


  // ------------------------------------------------------------
  // DISCONNECT
  // ------------------------------------------------------------

  ws.on("close", () => {

    if (
      ws.playerName &&
      players.get(ws.playerName) === ws
    ) {
      players.delete(
        ws.playerName
      );
    }

  });


  ws.on("error", (err) => {

    console.error(
      "WebSocket Fehler:",
      err.message
    );

  });

});


// ============================================================
// SERVER START
// ============================================================

server.listen(
  PORT,
  () => {
    console.log(
      `Cube Rush Server läuft auf Port ${PORT}`
    );
  }
);
