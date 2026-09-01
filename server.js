const path = require("path");
const http = require("http");
const express = require("express");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 3000);
const ADMIN_KEY = process.env.ADMIN_KEY || "4729";

const app = express();

app.use(express.json({limit:"50kb"}));
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);
const wss = new WebSocket.Server({server});

const players = new Map(); // name -> ws

function cleanName(name){
  return String(name || "").trim().slice(0,40);
}

function send(ws, payload){
  if(ws && ws.readyState === WebSocket.OPEN){
    ws.send(JSON.stringify(payload));
  }
}

function broadcast(payload){
  const data=JSON.stringify(payload);

  for(const ws of wss.clients){
    if(ws.readyState===WebSocket.OPEN){
      ws.send(data);
    }
  }
}

wss.on("connection", ws=>{
  ws.playerName="";

  ws.on("message", raw=>{
    let msg;

    try{
      msg=JSON.parse(raw.toString());
    }catch{
      return;
    }

    if(msg.type==="identify"){
      let name=cleanName(msg.name);

      if(!name){
        name=
          "Spieler-" +
          Math.random().toString(36).slice(2,8).toUpperCase();
      }

      // Verhindert gleiche Namen bei zwei gleichzeitig verbundenen Spielern.
      if(players.has(name) && players.get(name)!==ws){
        name =
          name +
          "-" +
          Math.random().toString(36).slice(2,5).toUpperCase();
      }

      if(ws.playerName){
        players.delete(ws.playerName);
      }

      ws.playerName=name;
      players.set(name,ws);

      send(ws,{
        type:"connected",
        name
      });

      return;
    }
  });

  ws.on("close",()=>{
    if(ws.playerName && players.get(ws.playerName)===ws){
      players.delete(ws.playerName);
    }
  });
});


/* =========================================
   ADMIN: 2× MÜNZEN EVENT
   ========================================= */

app.post("/api/admin/coins-event",(req,res)=>{

  if(req.headers["x-admin-key"]!==ADMIN_KEY){
    return res.status(401).json({
      error:"Unauthorized"
    });
  }

  const action=String(req.body.action||"");

  if(action==="start"){

    const durationMs=Math.max(
      1000,
      Math.min(
        10080*60*1000,
        Math.floor(Number(req.body.durationMs)||0)
      )
    );

    const until=Date.now()+durationMs;

    const message={
      type:"coinEvent",
      until
    };

    // Event an alle aktuell verbundenen Spieler senden.
    broadcast(message);

    return res.json({
      ok:true,
      ...message
    });
  }

  if(action==="stop"){

    const message={
      type:"coinEvent",
      until:0
    };

    broadcast(message);

    return res.json({
      ok:true,
      ...message
    });
  }

  return res.status(400).json({
    error:"Invalid action"
  });
});


/* =========================================
   ADMIN: SPIELER BELOHNEN
   ========================================= */

app.post("/api/admin/give",(req,res)=>{

  if(req.headers["x-admin-key"]!==ADMIN_KEY){
    return res.status(401).json({
      error:"Unauthorized"
    });
  }

  const player=cleanName(req.body.player);

  const coins=Math.max(
    0,
    Math.floor(Number(req.body.coins)||0)
  );

  const skin=String(req.body.skin||"")
    .trim()
    .slice(0,40);

  if(!player || (coins===0 && !skin)){
    return res.status(400).json({
      error:"Player and coins or skin required"
    });
  }

  const target=players.get(player);

  if(!target){
    return res.status(404).json({
      error:"Player is not online"
    });
  }

  send(target,{
    type:"gift",
    target:player,
    coins,
    skin
  });

  return res.json({
    ok:true,
    player,
    coins,
    skin
  });
});


/* =========================================
   ADMIN: SERVERNACHRICHT
   ========================================= */

app.post("/api/admin/message",(req,res)=>{

  if(req.headers["x-admin-key"]!==ADMIN_KEY){
    return res.status(401).json({
      error:"Unauthorized"
    });
  }

  const text=String(req.body.text||"")
    .trim()
    .slice(0,120);

  const minutes=Math.max(
    0,
    Math.floor(Number(req.body.minutes)||0)
  );

  const seconds=Math.max(
    0,
    Math.min(
      59,
      Math.floor(Number(req.body.seconds)||0)
    )
  );

  const duration=(minutes*60+seconds)*1000;

  if(!text || duration<=0){
    return res.status(400).json({
      error:"Text and duration required"
    });
  }

  const message={
    type:"serverMessage",
    text,
    endsAt:Date.now()+duration
  };

  // Nachricht an alle verbundenen Spieler senden.
  broadcast(message);

  return res.json({
    ok:true,
    ...message
  });
});


/* =========================================
   ADMIN: ONLINE-SPIELER
   ========================================= */

app.get("/api/status",(req,res)=>{

  if(req.headers["x-admin-key"]!==ADMIN_KEY){
    return res.status(401).json({
      error:"Unauthorized"
    });
  }

  res.json({
    onlinePlayers:[...players.keys()]
  });
});


/* =========================================
   SPIEL AUSLIEFERN
   ========================================= */

app.use((req,res)=>{
  res.sendFile(
    path.join(
      __dirname,
      "public",
      "index.html"
    )
  );
});


/* =========================================
   SERVER START
   ========================================= */

server.listen(PORT,()=>{
  console.log(
    `Cube Rush server running on http://localhost:${PORT}`
  );
});
