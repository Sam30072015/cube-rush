
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
    if(ws.readyState===WebSocket.OPEN) ws.send(data);
  }
}

wss.on("connection", ws=>{
  ws.playerName="";

  ws.on("message", raw=>{
    let msg;
    try{ msg=JSON.parse(raw.toString()); }catch{return;}

    if(msg.type==="identify"){
      const name=cleanName(msg.name);
      if(!name)return;
      if(ws.playerName) players.delete(ws.playerName);
      ws.playerName=name;
      players.set(name,ws);
      send(ws,{type:"connected",name});
      return;
    }
  });

  ws.on("close",()=>{
    if(ws.playerName && players.get(ws.playerName)===ws){
      players.delete(ws.playerName);
    }
  });
});

/* Admin: give coins/skin to a named online player. */
app.post("/api/admin/give", (req,res)=>{
  if(req.headers["x-admin-key"] !== ADMIN_KEY){
    return res.status(401).json({error:"Unauthorized"});
  }

  const player=cleanName(req.body.player);
  const coins=Math.max(0,Math.floor(Number(req.body.coins)||0));
  const skin=String(req.body.skin||"").trim().slice(0,40);

  if(!player || (coins===0 && !skin)){
    return res.status(400).json({error:"Player and coins or skin required"});
  }

  const target=players.get(player);
  if(!target){
    return res.status(404).json({error:"Player is not online"});
  }

  send(target,{type:"gift",target:player,coins,skin});
  return res.json({ok:true,player,coins,skin});
});

/* Admin: broadcast a server message with explicit minutes + seconds. */
app.post("/api/admin/message", (req,res)=>{
  if(req.headers["x-admin-key"] !== ADMIN_KEY){
    return res.status(401).json({error:"Unauthorized"});
  }

  const text=String(req.body.text||"").trim().slice(0,120);
  const minutes=Math.max(0,Math.floor(Number(req.body.minutes)||0));
  const seconds=Math.max(0,Math.min(59,Math.floor(Number(req.body.seconds)||0)));
  const duration=(minutes*60+seconds)*1000;

  if(!text || duration<=0){
    return res.status(400).json({error:"Text and duration required"});
  }

  const message={type:"serverMessage",text,endsAt:Date.now()+duration};
  broadcast(message);
  return res.json({ok:true,...message});
});

app.get("/api/status", (req,res)=>{
  if(req.headers["x-admin-key"] !== ADMIN_KEY){
    return res.status(401).json({error:"Unauthorized"});
  }
  res.json({onlinePlayers:[...players.keys()]});
});

app.get("*", (req,res)=>{
  res.sendFile(path.join(__dirname,"public","index.html"));
});

server.listen(PORT,()=>{
  console.log(`Cube Rush server running on http://localhost:${PORT}`);
});
