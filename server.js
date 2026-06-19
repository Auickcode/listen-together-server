const http = require("http");
const { Server } = require("socket.io");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;
const HOST_PASSWORD = "hawk";

const httpServer = http.createServer((req, res) => {
  res.writeHead(200, {"content-type":"text/plain"});
  res.end("Spotify Listen Together queue server OK\n");
});

const io = new Server(httpServer, {
  cors:{origin:"*", methods:["GET","POST"]},
  transports:["websocket","polling"]
});

const clients = new Map();
let hostId = null;
let lastTrackUri = "";
let lastPaused = false;
let lastPositionMs = 0;
let queue = [];

function isHost(socket){ return socket.id === hostId; }

function emitQueue(){
  io.emit("queueUpdated", queue);
}

function makeHost(socket){
  if(hostId && clients.has(hostId))
    io.to(hostId).emit("isHost", false);

  hostId = socket.id;
  socket.emit("isHost", true);
  emitListeners();
}

function emitListeners(){
  io.emit("listeners", [...clients.values()].map(c=>({
    name:c.name,
    isHost:c.id===hostId,
    watchingAD:false,
    watchingAd:false
  })));
}

io.on("connection", socket=>{
  clients.set(socket.id,{id:socket.id,name:"Unnamed"});

  socket.on("login",(name)=>{
    clients.get(socket.id).name=name||"Unnamed";

    if(!hostId) makeHost(socket);
    else socket.emit("isHost",isHost(socket));

    emitListeners();
    socket.emit("queueUpdated",queue);

    if(lastTrackUri){
      socket.emit("changeSong",lastTrackUri);
      socket.emit("updateSong",lastPaused,lastPositionMs);
    }
  });

  socket.on("requestHost",password=>{
    if(password===HOST_PASSWORD) makeHost(socket);
  });

  socket.on("cancelHost",()=>{
    if(isHost(socket)){
      hostId=null;
      socket.emit("isHost",false);
      emitListeners();
    }
  });

  socket.on("requestSong",(trackUri,trackName)=>{
    if(!trackUri) return;

    queue.push({
      id:crypto.randomUUID(),
      trackUri,
      trackName:trackName||trackUri,
      requester:clients.get(socket.id)?.name||"Listener"
    });

    emitQueue();
  });

  socket.on("removeQueueSong",id=>{
    if(!isHost(socket)) return;
    queue=queue.filter(x=>x.id!==id);
    emitQueue();
  });

  socket.on("clearQueue",()=>{
    if(!isHost(socket)) return;
    queue=[];
    emitQueue();
  });

  socket.on("songFinished",()=>{
    if(!isHost(socket) || !queue.length) return;

    const next=queue.shift();
    emitQueue();

    lastTrackUri=next.trackUri;
    lastPaused=false;
    lastPositionMs=0;

    io.emit("changeSong",next.trackUri);
  });

  socket.on("loadingSong",trackUri=>{
    if(!isHost(socket)) return;
    lastTrackUri=trackUri;
    socket.broadcast.emit("changeSong",trackUri);
  });

  socket.on("changedSong",(trackUri)=>{
    if(!isHost(socket)) return;
    lastTrackUri=trackUri;
    socket.broadcast.emit("changeSong",trackUri);
  });

  socket.on("requestUpdateSong",(paused,ms)=>{
    if(!isHost(socket)) return;
    lastPaused=!!paused;
    lastPositionMs=typeof ms==="number"?ms:0;
    socket.broadcast.emit("updateSong",lastPaused,lastPositionMs);
  });

  socket.on("disconnect",()=>{
    const wasHost=isHost(socket);
    clients.delete(socket.id);

    if(wasHost){
      hostId=null;
      const next=clients.keys().next().value;
      if(next){
        hostId=next;
        io.to(next).emit("isHost",true);
      }
    }
    emitListeners();
  });
});

httpServer.listen(PORT,()=>console.log("Queue server listening on "+PORT));