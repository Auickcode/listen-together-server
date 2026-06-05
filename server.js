const http = require("http");
const { Server } = require("socket.io");

const PORT = process.env.PORT || 3000;
const HOST_PASSWORD = process.env.HOST_PASSWORD || "change-me";
const REQUIRED_CLIENT = process.env.REQUIRED_CLIENT || "0.4.x";

const server = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
  res.end("Spotify Listen Together server OK\n");
});

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  transports: ["websocket", "polling"]
});

const clients = new Map();
let hostId = null;
let currentTrack = "";
let currentPaused = true;
let currentPosition = 0;

function publicClients() {
  return [...clients.values()].map(c => ({
    name: c.name,
    isHost: c.id === hostId,
    watchingAD: !!c.watchingAD
  }));
}

function sendListeners() {
  io.emit("listeners", publicClients());
}

function setHost(id) {
  if (hostId && clients.has(hostId)) io.to(hostId).emit("isHost", false);
  hostId = id || null;
  if (hostId && clients.has(hostId)) io.to(hostId).emit("isHost", true);
  sendListeners();
}

io.on("connection", socket => {
  clients.set(socket.id, { id: socket.id, name: "Unnamed", watchingAD: false });

  socket.on("login", (name, version, incompatible) => {
    const c = clients.get(socket.id);
    if (!c) return;
    c.name = String(name || "Unnamed").slice(0, 40);

    if (!hostId) setHost(socket.id);
    else socket.emit("isHost", socket.id === hostId);

    if (currentTrack) {
      socket.emit("changeSong", currentTrack);
      socket.emit("updateSong", currentPaused, currentPosition);
    }
    sendListeners();
  });

  socket.on("requestHost", password => {
    if (String(password || "") === HOST_PASSWORD) {
      setHost(socket.id);
      socket.emit("bottomMessage", "You are now the host.");
    } else {
      socket.emit("windowMessage", "Wrong host password.");
    }
  });

  socket.on("cancelHost", () => {
    if (socket.id === hostId) setHost(null);
  });

  socket.on("loadingSong", trackUri => {
    if (socket.id !== hostId) return;
    currentTrack = trackUri || "";
    socket.broadcast.emit("changeSong", currentTrack);
  });

  socket.on("changedSong", (trackUri, trackName, imageUrl) => {
    if (socket.id !== hostId) return;
    currentTrack = trackUri || "";
    currentPaused = true;
    currentPosition = 0;
    socket.broadcast.emit("changeSong", currentTrack);
  });

  socket.on("requestChangeSong", trackUri => {
    if (socket.id !== hostId) return;
    currentTrack = trackUri || "";
    io.emit("changeSong", currentTrack);
  });

  socket.on("requestUpdateSong", (paused, milliseconds) => {
    if (socket.id !== hostId) return;
    currentPaused = !!paused;
    if (Number.isFinite(milliseconds)) currentPosition = milliseconds;
    socket.broadcast.emit("updateSong", currentPaused, currentPosition);
  });

  socket.on("requestSong", (trackUri, trackName) => {
    if (!hostId || !clients.has(hostId)) {
      socket.emit("windowMessage", "No host is connected right now.");
      return;
    }
    const from = clients.get(socket.id)?.name || "A listener";
    io.to(hostId).emit("songRequested", trackUri, trackName || "UNKNOWN NAME", from);
  });

  socket.on("disconnect", () => {
    const wasHost = socket.id === hostId;
    clients.delete(socket.id);
    if (wasHost) {
      const next = clients.keys().next().value || null;
      setHost(next);
    } else {
      sendListeners();
    }
  });
});

server.listen(PORT, () => {
  console.log(`Spotify Listen Together server listening on ${PORT}`);
  console.log("Set HOST_PASSWORD to choose the host password.");
});
