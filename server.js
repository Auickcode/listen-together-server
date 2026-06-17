const http = require("http");
const { Server } = require("socket.io");

const PORT = process.env.PORT || 8080;
const HOST_PASSWORD = "hawk";

console.log("Host password is built into server.js.");

const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "text/plain" });
  res.end("Spotify Listen Together server OK\n");
});

const io = new Server(httpServer, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  transports: ["websocket", "polling"]
});

const clients = new Map();
let hostId = null;
let lastTrackUri = "";
let lastPaused = false;
let lastPositionMs = 0;
let queue = [];

function emitQueue() {
  io.emit("queueUpdated", queue);
}

function publicClients() {
  return Array.from(clients.values()).map(c => ({
    name: c.name,
    isHost: c.id === hostId,
    watchingAD: false,
    watchingAd: false
  }));
}

function emitListeners() {
  io.emit("listeners", publicClients());
}

function isHost(socket) {
  return socket.id === hostId;
}

function makeHost(socket) {
  if (hostId && clients.has(hostId)) {
    io.to(hostId).emit("isHost", false);
  }
  hostId = socket.id;
  socket.emit("isHost", true);
  emitListeners();
  console.log(`Host is now ${clients.get(socket.id)?.name || socket.id}`);
}

io.on("connection", socket => {
  clients.set(socket.id, { id: socket.id, name: "Unnamed" });
  console.log(`Connected ${socket.id}`);

  socket.on("login", (name, version, incompatibleCallback) => {
    const client = clients.get(socket.id);
    if (client) client.name = name || "Unnamed";

    console.log(`Login ${client?.name} version=${version || "unknown"}`);

    if (!hostId) makeHost(socket);
    else socket.emit("isHost", socket.id === hostId);

    emitListeners();

    // If someone joins late, try to push the latest known song to them.
    if (lastTrackUri) {
      console.log(`Sending last track to late joiner: ${lastTrackUri}`);
      socket.emit("changeSong", lastTrackUri);
      setTimeout(() => socket.emit("updateSong", lastPaused, lastPositionMs), 1000);
    }

    socket.emit("queueUpdated", queue);
  });

  socket.on("requestHost", password => {
    if (password === HOST_PASSWORD) {
      makeHost(socket);
      socket.emit("bottomMessage", "You are now a host.");
    } else {
      socket.emit("windowMessage", "Wrong host password.");
    }
  });

  socket.on("cancelHost", () => {
    if (isHost(socket)) {
      socket.emit("isHost", false);
      hostId = null;
      emitListeners();
      console.log("Host cancelled hosting");
    }
  });

  // Host changed/loaded a song. Relay it to every other listener.
  socket.on("loadingSong", trackUri => {
    if (!isHost(socket)) return;
    if (!trackUri) return;
    lastTrackUri = trackUri;
    lastPaused = false;
    lastPositionMs = 0;
    console.log(`Host loadingSong: ${trackUri}`);
    socket.broadcast.emit("changeSong", trackUri);
  });

  socket.on("changedSong", (trackUri, trackName, imageUrl) => {
    if (!isHost(socket)) return;
    if (!trackUri) return;
    lastTrackUri = trackUri;
    lastPaused = false;
    lastPositionMs = 0;
    console.log(`Host changedSong: ${trackUri} ${trackName || ""}`);
    socket.broadcast.emit("changeSong", trackUri);
  });

  socket.on("requestChangeSong", trackUri => {
    if (!isHost(socket)) return;
    if (!trackUri) return;
    lastTrackUri = trackUri;
    lastPaused = false;
    lastPositionMs = 0;
    console.log(`Host requestChangeSong: ${trackUri}`);
    socket.broadcast.emit("changeSong", trackUri);
  });

  socket.on("requestUpdateSong", (paused, milliseconds) => {
    if (!isHost(socket)) return;
    lastPaused = !!paused;
    if (typeof milliseconds === "number") lastPositionMs = milliseconds;
    console.log(`Host updateSong paused=${lastPaused} ms=${lastPositionMs}`);
    socket.broadcast.emit("updateSong", lastPaused, lastPositionMs);
  });

  socket.on("requestSong", (trackUri, trackName) => {
    const requester = clients.get(socket.id)?.name || "A listener";

    queue.push({
      trackUri,
      trackName: trackName || trackUri,
      requester
    });

    emitQueue();

    socket.emit("bottomMessage", `" + '${trackName || trackUri}' + "` added to queue.");
  });

  socket.on("songFinished", () => {
    if (!isHost(socket)) return;
    if (!queue.length) return;

    const next = queue.shift();
    emitQueue();

    lastTrackUri = next.trackUri;
    lastPaused = false;
    lastPositionMs = 0;

    io.emit("changeSong", next.trackUri);
  });

  socket.on("disconnect", () => {
    const wasHost = socket.id === hostId;
    const name = clients.get(socket.id)?.name || socket.id;
    clients.delete(socket.id);
    console.log(`Disconnected ${name}`);

    if (wasHost) {
      hostId = null;
      const next = clients.keys().next().value;
      if (next) {
        hostId = next;
        io.to(next).emit("isHost", true);
        console.log(`Auto-promoted new host ${clients.get(next)?.name || next}`);
      }
    }
    emitListeners();
  });
});

httpServer.listen(PORT, () => {
  console.log(`Spotify Listen Together server listening on ${PORT}`);
});
