import express from "express";
import { createServer } from "node:http";
import { Server } from "socket.io";
import cors from "cors";

const app = express();
app.use(cors());

// Render.com health check ke liye ek simple GET route
app.get("/", (req, res) => {
  res.send("SyncRadar Backend is Live & Running! 🚀");
});

const httpServer = createServer(app);
const io = new Server(httpServer, {
    cors: { 
      origin: ["http://localhost:3000","http://localhost:3001","http://localhost:3002", "https://syncradar.netlify.app"], 
      methods: ["GET", "POST"] 
    }
  });

const activeUsers = new Map();
const screenPins = new Map(); // pin -> host socketId (screen-share access PIN)

io.on("connection", (socket) => {
  console.log("🟢 Peer Connected:", socket.id);

  socket.on("user-joined", (userData) => {
    const newUser = { socketId: socket.id, ...userData };
    activeUsers.set(socket.id, newUser);

    const existingUsers = Array.from(activeUsers.values()).filter(user => user.socketId !== socket.id);
    socket.emit("all-peers", existingUsers);
    socket.broadcast.emit("new-peer-detected", newUser);
  });

  socket.on("send-chat", (data) => {
    socket.broadcast.emit("receive-chat", data);
  });

  socket.on("send-private-chat", (data) => {
    // Ye line message ko sirf target user (data.to) tak bhejti hai
    socket.to(data.to).emit("receive-private-chat", data);
  });

  // Only relay to a currently-connected target socket.
  const isValidTarget = (data) => data && activeUsers.has(data.to);

  socket.on("file-request", (data) => {
    if (!isValidTarget(data)) return;
    // Never trust client-supplied identity — derive it from the authenticated socket.
    const sender = activeUsers.get(socket.id);
    socket.to(data.to).emit("file-request", {
      ...data,
      from: socket.id,
      senderName: sender ? sender.name : "Unknown",
    });
  });

  socket.on("file-response", (data) => {
    if (!isValidTarget(data)) return;
    const responder = activeUsers.get(socket.id);
    socket.to(data.to).emit("file-response", {
      ...data,
      from: socket.id,
      receiverName: responder ? responder.name : "Unknown",
    });
  });

  socket.on("offer", (data) => {
    if (!isValidTarget(data)) return;
    socket.to(data.to).emit("offer", { from: socket.id, offer: data.offer });
  });
  socket.on("answer", (data) => {
    if (!isValidTarget(data)) return;
    socket.to(data.to).emit("answer", { from: socket.id, answer: data.answer });
  });
  socket.on("ice-candidate", (data) => {
    if (!isValidTarget(data)) return;
    socket.to(data.to).emit("ice-candidate", { from: socket.id, candidate: data.candidate });
  });

  // Typing indicator (server stamps the trusted sender name)
  socket.on("typing", (data) => {
    if (!isValidTarget(data)) return;
    const sender = activeUsers.get(socket.id);
    socket.to(data.to).emit("typing", {
      from: socket.id,
      senderName: sender ? sender.name : "Unknown",
      isTyping: !!data.isTyping,
    });
  });

  // Chat delivery acknowledgement (→ double-tick on the sender's side)
  socket.on("chat-ack", (data) => {
    if (!isValidTarget(data)) return;
    const sender = activeUsers.get(socket.id);
    socket.to(data.to).emit("chat-ack", {
      from: socket.id,
      senderName: sender ? sender.name : "Unknown",
      msgId: data.msgId,
    });
  });

  // --- Screen sharing signaling (separate from file-transfer offer/answer/ice) ---
  socket.on("screen-offer", (data) => {
    if (!isValidTarget(data)) return;
    const sender = activeUsers.get(socket.id);
    socket.to(data.to).emit("screen-offer", {
      from: socket.id,
      senderName: sender ? sender.name : "Unknown",
      offer: data.offer,
    });
  });
  socket.on("screen-answer", (data) => {
    if (!isValidTarget(data)) return;
    socket.to(data.to).emit("screen-answer", { from: socket.id, answer: data.answer });
  });
  socket.on("screen-ice", (data) => {
    if (!isValidTarget(data)) return;
    socket.to(data.to).emit("screen-ice", { from: socket.id, candidate: data.candidate });
  });
  socket.on("screen-stop", (data) => {
    if (!isValidTarget(data)) return;
    socket.to(data.to).emit("screen-stop", { from: socket.id });
  });
  // Host registers a 6-digit PIN for their screen
  socket.on("screen-host", (data) => {
    if (!data || typeof data.pin !== "string") return;
    for (const [pin, id] of screenPins) if (id === socket.id) screenPins.delete(pin);
    screenPins.set(data.pin, socket.id);
  });
  socket.on("screen-unhost", () => {
    for (const [pin, id] of screenPins) if (id === socket.id) screenPins.delete(pin);
  });
  // Viewer asks to connect to a host's screen using their PIN
  socket.on("screen-connect", (data) => {
    if (!data || typeof data.pin !== "string") return;
    const hostId = screenPins.get(data.pin);
    if (!hostId || !activeUsers.has(hostId)) {
      socket.emit("screen-pin-invalid");
      return;
    }
    const viewer = activeUsers.get(socket.id);
    io.to(hostId).emit("screen-viewer", {
      from: socket.id,
      name: viewer ? viewer.name : "Someone",
    });
  });

  socket.on("disconnect", () => {
    console.log("🔴 Peer Disconnected:", socket.id);
    activeUsers.delete(socket.id);
    for (const [pin, id] of screenPins) if (id === socket.id) screenPins.delete(pin);
    io.emit("peer-disconnected", socket.id);
  });
});

// Port dynamically set hoga Render ke through, local me 3001
const port = process.env.PORT || 3001;
httpServer.listen(port, () => {
  console.log(`> 📡 Socket.io signaling server running on port ${port}`);
});