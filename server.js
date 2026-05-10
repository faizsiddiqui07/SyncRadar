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
      origin: ["http://localhost:3000", "https://syncradar.netlify.app"], 
      methods: ["GET", "POST"] 
    }
  });

const activeUsers = new Map();

io.on("connection", (socket) => {
  console.log("🟢 Peer Connected:", socket.id);

  socket.on("user-joined", (userData) => {
    const newUser = { socketId: socket.id, ...userData };
    activeUsers.set(socket.id, newUser);

    const existingUsers = Array.from(activeUsers.values()).filter(user => user.socketId !== socket.id);
    socket.emit("all-peers", existingUsers);
    socket.broadcast.emit("new-peer-detected", newUser);
  });

  socket.on("file-request", (data) => socket.to(data.to).emit("file-request", data));
  socket.on("file-response", (data) => socket.to(data.to).emit("file-response", data));
  socket.on("offer", (data) => socket.to(data.to).emit("offer", { from: socket.id, offer: data.offer }));
  socket.on("answer", (data) => socket.to(data.to).emit("answer", { from: socket.id, answer: data.answer }));
  socket.on("ice-candidate", (data) => socket.to(data.to).emit("ice-candidate", { from: socket.id, candidate: data.candidate }));

  socket.on("disconnect", () => {
    console.log("🔴 Peer Disconnected:", socket.id);
    activeUsers.delete(socket.id);
    io.emit("peer-disconnected", socket.id);
  });
});

// Port dynamically set hoga Render ke through, local me 3001
const port = process.env.PORT || 3001;
httpServer.listen(port, () => {
  console.log(`> 📡 Socket.io signaling server running on port ${port}`);
});