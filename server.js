const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;
const HOST = process.env.HOST || "0.0.0.0"; // Ensures it works on EC2

const mainRoutes = require("./routes/masterDataRoutes");

// CORS setup – allows local dev and production frontend
const allowedOrigins = [
  "http://localhost:5173",
  process.env.FRONTEND_URL // Optional: set this in .env for production
];

app.use(cors({
  origin: function (origin, callback) {
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error("Not allowed by CORS"));
    }
  }
}));

app.use(express.json());

// Routes
app.use("/api", mainRoutes);

// Health check
app.get("/", (req, res) => {
  res.send("✅ Backend API is running");
});

// Start server
app.listen(PORT, HOST, () => {
  console.log(`🚀 Server is running on http://${HOST}:${PORT}`);
});