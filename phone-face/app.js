(() => {
  "use strict";

  const face = document.getElementById("face");
  const statusLabel = document.getElementById("statusLabel");
  const providerLabel = document.getElementById("providerLabel");
  const connectionDot = document.getElementById("connectionDot");
  const announcement = document.getElementById("announcement");
  const fullscreenButton = document.getElementById("fullscreenButton");

  const stateFromStatus = {
    connected: "sleeping",
    idle: "sleeping",
    "listening...": "listening",
    "transcribing...": "thinking",
    "thinking...": "thinking",
    "speaking...": "speaking",
    disconnected: "disconnected",
    error: "error"
  };

  let socket = null;
  let reconnectTimer = null;
  let heartbeatTimer = null;
  let reconnectDelay = 1000;
  let wakeLock = null;

  function websocketUrl () {
    const configured = new URLSearchParams(window.location.search).get("ws");
    if (configured && /^wss?:\/\//i.test(configured)) {
      const url = new URL(configured);
      url.searchParams.set("client", "phone");
      return url.toString();
    }
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${protocol}//${window.location.host}/ws?client=phone`;
  }

  function setState (state, label) {
    const normalized = ["sleeping", "listening", "thinking", "speaking", "error", "disconnected"].includes(state)
      ? state
      : "sleeping";
    face.dataset.state = normalized;
    const readable = label || normalized;
    statusLabel.textContent = readable.toUpperCase();
    face.setAttribute("aria-label", `Jarvis is ${readable}`);
    announcement.textContent = `Jarvis is ${readable}`;
  }

  function setConnected (connected) {
    connectionDot.classList.toggle("connected", connected);
    if (!connected) setState("disconnected", "offline");
  }

  function handleMessage (message) {
    if (!message || typeof message !== "object") return;

    if (message.type === "state") {
      const state = message.state === "listening-followup" ? "listening" : String(message.state || "sleeping");
      setState(state);
      return;
    }

    if (message.type === "status") {
      const status = String(message.data || "idle").toLowerCase();
      setState(stateFromStatus[status] || (status.startsWith("error") ? "error" : "sleeping"), status.replace(/\.+$/, ""));
      return;
    }

    if (message.type === "amplitude") {
      const amplitude = Math.max(0, Math.min(1, Number(message.value) || 0));
      face.style.setProperty("--amplitude-scale", (1 + amplitude * 0.2).toFixed(3));
      return;
    }

    if (message.type === "provider") {
      providerLabel.textContent = message.provider === "gemini-live" && message.status === "active" ? "CLOUD" : "LOCAL";
      return;
    }

    if (message.type === "face_cue") {
      const expression = ["neutral", "focused", "excited", "music"].includes(message.expression) ? message.expression : "neutral";
      face.dataset.expression = expression;
      window.setTimeout(() => {
        if (face.dataset.expression === expression) face.dataset.expression = "neutral";
      }, expression === "music" ? 12000 : 3000);
      return;
    }

    if (message.type === "tts_cancel") setState("listening", "listening");
  }

  function stopHeartbeat () {
    if (heartbeatTimer) window.clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }

  function scheduleReconnect () {
    if (reconnectTimer) return;
    reconnectTimer = window.setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 10000);
  }

  function connect () {
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;
    try {
      socket = new WebSocket(websocketUrl());
    } catch {
      scheduleReconnect();
      return;
    }

    socket.addEventListener("open", () => {
      reconnectDelay = 1000;
      setConnected(true);
      stopHeartbeat();
      heartbeatTimer = window.setInterval(() => {
        if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ action: "ping" }));
      }, 20000);
    });

    socket.addEventListener("message", (event) => {
      try {
        handleMessage(JSON.parse(event.data));
      } catch {
        // Ignore malformed server frames without disturbing the face display.
      }
    });

    socket.addEventListener("close", () => {
      stopHeartbeat();
      setConnected(false);
      socket = null;
      scheduleReconnect();
    });

    socket.addEventListener("error", () => socket?.close());
  }

  async function keepScreenAwake () {
    if (!("wakeLock" in navigator) || document.visibilityState !== "visible") return;
    try {
      wakeLock = await navigator.wakeLock.request("screen");
      wakeLock.addEventListener("release", () => {
        wakeLock = null;
      });
    } catch {
      wakeLock = null;
    }
  }

  async function toggleFullscreen () {
    try {
      if (!document.fullscreenElement) await document.documentElement.requestFullscreen();
      else await document.exitFullscreen();
    } catch {
      // Mobile browsers may require installation as a home-screen app for fullscreen.
    }
    await keepScreenAwake();
  }

  fullscreenButton.addEventListener("click", toggleFullscreen);
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") keepScreenAwake();
  });

  // Camera and face recognition are intentionally not requested in this release.
  // A future camera provider can publish separate vision events without changing this face protocol.
  connect();
  keepScreenAwake();
})();
