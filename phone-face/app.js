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
  let blinkTimer = null;
  let expressionTimer = null;
  let idleTimer = null;
  let gazeResetTimer = null;
  let backendExpressionUntil = 0;
  let tapExpressionIndex = 0;
  let blinkStep = 0;
  let behaviorStep = 0;
  let protocolFaults = 0;
  let shuttingDown = false;
  const motionPreference = window.matchMedia("(prefers-reduced-motion: reduce)");
  const expressions = ["neutral", "curious", "happy", "sleepy", "wink", "surprised", "focused", "excited", "music"];
  const tapExpressions = ["curious", "happy", "surprised", "excited", "wink"];
  const blinkSequence = [4200, 6100, 4800, 7200, 5300];
  const idleSequence = [
    { expression: "curious", gaze: [-0.58, -0.12], hold: 850, pause: 5200 },
    { expression: "curious", gaze: [0.58, -0.12], hold: 850, pause: 1600 },
    { expression: "neutral", gaze: [0, 0], hold: 700, pause: 6200 },
    { expression: "happy", gaze: [0, 0.12], hold: 1150, pause: 6800 },
    { expression: "sleepy", gaze: [0, 0.2], hold: 1350, pause: 7600 },
    { expression: "neutral", gaze: [0, 0], hold: 700, pause: 5600 }
  ];

  function clearTimer (timer) {
    if (timer) window.clearTimeout(timer);
    return null;
  }

  function setGaze (x = 0, y = 0) {
    face.style.setProperty("--gaze-x", Math.max(-1, Math.min(1, x)).toFixed(3));
    face.style.setProperty("--gaze-y", Math.max(-1, Math.min(1, y)).toFixed(3));
  }

  function setExpression (expression, duration = 0, source = "local") {
    const normalized = expressions.includes(expression) ? expression : "neutral";
    if (source !== "backend" && (face.dataset.state !== "sleeping" || Date.now() < backendExpressionUntil)) return;

    expressionTimer = clearTimer(expressionTimer);
    face.dataset.expression = normalized;
    if (source === "backend") backendExpressionUntil = Date.now() + duration;

    if (duration > 0) {
      expressionTimer = window.setTimeout(() => {
        face.dataset.expression = "neutral";
        expressionTimer = null;
        if (source !== "sequence") scheduleIdleExpression();
      }, duration);
    }
  }

  function scheduleBlink () {
    blinkTimer = clearTimer(blinkTimer);
    if (motionPreference.matches || document.hidden) return;
    const delay = blinkSequence[blinkStep % blinkSequence.length];
    const doubleBlink = blinkStep % blinkSequence.length === 3;
    blinkStep += 1;
    blinkTimer = window.setTimeout(() => {
      face.classList.add("is-blinking");
      window.setTimeout(() => face.classList.remove("is-blinking"), 115);
      if (doubleBlink) {
        window.setTimeout(() => {
          face.classList.add("is-blinking");
          window.setTimeout(() => face.classList.remove("is-blinking"), 105);
        }, 235);
      }
      scheduleBlink();
    }, delay);
  }

  function scheduleIdleExpression (delay = 4200) {
    idleTimer = clearTimer(idleTimer);
    if (motionPreference.matches || face.dataset.state !== "sleeping" || Date.now() < backendExpressionUntil) return;
    idleTimer = window.setTimeout(() => {
      const behavior = idleSequence[behaviorStep % idleSequence.length];
      behaviorStep += 1;
      setGaze(...behavior.gaze);
      setExpression(behavior.expression, behavior.hold, "sequence");
      gazeResetTimer = clearTimer(gazeResetTimer);
      gazeResetTimer = window.setTimeout(() => setGaze(), behavior.hold + 180);
      idleTimer = window.setTimeout(() => scheduleIdleExpression(), behavior.hold + behavior.pause);
    }, delay);
  }

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
    const readable = String(label || normalized).replace(/[\u0000-\u001f]/g, "").slice(0, 32) || normalized;
    statusLabel.textContent = readable.toUpperCase();
    face.setAttribute("aria-label", `Jarvis is ${readable}`);
    announcement.textContent = `Jarvis is ${readable}`;
    idleTimer = clearTimer(idleTimer);
    if (normalized === "sleeping") {
      scheduleIdleExpression();
    } else if (Date.now() >= backendExpressionUntil) {
      setExpression("neutral", 0, "backend");
      setGaze();
    }
  }

  function setConnected (connected) {
    connectionDot.classList.toggle("connected", connected);
    if (!connected) setState("disconnected", "offline");
    else if (face.dataset.state === "disconnected" || face.dataset.state === "error") setState("sleeping", "ready");
  }

  function reportDisplayError () {
    setState("error", "signal error");
  }

  function handleMessage (message) {
    if (!message || typeof message !== "object" || Array.isArray(message) || typeof message.type !== "string") return;

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
      face.style.setProperty("--talk", amplitude.toFixed(3));
      return;
    }

    if (message.type === "provider") {
      providerLabel.textContent = message.provider === "gemini-live" && message.status === "active" ? "CLOUD" : "LOCAL";
      return;
    }

    if (message.type === "face_cue") {
      const expression = expressions.includes(message.expression) ? message.expression : "neutral";
      setExpression(expression, expression === "music" ? 12000 : 3000, "backend");
      return;
    }

    if (message.type === "tts_cancel") setState("listening", "listening");
  }

  function stopHeartbeat () {
    if (heartbeatTimer) window.clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }

  function scheduleReconnect () {
    if (reconnectTimer || shuttingDown) return;
    reconnectTimer = window.setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 10000);
  }

  function connect () {
    if (shuttingDown || socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;
    try {
      socket = new WebSocket(websocketUrl());
    } catch {
      scheduleReconnect();
      return;
    }

    socket.addEventListener("open", () => {
      reconnectDelay = 1000;
      protocolFaults = 0;
      setConnected(true);
      stopHeartbeat();
      heartbeatTimer = window.setInterval(() => {
        if (socket?.readyState !== WebSocket.OPEN) return;
        try {
          socket.send(JSON.stringify({ action: "ping" }));
        } catch {
          socket.close();
        }
      }, 20000);
    });

    socket.addEventListener("message", (event) => {
      if (typeof event.data !== "string" || event.data.length > 131072) {
        protocolFaults += 1;
        if (protocolFaults >= 3) reportDisplayError();
        return;
      }
      try {
        handleMessage(JSON.parse(event.data));
        protocolFaults = 0;
      } catch {
        protocolFaults += 1;
        if (protocolFaults >= 3) reportDisplayError();
      }
    });

    socket.addEventListener("close", () => {
      stopHeartbeat();
      setConnected(false);
      socket = null;
      if (!shuttingDown) scheduleReconnect();
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

  function trackPointer (event) {
    if (motionPreference.matches || event.pointerType === "touch" && event.buttons === 0) return;
    const x = event.clientX / window.innerWidth * 2 - 1;
    const y = event.clientY / window.innerHeight * 2 - 1;
    setGaze(x, y);
    gazeResetTimer = clearTimer(gazeResetTimer);
  }

  function releasePointer () {
    gazeResetTimer = clearTimer(gazeResetTimer);
    gazeResetTimer = window.setTimeout(() => setGaze(), 650);
  }

  function reactToTap (event) {
    if (event.target.closest("button") || face.dataset.state !== "sleeping") return;
    const expression = tapExpressions[tapExpressionIndex % tapExpressions.length];
    tapExpressionIndex += 1;
    const x = event.clientX / window.innerWidth * 2 - 1;
    const y = event.clientY / window.innerHeight * 2 - 1;
    setGaze(x, y);
    setExpression(expression, expression === "excited" ? 1500 : 900);
    releasePointer();
  }

  fullscreenButton.addEventListener("click", toggleFullscreen);
  face.addEventListener("pointermove", trackPointer);
  face.addEventListener("pointerup", releasePointer);
  face.addEventListener("pointercancel", releasePointer);
  face.addEventListener("click", reactToTap);
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      keepScreenAwake();
      scheduleBlink();
      scheduleIdleExpression();
    } else {
      blinkTimer = clearTimer(blinkTimer);
      idleTimer = clearTimer(idleTimer);
    }
  });
  const handleMotionChange = () => {
    scheduleBlink();
    scheduleIdleExpression();
    if (motionPreference.matches) {
      face.classList.remove("is-blinking");
      setGaze();
      setExpression("neutral", 0, "backend");
    }
  };
  if (typeof motionPreference.addEventListener === "function") motionPreference.addEventListener("change", handleMotionChange);
  else motionPreference.addListener(handleMotionChange);

  window.addEventListener("error", reportDisplayError);
  window.addEventListener("unhandledrejection", (event) => {
    event.preventDefault();
    reportDisplayError();
  });
  window.addEventListener("pagehide", () => {
    shuttingDown = true;
    reconnectTimer = clearTimer(reconnectTimer);
    blinkTimer = clearTimer(blinkTimer);
    idleTimer = clearTimer(idleTimer);
    expressionTimer = clearTimer(expressionTimer);
    gazeResetTimer = clearTimer(gazeResetTimer);
    stopHeartbeat();
    socket?.close(1000, "page hidden");
  });

  // Camera and face recognition are intentionally not requested in this release.
  // A future camera provider can publish separate vision events without changing this face protocol.
  face.dataset.expression = "neutral";
  scheduleBlink();
  scheduleIdleExpression();
  connect();
  keepScreenAwake();
})();
