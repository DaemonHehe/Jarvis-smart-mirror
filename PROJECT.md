# Jarvis Smart Mirror Architecture

## Product boundary

This repository owns the complete Jarvis application. It does not vendor or execute MagicMirror, Electron, or third-party MagicMirror modules.

```text
MSI laptop
  OpenWakeWord -> Gemini Live
               -> Whisper -> Ollama -> Edge-TTS fallback
  FastAPI :8000
    /mirror      standalone Pi UI
    /face        standalone phone UI
    /dashboard   diagnostics UI
    /ws          normalized event stream
    /api/*       health, content, pipeline, and knowledge APIs

Raspberry Pi                 Phone
  Chromium kiosk               Mobile browser/PWA
  GET /mirror                  GET /face
  one WebSocket                one WebSocket
```

The MSI is authoritative for conversation and persistent data. Frontends retain only presentation state and local display preferences. The Pi retains timer countdowns in browser storage so a display refresh does not discard an active visual timer.

## Standalone mirror frontend

`mirror-ui/` implements the former module concepts directly:

- **Scenes:** ambient, conversation, music, and scheduled night layouts.
- **Cards:** bounded temporary weather and structured result cards.
- **Topology:** fault-first MSI, Pi, phone, Gemini, Ollama, microphone, and speaker status.
- **Timers:** persistent countdowns, structured recurring events, and English/Thai voice parsing.
- **Media:** normalized provider-neutral now-playing display.
- **Presence:** profile display from sanitized presence events; no camera API.
- **Metrics:** bounded local/Gemini samples with rolling p50 and p95.

The frontend has no package manager or compilation step. It uses semantic HTML, CSS, and browser JavaScript served by FastAPI.

## WebSocket events

Server-to-client messages preserve the normalized JSON envelope:

- `status`: `{ "type": "status", "data": "Listening..." }`
- `state`: `{ "type": "state", "state": "listening", "timestamp": 0 }`
- `amplitude`: `{ "type": "amplitude", "value": 0.0, "source": "mic", "timestamp": 0 }`
- `transcript`: `{ "type": "transcript", "role": "user", "text": "...", "is_final": true }`
- `token`: incremental local model output.
- `response`: final user and assistant text.
- `pipeline_state`: cycle ID, stages, outcomes, and available timing fields.
- `latency`: stage and total metrics; absent transitions remain `null`.
- `provider`: cloud/local state with sanitized reason categories.
- `timer`, `card`, `media`, `presence`: structured presentation events.
- `face_cue`: allowlisted non-sensitive expression synchronization.

Clients may send `trigger`, `barge_in`, `stop`, `cancel`, and `ping`. The bounded `client_event` relay accepts only `face_cue`; arbitrary client broadcasts are rejected.

## Security and privacy

- Credentials exist only in the MSI process environment.
- Local wake-word detection gates all cloud microphone streaming.
- RSS and ICS URLs come from server configuration, not request parameters, and responses are size/time bounded.
- WebSocket input has a configurable size limit and stable error codes.
- Routine logs exclude transcript content and secrets.
- Camera capture is absent from both current frontends.
- Use TLS and firewall restrictions outside a trusted LAN.

## Deployment

The MSI launches one Python process with `start_jarvis.ps1` or `start_jarvis.sh`. The Pi user service launches Chromium in kiosk mode against the MSI `/mirror` route. The phone opens `/face`. This avoids application synchronization, duplicate sockets on one frontend, and framework-version coupling.
