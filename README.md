# Jarvis Smart Mirror

Jarvis Smart Mirror is a standalone conversational display system. It is not a MagicMirror fork and has no MagicMirror runtime dependency.

The system has three synchronized devices:

- **MSI laptop:** authoritative FastAPI backend, local wake word, microphone and speaker, Gemini Live or Whisper/Ollama/Edge-TTS fallback, knowledge graph, and all HTTP/WebSocket services.
- **Raspberry Pi:** thin Chromium kiosk rendering the standalone `/mirror` web application.
- **Phone:** thin Chromium/Safari client rendering the expressive `/face` robot display.

The Pi and phone perform no AI inference and contain no API credentials.

## Features

- Local OpenWakeWord activation with no pre-wake cloud audio.
- Opt-in Gemini Live multilingual voice conversations with automatic local fallback.
- Standalone responsive mirror UI with ambient, conversation, music, and night scenes.
- Temporary result cards, persistent timers, media state, presence profiles, topology faults, and rolling provider latency.
- OLED robot face synchronized to voice amplitude and allowlisted timer/media expressions.
- SQLite knowledge graph and pipeline diagnostics dashboard.
- No frontend build step, Node.js server, MagicMirror module API, or Electron runtime.

## MSI setup

Python 3.11 or 3.12, FFmpeg, Ollama, a microphone, and speakers are recommended.

Windows:

```powershell
.\setup.ps1
notepad .env
.\start_jarvis.ps1
```

Linux:

```bash
chmod +x setup.sh start_jarvis.sh
./setup.sh
${EDITOR:-nano} .env
./start_jarvis.sh
```

Open these routes after startup:

- Mirror: `http://<MSI-IP>:8000/mirror`
- Phone face: `http://<MSI-IP>:8000/face`
- Diagnostics: `http://<MSI-IP>:8000/dashboard`
- Health: `http://<MSI-IP>:8000/health`

The committed [.env.sample](.env.sample) contains no credentials. Keep `.env` private.

## Raspberry Pi kiosk

Install Chromium, then run the kiosk installer from a clone of this repository:

```bash
export JARVIS_MIRROR_URL=http://<MSI-IP>:8000/mirror
chmod +x deploy/pi/install-kiosk.sh
./deploy/pi/install-kiosk.sh
```

The installer creates a user-level `jarvis-mirror-kiosk.service`. The Pi only opens the MSI-hosted frontend, so UI updates arrive after restarting or refreshing Chromium; no duplicate application server is installed on the Pi.

Useful commands:

```bash
systemctl --user status jarvis-mirror-kiosk
systemctl --user restart jarvis-mirror-kiosk
journalctl --user -u jarvis-mirror-kiosk -f
```

## Phone face

Open `http://<MSI-IP>:8000/face`, enter fullscreen, and optionally add it to the home screen. The current client does not request camera permission. Camera recognition and video streaming remain a future explicit opt-in capability.

Wake Lock, installable fullscreen behavior, and future camera access generally require HTTPS. Trusted-LAN HTTP remains sufficient for basic display operation.

## Configuration

Mirror content is configured on the MSI through environment variables:

| Variable | Purpose |
| --- | --- |
| `JARVIS_WEATHER_LATITUDE`, `JARVIS_WEATHER_LONGITUDE` | Open-Meteo weather location. |
| `JARVIS_LOCATION_NAME` | Mirror location label. |
| `JARVIS_NEWS_FEED_URL` | RSS feed fetched by the backend with size/time limits. |
| `JARVIS_CALENDAR_URL` | ICS calendar fetched by the backend with size/time limits. |
| `JARVIS_NIGHT_START_HOUR`, `JARVIS_NIGHT_END_HOUR` | Minimal night-scene schedule. |
| `JARVIS_MIRROR_LOCALE` | Clock and date locale. |

Gemini Live is explicitly enabled with:

```text
JARVIS_CONVERSATION_PROVIDER=gemini-live
GEMINI_API_KEY=<server-side-key>
```

Gemini receives audio only after local wake detection. Raw session audio is not stored. Removing the key or selecting `local` keeps the entire conversation pipeline local.

## Development and testing

Install development dependencies separately:

```bash
python -m pip install -r backend/requirements-dev.txt
python -m pytest
```

Tests cover backend APIs, pipeline transitions, provider fallback, phone/mirror source contracts, and voice scenarios. Physical microphone, speaker, GPU, mirror, phone, and Gemini billing acceptance remain manual hardware checks.

See [PROJECT.md](PROJECT.md) for event contracts and architecture details.
