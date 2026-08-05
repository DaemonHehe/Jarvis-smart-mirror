# Architecture

Jarvis Smart Mirror is a standalone three-device application. See [PROJECT.md](../PROJECT.md) for the authoritative topology, frontend responsibilities, WebSocket protocol, security model, and deployment boundary.

The repository intentionally contains no framework or desktop-shell runtime. `mirror-ui/`, `phone-face/`, and `dashboard/` are static clients served by `backend/bridge_api.py`.
