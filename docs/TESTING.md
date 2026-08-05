# Testing

Install the isolated development dependencies and run the Python suite:

```bash
python -m pip install -r backend/requirements-dev.txt
python -m pytest
```

Automated coverage includes provider selection and fallback, Gemini events, local audio boundaries, pipeline timing, backend HTTP/WebSocket behavior, malformed input, concurrency, and standalone frontend source contracts.

Manual deployment acceptance covers wake detection, microphone endpointing, speaker playback, barge-in, Ollama/GPU behavior, Gemini billing/network limits, Raspberry Pi kiosk rendering, phone fullscreen/wake lock, and Thai/English switching.
