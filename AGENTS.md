# EdgeEye Image Reassembler

## Architecture

Two services communicate through **Redis** and receive LoRaWAN uplinks from Chirpstack v4 MQTT.

| Service | Language | Entrypoint | Role |
|---|---|---|---|---|
| `edgeeye_reassembler` | Python 3 | `post-processor/EdgeEye.py` | Image fragment reassembly |
| `mjpeg_streamer` | Node.js (ESM) | `mjpeg-streamer/app.js` | MJPEG HTTP streaming + composite + HTTP upload |

## Run

```bash
docker compose up -d --build
```

Required `.env` keys: `MQTT_URL`, `REDIS_URL`, `DEVICE_PROFILE_ID`

## Redis key conventions

| Pattern | Purpose |
|---|---|
| `PP:EdgeEye:*` | Reassembly state (buffer, state, missing blocks) |
| `ImageToRtsp:{devEui}:image` | Current in-progress image |
| `ImageToRtsp:{devEui}:image:last` | Last completed image |
| `ImageToRtsp:{devEui}:sense_time` | Sensor timestamp |
| `ImageToRtsp:{devEui}:det` | Latest detection data (set by Python on OBJDET) |
| `ImageToRtsp:{devEui}:det:last` | Detection data for last completed image |
| `ImageToRtsp:{devEui}:upload:ready` | Upload trigger metadata (set by Python, consumed by Node.js via GETDEL) |
| `EdgeEye:updated:{devEui}` | Pub/Sub channel (reassembly done notification) |

## Ports

| Service | Container | Host (default) |
|---|---|---|
| mjpeg_streamer | 8080 | 8083 |

## Protocol

### Uplink — Image Fragment (FPORT 1)

Header (9–12 bytes), immediately followed by JPEG data:

| Offset | Size | Field | Description |
|---|---|---|---|---|
| 0 | 1 | **Flags** | Bitmask: `0x01`=first frag, `0x02`=last frag, `0x04`=sysv present, `0x10`=multi-uplink active, `0x20`=object detection payload |
| 1–5 | 5 | **Reassembly ID** | `tv_sec & 0xFFFFFFFFFF` (LE) |
| 6–8 | 3 | **Offset / Size** | First frag = total size; subsequent = data offset (LE) |
| 9–10 | 2 | **System Voltage** | mV (LE). Only if FLAG_SYSV (0x04) |

Frame type: first and last fragments use **CONFIRMED**; middle fragments use **UNCONFIRMED**.

### Uplink — Object Detection (FPORT 1, FLAG_OBJDET)

When `FLAG_OBJDET` (0x20) is set (first fragment only), the payload contains object detection data instead of image data. Each object is 11 bytes:

| Offset | Size | Field | Description |
|---|---|---|---|
| 0–1 | 2 | x | Center X (0–65535 normalized) |
| 2–3 | 2 | y | Center Y (0–65535 normalized) |
| 4–5 | 2 | w | Width (0–65535 normalized) |
| 6–7 | 2 | h | Height (0–65535 normalized) |
| 8 | 1 | class | Class ID |
| 9–10 | 2 | score | Detection score (0–65535 normalized) |

This fragment does NOT advance the `received` reassembly cursor. Subsequent fragments carry image data as normal.

### Uplink — Failure Report (FPORT 2)

4 bytes: Category (1B), Sub-code (1B), System voltage (2B LE mV).

Categories: `FAIL_BOOT=0`, `FAIL_SNAP=1`, `FAIL_SEND=2`.

### Downlink — Fragment Retransmission (FPORT 4)

- **5 bytes** (Reassembly ID only) → ACK, reassembly confirmed.
- **8 bytes** (Reassembly ID + 3B offset LE) → single offset retransmission.
- **11 bytes** (Reassembly ID + 3B start + 3B end) → range retransmission.

### Downlink — Configuration (FPORT 3)

6 bytes LE: Period (2B), Width (2B signed, negative = hMirror), Height (2B signed, negative = vFlip).

### Downlink — Add Secondary Sessions (FPORT 5)

Adds up to 2 extra LoRaWAN sessions: Reassembly ID (5B) + Session0 DevAddr/NwkSKey/AppSKey (36B) + optional Session1 (36B).

### Port summary

| Port | Direction | Name |
|---|---|---|
| 1 | Up | Image fragment |
| 1 | Down | Reset (placeholder) |
| 2 | Up | Failure report |
| 2 | Down | Trigger snap (not implemented) |
| 3 | Down | Configuration (Period, Width, Height) |
| 4 | Down | Fragment retransmission request |
| 5 | Down | Add secondary sessions |
| 67 | Down | FUOTA firmware update |

## EdgeEye.py key behavior

- `asyncio` + `paho-mqtt` blocking callbacks bridged via `asyncio.run_coroutine_threadsafe()`
- Per-device `asyncio.Lock` ensures sequential processing
- Fragment reassembly: `redis SETRANGE` writes to buffer
- Gap detection → `missing_blocks` tracking → automatic retransmission request
- Downlink expiry: 60–70 seconds (random 0–10s offset)

## Build / Deploy

- Docker build only. No local npm/pip test runners, linters, or type checkers.
- `Dockerfile-reassembler` base: `python:3-alpine` (pip deps: redis, pillow, numpy, pyjwt, paho-mqtt)
- `Dockerfile-streamer` base: `node:22-alpine` (npm deps: commander, sharp, redis)
- MJPEG streamer requires `--experimental-json-modules` flag (in `package.json` `start` script)

## Caveats

- `.env` is `.gitignore`d. Update `.env.template` when adding new variables.
- `DEVICE_PROFILE_ID` is a Chirpstack Device Profile UUID. Use a dedicated EdgeEye profile to filter unrelated traffic.
- Streamer's `sharp` uses `failOn: 'none'` mode (renders partial JPEGs).
- Partial images are stored in Redis during reassembly, so the streamer shows real-time progress.
