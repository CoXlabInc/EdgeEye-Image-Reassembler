# EdgeEye Image Reassembler

This project is a reference bridge for integrating [EdgeEye](https://www.coxlab.kr/products/edgeeye/) (a LoRaWAN camera) with [Chirpstack](https://www.chirpstack.io/) v4. It reassembles fragmented image data received via Chirpstack MQTT, provides MJPEG streaming, and supports automatic image uploads.

## Features

- **EdgeEye Integration**: Implements the communication protocol for EdgeEye LoRaWAN cameras.
- **Chirpstack v4 Support**: Connects to Chirpstack MQTT and filters traffic by Device Profile ID.
- **Image Reassembly**: Reconstructs fragmented data into complete JPEG images.
- **MJPEG Streaming**: Provides real-time visual progress and the latest completed images via HTTP.
- **Automatic Upload**: Optionally POSTs reassembled images to a remote HTTP endpoint.

## Prerequisites

- Docker and Docker Compose
- Chirpstack v4 (MQTT Broker)
- Redis (included in Docker Compose)

## Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/CoXlabInc/EdgeEye-Image-Reassembler.git
   cd EdgeEye-Image-Reassembler
   ```

2. **Configure environment variables**:
   Copy the template file and create your `.env` file:
   ```bash
   cp .env.template .env
   ```
   
   Edit `.env` with your settings:
   - `MQTT_URL`: Chirpstack MQTT broker URL.
   - `MQTT_USER`: (Optional) MQTT username for authentication.
   - `MQTT_PASS`: (Optional) MQTT password for authentication.
   - `DEVICE_PROFILE_ID`: EdgeEye device profile UUID from Chirpstack (Required).
     - **Note**: This bridge uses the Device Profile ID to identify EdgeEye packets. It is **highly recommended** to create and use a dedicated Device Profile for EdgeEye devices to avoid processing unrelated traffic.
   - `UPLOAD_URL`: (Optional) Remote HTTP endpoint for image uploads.
   - `UPLOAD_HEADERS`: (Optional) JSON string of HTTP headers for uploads (e.g., `{"X-API-Key": "your-token"}`).
   - `DET_UPLOAD_MODE`: (Optional) How to upload object detection data. `1`=included with snap, `2`=det first then snap (default), `3`=det first then snap alone (without det).
   - `UPLOAD_OVERLAY`: (Optional) Comma-separated overlay types for the upload snap. `timestamp,bbox` (default) includes both; `timestamp` for timestamp only; `bbox` for bbox only; `none` for raw JPEG.
   - `TZ`: Local timezone (e.g., `Asia/Seoul`).
   - `LANG`: Locale (e.g., `ko_KR.UTF-8`).

## Execution

Start the system using Docker Compose:

```bash
docker-compose up -d --build
```

Services:
- `redis`: State storage and messaging.
- `mjpeg_streamer`: HTTP MJPEG server (Port 8080).
- `edgeeye_reassembler`: Image reassembly engine.

## Usage

### HTTP API

MJPEG streamer is built on raw `http.createServer` and handles all HTTP methods (GET, POST, etc.) the same way.

| Endpoint | Description |
|---|---|
| `GET /{devEui}` | Live reassembly image streaming / snapshot |
| `GET /{devEui}/last` | Last completed image streaming / snapshot |

**Query Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mjpeg` | `"true"` / `"false"` | `"true"` | Returns a single JPEG instead of MJPEG stream |
| `det` | `"true"` / omitted | omitted | Enables detection bbox overlay |

**Examples:**

| URL | Description |
|---|---|
| `http://host:8083/{devEui}` | MJPEG live streaming |
| `http://host:8083/{devEui}?mjpeg=false` | Single JPEG of in-progress image |
| `http://host:8083/{devEui}?det=true` | MJPEG + bbox overlay |
| `http://host:8083/{devEui}?mjpeg=false&det=true` | Single JPEG + bbox overlay |
| `http://host:8083/{devEui}/last` | Last completed image MJPEG stream |
| `http://host:8083/{devEui}/last?mjpeg=false` | Last completed image as JPEG |
| `http://host:8083/{devEui}/last?det=true` | Last completed MJPEG + bbox |

**Validation:**

| Condition | Response |
|---|---|
| `{devEui}` is not a 16-character hex string | `400 Invalid device EUI` |
| No available image for `{devEui}` | `404 Device not found` |
| Undefined route | `404 Not Found` |

**Port:** `8080` (container). See `docker-compose.yml` for host mapping.

### Automatic Upload
If `UPLOAD_URL` is set, the system performs a `multipart/form-data` POST request when reassembly is complete. The uploaded `snap` is a composed JPEG with timestamp overlay (and bbox overlay if available).

**Upload fields:**
- `snap`: Composed JPEG binary (filename: `image.jpg`, content-type: `image/jpeg`).
- `deviceId`: DevEUI of the device.
- `_timestamp`: ISO 8601 timestamp string (UTC/local).
- `data`: JSON string containing sensor data:
    - `system_voltage`: System voltage in Volts (typically present).
    - `ambient_light_lux`: Ambient light level in Lux (optional).
    - `det`: Object detection list (optional, mode-dependent).

**`DET_UPLOAD_MODE` behavior:**
- **1**: Detection data is included in the `data` field of a single upload with `snap`.
- **2** (default): Detection data is uploaded first (no `snap`). When reassembly completes, `snap` (with bbox overlay) + detection data is uploaded again.
- **3**: Detection data is uploaded first (no `snap`). When reassembly completes, `snap` without detection data is uploaded separately.

## License

Copyright (c) 2026 CoXlab Inc. All rights reserved.
