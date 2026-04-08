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
   - `TZ`: Local timezone (e.g., `Asia/Seoul`).

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

### MJPEG Streaming
- **Live Reassembly**: `http://[SERVER_IP]:8080/[DevEUI]/live`
- **Last Completed**: `http://[SERVER_IP]:8080/[DevEUI]/last`

### Automatic Upload
If `UPLOAD_URL` is set, the system performs a `multipart/form-data` POST request when reassembly is complete. The request follows this structure:

- `snap`: JPEG binary file (filename: `image.jpg`, content-type: `image/jpeg`).
- `deviceId`: DevEUI of the device.
- `_timestamp`: ISO 8601 timestamp string (UTC).
- `data`: JSON string containing sensor data:
    - `system_voltage`: System voltage in Volts (typically present).
    - `ambient_light_lux`: Ambient light level in Lux (optional).

## License

Copyright (c) 2026 CoXlab Inc. All rights reserved.
