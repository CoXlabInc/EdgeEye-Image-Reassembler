import base64
import json
import io
import sys
import asyncio
import threading
import traceback
import argparse
import aiohttp
import random
from datetime import datetime, timezone
from urllib.parse import urlparse

import redis.asyncio as redis
import paho.mqtt.client as mqtt
from PIL import Image, ImageFile

ImageFile.LOAD_TRUNCATED_IMAGES = True

class ImageReassembler:
    def __init__(self, mqtt_info, redis_url, device_profile_id, upload_url=None, upload_headers=None):
        self.mqtt_info = mqtt_info
        self.redis_url = redis_url
        self.device_profile_id = device_profile_id
        self.upload_url = upload_url
        self.upload_headers = upload_headers if upload_headers else {}
        
        self.pool = None
        self.event_loop = None
        self.mqtt_client = None
        self.device_locks = {} # Per-device locks to ensure sequential processing
        
    def start(self):
        """Initialize and start the processor"""
        if not self.redis_url:
            print("Redis URL is required.")
            return None

        self.pool = redis.ConnectionPool.from_url(self.redis_url)

        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)
        
        def run_loop():
            self.event_loop.run_forever()
        threading.Thread(target=run_loop, daemon=True).start()

        url_parsed = urlparse(self.mqtt_info['url'])
        host = url_parsed.hostname
        port = url_parsed.port or 1883
        
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if self.mqtt_info.get('user') and self.mqtt_info.get('pass'):
            self.mqtt_client.username_pw_set(self.mqtt_info['user'], self.mqtt_info['pass'])
            
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_message = self._on_mqtt_message
        
        print(f"Connecting to MQTT Broker: {host}:{port}...")
        self.mqtt_client.connect(host, port, 60)
        return self.mqtt_client

    def loop_forever(self):
        if self.mqtt_client:
            self.mqtt_client.loop_forever()

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        print(f"Connected to MQTT Broker (Reason Code: {reason_code})")
        client.subscribe("application/+/device/+/event/up")

    def _on_mqtt_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
            device_info = data.get('deviceInfo', {})
            
            if device_info.get('deviceProfileId') != self.device_profile_id:
                return

            dev_eui = device_info.get('devEui')
            f_port = data.get('fPort')
            raw_b64 = data.get('data')
            
            if not raw_b64:
                return

            message = {
                'dev_eui': dev_eui,
                'app_id': device_info.get('applicationId', 'default'),
                'f_port': f_port,
                'f_cnt': data.get('fCnt'),
                'raw': base64.b64decode(raw_b64)
            }
            
            asyncio.run_coroutine_threadsafe(self._process_uplink(message), self.event_loop)
            
        except Exception as e:
            print(f"Error handling MQTT message: {e}")

    async def _send_downlink(self, app_id, dev_eui, f_port, payload, confirmed=False):
        topic = f"application/{app_id}/device/{dev_eui}/command/down"

        # Calculate expiry time (60-70 seconds from now) with random offset (0-10s) including milliseconds
        from datetime import timedelta
        random_offset = random.uniform(0, 10)
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=60 + random_offset)).isoformat()

        downlink = {
            "devEui": dev_eui,
            "confirmed": confirmed,
            "fPort": f_port,
            "data": base64.b64encode(payload).decode('utf-8'),
            "expiresAt": expires_at
        }
        self.mqtt_client.publish(topic, json.dumps(downlink))
        print(f"[{dev_eui}] Downlink sent (FPort {f_port}, Expires: {expires_at}): {payload.hex()}")
    async def _process_uplink(self, msg):
        dev_eui = msg['dev_eui']
        
        # Get or create a lock for this specific device to handle packets sequentially
        if dev_eui not in self.device_locks:
            self.device_locks[dev_eui] = asyncio.Lock()
        
        async with self.device_locks[dev_eui]:
            try:
                r = redis.Redis(connection_pool=self.pool)
                if msg['f_port'] == 2:
                    self._handle_fail_report(dev_eui, msg['raw'])
                elif msg['f_port'] == 1:
                    await self._handle_image_fragment(r, msg)
                await r.aclose()
            except Exception:
                traceback.print_exc()

    def _handle_fail_report(self, dev_eui, raw):
        errors = {0: "Camera boot failed", 1: "Camera snap failed", 2: "Send failed"}
        error_msg = errors.get(raw[0], f"Unknown fail ({raw[0]})")
        print(f"[{dev_eui}] Device Error: {error_msg}")

    async def _handle_image_fragment(self, r, msg):
        dev_eui = msg['dev_eui']
        app_id = msg['app_id']
        raw = msg['raw']
        
        if len(raw) < 9:
            return

        flags = raw[0]
        epoch = int.from_bytes(raw[1:6], 'little')
        offset = int.from_bytes(raw[6:9], 'little')
        
        i = 9
        sysv = None
        if (flags & (1 << 2)):
            sysv = int.from_bytes(raw[i:i+2], 'little') / 1000.0
            i += 2
        
        als = None
        if (flags & (1 << 3)):
            als = int.from_bytes(raw[i:i+3], 'little')
            i += 3
        
        first_frag = bool(flags & (1 << 0))
        last_frag = bool(flags & (1 << 1))
        frag_data = raw[i:]
        sense_time = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace('+00:00', 'Z')

        prefix = f"PP:EdgeEye"
        active_epoch_key = f"{prefix}:active_epoch:{dev_eui}"
        completed_key = f"{prefix}:completed:{dev_eui}:{epoch}"
        buffer_key = f"{prefix}:buffer:{dev_eui}:{epoch}"
        state_key = f"{prefix}:state:{dev_eui}:{epoch}"
        missing_key = f"{prefix}:missing:{dev_eui}:{epoch}"
        last_dl_key = f"{prefix}:last_dl:{dev_eui}:{epoch}"

        if await r.exists(completed_key):
            if not await r.exists(last_dl_key):
                await self._send_downlink(app_id, dev_eui, 4, epoch.to_bytes(5, 'little'))
                await r.set(last_dl_key, 1, ex=10)
            return

        active_epoch = await r.get(active_epoch_key)
        active_epoch = int(active_epoch) if active_epoch else 0
        if epoch < active_epoch:
            return
        elif epoch > active_epoch:
            print(f"[{dev_eui}] New image detected: 0x{epoch:08X} (replacing 0x{active_epoch:08X})")
            await r.set(active_epoch_key, epoch, ex=86400)

        # Removed: if len(frag_data) == 0 and not first_frag: return
        
        state = await r.get(state_key)
        state = json.loads(state) if state else {'received': 0, 'total_size': None, 'meta': []}
        if 'meta' not in state: state['meta'] = []
        
        if sysv is not None: state['system_voltage'] = sysv
        if als is not None: state['ambient_light_lux'] = als
        
        if first_frag:
            state['total_size'] = offset
            offset = 0
        elif state['total_size'] is None:
            if not await r.exists(last_dl_key):
                print(f"[{dev_eui}:{sense_time}] Missing first fragment (Epoch: 0x{epoch:08X}). Requesting...")
                await self._send_downlink(app_id, dev_eui, 4, raw[1:6] + b'\x00\x00\x00')
                await r.set(last_dl_key, 1, ex=10)
            return

        missing_raw = await r.get(missing_key)
        missing_blocks = json.loads(missing_raw) if missing_raw else []
        
        new_offset_next = await self._apply_fragment(r, buffer_key, offset, frag_data, state['received'], missing_blocks)
        # Send gap request if we have missing blocks and not throttled
        # Verification packets (empty) can trigger a downlink every 5 seconds to speed up final reassembly.
        is_verification = (len(frag_data) == 0)
        throttle_ttl = await r.ttl(last_dl_key)
        
        if missing_blocks:
            reassembled_offset = min(b[0] for b in missing_blocks)
            await r.set(missing_key, json.dumps(missing_blocks), ex=86400)

            # Allow if: 
            # 1. No throttle exists (ttl < 0)
            # 2. OR it's a verification packet and at least 5s have passed since last downlink (ttl < 55 assuming 60s base)
            if throttle_ttl < 0 or (is_verification and throttle_ttl < 55):
                m = min(missing_blocks, key=lambda x: x[0])
                print(f"[{dev_eui}:{sense_time}] Packet loss! Missing: {m[0]}~{m[1]} (Epoch: 0x{epoch:08X})")
                req = raw[1:6] + m[0].to_bytes(3, 'little') + m[1].to_bytes(3, 'little')
                await self._send_downlink(app_id, dev_eui, 4, req)
                await r.set(last_dl_key, 1, ex=60)
        else:
            await r.delete(missing_key)

        state['received'] = new_offset_next
        state['meta'].append({'fCnt': msg['f_cnt'], 'ts': datetime.now(timezone.utc).isoformat()})
        await r.set(state_key, json.dumps(state), ex=3600)

        total = state['total_size']
        percent = (reassembled_offset / total * 100) if total > 0 else 0
        print(f"[{dev_eui}:{sense_time}] Progress: {reassembled_offset}/{total} ({percent:.2f}%) +{len(frag_data)}B (fCnt:{msg['f_cnt']})")

        # Always check for finalization to allow empty verification packets to finish the image
        await self._finalize_image(r, dev_eui, app_id, epoch, sense_time, buffer_key, 
                                   reassembled_offset, total, last_frag, completed_key, state_key, state)

    async def _apply_fragment(self, r, key, offset, data, received, missing_blocks):
        offset_end = offset + len(data)

        # Gap detection from the header's offset
        if offset > received:
            if [received, offset] not in missing_blocks:
                missing_blocks.append([received, offset])

        if len(data) == 0:
            return received

        if offset < received:
            new_missing = []
            for b in missing_blocks:
                if offset <= b[0] and offset_end >= b[1]: continue
                if offset <= b[0] and b[0] < offset_end < b[1]: b[0] = offset_end
                elif b[0] < offset < b[1] <= offset_end: b[1] = offset
                elif b[0] < offset and offset_end < b[1]:
                    new_missing.append([offset_end, b[1]])
                    b[1] = offset
                new_missing.append(b)
            missing_blocks[:] = new_missing

        return int(await r.setrange(key, offset, data))

    async def _finalize_image(self, r, dev_eui, app_id, epoch, sense_time, buffer_key, 
                              reassembled_len, total_size, is_last, completed_key, state_key, state):
        rtsp_base = f"ImageToRtsp:{dev_eui}"
        
        img_data = await r.get(buffer_key)
        if not img_data: return
        
        img_data = img_data[:reassembled_len]
        try:
            img = Image.open(io.BytesIO(img_data))
            with io.BytesIO() as output:
                img.save(output, format="JPEG")
                jpeg_bytes = output.getvalue()
            
            await r.set(f"{rtsp_base}:image", jpeg_bytes, ex=86400)
            await r.set(f"{rtsp_base}:sense_time", sense_time, ex=86400)
            
            # Notify streamers
            await r.publish(f"EdgeEye:updated:{dev_eui}", "updated")

            if total_size and reassembled_len >= total_size:
                print(f"[{dev_eui}] Reassembly complete! {len(jpeg_bytes)} bytes")
                await r.set(f"{rtsp_base}:image:last", jpeg_bytes, ex=86400)
                await r.set(f"{rtsp_base}:sense_time:last", sense_time, ex=86400)
                await r.set(completed_key, 1, ex=86400)
                await self._send_downlink(app_id, dev_eui, 4, epoch.to_bytes(5, 'little'))
                await r.delete(buffer_key)
                await r.delete(state_key)
                
                # Perform HTTP upload if configured
                if self.upload_url:
                    data_extra = {}
                    if 'system_voltage' in state: data_extra['system_voltage'] = state['system_voltage']
                    if 'ambient_light_lux' in state: data_extra['ambient_light_lux'] = state['ambient_light_lux']
                    asyncio.create_task(self._upload_image(dev_eui, jpeg_bytes, sense_time, data_extra))
                
        except Exception as e:
            if is_last:
                print(f"[{dev_eui}] Final image error: {e}")

    async def _upload_image(self, dev_eui, jpeg_bytes, sense_time, data_extra=None):
        """Upload reassembled image to a remote server"""
        try:
            form = aiohttp.FormData()
            form.add_field('snap', jpeg_bytes, filename='image.jpg', content_type='image/jpeg')
            form.add_field('deviceId', dev_eui)
            
            data_payload = {
                "type": "image",
                "file": "snap",
                "sense_time": sense_time
            }
            if data_extra:
                data_payload.update(data_extra)
                
            form.add_field('data', json.dumps(data_payload))

            async with aiohttp.ClientSession(headers=self.upload_headers) as session:
                async with session.post(self.upload_url, data=form, timeout=30) as resp:
                    if 200 <= resp.status <= 299:
                        print(f"[{dev_eui}] Successfully uploaded image to {self.upload_url}")
                    else:
                        resp_text = await resp.text()
                        print(f"[{dev_eui}] Upload failed ({resp.status}): {resp_text}")
        except Exception as e:
            print(f"[{dev_eui}] Upload exception: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="EdgeEye Image Reassembler (Chirpstack v4 MQTT)")
    parser.add_argument("--mqtt_url", help="Chirpstack MQTT broker URL", required=True)
    parser.add_argument("--mqtt_user", help="MQTT username", required=False, default=None)
    parser.add_argument("--mqtt_pass", help="MQTT password", required=False, default=None)
    parser.add_argument("--redis_url", help="Redis URL for context storage", required=True)
    parser.add_argument("--device_profile_id", help="Filter by Device Profile ID", required=True)
    parser.add_argument("--upload_url", help="HTTP URL to upload reassembled images", required=False, default=None)
    parser.add_argument("--upload_headers", help="JSON string of HTTP headers for upload", required=False, default=None)
    args = parser.parse_args()

    mqtt_info = {
        'url': args.mqtt_url.strip(),
        'user': args.mqtt_user.strip() if args.mqtt_user else None,
        'pass': args.mqtt_pass.strip() if args.mqtt_pass else None
    }
    
    upload_headers = {}
    if args.upload_headers:
        try:
            upload_headers = json.loads(args.upload_headers)
        except Exception as e:
            print(f"Failed to parse upload_headers: {e}")

    print(f"EdgeEye Reassembler starting...")
    print(f"MQTT Broker: {mqtt_info['url']}")
    print(f"Filtering by Device Profile ID: {args.device_profile_id}")
    if args.upload_url:
        print(f"Upload enabled: {args.upload_url}")
    
    processor = ImageReassembler(
        mqtt_info, 
        args.redis_url.strip(), 
        args.device_profile_id.strip(),
        upload_url=args.upload_url,
        upload_headers=upload_headers
    )
    processor.start()
    processor.loop_forever()
