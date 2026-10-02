import base64
import json
import io
import sys
import asyncio
import threading
import traceback
import argparse
import random
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import redis.asyncio as redis
import paho.mqtt.client as mqtt
from PIL import Image, ImageFile

from session_manager import SessionManager

ImageFile.LOAD_TRUNCATED_IMAGES = True

def parse_device_profiles(spec, max_sessions):
    """Parse "PROFILE_ID[:SESSIONS],..." into {profile_id: virtual sessions, 0 = single session}."""
    profiles = {}
    for entry in spec.split(","):
        entry = entry.strip()
        if not entry:
            continue
        profile_id, _, count = entry.partition(":")
        profile_id = profile_id.strip()
        try:
            sessions = int(count) if count.strip() else 0
        except ValueError:
            raise ValueError(f"virtual session count must be a number in {entry!r}")
        if not profile_id or not 0 <= sessions <= max_sessions:
            raise ValueError(f"expected PROFILE_ID or PROFILE_ID:0..{max_sessions}, got {entry!r}")
        if profile_id in profiles:
            raise ValueError(f"device profile listed twice: {profile_id}")
        profiles[profile_id] = sessions
    return profiles

class ImageReassembler:
    def __init__(self, mqtt_info, redis_url, profile_sessions, session_manager=None):
        self.mqtt_info = mqtt_info
        self.redis_url = redis_url
        # Device profile ID -> virtual sessions per image for its cameras (0 = single session)
        self.profile_sessions = profile_sessions
        # Multi-session uplink (virtual sessions decoded from gateway traffic); None = disabled
        self.sessions = session_manager
        
        self.pool = None
        self.event_loop = None
        self.mqtt_client = None
        self.device_locks = {} # Per-device locks to ensure sequential processing

        self.FLAG_FIRST_FRAG = 0x01
        self.FLAG_LAST_FRAG  = 0x02
        self.FLAG_SYSV       = 0x04
        self.FLAG_ALS        = 0x08
        self.FLAG_MULTI_UP   = 0x10
        self.FLAG_OBJDET     = 0x20

        self.BUFFER_TTL = 3600         # seconds; a partial image buffer must not outlive its state
        self.GAP_REQUEST_DELAY = 5.0   # seconds a gap must persist before it is treated as a loss
        self.GAP_PENDING_TTL = 30      # seconds to wait for the device to resend a requested range
        self.PORT5_RESEND_DELAY = 15   # seconds without a session uplink before the grant is resent
        self.PORT5_MAX_SENDS = 3       # session grant attempts per image

    def start(self):
        """Initialize and start the processor"""
        if not self.redis_url:
            print("Redis URL is required.")
            return None

        # Add socket timeouts to prevent indefinite hanging
        self.pool = redis.ConnectionPool.from_url(
            self.redis_url, 
            socket_timeout=10, 
            socket_connect_timeout=10,
            health_check_interval=30
        )

        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)
        
        def run_loop():
            try:
                print("Asyncio event loop started.")
                self.event_loop.run_forever()
            except Exception as e:
                print(f"Asyncio event loop crashed: {e}")
                traceback.print_exc()
        threading.Thread(target=run_loop, daemon=True).start()

        url_parsed = urlparse(self.mqtt_info['url'])
        host = url_parsed.hostname
        port = url_parsed.port or 1883
        
        # Use a unique client ID to avoid session conflicts
        client_id = f"edgeeye-reassembler-{random.randint(1000, 9999)}"
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        if self.mqtt_info.get('user') and self.mqtt_info.get('pass'):
            self.mqtt_client.username_pw_set(self.mqtt_info['user'], self.mqtt_info['pass'])
            
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
        self.mqtt_client.on_message = self._on_mqtt_message
        self.mqtt_client.on_subscribe = self._on_mqtt_subscribe
        
        print(f"Connecting to MQTT Broker: {host}:{port} with ClientID: {client_id}...")
        self.mqtt_client.connect(host, port, 60)
        
        self._message_count = 0
        
        return self.mqtt_client

    def loop_forever(self):
        if self.mqtt_client:
            # Add a heartbeat check
            def heartbeat():
                while True:
                    mqtt_status = "CONNECTED" if self.mqtt_client.is_connected() else "DISCONNECTED"
                    loop_status = "RUNNING" if self.event_loop.is_running() else "STOPPED"
                    print(f"[{datetime.now().isoformat()}] Heartbeat: MQTT={mqtt_status}, Loop={loop_status}, MsgCount={self._message_count}")
                    import time
                    time.sleep(60)
            threading.Thread(target=heartbeat, daemon=True).start()
            
            self.mqtt_client.loop_forever()

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print(f"Connected to MQTT Broker (Reason Code: {reason_code})")
            client.subscribe("application/+/device/+/event/up")
            if self.sessions is not None:
                # Frames on virtual sessions never reach the application topics: ChirpStack
                # does not know those sessions. They are taken from the gateway bridge instead.
                for topic in self.sessions.gateway_topics:
                    client.subscribe(topic)
        else:
            print(f"Failed to connect to MQTT Broker (Reason Code: {reason_code})")

    def _on_mqtt_disconnect(self, client, userdata, flags, reason_code, properties):
        print(f"Disconnected from MQTT Broker (Reason Code: {reason_code})")

    def _on_mqtt_subscribe(self, client, userdata, mid, reason_codes, properties):
        print(f"Subscribed to topic (MID: {mid}, Reason Codes: {reason_codes})")

    def _on_mqtt_message(self, client, userdata, msg):
        self._message_count += 1
        try:
            if self.sessions is not None and self.sessions.is_gateway_topic(msg.topic):
                # Raw gateway traffic: only frames on virtual sessions issued here matter.
                phy = self.sessions.candidate(msg.payload)
                if phy:
                    asyncio.run_coroutine_threadsafe(self._process_gateway_uplink(phy), self.event_loop)
                return

            data = json.loads(msg.payload)
            device_info = data.get('deviceInfo', {})
            profile_id = device_info.get('deviceProfileId')
            if profile_id not in self.profile_sessions:
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
                'raw': base64.b64decode(raw_b64),
                'num_sessions': self.profile_sessions[profile_id],
            }

            asyncio.run_coroutine_threadsafe(self._process_uplink(message), self.event_loop)
            
        except Exception as e:
            print(f"Error handling MQTT message: {e}")

    async def _process_gateway_uplink(self, phy):
        """A frame that may belong to a virtual session, straight from the gateway bridge."""
        r = redis.Redis(connection_pool=self.pool)
        try:
            frame = await self.sessions.decode_uplink(r, phy)
        except Exception:
            traceback.print_exc()
            return
        finally:
            await r.aclose()
        if frame is None:
            return
        await self._process_uplink({
            'dev_eui': frame['parent'],
            'app_id': None,                 # taken from the camera's own uplinks
            'f_port': frame['f_port'],
            'f_cnt': frame['f_cnt'],
            'raw': frame['raw'],
            'session_index': frame['index'],
        })

    async def _send_downlink(self, app_id, dev_eui, f_port, payload, confirmed=True):
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
        shown = payload.hex() if f_port != 5 else f"{payload[:5].hex()}... ({len(payload)} bytes, session keys not logged)"
        print(f"[{dev_eui}] Downlink sent (FPort {f_port}, Expires: {expires_at}): {shown}")
    async def _process_uplink(self, msg):
        # Virtual-session fragments arrive already mapped to their camera (dev_eui is the
        # camera), so the lock and all state below are keyed by the camera EUI.
        dev_eui = msg['dev_eui']

        # Get or create a lock for this specific device to handle packets sequentially
        if dev_eui not in self.device_locks:
            self.device_locks[dev_eui] = asyncio.Lock()

        async with self.device_locks[dev_eui]:
            try:
                r = redis.Redis(connection_pool=self.pool)
                if msg['f_port'] == 2:
                    category = self._handle_fail_report(dev_eui, msg['raw'])
                    if category == 2 and self.sessions is not None:
                        # The device gave up sending; its virtual sessions are no longer needed.
                        await self.sessions.release(r, dev_eui)
                elif msg['f_port'] == 1:
                    await self._handle_image_fragment(r, msg)
                await r.aclose()
            except Exception:
                traceback.print_exc()

    def _handle_fail_report(self, dev_eui, raw):
        categories = {0: "Boot", 1: "Snap", 2: "Send"}
        snap_sub = {0: "Memory", 1: "Filesystem", 2: "Encoding"}
        send_sub = {0: "Memory", 1: "Filesystem", 2: "Busy", 3: "User interrupt"}

        cat = raw[0] if len(raw) > 0 else None
        sub = raw[1] if len(raw) > 1 else None
        sysv = int.from_bytes(raw[2:4], 'little') / 1000.0 if len(raw) > 3 else None

        cat_name = categories.get(cat, f"Unknown({cat})")

        sub_name = ""
        if cat == 1:
            sub_name = snap_sub.get(sub, f"Unknown({sub})")
        elif cat == 2:
            sub_name = send_sub.get(sub, f"Unknown({sub})")
        elif sub is not None:
            sub_name = f"Sub={sub}"

        parts = [f"[{dev_eui}] Device Error: {cat_name}"]
        if sub_name:
            parts.append(f"({sub_name})")
        if sysv is not None:
            parts.append(f"| {sysv:.3f}V")

        print(" ".join(parts))
        return cat

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
        if (flags & self.FLAG_SYSV):
            sysv = int.from_bytes(raw[i:i+2], 'little') / 1000.0
            i += 2
        
        als = None
        if (flags & self.FLAG_ALS):
            als = int.from_bytes(raw[i:i+3], 'little')
            i += 3
        
        first_frag = bool(flags & self.FLAG_FIRST_FRAG)
        last_frag = bool(flags & self.FLAG_LAST_FRAG)
        frag_data = raw[i:]
        sense_time = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace('+00:00', 'Z')

        prefix = f"PP:EdgeEye"
        active_epoch_key = f"{prefix}:active_epoch:{dev_eui}"
        completed_key = f"{prefix}:completed:{dev_eui}:{epoch}"
        buffer_key = f"{prefix}:buffer:{dev_eui}:{epoch}"
        state_key = f"{prefix}:state:{dev_eui}:{epoch}"
        started_key = f"{prefix}:started:{dev_eui}:{epoch}"
        missing_key = f"{prefix}:missing:{dev_eui}:{epoch}"
        last_dl_key = f"{prefix}:last_dl:{dev_eui}:{epoch}"

        session_index = msg.get('session_index', 0)
        from_session = session_index > 0
        parent_app_key = f"{prefix}:parent_app:{dev_eui}"
        if from_session:
            # Virtual-session frames carry no application; downlinks go to the camera's own
            # application, learned from its own uplinks (which always precede the grant).
            parent_app = await r.get(parent_app_key)
            if not parent_app:
                print(f"[{dev_eui}] virtual-session fragment before any uplink of the camera itself; dropped")
                return
            app_id = parent_app.decode() if isinstance(parent_app, bytes) else str(parent_app)
            await r.set(f"{prefix}:session_seen:{dev_eui}:{epoch}", 1, ex=3600)
        else:
            await r.set(parent_app_key, app_id, ex=86400)

        if await r.exists(completed_key):
            if not await r.exists(last_dl_key):
                await self._send_downlink(app_id, dev_eui, 4, epoch.to_bytes(5, 'little'))
                await r.set(last_dl_key, 1, ex=10)
            return

        active_epoch = await r.get(active_epoch_key)
        active_epoch = int(active_epoch) if active_epoch else 0
        if epoch != active_epoch:
            # A FIRST fragment always starts a new image, whatever its epoch: the device
            # clock may move backwards (reboot before time sync). Non-first fragments
            # of an older epoch are late stragglers and are dropped.
            if not first_frag and epoch < active_epoch:
                return
            print(f"[{dev_eui}] New image detected: 0x{epoch:08X} (replacing 0x{active_epoch:08X})")
            if active_epoch:
                await self._cleanup_epoch(r, prefix, dev_eui, active_epoch)
                if self.sessions is not None:
                    await self.sessions.release(r, dev_eui)
            await r.set(active_epoch_key, epoch, ex=86400)
            await r.set(started_key, time.time(), ex=86400)  # transfer time is measured from here
            await r.delete(f"ImageToRtsp:{dev_eui}:det")

        num_sessions = msg.get('num_sessions', 0)
        if self.sessions is not None and num_sessions > 0 and first_frag and not last_frag and not from_session:
            await self._start_sessions(r, prefix, dev_eui, app_id, epoch, num_sessions)

        # Detection fragment: handled only after the completed/active checks above so
        # that a stale detection fragment cannot clobber the current image.
        if (flags & self.FLAG_OBJDET) and frag_data:
            det = []
            obj_size = 11
            obj_count = len(frag_data) // obj_size
            for j in range(obj_count):
                o = frag_data[j*obj_size:(j+1)*obj_size]
                if len(o) == obj_size:
                    det.append({
                        'x':     int.from_bytes(o[0:2], 'little') / 65535.0,
                        'y':     int.from_bytes(o[2:4], 'little') / 65535.0,
                        'w':     int.from_bytes(o[4:6], 'little') / 65535.0,
                        'h':     int.from_bytes(o[6:8], 'little') / 65535.0,
                        'class': o[8],
                        'score': int.from_bytes(o[9:11], 'little') / 65535.0,
                    })
            if first_frag:
                state = {'received': 0, 'total_size': offset, 'meta': []}
            else:
                existing = await r.get(state_key)
                state = json.loads(existing) if existing else {'received': 0, 'total_size': None, 'meta': []}
                if 'meta' not in state:
                    state['meta'] = []
            state['det'] = det
            if sysv is not None:
                state['system_voltage'] = sysv
            state['meta'].append({'fCnt': msg['f_cnt'], 'ts': datetime.now(timezone.utc).isoformat()})
            await r.set(state_key, json.dumps(state), ex=3600)
            await r.set(f"ImageToRtsp:{dev_eui}:det", json.dumps(det), ex=86400)
            await r.set(f"ImageToRtsp:{dev_eui}:sense_time", sense_time, ex=86400)
            await r.set(f"ImageToRtsp:{dev_eui}:app_id", app_id, ex=86400)
            await r.publish(f"EdgeEye:updated:{dev_eui}", "det")
            print(f"[{dev_eui}:{sense_time}] Object detection: {obj_count} objects (Epoch: 0x{epoch:08X})")
            return

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

        now = time.time()
        missing_raw = await r.get(missing_key)
        missing_blocks = self._load_missing_blocks(missing_raw, now)

        new_offset_next = await self._apply_fragment(r, buffer_key, offset, frag_data, state['received'], missing_blocks, now)

        # An outstanding range request is fulfilled once the device has resent the
        # whole requested range; only then may the next range be requested.
        gap_pending_key = f"{prefix}:gap_pending:{dev_eui}:{epoch}"
        if frag_data:
            pending = self._parse_gap_pending(await r.get(gap_pending_key))
            # Only a resent fragment that lies inside the requested range and reaches
            # its end fulfils the request; in-order fragments beyond the range do not.
            if pending and pending[0] <= offset < pending[1] and offset + len(frag_data) >= pending[1]:
                await r.delete(gap_pending_key)
        reassembled_offset = new_offset_next

        # Send gap request if we have missing blocks
        is_verification = (len(frag_data) == 0)
        if missing_blocks:
            reassembled_offset = min(b[0] for b in missing_blocks)
            await r.set(missing_key, json.dumps(missing_blocks), ex=86400)

            # Request the lowest missing block, one outstanding request at a time
            m = min(missing_blocks, key=lambda x: x[0])
            block_dl_key = f"{prefix}:last_dl:{dev_eui}:{epoch}:{m[0]}_{m[1]}"

            if is_verification:
                # The device explicitly asked (empty LAST fragment), i.e. it has no resend
                # range in progress. A request sent moments ago is still queued and rides
                # on this very uplink; anything older is treated as lost and sent again.
                pending = self._parse_gap_pending(await r.get(gap_pending_key))
                should_request = not (pending and (now - pending[2]) < 3.0)
            else:
                # A gap must persist for a while before it counts as a loss: fragments
                # from parallel sessions can arrive slightly out of order. The device
                # keeps only one resend range, so never queue a second request while
                # one is outstanding.
                should_request = ((now - m[2]) >= self.GAP_REQUEST_DELAY
                                  and not await r.exists(block_dl_key)
                                  and not await r.exists(gap_pending_key))

            if should_request:
                print(f"[{dev_eui}:{sense_time}] Packet loss! Missing: {m[0]}~{m[1]} (Epoch: 0x{epoch:08X})")
                req = raw[1:6] + m[0].to_bytes(3, 'little') + m[1].to_bytes(3, 'little')
                await self._send_downlink(app_id, dev_eui, 4, req)
                await r.set(gap_pending_key, f"{m[0]}_{m[1]}_{now}", ex=self.GAP_PENDING_TTL)

                if not is_verification:
                    await r.set(block_dl_key, 1, ex=30) # 30s throttle for THIS specific block
        else:
            await r.delete(missing_key)
            await r.delete(gap_pending_key)

        state['received'] = new_offset_next
        per_session = state.setdefault('per_session', {})
        slot = f"s{session_index}"
        per_session[slot] = per_session.get(slot, 0) + 1
        state['meta'].append({'fCnt': msg['f_cnt'], 'ts': datetime.now(timezone.utc).isoformat()})
        await r.set(state_key, json.dumps(state), ex=3600)

        total = state['total_size']
        percent = (reassembled_offset / total * 100) if total > 0 else 0
        max_reached = state['received']
        max_percent = (max_reached / total * 100) if total > 0 else 0
        
        if len(frag_data) > 0:
            info = f"(Frag:{offset}-{offset+len(frag_data)}, {len(frag_data)}B, fCnt:{msg['f_cnt']}, {slot})"
        else:
            info = f"(Verification, fCnt:{msg['f_cnt']})"
            
        print(f"[{dev_eui}:{sense_time}] Progress: {reassembled_offset}/{total} ({percent:.2f}%) [Max: {max_reached}/{total} ({max_percent:.2f}%)] {info}")

        # Always check for finalization using the actual contiguous offset
        await self._finalize_image(r, dev_eui, app_id, epoch, sense_time, buffer_key, 
                                   reassembled_offset, total, last_frag, completed_key, state_key, started_key, state)

    async def _start_sessions(self, r, prefix, dev_eui, app_id, epoch, num_sessions):
        """Create the virtual sessions for this image and hand them to the camera (fPort 5)."""
        sent_key = f"{prefix}:port5_sent:{dev_eui}:{epoch}"
        if await r.exists(sent_key):
            return  # retransmitted first fragment: the grant was already issued
        try:
            payload = await self.sessions.create_for_image(r, dev_eui, epoch, num_sessions)
        except Exception as e:
            print(f"[{dev_eui}] Multi-session setup failed, continuing single-session: {e}")
            return
        await r.set(sent_key, 1, ex=3600)
        await self._send_downlink(app_id, dev_eui, 5, payload, confirmed=False)
        print(f"[{dev_eui}] Session grant sent for epoch 0x{epoch:08X} ({(len(payload) - 5) // 36} sessions)")
        asyncio.get_running_loop().create_task(
            self._port5_watchdog(prefix, dev_eui, app_id, epoch, payload))

    async def _port5_watchdog(self, prefix, dev_eui, app_id, epoch, payload):
        """Resend the session grant while the camera still sends on its own session only."""
        for attempt in range(2, self.PORT5_MAX_SENDS + 1):
            await asyncio.sleep(self.PORT5_RESEND_DELAY)
            r = redis.Redis(connection_pool=self.pool)
            try:
                active = await r.get(f"{prefix}:active_epoch:{dev_eui}")
                if not active or int(active) != epoch:
                    return
                if await r.exists(f"{prefix}:completed:{dev_eui}:{epoch}"):
                    return
                if await r.exists(f"{prefix}:session_seen:{dev_eui}:{epoch}"):
                    return
                print(f"[{dev_eui}] No session uplink yet; resending session grant ({attempt}/{self.PORT5_MAX_SENDS})")
                await self._send_downlink(app_id, dev_eui, 5, payload, confirmed=False)
            except Exception:
                traceback.print_exc()
            finally:
                await r.aclose()

    @staticmethod
    def _transfer_timing(started, image_bytes):
        """Seconds from the first fragment seen of an image until now, and the rate in bytes/s."""
        if not started:
            return None, None
        seconds = time.time() - float(started)
        if seconds <= 0:
            return None, None
        return round(seconds, 1), round(image_bytes / seconds)

    @staticmethod
    def _transfer_stats(state, transfer_sec, bytes_per_sec):
        per_session = state.get('per_session') or {}
        parts = [f"{k}={per_session[k]}" for k in sorted(per_session)]
        elapsed = f", elapsed {transfer_sec}s, {bytes_per_sec} B/s" if transfer_sec else ""
        return f"Transfer stats: fragments per session {' '.join(parts) or 'n/a'}{elapsed}"

    @staticmethod
    def _load_missing_blocks(missing_raw, now):
        """Missing blocks are [start, end, first_seen]; older entries lacked the timestamp."""
        blocks = json.loads(missing_raw) if missing_raw else []
        return [[int(b[0]), int(b[1]), float(b[2]) if len(b) > 2 else now] for b in blocks]

    @staticmethod
    def _parse_gap_pending(raw):
        """Outstanding range request as (start, end, sent_at), or None."""
        if not raw:
            return None
        parts = (raw.decode() if isinstance(raw, bytes) else str(raw)).split('_')
        if len(parts) < 3:
            return None
        return int(parts[0]), int(parts[1]), float(parts[2])

    async def _cleanup_epoch(self, r, prefix, dev_eui, old_epoch):
        """Drop the per-image keys of an image that was abandoned for a newer one."""
        await r.delete(
            f"{prefix}:buffer:{dev_eui}:{old_epoch}",
            f"{prefix}:state:{dev_eui}:{old_epoch}",
            f"{prefix}:started:{dev_eui}:{old_epoch}",
            f"{prefix}:missing:{dev_eui}:{old_epoch}",
            f"{prefix}:gap_pending:{dev_eui}:{old_epoch}",
            f"{prefix}:last_dl:{dev_eui}:{old_epoch}",
        )

    async def _apply_fragment(self, r, key, offset, data, received, missing_blocks, now):
        offset_end = offset + len(data)

        # Gap detection from the header's offset
        if offset > received:
            if not any(b[0] == received and b[1] == offset for b in missing_blocks):
                missing_blocks.append([received, offset, now])

        if len(data) == 0:
            return received

        if offset < received:
            new_missing = []
            for b in missing_blocks:
                if offset <= b[0] and offset_end >= b[1]: continue
                if offset <= b[0] and b[0] < offset_end < b[1]: b[0] = offset_end
                elif b[0] < offset < b[1] <= offset_end: b[1] = offset
                elif b[0] < offset and offset_end < b[1]:
                    new_missing.append([offset_end, b[1], b[2]])
                    b[1] = offset
                new_missing.append(b)
            missing_blocks[:] = new_missing

        new_len = int(await r.setrange(key, offset, data))
        # SETRANGE never sets a TTL: without this, buffers of images that never
        # complete stay in Redis forever.
        await r.expire(key, self.BUFFER_TTL)
        return new_len

    async def _finalize_image(self, r, dev_eui, app_id, epoch, sense_time, buffer_key, 
                              reassembled_len, total_size, is_last, completed_key, state_key, started_key, state):
        rtsp_base = f"ImageToRtsp:{dev_eui}"
        
        img_data = await r.get(buffer_key)
        if not img_data: return
        
        img_data = img_data[:reassembled_len]
        complete = bool(total_size) and reassembled_len >= total_size

        # Update raw data even if the image is partial
        # Sharp in mjpeg-streamer can handle truncated JPEGs with failOn: 'none'
        await r.set(f"{rtsp_base}:image", img_data, ex=86400)
        await r.set(f"{rtsp_base}:sense_time", sense_time, ex=86400)

        try:
            img = Image.open(io.BytesIO(img_data))
            # (Optional) Re-save with Pillow if we want to ensure format or apply transformations
            # For now, we rely on raw data for streaming speed
            
            if complete:
                with io.BytesIO() as output:
                    img.save(output, format="JPEG")
                    jpeg_bytes = output.getvalue()
                
                transfer_sec, bytes_per_sec = self._transfer_timing(await r.getdel(started_key), total_size)
                print(f"[{dev_eui}] Reassembly complete! {len(jpeg_bytes)} bytes (Original: {total_size} bytes)")
                print(f"[{dev_eui}] {self._transfer_stats(state, transfer_sec, bytes_per_sec)}")
                if self.sessions is not None:
                    await self.sessions.release(r, dev_eui)
                await r.set(f"{rtsp_base}:image:last", jpeg_bytes, ex=86400)
                await r.set(f"{rtsp_base}:sense_time:last", sense_time, ex=86400)
                if 'det' in state:
                    await r.set(f"{rtsp_base}:det:last", json.dumps(state['det']), ex=86400)
                else:
                    await r.delete(f"{rtsp_base}:det:last")
                await r.set(completed_key, 1, ex=86400)
                await self._send_downlink(app_id, dev_eui, 4, epoch.to_bytes(5, 'little'))
                await r.delete(buffer_key)
                await r.delete(state_key)
                
                # Notify Node.js streamer to perform composite + upload
                upload_meta = {
                    'app_id': app_id,
                    'sense_time': sense_time,
                    'system_voltage': state.get('system_voltage'),
                    'ambient_light_lux': state.get('ambient_light_lux'),
                    'det': state.get('det'),
                    'transfer_sec': transfer_sec,
                    'image_bytes': total_size,
                    'bytes_per_sec': bytes_per_sec,
                }
                await r.set(f"{rtsp_base}:upload:ready", json.dumps(upload_meta), ex=300)
                
        except Exception as e:
            if complete:
                # Every byte is in but the JPEG cannot be decoded. Resending the same
                # bytes cannot fix that, so release the device instead of letting it
                # poll for a "done" that would never come.
                print(f"[{dev_eui}] Reassembly complete but image undecodable ({e}); releasing device")
                if self.sessions is not None:
                    await self.sessions.release(r, dev_eui)
                await r.set(completed_key, 1, ex=86400)
                await self._send_downlink(app_id, dev_eui, 4, epoch.to_bytes(5, 'little'))
                await r.delete(buffer_key)
                await r.delete(state_key)
            elif is_last:
                print(f"[{dev_eui}] Final image error: {e}")

        # Notify streamers last (even if the image is partial) so that on completion
        # upload:ready already exists when mjpeg-streamer's doUpload() runs GETDEL on it
        await r.publish(f"EdgeEye:updated:{dev_eui}", "updated")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="EdgeEye Image Reassembler (Chirpstack v4 MQTT)")
    parser.add_argument("--mqtt_url", help="Chirpstack MQTT broker URL", required=True)
    parser.add_argument("--mqtt_user", help="MQTT username", required=False, default=None)
    parser.add_argument("--mqtt_pass", help="MQTT password", required=False, default=None)
    parser.add_argument("--redis_url", help="Redis URL for context storage", required=True)
    parser.add_argument("--device_profile_id", default="",
                        help="Filter by Device Profile ID (a single profile; see --device_profiles)")
    parser.add_argument("--device_profiles", default="",
                        help="Device profiles to serve and their virtual sessions per image, e.g. "
                             "'PROFILE_A:0,PROFILE_B:2' (0 = single session). Overrides "
                             "--device_profile_id, --multi_session and --num_sessions")
    parser.add_argument("--multi_session", type=int, default=0,
                        help="1 enables multi-session uplink: virtual sessions decoded from gateway traffic")
    parser.add_argument("--gateway_topic_prefix", default="kr920",
                        help="Gateway bridge MQTT topic prefix(es), comma-separated, e.g. kr920")
    parser.add_argument("--session_devaddr_prefix", default="fe000000/7",
                        help="DevAddr prefix for virtual sessions, outside the network's NetID range")
    parser.add_argument("--num_sessions", type=int, default=2, help="Virtual sessions per camera (max 2)")
    args = parser.parse_args()

    mqtt_info = {
        'url': args.mqtt_url.strip(),
        'user': args.mqtt_user.strip() if args.mqtt_user else None,
        'pass': args.mqtt_pass.strip() if args.mqtt_pass else None
    }

    print(f"EdgeEye Reassembler starting...")
    print(f"MQTT Broker: {mqtt_info['url']}")

    if args.device_profiles.strip():
        try:
            profile_sessions = parse_device_profiles(args.device_profiles, SessionManager.MAX_SESSIONS)
        except ValueError as e:
            print(f"Device profile configuration error: {e}")
            sys.exit(2)
    elif args.device_profile_id.strip():
        sessions = max(1, min(args.num_sessions, SessionManager.MAX_SESSIONS)) if args.multi_session else 0
        profile_sessions = {args.device_profile_id.strip(): sessions}
    else:
        profile_sessions = {}
    if not profile_sessions:
        print("Device profile configuration error: set --device_profiles or --device_profile_id")
        sys.exit(2)
    for profile_id, sessions in profile_sessions.items():
        print(f"Serving device profile {profile_id}: "
              + (f"{sessions} virtual sessions per image" if sessions else "single session"))

    session_manager = None
    if max(profile_sessions.values()) > 0:
        try:
            session_manager = SessionManager(max(profile_sessions.values()), args.gateway_topic_prefix,
                                             args.session_devaddr_prefix)
        except ValueError as e:
            print(f"Multi-session configuration error: {e}")
            sys.exit(2)
        print(f"Multi-session enabled: virtual sessions decoded from {', '.join(session_manager.gateway_topics)}, "
              f"DevAddr prefix {session_manager.devaddr_prefix}")

    processor = ImageReassembler(
        mqtt_info,
        args.redis_url.strip(),
        profile_sessions,
        session_manager=session_manager,
    )
    processor.start()
    processor.loop_forever()
