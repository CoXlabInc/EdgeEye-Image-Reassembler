import base64
from datetime import datetime, timedelta
import time
import json
import redis.asyncio as redis
from urllib.parse import urlparse
import io
from PIL import ImageFile
from PIL import Image
import sys
import asyncio
import threading
import paho.mqtt.client as mqtt
import traceback

TAG = 'EdgeEye'
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Global variables
pool = None
event_loop = None
mqtt_client = None
filter_device_profile_id = None

def init(mqtt_info, redis_url, device_profile_id):
    global pool, event_loop, mqtt_client, filter_device_profile_id

    if redis_url is None:
        print(f"Redis is required for EdgeEye.")
        return None
    
    filter_device_profile_id = device_profile_id

    pool = redis.ConnectionPool.from_url(redis_url)

    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)

    def event_loop_thread():
        event_loop.run_forever()
    threading.Thread(target=event_loop_thread, daemon=True).start()

    # Setup MQTT client
    url_parsed = urlparse(mqtt_info['url'])
    host = url_parsed.hostname
    port = url_parsed.port if url_parsed.port else 1883
    
    mqtt_client = mqtt.Client()
    if mqtt_info['user'] and mqtt_info['pass']:
        mqtt_client.username_pw_set(mqtt_info['user'], mqtt_info['pass'])
    
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    
    print(f"Connecting to MQTT Broker: {host}:{port}...")
    mqtt_client.connect(host, port, 60)

    return mqtt_client

def on_mqtt_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT Broker with result code {rc}")
    # Subscribe to Chirpstack v4 Uplink events
    client.subscribe("application/+/device/+/event/up")

def on_mqtt_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload)
        
        # Filter by Device Profile ID
        device_profile_id = data.get('deviceInfo', {}).get('deviceProfileId')
        if device_profile_id != filter_device_profile_id:
            # Silently ignore packets from other device profiles
            return

        # Extract necessary fields from Chirpstack v4 JSON
        dev_eui = data.get('deviceInfo', {}).get('devEui')
        f_port = data.get('fPort')
        f_cnt = data.get('fCnt')
        raw_b64 = data.get('data')
        
        if raw_b64 is None:
            return

        # Map to the format expected by async_post_process
        message = {
            'nid': dev_eui,
            'grpid': data.get('deviceInfo', {}).get('applicationId', 'default'),
            'key': dev_eui,
            'meta': {
                'fPort': f_port,
                'fCnt': f_cnt,
                'raw': raw_b64
            },
            'data': {} # Placeholder for processed data
        }
        
        # Execute post process in the event loop
        asyncio.run_coroutine_threadsafe(async_post_process(message), event_loop)
        
    except Exception as e:
        print(f"Error processing MQTT message: {e}")
        traceback.print_exc()

async def send_downlink(application_id, dev_eui, f_port, payload_bytes, confirmed=False):
    """Sends a downlink message via Chirpstack v4 MQTT"""
    topic = f"application/{application_id}/device/{dev_eui}/command/down"
    
    downlink_data = {
        "devEui": dev_eui,
        "confirmed": confirmed,
        "fPort": f_port,
        "data": base64.b64encode(payload_bytes).decode('utf-8')
    }
    
    mqtt_client.publish(topic, json.dumps(downlink_data))
    print(f"[{TAG}:{dev_eui}] Downlink sent to FPort {f_port}: {payload_bytes.hex()}")

async def async_post_process(message):
    r = redis.Redis(connection_pool=pool)
    
    # MUTEX
    mutex_key = f"PP:EdgeEye:MUTEX:{message['nid']}"
    lock = await r.set(mutex_key, 'lock', ex=30, nx=True)
    if lock != True:
        await r.aclose()
        return None

    message['data']['image'] = None
    message['data']['error'] = ''
    message['data']['meta_total'] = []
    
    fport = message['meta'].get('fPort')
    raw = base64.b64decode(message['meta']['raw'])
    dev_eui = message['nid']
    app_id = message['grpid']

    if fport == 2:
        # Fail Report logic remains the same
        if raw[0] == 0:
            message['data']['error'] = "Camera boot failed"
        elif raw[0] == 1:
            message['data']['error'] = "Camera snap failed"
        elif raw[0] == 2:
            message['data']['error'] = "Send failed"
        else:
            message['data']['error'] = f"Unknown fail ({raw[0]})"
        print(f"[{TAG}:{dev_eui}] Error: {message['data']['error']}")
        await r.delete(mutex_key)
        await r.aclose()
        return message
    elif fport != 1:
        # Ignore other ports
        await r.delete(mutex_key)
        await r.aclose()
        return None
    
    fcnt = message['meta'].get('fCnt')
    flags = raw[0]
    epoch = int.from_bytes(raw[1:6], 'little', signed=False)
    offset = int.from_bytes(raw[6:9], 'little', signed=False)

    i = 9
    if (flags & (1 << 2)) != 0: i += 2 # sysv
    if (flags & (1 << 3)) != 0: i += 3 # als
 
    first_frag = ((flags & (1 << 0)) != 0)
    last_frag = ((flags & (1 << 1)) != 0)

    frag = raw[i:]
    sense_time = datetime.utcfromtimestamp(epoch).isoformat() + 'Z'

    missing_blocks_key = f"PP:EdgeEye:missing:{dev_eui}:{epoch}"
    image_buffer_key = f"PP:EdgeEye:buffer:{dev_eui}:{epoch}"
    state_key = f"PP:EdgeEye:state:{dev_eui}:{epoch}"

    # Check if already completed
    if await r.get(f"PP:EdgeEye:completed:{dev_eui}:{epoch}") is not None:
        await send_downlink(app_id, dev_eui, 4, epoch.to_bytes(5, byteorder='little', signed=False))
        await r.delete(mutex_key)
        await r.aclose()
        return None

    # Load reassembly state from Redis
    state = await r.get(state_key)
    if state:
        state = json.loads(state)
        offset_next = state.get('received', 0)
        total_size = state.get('total_size')
        meta = state.get('meta_total', [])
    else:
        offset_next = 0
        total_size = None
        meta = []

    if first_frag:
        total_size = offset
        offset = 0
    elif total_size is None:
        # Missing first fragment - Request it
        print(f"[{TAG}:{dev_eui}:{sense_time}] Missing the first fragment")
        frag_req = raw[1:6] + b'\x00\x00\x00'
        await send_downlink(app_id, dev_eui, 4, frag_req)
        await r.delete(mutex_key)
        await r.aclose()
        return None

    # Handle missing blocks tracking
    missing_blocks_raw = await r.get(missing_blocks_key)
    try:
        missing_blocks = json.loads(missing_blocks_raw) if missing_blocks_raw else []
    except:
        missing_blocks = []

    offset_end = offset + len(frag)
    print(f"[{TAG}:{dev_eui}:{sense_time}] Frag:{offset}~{offset_end}, Next:{offset_next}, Total:{total_size}")

    if offset > offset_next:
        if [offset_next, offset] not in missing_blocks:
            missing_blocks.append([offset_next, offset])
    elif offset < offset_next:
        # Shrink/remove existing missing blocks
        updated_missing_blocks = []
        for b in missing_blocks:
            if offset <= b[0] and offset_end >= b[1]: continue
            elif offset <= b[0] and b[0] < offset_end < b[1]: b[0] = offset_end
            elif b[0] < offset < b[1] <= offset_end: b[1] = offset
            elif b[0] < offset and offset_end < b[1]:
                updated_missing_blocks.append([offset_end, b[1]])
                b[1] = offset
            updated_missing_blocks.append(b)
        missing_blocks = updated_missing_blocks

    # Write to Redis buffer
    offset_next = int(await r.setrange(image_buffer_key, offset, frag))
    reassembled_offset = offset_next
    
    if len(missing_blocks) > 0:
        for b in missing_blocks:
            if b[0] < reassembled_offset: reassembled_offset = b[0]
        await r.set(missing_blocks_key, json.dumps(missing_blocks), ex=86400)
        
        # Request missing fragment
        missing_min = min(missing_blocks, key=lambda x: x[0])
        print(f"[{TAG}:{dev_eui}:{sense_time}] Packet loss detected. Missing: {missing_min[0]}~{missing_min[1]}")
        frag_req = raw[1:6] + (missing_min[0]).to_bytes(3, 'little') + (missing_min[1]).to_bytes(3, 'little')
        await send_downlink(app_id, dev_eui, 4, frag_req)
    else:
        await r.delete(missing_blocks_key)

    # Update metadata
    meta.append({'fCnt': fcnt, 'time': datetime.now().isoformat()})
    
    # Save reassembly state back to Redis
    new_state = {
        'received': offset_next,
        'total_size': total_size,
        'meta_total': meta
    }
    await r.set(state_key, json.dumps(new_state), ex=3600)

    # Image conversion logic
    rtsp_buffer_key = f"ImageToRtsp:{dev_eui}:image"
    rtsp_timestamp_key = f"ImageToRtsp:{dev_eui}:sense_time"

    if not (first_frag and last_frag):
        image_data = await r.get(image_buffer_key)
        if image_data:
            image_data = image_data[:reassembled_offset]
            try:
                img = Image.open(io.BytesIO(image_data))
                f = io.BytesIO()
                img.save(f, 'JPEG')
                jpeg_reassembled = f.getvalue()
                
                await r.set(rtsp_buffer_key, jpeg_reassembled, ex=86400)
                await r.set(rtsp_timestamp_key, sense_time, ex=86400)

                if total_size <= offset_next and len(missing_blocks) == 0:
                    print(f"[{TAG}:{dev_eui}:{sense_time}] Image reassembly completed. Size:{len(jpeg_reassembled)}")
                    
                    # Store as the last completed image
                    await r.set(rtsp_buffer_key + ":last", jpeg_reassembled, ex=86400)
                    await r.set(rtsp_timestamp_key + ":last", sense_time, ex=86400)
                    
                    await r.set(f"PP:EdgeEye:completed:{dev_eui}:{epoch}", 1, ex=86400)
                    await send_downlink(app_id, dev_eui, 4, epoch.to_bytes(5, 'little'))
                    await r.delete(image_buffer_key)
                    await r.delete(state_key)
            except Exception as e:
                if last_frag:
                    print(f"[{TAG}:{dev_eui}:{sense_time}] Final image process error: {e}")

    await r.delete(mutex_key)
    await r.aclose()
    return message
