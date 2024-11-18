import base64
from datetime import datetime, timedelta
import time
import json
import pyiotown.post_process
import pyiotown.get
import pyiotown.delete
import pyiotown.post
import redis.asyncio as redis
from urllib.parse import urlparse
import io
from PIL import ImageFile
from PIL import Image
import sys
import asyncio
import aiohttp
import threading
import jwt
import secrets

TAG = 'EdgeEye'
ImageFile.LOAD_TRUNCATED_IMAGES = True

def init(url, pp_name, mqtt_url, redis_url, chirpstack=None, dry_run=False):
    global iotown_url, iotown_token
    
    url_parsed = urlparse(url)
    iotown_url = f"{url_parsed.scheme}://{url_parsed.hostname}" + (f":{url_parsed.port}" if url_parsed.port is not None else "")
    iotown_token = url_parsed.password
    
    if redis_url is None:
        print(f"Redis is required for EdgeEye.")
        return None

    global pool
    pool = redis.ConnectionPool.from_url(redis_url)

    global event_loop
    event_loop = asyncio.new_event_loop()

    def event_loop_thread():
        event_loop.run_forever()
    threading.Thread(target=event_loop_thread, daemon=True).start()
    
    global chirp
    chirp = None
    if chirpstack is not None:
        url_parsed = urlparse(chirpstack.get('url'))

        chirp = {
            'url': f"{url_parsed.scheme}://{url_parsed.hostname}" + (f":{url_parsed.port}" if url_parsed.port is not None else ""),
            'secret': base64.b64decode(chirpstack.get('secret')),
            'username': url_parsed.username,
            'password': url_parsed.password
        }
        #print(f"Chirpstack is used: {chirp}")
        def logged_in(future):
            token = future.result()
            if token is not None:
                print("Login to chirpstack success")
            else:
                print("Login to chirpstack failed")
        
        asyncio.run_coroutine_threadsafe(chirpstack_login(), event_loop).add_done_callback(logged_in)

    return pyiotown.post_process.connect_common(url, pp_name, post_process, mqtt_url=mqtt_url, dry_run=dry_run)

async def chirpstack_login():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=True, verify_ssl=False)) as session:
        async with session.post(chirp['url'] + '/api/internal/login',
                                headers = {
                                    'Content-Type': 'application/json',
                                    'Accept': 'application/json',
                                    'Grpc-Metadata-Authorization': jwt.encode({
                                        'iss': 'chirpstack-application-server',
                                        'aud': 'chirpstack-application-server',
                                        'nbf': int(time.time()),
                                        'exp': int(time.time()) + 60,
                                        'sub': 'EdgeEye-PP',
                                        'username': chirp['username'],
                                    }, chirp['secret'], algorithm='HS256')
                                },
                                json = {
                                    'email': chirp['username'],
                                    'password': chirp['password']
                                }) as response:
            content = await response.text()

            try:
                content = json.loads(content)
            except:
                pass
            
            if response.status == 200:
                return content['jwt']
            else:
                return None

async def create_new_session():
    token = await chirpstack_login()
    dev_eui = '70b3d5df1fff' + secrets.token_hex(2)
    payload = {
        "device": {
            "applicationID": "8", #TODO how to get automatically ?
            "description": dev_eui,
            "devEUI": dev_eui,
            "deviceProfileID": "465ec6b0-11ab-486b-b6d7-5db69f504c02", #TODO how to get automatically?
            "isDisabled": False,
            "name": dev_eui,
            "referenceAltitude": 0,
            "skipFCntCheck": False,
            "tags": {},
            "variables": {}
        }
    }
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=True, verify_ssl=False)) as session:
        async with session.post(chirp['url'] + '/api/devices',
                                headers={
                                    'Content-Type': 'application/json',
                                    'Accept': 'application/json',
                                    'Grpc-Metadata-Authorization': 'Bearer ' + token
                                },
                                json=payload) as response:
            content = await response.text()

            try:
                content = json.loads(content)
            except:
                pass
            
            if response.status != 200:
                print(content)
                return None

        nwkSKey = secrets.token_hex(16)
        device_activation = {
            "aFCntDown": 0,
            "appSKey": secrets.token_hex(16),
            "devAddr": secrets.token_hex(4),
            "devEUI": dev_eui,
            "fCntUp": 0,
            "fNwkSIntKey": nwkSKey,
            "nFCntDown": 0,
            "nwkSEncKey": nwkSKey,
            "sNwkSIntKey": nwkSKey
        }
        async with session.post(chirp['url'] + f"/api/devices/{dev_eui}/activate",
                                headers={
                                    'Content-Type': 'application/json',
                                    'Accept': 'application/json',
                                    'Grpc-Metadata-Authorization': 'Bearer ' + token
                                },
                                json={
                                    "deviceActivation": device_activation
                                }) as response:
            content = await response.text()

            try:
                content = json.loads(content)
            except:
                pass
            
            if response.status == 200:
                session = {
                    'dev_eui': dev_eui,
                    'dev_addr': device_activation['devAddr'],
                    'appskey': device_activation['appSKey'],
                    'nwkskey': nwkSKey
                }
                return session
            else:
                print(content)

        async with session.delete(chirp['url'] + f"/api/devices/{dev_eui}",
                                headers={
                                    'Accept': 'application/json',
                                    'Grpc-Metadata-Authorization': 'Bearer ' + token
                                }) as response:
            content = await response.text()

            try:
                content = json.loads(content)
            except:
                pass
            
            if response.status != 200:
                print(content)
            
    return None

async def get_existing_session(dev_eui):
    token = await chirpstack_login()
    print(f"dev_eui:{dev_eui}")
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=True, verify_ssl=False)) as session:
        async with session.get(chirp['url'] + f'/api/devices/{dev_eui}/activation',
                               headers={
                                   'Accept': 'application/json',
                                   'Grpc-Metadata-Authorization': 'Bearer ' + token
                               }) as response:
            content = await response.text()

            try:
                content = json.loads(content)
            except:
                pass
            
            if response.status == 200:
                session = {
                    'dev_eui': content['deviceActivation']['devEUI'],
                    'dev_addr': content['deviceActivation']['devAddr'],
                    'appskey': content['deviceActivation']['appSKey'],
                    'nwkskey': content['deviceActivation']['fNwkSIntKey']
                }
                return session
            else:
                print(content)
                return None
    
async def async_post_process(message):
    r = redis.Redis(connection_pool=pool)
    
    #MUTEX
    mutex_key = f"PP:EdgeEye:MUTEX:{message['grpid']}:{message['nid']}:{message['key']}"
    lock = await r.set(mutex_key, 'lock', ex=30, nx=True)
    print(f"[{TAG}:{message['nid']}] lock with '{mutex_key}': {lock}")
    if lock != True:
        await r.aclose()
        return None

    message['data']['image'] = None
    message['data']['error'] = ''
    message['data']['meta_total'] = []
    
    fport = message['meta'].get('fPort')
    raw = base64.b64decode(message['meta']['raw'])

    if fport == 2:
        # Fail Report
        if raw[0] == 0:
            message['data']['error'] = "Camera boot failed"
            if raw[1] == 1:
                message['data']['error_sub'] = 'File system error'
            elif raw[1] == 2:
                message['data']['error_sub'] = 'Memory error'
            elif raw[1] == 3:
                message['data']['error_sub'] = 'Sensor error'
            else:
                message['data']['error_sub'] = raw[1]
        elif raw[0] == 1:
            message['data']['error'] = "Camera snap failed"
            if raw[1] == 0:
                message['data']['error_sub'] = 'Memory error'
            elif raw[1] == 1:
                message['data']['error_sub'] = 'File system error'
            elif raw[1] == 2:
                message['data']['error_sub'] = 'Encoding error'
            else:
                message['data']['error_sub'] = raw[1]
        elif raw[0] == 2:
            message['data']['error'] = "Send failed"
            if raw[1] == 1:
                message['data']['error_sub'] = 'File system error'
            elif raw[1] == 2:
                message['data']['error_sub'] = 'Too busy'
            elif raw[1] == 3:
                message['data']['error_sub'] = 'User interrupt'
            else:
                message['data']['error_sub'] = raw[1]
        else:
            message['data']['error'] = f"Unknown fail ({raw[0]})"
        await r.delete(mutex_key)
        await r.aclose()
        return message
    elif fport != 1:
        message['data']['error'] = f"Not supported FPort ({fport})"
        await r.delete(mutex_key)
        await r.aclose()
        return message
    
    #TODO length check

    fcnt = message['meta'].get('fCnt')
    flags = raw[0]
    epoch = int.from_bytes(raw[1:6], 'little', signed=False)
    offset = int.from_bytes(raw[6:9], 'little', signed=False)

    total_size_key = f"PP:EdgeEye:size:{message['nid']}:{epoch}"
    missing_blocks_key = f"PP:EdgeEye:missing:{message['nid']}:{epoch}"

    i = 9
    
    if (flags & (1 << 2)) == 0:
        sysv = None
    else:
        sysv = int.from_bytes(raw[i:i+2], 'little', signed=False) / 1000.0
        i += 2

    if (flags & (1 << 3)) == 0:
        als = None
    else:
        als = int.from_bytes(raw[i:i+3], 'little', signed=False)
        i += 3

    frag = raw[i:]

    sense_time = datetime.utcfromtimestamp(epoch).isoformat() + 'Z'
    message['data']['sense_time'] = sense_time
    message['data']['system_voltage'] = sysv
    message['data']['ambient_light_lux'] = als

    success, result = await pyiotown.get.async_storage(iotown_url, iotown_token,
                                                       message['nid'],
                                                       group_id=message['grpid'],
                                                       count=10,
                                                       verify=False)
    prev_data = None
    if success == True:
        for i in range(len(result['data'])):
            if result['data'][i]['value'].get('fPort') == fport and result['data'][i]['value'].get('sense_time') == sense_time:
                prev_data = result['data'][i]['value']
                prev_data_id = result['data'][i]['_id']
                
                offset_next = prev_data.get('received')
                if offset_next is None:
                    offset_next = 0
                
                if message['data'].get('system_voltage') is None:
                    message['data']['system_voltage'] = prev_data.get('system_voltage')
                if message['data'].get('ambient_light_lux') is None:
                    message['data']['ambient_light_lux'] = prev_data.get('ambient_light_lux')

                total_size = prev_data.get('total_size')

                meta = prev_data.get('meta_total')
                if meta is None:
                    meta = []
                break

    if prev_data is None:
        offset_next = 0
        total_size = None
        meta = []

    first_frag = ((flags & (1 << 0)) != 0)
    last_frag = ((flags & (1 << 1)) != 0)

    if first_frag:
        total_size = offset
        offset = 0
    elif total_size is None:
        # to keep backward compatible
        total_size = await r.get(total_size_key)
        if total_size is not None:
            total_size = int(total_size)
        else:
            print(f"[{TAG}:{message['nid']}] GET '{total_size_key}' returned None")
            offset_next = 0
            total_size = 0

    if not first_frag or not last_frag:
        sessions_key = f"PP:EdgeEye:sessions:{message['nid']}:{epoch}"
        sessions = await r.smembers(sessions_key)
        print(f"[{TAG}:{message['nid']}:{sense_time}] assigned sessions: {sessions}")
        if len(sessions) == 0:
            session = await create_new_session()
            if session is not None:
                await r.sadd(sessions_key, session['dev_eui'])

                session_usage_key = f"PP:EdgeEye:sessions:{message['nid']}:{epoch}:session:{session['dev_eui']}"
                await r.set(session_usage_key, 0, ex=10)
                
                #Send the additional session to the device.
                dev_addr = bytearray.fromhex(session['dev_addr'])
                dev_addr.reverse()
                nwkskey = bytes.fromhex(session['nwkskey'])
                appskey = bytes.fromhex(session['appskey'])
                await pyiotown.post.async_command(iotown_url, iotown_token,
                                                  message['nid'],
                                                  raw[1:6] + dev_addr + nwkskey + appskey,
                                                  lorawan={ 'f_port': 5, 'confirmed': False },
                                                  group_id=message['grpid'],
                                                  verify=False)
            else:
                print(f"[{TAG}:{message['nid']}:{sense_time}] creating session failed")
        else:
            dev_eui = str(sessions.pop(), 'utf-8') #TODO Currently only one session is supported.
            session_usage_key = f"PP:EdgeEye:sessions:{message['nid']}:{epoch}:session:{dev_eui}"
            if await r.get(session_usage_key) is None:
                print(f"[{TAG}:{message['nid']}:{sense_time}] resend the session")
                session = await get_existing_session(dev_eui)
                await r.set(session_usage_key, 0, ex=10)

                #It seems the additional session information is not reached out to the device.
                dev_addr = bytearray.fromhex(session['dev_addr'])
                dev_addr.reverse()
                nwkskey = bytes.fromhex(session['nwkskey'])
                appskey = bytes.fromhex(session['appskey'])
                await pyiotown.post.async_command(iotown_url, iotown_token,
                                                  message['nid'],
                                                  raw[1:6] + dev_addr + nwkskey + appskey,
                                                  lorawan={ 'f_port': 5, 'confirmed': False },
                                                  group_id=message['grpid'],
                                                  verify=False)
        
    missing_blocks = await r.get(missing_blocks_key)
    try:
        missing_blocks = json.loads(missing_blocks)
    except:
        missing_blocks = []

    offset_end = offset + len(frag)

    print(f"[{TAG}:{message['nid']}:{sense_time}] current(first:{first_frag}):{offset}~{offset_end}, max pos:{offset_next}, total size:{total_size}")
    if offset > offset_next:
        print(f"[{TAG}:{message['nid']}:{sense_time}] {offset_next} expected but {offset}. add a missing block")
        if (offset_next, offset) not in missing_blocks:
            missing_blocks.append((offset_next, offset))
            await r.set(missing_blocks_key, json.dumps(missing_blocks), timedelta(hours=24))
        else:
            await r.expire(missing_blocks_key, timedelta(hours=24))
    elif offset < offset_next:
        # remove missing blocks
        updated_missing_blocks = []
        for b in missing_blocks:
            print(f"[{TAG}:{message['nid']}:{sense_time}] missing:{b[0]}~{b[1]}, current:{offset}~{offset_end} => ", end="")
            if offset <= b[0] and offset_end >= b[1]:
                # The current includes the missing block
                print("Found!")
                continue
            elif offset <= b[0] and b[0] < offset_end and offset_end < b[1]:
                # shrinks head
                print("shrinks head")
                b[0] = offset_end
                updated_missing_blocks.append(b)
            elif b[0] < offset and offset < b[1] and b[1] <= offset_end:
                # shrinks tail
                print("shirnks tail")
                b[1] = offset
                updated_missing_blocks.append(b)
            elif offset > b[0] and offset_end < b[1]:
                # splits
                print("split")
                c = [offset_end, b[1]]
                updated_missing_blocks.append(c)
                b[1] = offset
                updated_missing_blocks.append(b)
            else:
                print("out of range")
                # out of range
                updated_missing_blocks.append(b)
        missing_blocks = updated_missing_blocks
        if len(missing_blocks) > 0:
            await r.set(missing_blocks_key, json.dumps(missing_blocks), timedelta(hours=24))
        else:
            await r.delete(missing_blocks_key)

    reassembled_offset = offset_next + len(frag)

    if len(missing_blocks) > 0:
        success, result = await pyiotown.get.async_command(iotown_url, iotown_token, message['nid'],
                                                           group_id=message['grpid'], verify=False)
        if success == True:
            command_status = result.get('command')

            missing_min = 0
            for b in range(len(missing_blocks)):
                if missing_blocks[b][0] < missing_blocks[missing_min][0]:
                    missing_min = b

            missing_min = missing_blocks[missing_min]
            
            print(f"[{TAG}:{message['nid']}:{sense_time}] There was packet loss. (fcnt:{fcnt}, missing:{missing_min[0]}~{missing_min[1]}, total:{total_size})")
            if command_status is not None and len(command_status) == 0:
                frag_req = raw[1:6]
                # if total_size == 0:
                #     frag_req += b'\x00\x00\x00'
                # else:
                frag_req += ((missing_min[0]).to_bytes(3, byteorder='little', signed=False) +
                             (missing_min[1]).to_bytes(3, byteorder='little', signed=False))
                await pyiotown.post.async_command(iotown_url, iotown_token,
                                                  message['nid'],
                                                  frag_req,
                                                  lorawan={ 'f_port': 4, 'confirmed': False },    # fragment request
                                                  group_id=message['grpid'],
                                                  verify=False)
            # else:
            #     print(command_status)

        for b in missing_blocks:
            if b[0] < reassembled_offset:
                reassembled_offset = b[0]

    l = message['meta']
    del l['raw']
    meta.append(l)

    message['data']['meta_total'] = meta

    if len(meta) > 1:
        fcnts = list(range(meta[0]['fCnt'], meta[-1]['fCnt'] + 1))
        fcnts_missing = fcnts.copy()
        for m in meta:
            fcnts_missing.remove(m['fCnt'])
        message['data']['prr'] = (len(fcnts) - len(fcnts_missing)) / len(fcnts) * 100

    start_time = datetime.strptime(sense_time, '%Y-%m-%dT%H:%M:%SZ')
    end_time = datetime.now()
    message['data']['sec_taken'] = end_time.timestamp() - start_time.timestamp()
    
    image_buffer_key = f"PP:EdgeEye:buffer:{message['nid']}:{epoch}"
    rtsp_buffer_key = f"ImageToRtsp:{message['nid']}:image"
    rtsp_timestamp_key = f"ImageToRtsp:{message['nid']}:sense_time"
    rtsp_last_buffer_key = rtsp_buffer_key + ':last'
    rtsp_last_timestamp_key = rtsp_timestamp_key + ':last'

    if last_frag:
        if first_frag == False:
            await r.setrange(image_buffer_key, offset, frag)

            image = (await r.get(image_buffer_key))[:reassembled_offset]

            try:
                image = Image.open(io.BytesIO(image))
            except Exception as e:
                print(f"[{TAG}:{message['nid']}:{sense_time}] open image error '{e}'", file=sys.stderr)
                image = None

            jpeg_completed = bytes()
            if image is not None:
                #Last reassembled
                f = io.BytesIO()
                image.save(f, 'JPEG')
                jpeg_completed = f.getvalue()
                message['data']['image'] = {
                    'raw': jpeg_completed,
                    'file_type': 'image',
                    'file_ext': 'jpeg',
                    'file_size': len(jpeg_completed),
                }

            await r.set(rtsp_buffer_key, jpeg_completed, timedelta(hours=24))
            await r.set(rtsp_timestamp_key, sense_time, timedelta(hours=24))

            if len(missing_blocks) == 0:
                await r.set(rtsp_last_buffer_key, jpeg_completed, timedelta(hours=24))
                await r.set(rtsp_last_timestamp_key, sense_time, timedelta(hours=24))
                await r.copy(rtsp_last_buffer_key, rtsp_buffer_key, replace=True)

                await r.delete(missing_blocks_key)
                await r.delete(image_buffer_key)
                print(f"[{TAG}:{message['nid']}:{sense_time}] image reassembly completed (fcnt:{fcnt}, size:{len(jpeg_completed)})")
    else:
        await r.setrange(image_buffer_key, offset, frag)

        jpeg_raw = (await r.get(image_buffer_key))[:reassembled_offset]

        try:
            image = Image.open(io.BytesIO(jpeg_raw))
        except Exception as e:
            print(f"[{TAG}] open image error '{e}'", file=sys.stderr)
            image = None

        if image is not None:
            #Being reassembled
            f = io.BytesIO()
            image.save(f, 'JPEG')
            jpeg_reassembled = f.getvalue()
            message['data']['image'] = {
                'raw': jpeg_reassembled,
                'file_type': 'image',
                'file_ext': 'jpeg',
                'file_size': len(jpeg_reassembled),
            }
            
            await r.set(rtsp_buffer_key, jpeg_reassembled, timedelta(hours=24))
            await r.set(rtsp_timestamp_key, sense_time, timedelta(hours=24))
        else:
            message['data']['image'] = {
                'raw': jpeg_raw,
                'file_type': 'image',
                'file_ext': 'jpeg',
                'file_size': len(jpeg_raw),
            }

        message['data']['received'] = offset_end if offset_end > offset_next else offset_next
        message['data']['reassembled'] = reassembled_offset
        message['data']['total_size'] = total_size

        await r.expire(image_buffer_key, timedelta(hours=1))
        print(f"[{TAG}:{message['nid']}:{sense_time}] image reassembly in progress (fcnt:{fcnt}, +{len(frag)} bytes, {reassembled_offset}/{total_size} ({(reassembled_offset / total_size * 100) if total_size > 0 else 0:.2f}%))")

    await r.delete(mutex_key)
    
    if prev_data is not None:
        result = pyiotown.delete.data(iotown_url, iotown_token, _id=prev_data_id, group_id=message['grpid'], verify=False)
        #print(f"[{TAG}] delete prev data _id:${prev_data_id}: {result}")
        
    await r.aclose()
    return message
    
def post_process(message, param=None):
    fport = [1, 2, 3]

    if message['meta'].get('fPort') not in fport:
        return message
    
    raw = message['meta'].get('raw')
    
    if raw is None:
        return message

    return asyncio.run_coroutine_threadsafe(async_post_process(message), event_loop)
