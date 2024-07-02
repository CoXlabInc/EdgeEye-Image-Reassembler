import base64
from datetime import datetime, timedelta
import json
import pyiotown.post_process
import pyiotown.get
import pyiotown.delete
import pyiotown.post
import redis
from urllib.parse import urlparse
import io
from PIL import ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True
from PIL import Image, ImageDraw
import sys

TAG = 'EdgeEye'

def init(url, pp_name, mqtt_url, redis_url, dry_run=False):
    global iotown_url, iotown_token
    
    url_parsed = urlparse(url)
    iotown_url = f"{url_parsed.scheme}://{url_parsed.hostname}" + (f":{url_parsed.port}" if url_parsed.port is not None else "")
    iotown_token = url_parsed.password
    
    if redis_url is None:
        print(f"Redis is required for EdgeEye.")
        return None

    global r
    
    try:
        r = redis.from_url(redis_url)
        if r.ping() == False:
            r = None
            raise Exception('Redis connection failed')
    except Exception as e:
        print(redis_url)
        raise(e)
    
    return pyiotown.post_process.connect_common(url, pp_name, post_process, mqtt_url, dry_run=dry_run)
    
def post_process(message, param=None):
    fport = [1, 2, 3]

    if message['meta'].get('fPort') not in fport:
        return message
    
    raw = message['meta'].get('raw')
    
    if raw is None:
        return message

    #MUTEX
    mutex_key = f"PP:EdgeEye:MUTEX:{message['grpid']}:{message['nid']}:{message['key']}"
    lock = r.set(mutex_key, 'lock', ex=30, nx=True)
    print(f"[{TAG}] lock with '{mutex_key}': {lock}")
    if lock != True:
        return None

    message['data']['image'] = None
    message['data']['error'] = ''
    message['data']['meta_total'] = []
    
    fport = message['meta'].get('fPort')
    raw = base64.b64decode(raw)

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
            else:
                message['data']['error_sub'] = raw[1]
        else:
            message['data']['error'] = f"Unknown fail ({raw[0]})"
        return message
    elif fport != 1:
        message['data']['error'] = f"Not supported FPort ({fport})"
        return message
    
    #TODO length check

    fcnt = message['meta'].get('fCnt')
    flags = raw[0]
    epoch = int.from_bytes(raw[1:6], 'little', signed=False)
    offset = int.from_bytes(raw[6:9], 'little', signed=False)

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

    result = pyiotown.get.storage(iotown_url, iotown_token,
                                  message['nid'],
                                  group_id=message['grpid'],
                                  count=10,
                                  verify=False)
    prev_data = None
    for i in range(len(result['data'])):
        if result['data'][i]['value'].get('fPort') == fport and result['data'][i]['value'].get('sense_time') == sense_time:
            prev_data = result['data'][i]['value']
            prev_data_id = result['data'][i]['_id']
            #print(f"[{TAG}] prev: {result}")
            
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
    if first_frag:
        total_size = offset
        offset = 0
    elif total_size is None:
        # to keep backward compatible
        total_size_key = f"PP:EdgeEye:size:{message['nid']}:{epoch}"
        total_size = r.get(total_size_key)
        if total_size is not None:
            total_size = int(total_size)
        else:
            print(f"[{TAG}] GET '{total_size_key}' returned None")
            offset_next = 0
            total_size = 0

    if offset > offset_next:
        result = pyiotown.get.command(iotown_url, iotown_token, message['nid'],
                                      group_id=message['grpid'], verify=False)
        command_status = result.get('command')
        print(f"[{TAG}] There was packet loss. (nid:{message['nid']}, fcnt:{fcnt}, offset {offset_next} is expected but {offset})")
        if command_status is not None and len(command_status) == 0:            
            frag_req = raw[1:6] + (offset_next).to_bytes(3, byteorder='little', signed=False)
            pyiotown.post.command(iotown_url, iotown_token,
                                  message['nid'],
                                  frag_req,
                                  lorawan={ 'f_port': 4 },    # fragment request
                                  group_id=message['grpid'],
                                  verify=False)
        r.delete(mutex_key)
        return None

    l = message['meta']
    del l['raw']
    meta.append(l)

    message['data']['meta_total'] = meta
    
    image_buffer_key = f"PP:EdgeEye:buffer:{message['nid']}:{epoch}"
    rtsp_buffer_key = f"ImageToRtsp:{message['nid']}:image"
    rtsp_timestamp_key = f"ImageToRtsp:{message['nid']}:sense_time"
    rtsp_last_buffer_key = rtsp_buffer_key + ':last'
    rtsp_last_timestamp_key = rtsp_timestamp_key + ':last'

    last_frag = ((flags & (1 << 1)) != 0)
    if last_frag:
        image = r.get(image_buffer_key)
        if image is not None:
            image += frag

            try:
                image = Image.open(io.BytesIO(image))
            except Exception as e:
                print(f"[{TAG}] open image error '{e}'", file=sys.stderr)
                image = None

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
        
                r.set(rtsp_last_buffer_key, jpeg_completed, timedelta(hours=24))
                r.set(rtsp_last_timestamp_key, sense_time, timedelta(hours=24))
                r.copy(rtsp_last_buffer_key, rtsp_buffer_key, replace=True)
                r.expire(rtsp_buffer_key, timedelta(hours=24))
                r.set(rtsp_timestamp_key, sense_time, timedelta(hours=24))
            
            r.delete(image_buffer_key)
            print(f"[{TAG}] image reassembly completed (nid:{message['nid']}, fcnt:{fcnt}, size:{len(jpeg_completed)})")
    else:
        r.setrange(image_buffer_key, offset, frag)
        offset += len(frag)

        jpeg_raw = r.get(image_buffer_key)

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
            
            r.set(rtsp_buffer_key, jpeg_reassembled, timedelta(hours=24))
            r.set(rtsp_timestamp_key, sense_time, timedelta(hours=24))
        else:
            message['data']['image'] = {
                'raw': jpeg_raw,
                'file_type': 'image',
                'file_ext': 'jpeg',
                'file_size': len(jpeg_raw),
            }

        message['data']['received'] = offset
        message['data']['total_size'] = total_size

        r.expire(image_buffer_key, timedelta(hours=1))
        print(f"[{TAG}] image reassembly in progress (nid:{message['nid']}, fcnt:{fcnt}, +{len(frag)} bytes, {offset}/{total_size} ({(offset / total_size * 100) if total_size > 0 else 0:.2f}%))")

    r.delete(mutex_key)
    
    if prev_data is not None:
        result = pyiotown.delete.data(iotown_url, iotown_token, _id=prev_data_id, group_id=message['grpid'], verify=False)
        #print(f"[{TAG}] delete prev data _id:${prev_data_id}: {result}")
        
    return message
