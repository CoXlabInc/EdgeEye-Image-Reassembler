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
from PIL import Image

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
            message['data']['error'] = "Snap failed"
        else:
            message['data']['error'] = f"Unknown fail ({raw[0]})"
        return message
    elif fport != 1:
        message['data']['error'] = f"Not supported FPort ({fport})"
        return message
    
    #TODO length check

    fcnt = message['meta'].get('fCnt')
    epoch = int.from_bytes(raw[0:5], 'little', signed=False)
    offset = int.from_bytes(raw[6:8], 'little', signed=False)
    flags = raw[8]
    frag = raw[9:]

    epoch = int.from_bytes(raw[1:6], 'little', signed=False)
    sense_time = datetime.utcfromtimestamp(epoch).isoformat() + 'Z'

    message['data']['sense_time'] = sense_time

    result = pyiotown.get.storage(iotown_url, iotown_token,
                                  message['nid'],
                                  group_id=message['grpid'],
                                  count=1,
                                  verify=False)
    try:
        if result['data'][0]['value']['sense_time'] == sense_time:
            prev_data = result['data'][0]['value']
            prev_data_id = result['data'][0]['_id']
            #print(f"[{TAG}] prev: {result}")
            offset_next = prev_data.get('received')
            if offset_next is None:
                offset_next = 0
        else:
            prev_data = None
            offset_next = 0
    except:
        prev_data = None
        offset_next = 0

    total_size_key = f"PP:EdgeEye:size:{message['nid']}:{epoch}"
    first_frag = (((raw[0] >> 0) & (1 << 0)) != 0)
    if first_frag:
        offset = 0
        total_size = int.from_bytes(raw[6:9], 'little', signed=False)
        r.set(total_size_key, total_size)
    else:
        offset = int.from_bytes(raw[6:9], 'little', signed=False)
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

    meta_key = f"PP:EdgeEye:meta:{message['nid']}:{epoch}"
    meta = r.get(meta_key)
    if meta is None:
        meta = []
    else:
        meta = json.loads(meta.decode('ascii'))

    l = message['meta']
    del l['raw']
    meta.append(l)

    message['data']['meta_total'] = meta
    
    image_buffer_key = f"PP:EdgeEye:buffer:{message['nid']}:{epoch}"
    rtsp_buffer_key = f"ImageToRtsp:{message['nid']}:image:buffer"
    rtsp_last_buffer_key = rtsp_buffer_key + ':last'

    last_frag = (((raw[0] >> 0) & (1 << 1)) != 0)
    if last_frag:
        image = r.get(image_buffer_key)
        image += raw[9:]

        image = image_to_jpeg(image)
        
        r.set(rtsp_buffer_key, image, timedelta(hours=24))
        r.copy(image_buffer_key, rtsp_last_buffer_key, replace=True)
        r.expire(rtsp_last_buffer_key, timedelta(hours=24))

        if image is not None:
            message['data']['image'] = {
                'raw': image,
                'file_type': 'image',
                'file_ext': 'jpeg',
                'file_size': len(image),
            }
        
        r.delete(image_buffer_key)
        r.delete(meta_key)
        r.delete(total_size_key)
        print(f"[{TAG}] image reassembly completed (nid:{message['nid']}, fcnt:{fcnt}, size:{len(image)})")
    else:
        r.setrange(image_buffer_key, offset, raw[9:])
        offset += len(raw) - 9

        image = image_to_jpeg(r.get(image_buffer_key))

        if image is not None:
            message['data']['image'] = {
                'raw': image,
                'file_type': 'image',
                'file_ext': 'jpeg',
                'file_size': len(image),
            }
        message['data']['received'] = offset

        r.set(meta_key, json.dumps(meta))
        r.expire(image_buffer_key, timedelta(minutes=1))
        r.expire(meta_key, timedelta(minutes=1))
        r.expire(total_size_key, timedelta(minutes=1))
        print(f"[{TAG}] image reassembly in progress (nid:{message['nid']}, fcnt:{fcnt}, +{len(raw) - 9} bytes, {offset}/{total_size} ({(offset / total_size * 100) if total_size > 0 else 0}))")

        r.copy(image_buffer_key, rtsp_buffer_key, replace=True)
        r.expire(rtsp_buffer_key, timedelta(hours=24))

    r.delete(mutex_key)
    
    if prev_data is not None:
        result = pyiotown.delete.data(iotown_url, iotown_token, _id=prev_data_id, group_id=message['grpid'], verify=False)
        #print(f"[{TAG}] delete prev data _id:${prev_data_id}: {result}")
        
    return message

def image_to_jpeg(image_data):
    try:
        image = Image.open(io.BytesIO(image_data))
        f = io.BytesIO()
        image.save(f, "JPEG")
        return f.getvalue()
    except Exception as e:
        print(f"[{TAG}] open image error '{e}'")
        return None
