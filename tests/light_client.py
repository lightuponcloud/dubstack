"""
LightUpon.cloud client for making HTTP requests.
Server source code is available by the following URL.
https://github.com/lightuponcloud/dubstack
"""
import os
import hashlib
import requests
from base64 import b64encode
import json

from dvvset import DVVSet


class LightClient:
    """
    LightUpon.cloud client.

    ``url`` -- The base upload API endpoint.
               For example "http://127.0.0.1:8082/"
    """
    FILE_UPLOAD_CHUNK_SIZE = 2000000

    def __init__(self, url, username, password):
        if url.endswith('/'):
            self.url = url
        else:
            self.url = "{}/".format(url)

        self.login(username, password)

    def login(self, username, password):
        """
        Tries to exchange username and password to authentication token,
        used to perform all the further requests.
        """
        creds = {"login": username, "password": password}
        url = "{}riak/login".format(self.url)
        response = requests.post(url, data=json.dumps(creds),
                                 headers={'content-type': 'application/json'})
        data = response.json()
        self.token = data['token']
        self.user_id = data['id']

    def _increment_version(self, last_seen_version, modified_utc):
        """
        Increments provided version or creates a new one, if not provided.

        ``last_seen_version`` -- casual version vector value.
                                 It should be encoded as base64(json(value))
        ``modified_utc`` -- it is used to display modified time in web UI.
        """
        if not last_seen_version:
            dvvset = DVVSet()
            dot = dvvset.create(dvvset.new(modified_utc), self.user_id)
            version = b64encode(json.dumps(dot).encode())
        else:
            # increment version
            context = dvvset.join(last_seen_version)
            new_dot = dvvset.update(dvvset.new_with_history(context, modified_utc),
                                    dot, self.user_id)
            version = dvvset.sync([last_seen_version, new_dot])
            version = b64encode(json.dumps(version)).encode()
        return version

    def upload_part(self, bucket_id, prefix, fn, chunk, file_size, part_num,
                    guid, upload_id, version, md5_list):
        md5 = hashlib.md5(chunk)
        md5_digest = md5.hexdigest()
        md5_list.append(md5_digest)
        multipart_form_data = {
            'prefix': prefix,
            'files[]': (fn, ''),  # first try empty request
            'md5': md5_digest,
            'guid': guid,
            'version': version
        }
        chunk_size = len(chunk)
        if file_size > self.FILE_UPLOAD_CHUNK_SIZE:
            offset = (part_num-1) * self.FILE_UPLOAD_CHUNK_SIZE
            limit = offset+chunk_size-1
            if limit < 0:
                limit = 0
            ct_range = "bytes {}-{}/{}".format(offset, limit, file_size)
        else:
            ct_range = "bytes 0-{}/{}".format(file_size-1, file_size)
        headers = {
            'accept': 'application/json',
            'authorization': 'Token {}'.format(self.token),
            'content-range': ct_range
        }
        if offset+chunk_size == file_size:
            # last chunk
            etags = ",".join(["{},{}".format(i+1, md5_list[i]) for i in range(len(md5_list))])
            multipart_form_data.update({
                'etags[]': etags
            })

        if part_num == 1:
            r_url = "{}riak/upload/{}/".format(self.url, bucket_id)
        else:
            r_url = "{}riak/upload/{}/{}/{}/".format(self.url, bucket_id, upload_id, part_num)
        # send request without binary data first
        response = requests.post(r_url, files=multipart_form_data, headers=headers)
        if response.status_code == 206:
            # skip chunk upload, as server has it aleady
            response_json = response.json()
            upload_id = response_json['upload_id']
            guid = response_json['guid']
            end_byte = response_json['end_byte']
            part_num += 1
            if offset+chunk_size == file_size:
                response_json.update({'md5_list': md5_list, 'part_num': part_num, 'end_byte': end_byte})
                return response_json
            else:
                return {'guid': guid, 'upload_id': upload_id, 'end_byte': end_byte,
                        'md5_list': md5_list, 'part_num': part_num}
        if response.status_code != 200:
            return {'error': response.json()}
        response_json = response.json()

        upload_id = response_json['upload_id']
        guid = response_json['guid'] # server could change GUID
        server_md5 = response_json['md5']
        if md5_digest != server_md5:
            return {'error': 'md5 mismatch'}

        # upload an actual data now
        multipart_form_data.update({
            'files[]': (fn, chunk),
            'guid': guid  # GUID could change
        })
        response = requests.post(r_url, files=multipart_form_data, headers=headers)
        if response.status_code != 200:
            return {'error': response.json()}
        response_json = response.json()
        end_byte = response_json['end_byte']
        if offset+chunk_size == file_size:
            # the last chunk has been processed, expect complete_upload response
            response_json.update({'md5_list': md5_list, 'part_num': part_num, 'end_byte': end_byte})
            return response_json
        else:
            server_md5 = response_json['md5']
            if md5_digest != server_md5:
                return {'error': 'md5 mismatch'}
            upload_id = response_json['upload_id']
            part_num += 1
        return {'guid': guid, 'upload_id': upload_id, 'end_byte': end_byte,
                'md5_list': md5_list, 'part_num': part_num}

    def upload(self, bucket_id, file_name, prefix='', guid='', last_seen_version=None):
        """
        Uploads file to server by splitting it to chunks and testing if server
        has chunk already, before actual upload.
        ``fn`` -- filename to upload
        ``fd`` -- file descriptor to read contents from
        ``prefix`` -- pseudo-directory on server. It must be encoded as hex string.
        ``guid`` -- unique identifier ( UUID4 ) for tracking history of changes
        ``last_seen_version`` -- casual history value, generated by DVVSet()
        """
        stat = os.stat(file_name)
        file_size = stat.st_size

        modified_utc = str(int(stat.st_mtime))
        version = self._increment_version(last_seen_version, modified_utc)

        md5_list = []
        result = None
        with open(file_name, 'rb') as fd:
            _read_chunk = lambda: fd.read(self.FILE_UPLOAD_CHUNK_SIZE)
            part_num = 1
            upload_id = None
            for chunk in iter(_read_chunk, ''):
                # avoiding recursion as there might be 1000's of parts
                result = self.upload_part(bucket_id, prefix, file_name, chunk,
                                          file_size, part_num, guid, upload_id, version, md5_list)
                if result and 'error' in result:
                    break
                upload_id = result['upload_id']
                guid = result['guid']
                part_num = result['part_num']
                md5_list = result['md5_list']
                end_byte = result['end_byte']
                if end_byte+1 == file_size:
                    break
        return result
