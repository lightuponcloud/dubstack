# -*- coding: utf-8 -*-

import logging
import json
import os
import io
import unittest
import hashlib
import xml.etree.ElementTree as ET
import requests
from base64 import b64encode
from environs import Env

from dvvset import DVVSet

import boto3
from botocore.config import Config
from botocore.utils import fix_s3_host

env = Env()
env.read_env('.env')

#logger = logging.getLogger(__name__)  # pylint: disable=invalid-name
for name in ['botocore', 's3transfer', 'boto3']:
    logging.getLogger(name).setLevel(logging.CRITICAL)

BASE_URL = env.str("BASE_URL", "https://lightupon.cloud")
USERNAME_1 = env.str("USERNAME_1")
PASSWORD_1 = env.str("PASSWORD_1")
TEST_BUCKET_1 = env.str("TEST_BUCKET_1")
TEST_BUCKET_2 = env.str("TEST_BUCKET_2")
UPLOADS_BUCKET_NAME = env.str("UPLOADS_BUCKET_NAME")

ACCESS_KEY = env.str("ACCESS_KEY")
SECRET_KEY = env.str("SECRET_KEY")
HTTP_PROXY = env.str("HTTP_PROXY")

RIAK_ACTION_LOG_FILENAME = ".riak_action_log.xml"

FILE_UPLOAD_CHUNK_SIZE = 2000000


def configure_boto3():
    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    resource = session.resource('s3', config=Config(proxies={'http': HTTP_PROXY}), use_ssl=False)
    resource.meta.client.meta.events.unregister('before-sign.s3', fix_s3_host)
    boto3.set_stream_logger('botocore')
    return resource


class TestClient(unittest.TestCase):
    def setUp(self):

        creds = {"login": USERNAME_1, "password": PASSWORD_1}
        response = requests.post("{}/riak/login".format(BASE_URL), data=json.dumps(creds),
                                 headers={'content-type': 'application/json'})
        data = response.json()
        self.token = data['token']
        self.user_id = data['id']
        # self.resource = configure_boto3()
        # self.purge_test_buckets()

    def get_json(self, url, status=200, **kwargs):
        response = requests.get(url, headers={'content-type': 'application/json',
                                          'authorization': 'Token {}'.format(self.token)}, **kwargs)
        assert response.status_code == status
        assert response.headers['content-type'] == 'application/json'
        return response.json()

    def post_json(self, url, data, status=200, **kwargs):
        response = requests.post(url, data=json.dumps(data),
                                 headers={'content-type': 'application/json',
                                          'authorization': 'Token {}'.format(self.token)})
        assert response.status_code == status
        return json.loads(response.data.decode('utf8'))

    def patch_json(self, url, data, status=200, **kwargs):
        response = self.patch(url, data=json.dumps(data),
                              headers={'content-type': 'application/json'})
        assert response.status_code == status
        assert response.content_type == 'application/json'
        return json.loads(response.data.decode('utf8'))

    def delete_json(self, url, data, status=200, **kwargs):
        response = requests.delete(url, data=json.dumps(data),
                                   headers={'content-type': 'application/json',
                                            'authorization': 'Token {}'.format(self.token)})
        assert response.status_code == status
        assert response.headers['content-type'] == 'application/json'
        return response.json()

    def upload_file(self, url, fn, prefix='', guid='',
                    last_seen_version=None, form_data=None, **kwargs):
        """
        Uploads file to server by splitting it to chunks and testing if server
        has chunk already, before actual upload.

        ``url`` -- The base upload API endpoint
        ``fn`` -- filename
        ``prefix`` -- an object's prefix on server
        ``guid`` -- unique identifier ( UUID4 ) for tracking history of changes
        ``last_seen_version`` -- casual history value, generated by DVVSet()
        """
        data = {}
        stat = os.stat(fn)
        modified_utc = str(int(stat.st_mtime))
        size = stat.st_size
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

        result = None
        with open(fn, 'rb') as fd:
            _read_chunk = lambda: fd.read(FILE_UPLOAD_CHUNK_SIZE)
            part_num = 1
            md5_list = []
            upload_id = None
            offset = 0
            for chunk in iter(_read_chunk, ''):
                md5 = hashlib.md5(chunk)
                md5_digest = md5.hexdigest()
                md5_list.append(md5_digest)
                multipart_form_data = {
                    'files[]': (fn, ''),
                    'md5': md5_digest,
                    'prefix': prefix,
                    'guid': guid,
                    'version': version
                }
                chunk_size = len(chunk)
                if form_data:
                    multipart_form_data.update(form_data)
                if size > FILE_UPLOAD_CHUNK_SIZE:
                    offset = (part_num-1) * FILE_UPLOAD_CHUNK_SIZE
                    limit = offset+chunk_size-1
                    if limit < 0:
                        limit = 0
                    ct_range = "bytes {}-{}/{}".format(offset, limit, size)
                else:
                    ct_range = "bytes 0-{}/{}".format(size-1, size)
                headers = {
                    'accept': 'application/json',
                    'authorization': 'Token {}'.format(self.token),
                    'content-range': ct_range
                }
                if part_num == 1:
                    r_url = url
                else:
                    r_url = "{}{}/{}/".format(url, upload_id, part_num)

                if offset+chunk_size == size:
                    # last chunk
                    etags = ",".join(["{},{}".format(i+1, md5_list[i]) for i in range(len(md5_list))])
                    multipart_form_data.update({
                        'etags[]': etags
                    })

                # send request without binary data first
                response = requests.post(r_url, files=multipart_form_data,
                    headers=headers)
                if response.status_code == 206:
                    # skip chunk upload, as server has it aleady
                    response_json = response.json()
                    upload_id = response_json['upload_id']
                    guid = response_json['guid']
                    part_num += 1
                    if offset+chunk_size == size:
                        result = response_json
                        break
                    else:
                        continue
                self.assertEqual(response.status_code, 200)
                response_json = response.json()
                upload_id = response_json['upload_id']
                guid = response_json['guid'] # server could change GUID
                server_md5 = response_json['md5']
                self.assertEqual(md5_digest, server_md5)

                # upload an actual data now
                multipart_form_data.update({
                    'files[]': (fn, chunk),
                    'guid': guid  # GUID could change
                })
                response = requests.post(r_url, files=multipart_form_data, headers=headers)
                self.assertEqual(response.status_code, 200)
                response_json = response.json()
                if offset+chunk_size == size:
                    # the last chunk has been processed, expect complete_upload response
                    expected = set(['lock_user_tel', 'lock_user_name', 'guid', 'upload_id',
                                    'lock_modified_utc', 'lock_user_id', 'is_locked',
                                    'author_tel', 'is_deleted', 'upload_time', 'md5',
                                    'version', 'height', 'author_id', 'author_name',
                                    'object_key', 'bytes', 'width', 'orig_name', 'end_byte'])
                    self.assertEqual(expected, set(response_json.keys()))
                    result = response_json
                    break
                else:
                    self.assertEqual(set(['end_byte', 'upload_id', 'guid', 'upload_id', 'md5']),
                                     set(response_json.keys()))
                    #self.assertEqual(response_json['guid'], guid)
                    #self.assertEqual(response_json['upload_id'], upload_id)
                    server_md5 = response_json['md5']
                    self.assertEqual(md5_digest, server_md5)
                    upload_id = response_json['upload_id']
                    part_num += 1
        return result


    def download_object(self, bucketId, objectKey):
        """
        This method downloads aby object from the object storage.
        Unlike download_file, it queries Riak CS directly.
        """
        bucket = self.resource.Bucket(bucketId)
        content = io.BytesIO()
        bucket.download_fileobj(Fileobj=content, Key=objectKey)
        return content.getvalue()

    def download_file(self, bucketId, objectKey):
        """
        This method uses /riak/download/ API endpoint to download file
        """
        url = '{}/riak/download/{}/{}'.format(BASE_URL, bucketId, objectKey)
        response = requests.get(url, headers={"authorization": "Token {}".format(self.token)})
        return response.content

    def head(self, bucketId, objectKey):
        obj = self.resource.Object(bucketId, objectKey)
        obj.load()
        return obj.metadata

    def remove_object(self, bucketId, objectKey):
        bucket = self.resource.Bucket(bucketId)
        bucket.Object(objectKey).delete()

    def purge_test_buckets(self):
        """
        Deletes all objects from bucket
        """
        bucket = self.resource.Bucket(TEST_BUCKET_1)
        try:
            objects = [i for i in bucket.objects.all()]
        except self.resource.meta.client.exceptions.NoSuchBucket:
            objects = []
        for obj in objects:
            obj.delete()

        bucket = self.resource.Bucket(UPLOADS_BUCKET_NAME)
        try:
            objects = [i for i in bucket.objects.all()]
        except self.resource.meta.client.exceptions.NoSuchBucket:
            objects = []
        for obj in objects:
            obj.delete()

    def create_bucket(self, name):
        pass

    def parse_action_log(self, xmlstring):
        tree = ET.ElementTree(ET.fromstring(xmlstring))
        root = tree.getroot()
        record = root.find("record")
        action = record.find("action").text
        details = record.find("details").text
        user_name = record.find("user_name").text
        tenant_name = record.find("tenant_name").text
        return {
            'action': action,
            'details': details,
            'user_name': user_name,
            'tenant_name': tenant_name
        }

    def create_pseudo_directory(self, name):
        req_headers = {
            'content-type': 'application/json',
            'authorization': 'Token {}'.format(self.token),
        }
        data = {
            'prefix': '',
            'directory_name': name
        }
        url = "{}/riak/list/{}/".format(BASE_URL, TEST_BUCKET_1)
        return requests.post(url, json=data, headers=req_headers)
