import os
import time
import unittest
from base64 import b64encode, b64decode
import json
import hashlib

import requests
from botocore import exceptions

from dvvset import DVVSet
from client_base import (TestClient, TEST_BUCKET_1, TEST_BUCKET_2,
                         FILE_UPLOAD_CHUNK_SIZE, UPLOADS_BUCKET_NAME,
                         BASE_URL, RIAK_ACTION_LOG_FILENAME)


class UploadTest(TestClient):

    def test_big_upload_success(self):
        url = "{}/riak/upload/{}/".format(BASE_URL, TEST_BUCKET_1)
        fn = "20180111_165127.jpg"
        result = self.upload_file(url, fn)
        self.assertEqual(result['orig_name'], '20180111_165127.jpg')

    def test_small_upload_success(self):
        url = "{}/riak/upload/{}/".format(BASE_URL, TEST_BUCKET_1)
        fn = "README.md"
        result = self.upload_file(url, fn)
        with open(fn, 'rb') as fd:
            contents = self.download_file(TEST_BUCKET_1, 'readme.md')
            self.assertEquals(fd.read(), contents)

    def _upload_request(self, headers, form_data, url=None, modified_utc=None):
        """
        Returns arguments for upload
        """
        if not url:
            url = "{}/riak/upload/{}/".format(BASE_URL, TEST_BUCKET_1)
        if 'files[]' in form_data:
            if not modified_utc:
                modified_utc = '1515683246'
        else:
            fn = "20180111_165127.jpg"
            if not modified_utc:
                modified_utc = str(int(os.stat(fn).st_mtime))
            form_data.update({
                'files[]': (fn, "something"),
            })

        if 'version' not in form_data:
            dvvset = DVVSet()
            dot = dvvset.create(dvvset.new(modified_utc), self.user_id)
            version = b64encode(json.dumps(dot).encode())
            form_data.update({
                'version': version,
            })
        if 'md5' in form_data:
            md5 = form_data['md5']
        else:
            md5 = "437b930db84b8079c2dd804a71936b5f"
            form_data.update({
                'md5': md5
            })
        if 'etags[]' not in form_data:
            form_data.update({
                'etags[]': "1,{}".format(md5)
            })
        req_headers = {
            'accept': 'application/json',
            'authorization': 'Token {}'.format(self.token),
        }
        req_headers.update(headers)
        # send request without the binary data first
        return requests.post(url, files=form_data, headers=req_headers)

    def test_validate_data_size(self):
        headers = {'content-range': 'bytes 0-1/1'}
        form_data = {
            'prefix': '',
            'guid': '',
        }
        response = self._upload_request(headers, form_data)
        response_json = response.json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response_json.get('error'), 1)

    def test_validate_version(self):
        headers = {
            'content-range': 'bytes 0-8/9'
        }
        form_data = {
            'prefix': '',
            'guid': '',
            'version': ''
        }
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 400)
        response_json = response.json()
        self.assertEqual(response_json.get('error'), 44)

        form_data = {
            'version': None
        }
        response = self._upload_request(headers, form_data)
        response_json = response.json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response_json.get('error'), 44)

    def test_validate_filename(self):
        headers = {
            'content-range': 'bytes 0-8/9'
        }
        for filename in ['', None, '~$blah', '.~blah', 'desktop.ini', 'thumbs.db',
                         '.ds_store', '.dropbox', '.dropbox.attr', 'blah<blah',
                         'blah>blah', 'blah:blah', 'blah|blah',
                         'blah?blah', 'blah*blah']:
            form_data = {
                'files[]': (filename, "something"),
            }
            response = self._upload_request(headers, form_data)
            response_json = response.json()
            self.assertEqual(response.status_code, 400)
            self.assertEqual(response_json.get('error'), 47)

        filename = 'blahb'*55
        form_data = {
            'files[]': (filename, "something"),
        }
        response = self._upload_request(headers, form_data)
        response_json = response.json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response_json.get('error'), 48)

    def test_validate_width_height(self):
        form_data = {
            'width': '1',
            'height': '1',
        }
        url = "{}/riak/upload/{}/".format(BASE_URL, TEST_BUCKET_1)
        fn = "20180111_165127.jpg"
        response_json = self.upload_file(url, fn, form_data=form_data)
        self.assertEqual(response_json['width'], 1)
        self.assertEqual(response_json['height'], 1)

        form_data = {
            'width': 'a',
            'height': '1',
        }
        response_json = self.upload_file(url, fn, form_data=form_data)
        self.assertEqual(response_json['width'], None)
        self.assertEqual(response_json['height'], None)

        form_data = {
            'width': '1',
            'height': 'a',
        }
        response_json = self.upload_file(url, fn, form_data=form_data)
        self.assertEqual(response_json['width'], None)
        self.assertEqual(response_json['height'], None)


    def test_validate_md5(self):
        headers = {
            'content-range': 'bytes 0-8/9'
        }
        form_data = {
            'md5': None,
        }
        response = self._upload_request(headers, form_data)
        response_json = response.json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response_json.get('error'), 40)

        form_data = {
            'md5': '',
        }
        response = self._upload_request(headers, form_data)
        response_json = response.json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response_json.get('error'), 40)

        form_data = {
            'md5': 'blah',
        }
        response = self._upload_request(headers, form_data)
        response_json = response.json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response_json.get('error'), 40)

    def test_validate_etags(self):
        headers = {
            'content-range': 'bytes 0-8/9'
        }
        form_data = {
            'md5': None,
        }
        response = self._upload_request(headers, form_data)
        response_json = response.json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response_json.get('error'), 40)

        form_data = {
            'etags[]': "99b21b9b146ec6fb088759afe7bccec6",
        }
        response = self._upload_request(headers, form_data)
        response_json = response.json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response_json.get('error'), 51)

    def test_validate_content_range(self):
        headers = {
            'content-range': None
        }
        response = self._upload_request(headers, {})
        response_json = response.json()
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response_json.get('error'), 52)

        response = self._upload_request({}, {})
        response_json = response.json()
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response_json.get('error'), 52)

        headers = {
            'content-range': 'nonsense'
        }
        response = self._upload_request(headers, {})
        response_json = response.json()
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response_json.get('error'), 25)

        headers = {
            'content-range': 'bytes 0-2/11811160065'
        }
        response = self._upload_request(headers, {})
        response_json = response.json()
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response_json.get('error'), 24)

        headers = {
            'content-range': 'bytes 0-2/11811160065'
        }
        response = self._upload_request(headers, {})
        response_json = response.json()
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response_json.get('error'), 24)

        headers = {
            'content-range': 'bytes 0-2000001/11811160065'
        }
        response = self._upload_request(headers, {})
        response_json = response.json()
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response_json.get('error'), 24)

        fn = "20180111_165127.jpg"
        stat = os.stat(fn)
        size = stat.st_size
        with open(fn, 'rb') as fd:
            _read_chunk = lambda: fd.read(FILE_UPLOAD_CHUNK_SIZE)
            for chunk in iter(_read_chunk, ''):
                headers = {
                    'content-range': 'bytes 0-1999999/2000000',
                }
                md5 = hashlib.md5(chunk)
                md5_digest = md5.hexdigest()
                form_data = {
                    'files[]': (fn, chunk),
                    'md5': md5_digest
                }
                url = "{}/riak/upload/{}//2/".format(BASE_URL, TEST_BUCKET_1)
                response = self._upload_request(headers, form_data, url=url)
                response_json = response.json()
                self.assertEqual(response.status_code, 403)
                self.assertEqual(response_json.get('error'), 25)
                break

    def test_add_action_log_record(self):
        url = "{}/riak/upload/{}/".format(BASE_URL, TEST_BUCKET_1)
        fn = "20180111_165127.jpg"
        self.upload_file(url, fn)
        xmlstring = self.download_object(TEST_BUCKET_1, RIAK_ACTION_LOG_FILENAME)
        result = self.parse_action_log(xmlstring)
        expected_action_log = {
            'action': 'upload',
            'details': 'Uploaded "20180111_165127.jpg" ( 2773205 B )',
            'user_name': '4a6f652041726d7374726f6e67',
            'tenant_name': '4d79204272616e642032'
        }
        self.assertEqual(result, expected_action_log)

    def test_get_guid(self):
        headers = {
            'content-range': 'bytes 0-8/9'
        }

        form_data = {
            'prefix': ''
        }
        dvvset = DVVSet()

        # no guid sent, no such object in db -> new GUID created

        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        guid = response_json['guid']
        upload_id = response_json['upload_id']
        md5 = response_json['md5']
        self.assertTrue(len(guid) == 36)

        data = self.download_object(TEST_BUCKET_1, "~object/{}/{}/1_{}".format(guid, upload_id, md5))
        self.assertEqual(data, b'something')

        # no guid sent, object exists -> existing GUID used

        # increment version
        dot = json.loads(b64decode(response.json()['version']))
        context = dvvset.join(dot)
        new_dot = dvvset.update(dvvset.new_with_history(context, "1515683247"), dot, self.user_id)
        new_version = dvvset.sync([dot, new_dot])

        form_data.update({'version': b64encode(json.dumps(new_version).encode())})
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        same_guid = response_json['guid']
        self.assertEqual(response_json['orig_name'], '20180111_165127.jpg')
        self.assertEqual(guid, same_guid)

        # check guid validation function
        response = self._upload_request(headers, {'guid': ''})
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        guid = response_json['guid']
        self.assertTrue(len(guid) == 36)  # new one should be created

        response = self._upload_request(headers, {'guid': '2418baa6-e39d-5adb-980f-25fc113a1a2d'})
        self.assertEqual(response.status_code, 400)
        response_json = response.json()
        self.assertEqual(response_json, {'error': 42})

        response = self._upload_request(headers, {'guid': '2418baa6-e39d-4adb-c80f-25fc113a1a2d'})
        self.assertEqual(response.status_code, 400)
        response_json = response.json()
        self.assertEqual(response_json, {'error': 42})

        response = self._upload_request(headers, {'guid': '2418baa6-e39d-4adb-880f-25fc113a1a2d'})
        self.assertEqual(response.status_code, 200)

        response = self._upload_request(headers, {'guid': '2418baa6-e39d-4adb-980f-25fc113a1a2d'})
        self.assertEqual(response.status_code, 200)

        response = self._upload_request(headers, {'guid': '2418baa6-e39d-4adb-a80f-25fc113a1a2d'})
        self.assertEqual(response.status_code, 200)

        response = self._upload_request(headers, {'guid': '2418baa6-e39d-4adb-b80f-25fc113a1a2d'})
        self.assertEqual(response.status_code, 200)

        # guid specified, no object in db -> guid from request should be used
        self.purge_test_buckets()

        response = self._upload_request(headers, {'guid': "9f274424-5048-4cb5-9c8c-5b9222e3933e"})
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        guid = response_json['guid']
        self.assertEqual(guid, "9f274424-5048-4cb5-9c8c-5b9222e3933e")

        # guid specified, object exists with a different guid -> existing GUID should be used

        response = self._upload_request(headers, {'guid': "9f274424-5048-4cb5-9c8c-5b9222e39331",
                                                  'version': b64encode(json.dumps(new_version).encode())})

        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertNotEqual(response_json['guid'], '9f274424-5048-4cb5-9c8c-5b9222e39331')

        # no guid sent, conflict -> existing GUID used

        # first increment version
        dot = json.loads(b64decode(response.json()['version']))
        context = dvvset.join(dot)
        new_dot = dvvset.update(dvvset.new_with_history(context, "1515683247"), dot, self.user_id)
        new_version = dvvset.sync([dot, new_dot])
        form_data.update({'version': b64encode(json.dumps(new_version).encode())})
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)
        existing_guid = response.json()['guid']

        # now create a conflict by uploading previous version
        form_data.pop('version')
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertTrue(response_json['orig_name'].startswith(
         '20180111_165127 (Joe Armstrong, conflicted copy '))
        self.assertEqual(response_json['guid'], existing_guid)

        # existing guid specified, conflict -> existing GUID used

        # increment version
        dot = json.loads(b64decode(response.json()['version']))
        context = dvvset.join(dot)
        new_dot = dvvset.update(dvvset.new_with_history(context, "1515683247"), dot, self.user_id)
        new_version = dvvset.sync([dot, new_dot])
        response = self._upload_request(headers, {'guid': existing_guid,
                                                  'version': b64encode(json.dumps(new_version).encode())})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response_json['guid'], existing_guid)

        dot = json.loads(b64decode(response.json()['version']))
        context = dvvset.join(dot)
        new_dot = dvvset.update(dvvset.new_with_history(context, "1515683247"), dot, self.user_id)
        new_version = dvvset.sync([dot, new_dot])
        response = self._upload_request(headers, {'guid': '34bc4542-af5d-40b0-964c-7b1b25c214c2',
                                                  'version': b64encode(json.dumps(new_version).encode())})
        response_json = response.json()
        existing_guid = response_json['guid']
        conflicted_fn = response_json['orig_name']
        self.assertEqual(response.status_code, 200)
        self.assertNotEqual('34bc4542-af5d-40b0-964c-7b1b25c214c2', existing_guid)

        # make sure guid do not change in case conflicted copy is edited

        dot = json.loads(b64decode(response.json()['version']))
        context = dvvset.join(dot)
        new_dot = dvvset.update(dvvset.new_with_history(context, "1515683247"), dot, self.user_id)
        new_version = dvvset.sync([dot, new_dot])
        response = self._upload_request(headers, {'guid': '34bc4542-af5d-40b0-964c-7b1b25c214c2',
                                                  'files[]': (conflicted_fn, "something"),
                                                  'version': b64encode(json.dumps(new_version).encode())})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['guid'], existing_guid)

    def test_check_part(self):
        headers = {'content-range': 'bytes 0-8/18'}
        dvvset = DVVSet()
        modified_utc = '1515683246'
        dot = dvvset.create(dvvset.new(modified_utc), self.user_id)
        version = b64encode(json.dumps(dot).encode())
        form_data = {
            'prefix': '',
            'md5': "437b930db84b8079c2dd804a71936b5f",
            'guid': '',
            'files[]': ("20180111_165127.jpg", "something"),
            'version': version
        }
        # check_part should work when
        # binary data sent, IsCorrectMd5 is False
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        upload_id = response_json['upload_id']
        guid = response_json['guid']

        # initiate a second upload
        headers = {'content-range': 'bytes 0-8/18'}
        form_data.update({'files[]': ("SECOND ONE.jpg", "something")})
        second_response = self._upload_request(headers, form_data)
        second_response_json = second_response.json()
        self.assertEqual(second_response.status_code, 200)
        second_upload_id = second_response_json['upload_id']
        second_guid = second_response_json['guid']

        self.assertNotEqual(second_upload_id, upload_id)
        self.assertNotEqual(second_guid, guid)

        # check_part should fail when IsCorrectMd5 is False
        headers = {'content-range': 'bytes 0-8/9'}
        form_data.update({'files[]': ("20180111_165127.jpg", "something")})
        form_data.update({'md5': '2e059990c316b9e93512937eafa8ef13'})
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 400)

        headers = {'content-range': 'bytes 9-17/18'}
        md5_list = ["437b930db84b8079c2dd804a71936b5f", "437b930db84b8079c2dd804a71936b5f"]
        etags = ",".join(["{},{}".format(i+1, md5_list[i]) for i in range(len(md5_list))])
        form_data.update({
            'md5': '437b930db84b8079c2dd804a71936b5f',
            'etags[]': etags
        })
        # send incorrect upload id and make sure error returned
        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, 'blah')
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {'error': 5})

        # send upload id from another upload, make sure server returns error
        form_data['guid'] = guid
        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, second_upload_id)
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {'error': 4})

        # create prefix and make sure prefix check works
        dir_name = 'test-dir'
        dir_response = self.create_pseudo_directory(dir_name)
        self.assertEqual(dir_response.status_code, 204)
        form_data['prefix'] = dir_name.encode().hex()
        form_data['guid'] = second_guid
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {'error': 36})

        # test bucket id check
        form_data['prefix'] = ''
        form_data['guid'] = guid
        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_2, second_upload_id)
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {'error': 37})

        # empty body, send incorrect upload id and make sure error returned
        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, 'blah')
        form_data['files[]'] = ("20180111_165127.jpg", "")
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {'error': 5})

        # empty body, send upload id from another upload, make sure server returns error
        form_data['guid'] = guid
        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, second_upload_id)
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {'error': 4})

        # empty body, create prefix and make sure prefix check works
        form_data['prefix'] = dir_name.encode().hex()
        form_data['guid'] = second_guid
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {'error': 36})

        # test bucket id check
        form_data['files[]'] = ("20180111_165127.jpg", "something")
        form_data['prefix'] = ''
        form_data['guid'] = guid
        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_2, second_upload_id)
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {'error': 37})

        # test version check
        modified_utc = '1515683247'
        dot = dvvset.create(dvvset.new(modified_utc), self.user_id)
        form_data['version'] = b64encode(json.dumps(dot).encode())
        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, second_upload_id)
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {'error': 22})

        # finish multipart upload to test if it succeeds
        form_data['version'] = version
        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, upload_id)
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 200)

    def test_create_upload_id(self):
        headers = {'content-range': 'bytes 0-8/18'}
        dvvset = DVVSet()
        modified_utc = '1515683246'
        dot = dvvset.create(dvvset.new(modified_utc), self.user_id)
        version = b64encode(json.dumps(dot).encode())
        fn = "20180111_165127.jpg"
        form_data = {
            'prefix': '',
            'md5': "437b930db84b8079c2dd804a71936b5f",
            'guid': '',
            'files[]': (fn, "something"),
            'version': version
        }
        # check_part should work when
        # binary data sent, IsCorrectMd5 is False
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        upload_id = response_json['upload_id']
        guid = response_json['guid']

        meta = self.head(UPLOADS_BUCKET_NAME, upload_id)
        self.assertEqual(json.loads(b64decode(meta['version'])), dot)
        self.assertEqual(bytes.fromhex(meta['orig-filename']), fn.encode())
        self.assertEqual(meta['guid'], guid)
        self.assertEqual(meta['bucket_id'], TEST_BUCKET_1)
        self.assertEqual(meta['author-id'], self.user_id)
        self.assertEqual(meta['is-deleted'], 'false')

        # upload id should be specified for the first part only
        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, upload_id)
        headers = {'content-range': 'bytes 9-17/18'}
        md5_list = ["437b930db84b8079c2dd804a71936b5f", "437b930db84b8079c2dd804a71936b5f"]
        etags = ",".join(["{},{}".format(i+1, md5_list[i]) for i in range(len(md5_list))])
        form_data.update({
            'md5': '437b930db84b8079c2dd804a71936b5f',
            'etags[]': etags
        })
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 200)

    def test_find_chunk(self):

        # first upload multi-part file
        headers = {'content-range': 'bytes 0-18/28'}
        dvvset = DVVSet()
        modified_utc = '1515683246'
        dot = dvvset.create(dvvset.new(modified_utc), self.user_id)
        version = b64encode(json.dumps(dot).encode())
        form_data = {
            'prefix': '',
            'md5': "79b547467286b3e20fad13f73fc1bf78",
            'guid': '',
            'files[]': ("20180111_165127.jpg", "something something"),
            'version': version
        }
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        upload_id = response_json['upload_id']
        guid = response_json['guid']

        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, upload_id)
        headers = {'content-range': 'bytes 19-27/28'}
        md5_list = ["79b547467286b3e20fad13f73fc1bf78", "437b930db84b8079c2dd804a71936b5f"]
        etags = ",".join(["{},{}".format(i+1, md5_list[i]) for i in range(len(md5_list))])
        form_data.update({
            'files[]': ("20180111_165127.jpg", "something"),
            'md5': '437b930db84b8079c2dd804a71936b5f',
            'etags[]': etags
        })
        response = self._upload_request(headers, form_data, url=url)
        response_json = response.json()
        guid = response_json['guid']
        self.assertEqual(response.status_code, 200)
        self.assertTrue(('orig_name' in response_json))
        self.assertEqual(response_json['orig_name'], '20180111_165127.jpg')

        # check its contents
        contents = self.download_file(TEST_BUCKET_1, '20180111_165127.jpg')
        self.assertEquals(contents, b"something somethingsomething")

        # now test if two existing chunks are used when parts with the same md5 sums uploaded
        dot = json.loads(b64decode(response_json['version']))
        context = dvvset.join(dot)
        new_dot = dvvset.update(dvvset.new_with_history(context, str(int(time.time()))), dot, self.user_id)
        new_version = dvvset.sync([dot, new_dot])

        headers = {'content-range': 'bytes 0-18/28'}
        form_data = {
            'prefix': '',
            'md5': "79b547467286b3e20fad13f73fc1bf78",
            'guid': guid,
            'files[]': ("7.jpg", ""),
            'version': b64encode(json.dumps(new_version).encode())
        }
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 206)
        upload_id = response.json()['upload_id']

        # make sure object do not exist after empty request
        with self.assertRaises(exceptions.ClientError):
            self.head(TEST_BUCKET_1, '7.jpg')

        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, upload_id)
        headers = {'content-range': 'bytes 19-27/28'}
        md5_list = ["79b547467286b3e20fad13f73fc1bf78", "437b930db84b8079c2dd804a71936b5f"]
        etags = ",".join(["{},{}".format(i+1, md5_list[i]) for i in range(len(md5_list))])
        form_data.update({
            'files[]': ("7.jpg", ""),
            'md5': '437b930db84b8079c2dd804a71936b5f',
            'etags[]': etags
        })
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 206)

        # make sure object exists this time
        obj_headers = self.head(TEST_BUCKET_1, '7.jpg')
        self.assertEqual(obj_headers['upload-id'], response.json()['upload_id'])

        # Another upload
        headers = {'content-range': 'bytes 0-18/27'}
        form_data = {
            'prefix': '',
            'md5': "79b547467286b3e20fad13f73fc1bf78",
            'guid': guid,
            'files[]': ("20180111_165127.jpg", ""),
            'version': b64encode(json.dumps(new_version).encode())
        }
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 206)
        upload_id = response.json()['upload_id']

        # the second part is different from what's on server

        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, upload_id)
        headers = {'content-range': 'bytes 19-26/27'}
        md5_list = ["79b547467286b3e20fad13f73fc1bf78", "25d55ad283aa400af464c76d713c07ad"]
        etags = ",".join(["{},{}".format(i+1, md5_list[i]) for i in range(len(md5_list))])
        form_data.update({
            'files[]': ("20180111_165127.jpg", ""),
            'md5': '25d55ad283aa400af464c76d713c07ad',
            'etags[]': etags
        })
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 200)

        # now finish upload by sending data
        form_data.update({
            'files[]': ("20180111_165127.jpg", "12345678"),
            'md5': '25d55ad283aa400af464c76d713c07ad',
        })
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['orig_name'], '20180111_165127.jpg')

        # Another upload. First part is different from what's on server
        headers = {'content-range': 'bytes 0-4/27'}
        dot = json.loads(b64decode(response_json['version']))
        context = dvvset.join(dot)
        new_dot = dvvset.update(dvvset.new_with_history(context, str(int(time.time()))), dot, self.user_id)
        new_version = dvvset.sync([dot, new_dot])
        form_data = {
            'prefix': '',
            'md5': "0ba4439ee9a46d9d9f14c60f88f45f87",
            'guid': guid,
            'files[]': ("20180111_165127.jpg", ""),
            'version': b64encode(json.dumps(new_version).encode())
        }
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)

        form_data.update({'files[]': ("20180111_165127.jpg", "check")})
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)
        upload_id = response.json()['upload_id']

        # the second part is the same as on server

        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, upload_id)
        headers = {'content-range': 'bytes 5-26/27'}
        md5_list = ["0ba4439ee9a46d9d9f14c60f88f45f87", "79b547467286b3e20fad13f73fc1bf78"]
        etags = ",".join(["{},{}".format(i+1, md5_list[i]) for i in range(len(md5_list))])
        form_data.update({
            'files[]': ("20180111_165127.jpg", ""),
            'md5': '79b547467286b3e20fad13f73fc1bf78',
            'etags[]': etags
        })
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.json()['orig_name'], '20180111_165127.jpg')

        # check its contents
        contents = self.download_file(TEST_BUCKET_1, '20180111_165127.jpg')
        self.assertEquals(contents, b"checksomething something")

        # check if no existing chunk is used in case when ther'a no matches
        form_data = {
            'prefix': '',
            'md5': "3daae0b62c8032c3c15171e09ef0b8fd",
            'guid': '',
            'files[]': ("20180111_165127.jpg", ""),
            'version': b64encode(json.dumps(new_version).encode())
        }
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        self.assertEqual(response_json['guid'], guid)

    def test_complete_upload(self):

        # first upload a multi-part file
        headers = {'content-range': 'bytes 0-18/28'}
        dvvset = DVVSet()
        modified_utc = '1515683246'
        dot = dvvset.create(dvvset.new(modified_utc), self.user_id)
        version = b64encode(json.dumps(dot).encode())
        form_data = {
            'prefix': '',
            'md5': "79b547467286b3e20fad13f73fc1bf78",
            'guid': '',
            'files[]': ("20180111_165127.jpg", "something something"),
            'version': version
        }
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        guid = response_json['guid']
        upload_id = response_json['upload_id']
        md5 = response_json['md5']

        # now remove the object
        real_path = "~object/{}/{}/1_{}".format(guid, upload_id, md5)
        self.remove_object(TEST_BUCKET_1, real_path)

        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, upload_id)
        headers = {'content-range': 'bytes 19-27/28'}
        md5_list = ["79b547467286b3e20fad13f73fc1bf78", "437b930db84b8079c2dd804a71936b5f"]
        etags = ",".join(["{},{}".format(i+1, md5_list[i]) for i in range(len(md5_list))])
        form_data.update({
            'files[]': ("20180111_165127.jpg", "something"),
            'md5': '437b930db84b8079c2dd804a71936b5f',
            'etags[]': etags
        })
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {'error': 51})

    def test_delete_previous_one(self):
        """
        Make sure the following objects are removed by the server
        - old versions of the same object ( with the same GUID )
        - old conflicted copies
        """
        # first upload a multi-part file
        headers = {'content-range': 'bytes 0-18/28'}
        dvvset = DVVSet()
        modified_utc = str(int(time.time()))
        dot = dvvset.create(dvvset.new(modified_utc), self.user_id)
        version = b64encode(json.dumps(dot).encode())
        form_data = {
            'prefix': '',
            'md5': "79b547467286b3e20fad13f73fc1bf78",
            'guid': '',
            'files[]': ("20180111_165127.jpg", "something something"),
            'version': version
        }
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        old_guid = response_json['guid']
        old_upload_id = response_json['upload_id']
        old_md5 = response_json['md5']

        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, old_upload_id)
        headers = {'content-range': 'bytes 19-27/28'}
        md5_list = ["79b547467286b3e20fad13f73fc1bf78", "437b930db84b8079c2dd804a71936b5f"]
        etags = ",".join(["{},{}".format(i+1, md5_list[i]) for i in range(len(md5_list))])
        form_data.update({
            'files[]': ("20180111_165127.jpg", "something"),
            'md5': '437b930db84b8079c2dd804a71936b5f',
            'etags[]': etags
        })
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 200)
        response_json = response.json()

        # make sure object exists, by downloading it and checking its contents
        data = self.download_object(TEST_BUCKET_1, "~object/{}/{}/1_{}".format(old_guid, old_upload_id, old_md5))
        self.assertEqual(data, b'something something')

        # upload a new version of file
        dot = json.loads(b64decode(response_json['version']))
        context = dvvset.join(dot)
        new_dot = dvvset.update(dvvset.new_with_history(context, str(int(time.time()))), dot, self.user_id)
        new_version = dvvset.sync([dot, new_dot])
        form_data = {
            'prefix': '',
            'md5': "79b547467286b3e20fad13f73fc1bf78",
            'guid': old_guid,
            'files[]': ("20180111_165127.jpg", "something something"),
            'version': b64encode(json.dumps(new_version).encode())
        }
        headers = {'content-range': 'bytes 0-18/28'}
        response = self._upload_request(headers, form_data)
        self.assertEqual(response.status_code, 200)
        response_json = response.json()
        upload_id = response_json['upload_id']

        url = "{}/riak/upload/{}/{}/2/".format(BASE_URL, TEST_BUCKET_1, upload_id)
        headers = {'content-range': 'bytes 19-27/28'}
        md5_list = ["79b547467286b3e20fad13f73fc1bf78", "437b930db84b8079c2dd804a71936b5f"]
        etags = ",".join(["{},{}".format(i+1, md5_list[i]) for i in range(len(md5_list))])
        form_data.update({
            'files[]': ("20180111_165127.jpg", "something"),
            'md5': '437b930db84b8079c2dd804a71936b5f',
            'etags[]': etags
        })
        response = self._upload_request(headers, form_data, url=url)
        self.assertEqual(response.status_code, 200)

        # now make sure old object is removed
        with self.assertRaises(exceptions.ClientError):
            self.download_object(TEST_BUCKET_1, "~object/{}/{}/1_{}".format(old_guid, old_upload_id, old_md5))

if __name__ == '__main__':
    unittest.main()
