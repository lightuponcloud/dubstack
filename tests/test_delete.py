import sys
import unittest
import json
from pprint import pprint
import requests
import logging

from client_base import (TestClient, BASE_URL, TEST_BUCKET_1, USERNAME_1, PASSWORD_1)

# data = {"authorization": "Token e5a3dbe5-278c-4263-a3f5-10b65cd476ae"}

def get_all_from_root():
    id_bucket = "the-integrationtests-integration1-res/"
    url = "https://lightupon.cloud/riak/list/"
    response = requests.get(url + id_bucket, headers={"authorization": 'Token 3409a565-60aa-4365-b0ea-58bd8c98ea33'})
    # func_exists = lambda key: [x for x in response.json()[key] if not x["is_deleted"]]
    # dirs = func_exists('dirs')
    # files = func_exists('list')
    # data = dirs + files
    return response.json()


# pprint(get_all_from_root())  # OK

class DeleteTest(TestClient):

    # def test_delete_none(self):  # "negative test case"
    #     url = "{}/riak/list/{}/".format(BASE_URL, TEST_BUCKET_1)
    #     data = {"object_keys": []}
    #     response = requests.delete(url, data=json.dumps(data), headers={'content-type': 'application/json',
    #                                         'authorization': 'Token {}'.format(self.token)})
    #     result = response.json()
    #     self.assertEqual(result, {"error": 34})  # "34": "Empty "object_keys".",
    #
    # def test_delete_onefile_from_root(self):
    #     # upload file
    #     url1 = "{}/riak/upload/{}/".format(BASE_URL, TEST_BUCKET_1)
    #     fn = "20180111_165127.jpg"
    #     result = self.upload_file(url1, fn)
    #     self.assertEqual(result['orig_name'], '20180111_165127.jpg')
    #
    #     # delete uploaded file
    #     url2 = "{}/riak/list/{}/".format(BASE_URL, TEST_BUCKET_1)
    #
    #     data = {"object_keys": [fn]}
    #     response = requests.delete(url2, data=json.dumps(data), headers={'content-type': 'application/json',
    #                                         'authorization': 'Token {}'.format(self.token)})
    #
    #     result = response.json()
    #     self.assertEqual(result, [fn])
    #
    # def test_delete_manyfiles_from_root(self):
    #     # upload files
    #     url1 = "{}/riak/upload/{}/".format(BASE_URL, TEST_BUCKET_1)
    #     fn = ["025587.jpg", "README.md", "requirements.txt"]
    #     object_keys = []
    #     for file in fn:
    #         result = self.upload_file(url1, file)
    #         object_keys.append(result['object_key'])
    #         self.assertEqual(result['orig_name'], file)
    #
    #     # delete uploaded files
    #     url2 = "{}/riak/list/{}/".format(BASE_URL, TEST_BUCKET_1)
    #     data = {"object_keys": object_keys}
    #     response = requests.delete(url2, data=json.dumps(data),
    #                                headers={'content-type': 'application/json',
    #                                         'authorization': 'Token {}'.format(self.token)})
    #     result = response.json()
    #     for element in object_keys:
    #         self.assertIn(element, result)
    #
    #     # final check for is_deleted: True
    #     response = requests.get(url2, headers={'content-type': 'application/json',
    #                                         'authorization': 'Token {}'.format(self.token)})
    #     result = response.json()
    #
    #     for filename in fn:
    #         for obj in result['list']:
    #             if filename in obj['orig_name']:
    #                 # print(filename, obj['orig_name'], obj['object_key'])
    #                 self.assertEqual(obj['is_deleted'], True)


    # def test_delete_files_from_pseudodirectory(self):
    #     # 1 create pseudo-directory
    #     dir_name = "DeleteTest2"
    #     response = self.create_pseudo_directory(dir_name)
    #     dir_name_prefix = dir_name.encode().hex() + "/"
    #
    #     # 2 upload file to pseudo-directory
    #     url = "{}/riak/upload/{}/".format(BASE_URL, TEST_BUCKET_1)
    #     fn = "20180111_165127.jpg"
    #     result = self.upload_file(url, fn, prefix=dir_name_prefix)
    #     # pprint(result)
    #     self.assertEqual(result['orig_name'], '20180111_165127.jpg')
    #     object_key = [result['object_key']]
    #
    #     # 2.1 delete file from pseudo-directory and check for is_deleted: True
    #     url = "{}/riak/list/{}/".format(BASE_URL, TEST_BUCKET_1)
    #     data = {"object_keys": object_key, 'prefix': dir_name_prefix}
    #     result = self.delete_json(url, data=data)
    #     self.assertEqual(result, object_key)
    #
    #     result = self.get_json(url)
    #
    #     for obj in result['list']:
    #         if fn in obj['orig_name']:
    #             # print(fn, obj['orig_name'], obj['object_key'])
    #             self.assertTrue(obj['is_deleted'])
    #
    #     # 3 upload files
    #     url = "{}/riak/upload/{}/".format(BASE_URL, TEST_BUCKET_1)
    #     fn = ["025587.jpg", "README.md", "requirements.txt"]
    #     object_keys = []
    #     for file in fn:
    #         result = self.upload_file(url, file, prefix=dir_name_prefix)
    #         object_keys.append(result['object_key'])
    #         self.assertEqual(result['orig_name'], file)
    #
    #     # 3.1 delete files
    #     # No need
    #
    #     # 4 delete created pseudo-directory... with uploaded files
    #     url = "{}/riak/list/{}/".format(BASE_URL, TEST_BUCKET_1)
    #     data = {"object_keys": [dir_name_prefix]}
    #     result = self.delete_json(url, data=data)
    #     self.assertEqual(result, data['object_keys'])

    def test_delete_pseudodirectories_from_root(self):

        # create 1 pseudo-directory
        dir_name = "DeleteTest3"
        response = self.create_pseudo_directory(dir_name)
        print(response.content)
        dir_name_prefix = dir_name.encode().hex() + "/"
        print(dir_name_prefix)

        # delete created pseudo-directory
        url = "{}/riak/list/{}".format(BASE_URL, TEST_BUCKET_1)
        data = {"object_keys": [dir_name_prefix]}
        result = self.delete_json(url, data=data)
        # self.assertEqual(result, [dir_name_prefix])

        # create directories
        dir_names = ["DeleteTest3_1", "DeleteTest3_2", "DeleteTest3_3"]
        for name in dir_names:
            self.create_pseudo_directory(name)

        # delete directories
        url = "{}/riak/list/{}".format(BASE_URL, TEST_BUCKET_1)
        object_keys = [x.encode().hex() + "/" for x in dir_names]
        print(object_keys)
        data = {"object_keys": object_keys}
        result = self.delete_json(url, data=data)
        print(result)
        # self.assertEqual(result, object_keys)


    # def test_delete_pseudodirectory_with_prefix(self):
    #     return


# creds = {"login": USERNAME_1, "password": PASSWORD_1}
# response = requests.post("{}/riak/login".format(BASE_URL), data=json.dumps(creds),
#                          headers={'content-type': 'application/json'})
# print(response)
# print(response.text)
# data = response.json()
# token = data['token']
# print("Token " + token)


# url = "https://lightupon.cloud/riak/list/the-integrationtests-integration1-res/"
# ?prefix=" + "RenamedFolder1".encode().hex()
# response = requests.get(url, headers={"authorization": "Token e5a3dbe5-278c-4263-a3f5-10b65cd476ae"})
# print(response)
# pprint(response.json())


# def get_all_from_root():
#     id_bucket = "the-integrationtests-integration1-res/"
#     url = "https://lightupon.cloud/riak/list/"
#     response = requests.get(url + id_bucket, headers={"authorization": "Token e5a3dbe5-278c-4263-a3f5-10b65cd476ae"})
#     func_exists = lambda key: [x for x in response.json()[key] if not x["is_deleted"]]
#     dirs = func_exists('dirs')
#     files = func_exists('list')
#     data = dirs + files
#     return response.json()
#
#
# pprint(get_all_from_root())  # OK


# result = response.json()
# print(type(result))
# print(result)
#
if __name__ == '__main__':
    unittest.main()


# fn = ["20180111_165127.jpg", "README.md", "requirements.txt"]

# url2 = "{}/riak/list/{}/".format(BASE_URL, TEST_BUCKET_1)
# data = {"object_keys": fn}
# response = requests.delete(url2, data=json.dumps(data),
#                            headers={'content-type': 'application/json',
#                                     'authorization': 'Token e5a3dbe5-278c-4263-a3f5-10b65cd476ae'})
#
# result = response.json()
# print(result)