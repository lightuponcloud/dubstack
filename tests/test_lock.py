

import unittest

from client_base import (BASE_URL, TEST_BUCKET_1, USERNAME_1, PASSWORD_1, USERNAME_2, PASSWORD_2)
from light_client import LightClient, generate_random_name, encode_to_hex


class LockTest(unittest.TestCase):
    """
    Test operation LOCK / UNLOCK

    # 1. upload file
    # 2. lock it
    # 3. make sure lock is set
    # 4. try to change lock from different user
    # 5. make sure the same value of lock remained as in step #2
    #

    #
    # upload file, lock it, upload new version from the same user
    # lock should remain
    #

    #
    # Make sure locked file can't be replaced using COPY/MOVE operations
    #

    #
    # Make sure deleted objects can't be locked
    """

    def setUp(self):
        self.client = LightClient(BASE_URL, USERNAME_1, PASSWORD_1)
        # self.client.login(USERNAME_1, PASSWORD_1)

    def test_lock(self):

        # 1. upload 1 file in dir
        fn = "20180111_165127.jpg"
        result = self.client.upload(TEST_BUCKET_1, fn)
        object_key = result['object_key']
        print(object_key)
        self.assertEqual(result['orig_name'], fn)

        # 2. lock it
        response = self.client.patch(TEST_BUCKET_1, "lock", [object_key])
        print(response.content.decode())
        self.assertEqual(response.status_code, 204)

        # 3. check for "lock": True
        response = self.client.get_list(TEST_BUCKET_1)
        result = response.json()
        for obj in result['list']:
            if obj['object_key'] == object_key:
                self.assertEqual(obj['lock'], True)

        # 4. try to change lock from different user
        self.client.login(USERNAME_2, PASSWORD_2)
        response = self.client.patch(TEST_BUCKET_1, "unlock", [object_key])
        print(response.content.decode())

        # 5. Check for the same value of lock remained as in step #2-3
        response = self.client.get_list(TEST_BUCKET_1)
        result = response.json()
        for obj in result['list']:
            if obj['object_key'] == object_key:
                self.assertEqual(obj['lock'], True)
        # self.assertEqual(response.status_code, 204)


if __name__ == '__main__':
    unittest.main()