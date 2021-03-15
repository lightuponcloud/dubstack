
import unittest

from client_base import (BASE_URL, TEST_BUCKET_1, USERNAME_1, PASSWORD_1, USERNAME_2, PASSWORD_2)
from light_client import LightClient, generate_random_name, encode_to_hex


class RenameTest(unittest.TestCase):
    """
    #
    # Rename pseudo-directory:
    # * source prefix do not exist
    # * pseudo-directory exists ( but in lower/uppercase )
    # * server failed to rename some nested objects
    #
    #
    # To make sure error returned in the following cases.
    #
    # * rename directory to the name of existing file
    # * rename file to the name of existing directory
    #
    # 1. rename file
    # 2. create directory with the same name
    # 3. make sure error appears
    #
    #
    # 1. rename file to the same name but with different case ( uppercase / lowercase )
    # 2. to make sure no server call made
    #
    #
    # 1. to create directory on client in UPPERCASE when LOWERCASE exists on server
    # 2. to make sure files put in NTFS directory uploaded to remote dir using server name,
    #    not client name.
    #

    #
    # 1. to lock file
    # 2. to rename it
    # 3. make sure it is not renamed
    #

    #
    # 1. to upload two files
    # 2. to lock second file
    # 3. to rename one of them to the name of the second one
    # 4. to make sure rename is not allowed
    #

    #
    # 1. upload file
    # 2. rename it
    # 3. rename it to its key
    # 4. file should not disappear
    #
    #
    # 1. rename object to the key that exists in destination directory
    # 2. move to the destination directory
    # 3. previous object, with different orig name, should not be replaced
    #
    """

    def setUp(self):
        self.client = LightClient(BASE_URL, USERNAME_1, PASSWORD_1)

    # def test_negative_case1(self):
    #     """
    #     # Rename pseudo-directory:
    #     # * source prefix do not exist
    #     """
    #     # 1. create a directory
    #     dir_name = generate_random_name()
    #     prefix = encode_to_hex(dir_name)
    #     random_dir_name = (generate_random_name(), generate_random_name())
    #     self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name)
    #
    #     # 2.1 try to rename directory that doesn't exists in root
    #     res = self.client.rename(TEST_BUCKET_1, random_dir_name[0], random_dir_name[1])
    #     self.assertEqual(res.status_code, 400)
    #     self.assertEqual(res.json(), {"error": 9})  # "9": "Incorrect object name.",
    #
    #     # 2.2 try to rename directory that doesn't exists in created directory
    #     res = self.client.rename(TEST_BUCKET_1, random_dir_name[0], random_dir_name[1], prefix)
    #     self.assertEqual(res.status_code, 400)
    #     self.assertEqual(res.json(), {"error": 9})
    #
    #     # Clean: delete created directory
    #     self.client.delete(TEST_BUCKET_1, [prefix])

    # def test_negative_case2(self):
    #     """
    #     # Rename pseudo-directory:
    #     # * pseudo-directory exists ( but in lower/uppercase )
    #     """
    #     # 1.1 create a directory with lower_case name
    #     dir_name1 = generate_random_name().lower()
    #     prefix1 = encode_to_hex(dir_name1)
    #     random_dir_name1 = generate_random_name()
    #     self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name1)
    #
    #     # 1.2 try to rename lower_case name directory by upper_case key
    #     res = self.client.rename(TEST_BUCKET_1, dir_name1.upper(), random_dir_name1)
    #     self.assertEqual(res.status_code, 400)
    #     self.assertEqual(res.json(), {"error": 9})  # "9": "Incorrect object name.",
    #
    #     # 2.1 create a directory with upper_case name
    #     dir_name2 = generate_random_name().upper()
    #     prefix2 = encode_to_hex(dir_name2)
    #     random_dir_name2 = generate_random_name()
    #     self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name2)
    #
    #     # 2.2 try to rename upper_case name directory by lower_case key
    #     res = self.client.rename(TEST_BUCKET_1, dir_name1.lower(), random_dir_name2)
    #     self.assertEqual(res.status_code, 400)
    #     self.assertEqual(res.json(), {"error": 9})
    #
    #     # Clean: delete created directories
    #     self.client.delete(TEST_BUCKET_1, [prefix1, prefix2])

    def test_negative_case3(self):
        """
        # Rename pseudo-directory:
        # * server failed to rename some nested objects
        """
        # 1.1 create a directory and a nested directory into
        dir_names = [generate_random_name() for i in range(2)]
        print(dir_names)
        hex_decoder = lambda x: bytes.fromhex(x).decode('utf-8')
        prefixes = encode_to_hex(dir_names=dir_names)
        print(prefixes)
        self.client.create_pseudo_directory(TEST_BUCKET_1, dir_names[0])
        self.client.create_pseudo_directory(TEST_BUCKET_1, dir_names[1], prefixes[0])

        # 1.2 delete nested directory
        self.client.delete(TEST_BUCKET_1, [prefixes[1]], prefixes[0])

        # 1.3 try to rename nested deleted directory
        res = self.client.get_list(TEST_BUCKET_1, prefixes[0])
        from pprint import pprint
        pprint(res.json())
        dir = None
        for obj in res.json()['dirs']:
            if hex_decoder(obj['prefix'].split('/')[1]).split("-")[0] == dir_names[1]:
                dir = obj
                break
        else:
            assert False

        # print(hex_decoder(dir['prefix'].split('/')[1]).split("-")[0])
        dst_name = generate_random_name()
        print(dst_name)
        print(dir['prefix'])
        res = self.client.rename(TEST_BUCKET_1, dir['prefix'].split("/")[1] + '/', dst_name, prefixes[0])
        print(res.json())
        # self.assertEqual(res.status_code, 400)
        # self.assertEqual(res.json(), {"error": 9})  # "9": "Incorrect object name.",

        # Clean: delete created directories
        # self.client.delete(TEST_BUCKET_1, [prefixes[0]])


if __name__ == '__main__':
    unittest.main()