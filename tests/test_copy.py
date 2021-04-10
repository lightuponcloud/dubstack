import unittest

from client_base import (BASE_URL, TEST_BUCKET_1, USERNAME_1, PASSWORD_1, USERNAME_2, PASSWORD_2)
from light_client import LightClient, generate_random_name, encode_to_hex


class CopyTest(unittest.TestCase):
    """
    Operation COPY

    # file from root to dir
    # renamed file from root dir
    # directory from root to dir
    #
    # file from nested dir to root
    # file from nested dir up
    # file from nested dir down
    # renamed file from nested dir to root
    #
    # dir from nested dir to root
    # dir from nested dir up
    # dir from nested dir down
    #
    # 1. upload file to root
    # 2. rename file
    # 3. copy to nested dir
    # 4. copy again. file should be replaced
    #
    # 1. upload file to nested dir
    # 2. create directory in root with the same name
    # 3. copy that directory to the nested one
    # 4. make sure file is renamed, as directory can't have the same name as file

    # Copy file when ther's such file in destination directory already
    #
    # rename object in nested dir
    # copy object with the same name ( as renamed ) to that dir
    #
    # copy empty directory from root to nested dir
    # copy empty dir from nested dir to root

    # 1. Copy many files
    # 2. Receive 202 response code and list of copied files in body
    # 3. Make sure list returns uncommitted flag == true
    # 4. Retry copy with files that were absent in response

    # 1. copy file to dir
    # 2. rename source file
    # 3. copy to pseudo-dir again
    # 4. make sure first copied file has original name

    # copy multiple times
    # try to download file, make sure it consistent

    # copy file, make sure .stop file created
    # upload a new version of copied file, make sure previous one is not deleted for the same day
    # check if both files can be downloaded and are consistent

    # 1. Copy file specifying its key as new name to the destination directory, where object with such key exists
    # 2. make sure object in destination directory is not replaced

    # on MOVE make sure .lock is removed from source pseudo-dir

    # MOVE operation should not delete object in source directory if it was locked by different user
    """

    def setUp(self):
        self.client = LightClient(BASE_URL, USERNAME_1, PASSWORD_1)

    def test_case1(self):
        """
        # file from root to dir
        # renamed file from root dir
        # directory from root to dir
        """
        # 1. upload a file to root and create 2 directories in root
        fn = '025587.jpg'
        res = self.client.upload(TEST_BUCKET_1, fn)
        object_key = res['object_key']

        dir_name1, dir_name2 = [generate_random_name() for _ in range(2)]
        prefix1, prefix2 = encode_to_hex(dir_names=[dir_name1, dir_name2])
        self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name1)
        self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name2)

        # 2. copy uploaded file from root to dir2
        object_keys = {object_key: fn}
        res = self.client.copy(TEST_BUCKET_1, TEST_BUCKET_1, object_keys, '', prefix2)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()['dst_prefix'], prefix2)
        self.assertEqual(res.json()['dst_orig_name'], fn)

        # 3. copy the renamed file to dir2
        new_name = generate_random_name()
        object_keys = {object_key: new_name}
        res = self.client.copy(TEST_BUCKET_1, TEST_BUCKET_1, object_keys, '', prefix2)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()['renamed'], True)

        # 4. copy dir1 to dir2
        object_keys2 = {prefix1: dir_name1}
        print(object_keys2)
        res = self.client.copy(TEST_BUCKET_1, TEST_BUCKET_1, object_keys2, '', prefix2)
        print(res.status_code)
        print(res.content.decode())
        # Неожиданный статус код 304 с пустым контентом, но операция прошла успешно (если посмотреть в Web UI).
        # Реакция на копирование папки.

        # 5. Сlean: delete all created
        self.client.delete(TEST_BUCKET_1, [object_key, prefix1, prefix2])

    def test_case2(self):
        """
        # file from nested dir to root
        # file from nested dir up
        # file from nested dir down
        # renamed file from nested dir to root
        """

        # 1. create a dir1 and a sub-dir2 and sub-sub-dir3
        dir_name1, dir_name2, dir_name3 = [generate_random_name() for _ in range(3)]
        prefix1, prefix2, prefix3 = encode_to_hex(dir_names=[dir_name1, dir_name2, dir_name3])
        self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name1)
        self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name2, prefix1)
        self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name3, prefix1+prefix2)

        # 2. upload a file in dir2
        fn = '025587.jpg'
        res = self.client.upload(TEST_BUCKET_1, fn, prefix1+prefix2)
        object_key = res['object_key']

        # 3. copy file from dir2 to root
        res = self.client.copy(TEST_BUCKET_1, TEST_BUCKET_1, {object_key: fn}, prefix1+prefix2, '')
        self.assertEqual(res.status_code, 200)
        object_key2 = res.json()['new_key']

        # 4.1 copy file up from dir2 to dir3
        res = self.client.copy(TEST_BUCKET_1, TEST_BUCKET_1, {object_key: fn}, prefix1 + prefix2, prefix1 + prefix2 + prefix3)
        self.assertEqual(res.status_code, 200)

        # 4.2 copy file down from dir2 to dir1
        res = self.client.copy(TEST_BUCKET_1, TEST_BUCKET_1, {object_key: fn}, prefix1 + prefix2, prefix1)
        self.assertEqual(res.status_code, 200)

        # 5. copy renamed file to root
        new_name = generate_random_name()
        res = self.client.copy(TEST_BUCKET_1, TEST_BUCKET_1, {object_key: new_name}, prefix1 + prefix2, '')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()['renamed'], True)

        # 6. Clean: delete all created
        object_key3 = res.json()['new_key']
        self.client.delete(TEST_BUCKET_1, [prefix1, object_key2, object_key3])
        
    def test_case3(self):
        """
        # dir from nested dir to root
        # dir from nested dir up
        # dir from nested dir down
        """
        test_dir, dir_name1, dir_name2, dir_name3 = ["test_dir", 'dir_1', 'dir_2', 'dir_3']  # не-рандом для наглядности, т.к. баг на response
        prefix_test, prefix1, prefix2, prefix3 = encode_to_hex(dir_names=[test_dir, dir_name1, dir_name2, dir_name3])
        dir1_path = prefix1
        dir2_path = prefix1 + prefix2
        dir3_path = prefix1 + prefix2 + prefix3
        self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name1, '')
        self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name2, dir1_path)
        self.client.create_pseudo_directory(TEST_BUCKET_1, test_dir, dir2_path)
        self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name3, dir2_path)

        # 2.1 copy test_dir from dir2 to root
        res = self.client.copy(TEST_BUCKET_1, TEST_BUCKET_1, {prefix_test: test_dir}, dir2_path, '')
        print(res.content.decode())
        print(res.status_code)
        # self.assertEqual(res.status_code, 200)
        # object_key1 = res.json()['new_key']
        # self.assertEqual(res.json(), 200)

        # 2.2 copy test_dir from dir2 down to dir1
        res = self.client.copy(TEST_BUCKET_1, TEST_BUCKET_1, {prefix_test: test_dir}, dir2_path, prefix1)
        print(res.content.decode())
        print(res.status_code)
        # self.assertEqual(res.status_code, 200)
        # object_key2 = res.json()['new_key']

        # 2.3 copy test_dir from dir2 up to dir3
        res = self.client.copy(TEST_BUCKET_1, TEST_BUCKET_1, {prefix_test: test_dir}, dir2_path, dir3_path)
        print(res.content.decode())
        print(res.status_code)
        # self.assertEqual(res.status_code, 200)
        # object_key3 = res.json()['new_key']

        """
        Таже ошибка что и в test_case1 - статус 304, без данных на response, но операция копирования проходит успешно как задана.
        """

        # clean: delete created dirs
        res = self.client.delete(TEST_BUCKET_1, [prefix1, prefix_test])
        self.assertEqual(set(res.json()), {prefix1, prefix_test})  # костыль, мини-ассерт

    def test_case4(self):
        """
        # 1. upload file to root
        # 2. rename file
        # 3. copy to nested dir
        # 4. copy again. file should be replaced and not created conflicted copy
        """
        # 1. upload a file to root and create 2 directories in root
        fn = '025587.jpg'
        res = self.client.upload(TEST_BUCKET_1, fn)
        object_key = res['object_key']

        dir_name1, dir_name2 = [generate_random_name() for _ in range(2)]
        prefix1, prefix2 = encode_to_hex(dir_names=[dir_name1, dir_name2])
        self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name1)
        self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name2, prefix1)

        # 2. copy uploaded file from root to dir2
        new_fn = generate_random_name()
        object_keys = {object_key: new_fn}
        res = self.client.copy(TEST_BUCKET_1, TEST_BUCKET_1, object_keys, '', prefix1 + prefix2)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()['dst_prefix'], prefix1+prefix2)
        self.assertEqual(res.json()['renamed'], True)
        self.assertEqual(res.json()['dst_orig_name'], new_fn)
        object_key1 = res.json()['new_key']

        # 3. copy again
        res = self.client.copy(TEST_BUCKET_1, TEST_BUCKET_1, object_keys, '', prefix1 + prefix2)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()['dst_prefix'], prefix1+prefix2)
        self.assertEqual(res.json()['dst_orig_name'], new_fn)

        # 4. check for File is replaced and not created conflicted copy
        res = self.client.get_list(TEST_BUCKET_1, prefix1+prefix2)
        data = res.json()
        self.assertEqual(len(data.get('list')), 1)  # if conflicted copy => length > 1
        self.assertEqual(data.get('list')[0].get('orig_name'), new_fn)
        self.assertEqual(data.get('list')[0].get('object_key'), object_key1)

        # clean: delete all created
        self.client.delete(TEST_BUCKET_1, [object_key, prefix1])


if __name__ == "__main__":
    unittest.main()

