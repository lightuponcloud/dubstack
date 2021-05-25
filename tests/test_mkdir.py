
import unittest

from client_base import (BASE_URL, TEST_BUCKET_1, USERNAME_1, PASSWORD_1)
from light_client import LightClient, generate_random_name, encode_to_hex


class MKdirTest(unittest.TestCase):
    """
    Test operation MKDIR

    # 1. Upload file(create pseudo-directory)
    # 2. Try to create directory with the same name
    # 3. Make sure error returned
    """

    def setUp(self):
        self.client = LightClient(BASE_URL, USERNAME_1, PASSWORD_1)
        self.client.login(USERNAME_1, PASSWORD_1)

    def test_mkdir(self):
        """
        1. Сreates pseudo-directory
        2. Makes request to create another one with same name
        3. Checks for error returned

        """
        # 1 Create pseudo-directory with random name
        dir_name = generate_random_name()
        response = self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name)
        self.assertEqual(response.status_code, 204)

        # 2 Try to create pseudo-directory with same name
        response = self.client.create_pseudo_directory(TEST_BUCKET_1, dir_name)

        # 3 Check for error is returned
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"error": 10})  # "10": "Directory exists already."

        # Clean: delete created pseudo-directory
        dir_name_prefix = [encode_to_hex(dir_name)]
        response = self.client.delete(TEST_BUCKET_1, dir_name_prefix)
        self.assertEqual(response.json(), dir_name_prefix)


if __name__ == '__main__':
    unittest.main()