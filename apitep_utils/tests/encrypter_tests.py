import unittest

from apitep import Encrypter

class TestEncrypter(unittest.TestCase):

    def test_encrypt_sha1(self):
        input_string = "0000000T"
        output_string = "a77a281c33079af99be8f91228d72c5966f336db"
        encrypted_string = Encrypter.encrypt(input_string, Encrypter.Algorithm.SHA1)
        self.assertEqual(
            encrypted_string, output_string,
            f"{input_string} encrypted string should be {output_string}")

    def test_encrypt_sha224(self):
        input_string = "0000000T"
        output_string = "2fcf766794a3109f1f689430320f1dfdca8a6698218f3c3fd90fd55d"
        encrypted_string = Encrypter.encrypt(input_string, Encrypter.Algorithm.SHA224)
        self.assertEqual(
            encrypted_string, output_string,
            f"{input_string} encrypted string should be {output_string}")

    def test_encrypt_sha256(self):
        input_string = "0000000T"
        output_string = "30b0e710590456985ca26460f35dd5a1f29607cdb0dc6763e33bf74b744488e3"
        encrypted_string = Encrypter.encrypt(input_string, Encrypter.Algorithm.SHA256)
        self.assertEqual(
            encrypted_string, output_string,
            f"{input_string} encrypted string should be {output_string}")

    def test_encrypt_sha384(self):
        input_string = "0000000T"
        output_string = "9534bfbb4b9dba8b0d451a241eacf9d7e57c54a73b352343da311b938f69253e6134c8a4ec641f805c090c80df64d4e4"
        encrypted_string = Encrypter.encrypt(input_string, Encrypter.Algorithm.SHA384)
        self.assertEqual(
            encrypted_string, output_string,
            f"{input_string} encrypted string should be {output_string}")

    def test_encrypt_sha512(self):
        input_string = "0000000T"
        output_string = "46fe2a097b664a78cd75e02a3b54cb7690f9094975a58a233fae6f59ce4051ab1e7c64cc758fe656c3b42e73641c442b46e9a6094bbd922e7190a3f395186d31"
        encrypted_string = Encrypter.encrypt(input_string, Encrypter.Algorithm.SHA512)
        self.assertEqual(
            encrypted_string, output_string,
            f"{input_string} encrypted string should be {output_string}")

