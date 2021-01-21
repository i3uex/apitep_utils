import hashlib
import logging
from enum import Enum


class Encrypter:
    class Algorithm(Enum):
        SHA1 = 1
        SHA224 = 2
        SHA256 = 3
        SHA384 = 4
        SHA512 = 5

    @staticmethod
    def encrypt(string: str, algorithm: Algorithm):
        """
        Encrypt a string with a given algorithm.

        :param string: string to encrypt.
        :param algorithm: algorithm to use in the encryption.
        :return: string encrypter input string.
        :rtype: str
        """
        # logging.info(f"Encrypt string")
        # logging.debug(f"Encrypter.encrypt("
        #               f"string={string}, ")
        #               f"algorithm={algorithm})")

        if not isinstance(algorithm, Encrypter.Algorithm):
            raise TypeError("algorithm must be an instance of Algorithm enum")

        if algorithm == Encrypter.Algorithm.SHA1:
            return hashlib.sha1(string.encode()).hexdigest()
        elif algorithm == Encrypter.Algorithm.SHA224:
            return hashlib.sha224(string.encode()).hexdigest()
        elif algorithm == Encrypter.Algorithm.SHA256:
            return hashlib.sha256(string.encode()).hexdigest()
        elif algorithm == Encrypter.Algorithm.SHA384:
            return hashlib.sha384(string.encode()).hexdigest()
        elif algorithm == Encrypter.Algorithm.SHA512:
            return hashlib.sha512(string.encode()).hexdigest()
        else:
            raise Exception("algorithm must be an instance of Algorithm enum")
