from hashlib import sha3_256
from random import randint
from typing import List, Tuple

from utilitybelt import dev_urandom_entropy

from aleph_vrf.types import Nonce


def xor_all(x: List[bytes]) -> bytes:
    """XORs all the bytes in the list together."""
    result = x[0]
    for i in range(1, len(x)):
        result = bytes([a ^ b for a, b in zip(result, x[i])])
    return result


def int_to_bytes(x: int, n: int = 0) -> bytes:
    """
    Converts an integer to bytes.
    If `n` is specified, pads the number to reach n bytes.
    """
    return x.to_bytes(max((x.bit_length() + 7) // 8, n), "big")


def bytes_to_int(x: bytes) -> int:
    """Converts bytes to an integer."""
    return int.from_bytes(x, "big")


def bytes_to_binary(x: bytes) -> str:
    """Converts bytes to a binary string."""
    return "".join(format(b, "08b") for b in x)


def binary_to_bytes(s: str, n: int = 0):
    """Converts binary string to bytes."""
    return int(s, 2).to_bytes(max((len(s) + 7) // 8, n), byteorder="big")


def generate_nonce() -> Nonce:
    """Generates pseudo-random nonce number."""
    return Nonce(randint(0, 100000000))


def generate(n: int, nonce: Nonce) -> Tuple[bytes, str]:
    """Generates a number of random bytes and hashes them with the nonce."""
    random_bytes: bytes = dev_urandom_entropy(n)
    random_hash = sha3_256(random_bytes + int_to_bytes(nonce)).hexdigest()
    return random_bytes, random_hash


def verify(random_bytes: bytes, nonce: int, random_hash: str) -> bool:
    """Verifies that the random bytes were generated by the given nonce."""
    return random_hash == sha3_256(random_bytes + int_to_bytes(nonce)).hexdigest()
