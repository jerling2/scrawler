import zlib

def zlib_decompress(data: bytes) -> str:
    return zlib.decompress(data).decode()

def zlib_compress(data: str) -> bytes:
    return zlib.compress(data.encode())