import hashlib

def calculate_file_hash_from_bytes(content: bytes) -> str:
    """Обчислює SHA-256 хеш з вмісту файлу в пам'яті."""
    return hashlib.sha256(content).hexdigest()


def calculate_file_hash_from_path(file_path: str, buffer_size: int = 65536) -> str:
    """Обчислює SHA-256 хеш файлу, читаючи його частинами."""
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(buffer_size)
            if not data:
                break
            sha256_hash.update(data)
    return sha256_hash.hexdigest()