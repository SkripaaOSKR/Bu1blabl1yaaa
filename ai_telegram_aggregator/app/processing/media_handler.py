"""Media hash helpers and grouping support."""
from __future__ import annotations

import hashlib
from pathlib import Path

from PIL import Image
import imagehash


class MediaHandler:
    """Computes media hashes without filtering media out of output."""

    @staticmethod
    def image_hash(path: Path) -> str:
        with Image.open(path) as image:
            return str(imagehash.phash(image))

    @staticmethod
    def bytes_hash(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()
