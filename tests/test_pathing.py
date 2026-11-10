import unittest
from pathlib import Path

from storage.pathing import resolve_path


class TestPathResolution(unittest.TestCase):
    @staticmethod
    def _cache_path(path: str) -> Path:
        return Path("/cache-root") / path.lstrip("/")

    def test_resolve_path_preserves_requested_and_canonical_cache_targets(self):
        resolved = resolve_path(
            "/videos/movie.mp4.remote0",
            ["remote0", "remote1"],
            self._cache_path,
        )

        self.assertEqual("/videos/movie.mp4.remote0", resolved.requested_path)
        self.assertEqual("/videos/movie.mp4", resolved.canonical_path)
        self.assertEqual("remote0", resolved.backend_name)
        self.assertEqual(Path("/cache-root/videos/movie.mp4.remote0"), resolved.cache_path)
        self.assertEqual(Path("/cache-root/videos/movie.mp4"), resolved.canonical_cache_path)
        self.assertEqual("/videos", resolved.parent_dir)

    def test_resolve_path_normalizes_relative_paths(self):
        resolved = resolve_path("docs/readme.txt", ["remote0"], self._cache_path)

        self.assertEqual("/docs/readme.txt", resolved.requested_path)
        self.assertEqual("/docs/readme.txt", resolved.canonical_path)
        self.assertIsNone(resolved.backend_name)
        self.assertEqual(Path("/cache-root/docs/readme.txt"), resolved.cache_path)
        self.assertEqual(Path("/cache-root/docs/readme.txt"), resolved.canonical_cache_path)

    def test_resolve_path_ignores_unknown_backend_suffixes(self):
        resolved = resolve_path("/docs/readme.txt.remote9", ["remote0"], self._cache_path)

        self.assertEqual("/docs/readme.txt.remote9", resolved.requested_path)
        self.assertEqual("/docs/readme.txt.remote9", resolved.canonical_path)
        self.assertIsNone(resolved.backend_name)
        self.assertEqual(Path("/cache-root/docs/readme.txt.remote9"), resolved.cache_path)
