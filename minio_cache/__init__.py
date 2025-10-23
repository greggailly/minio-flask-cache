"""
Custom Flask-Cache backend using MinIO for object storage.

Installation:

Simply install with pip !

pip install minio-flask-cache


Usage:
    from flask import Flask
    from flask_caching import Cache

    app = Flask(__name__)
    app.config['CACHE_TYPE'] = 'path.to.MinioCacheBackend'
    app.config['CACHE_MINIO_ENDPOINT'] = 'localhost:9000'
    app.config['CACHE_MINIO_ACCESS_KEY'] = 'minioadmin'
    app.config['CACHE_MINIO_SECRET_KEY'] = 'minioadmin'
    app.config['CACHE_MINIO_BUCKET'] = 'flask-cache'
    app.config['CACHE_MINIO_SECURE'] = False  # Set to True for HTTPS
    app.config['CACHE_DEFAULT_TIMEOUT'] = 300

    cache = Cache(app)
"""

import pickle
import time
from io import BytesIO
from typing import Any, Optional

from flask_caching.backends.base import BaseCache
from minio import Minio
from minio.error import S3Error


class MinioCacheBackend(BaseCache):
    """Cache backend that stores data in MinIO object storage."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str = "flask-cache",
        secure: bool = False,
        default_timeout: int = 300,
        key_prefix: str = "cache:",
        **kwargs,
    ):
        """
        Initialize MinIO cache backend.

        Args:
            endpoint: MinIO server endpoint (e.g., 'localhost:9000')
            access_key: MinIO access key
            secret_key: MinIO secret key
            bucket: Bucket name for cache storage
            secure: Use HTTPS if True, HTTP if False
            default_timeout: Default cache timeout in seconds
            key_prefix: Prefix for all cache keys
        """
        super().__init__(default_timeout)

        self.key_prefix = key_prefix
        self.bucket = bucket

        # Initialize MinIO client
        self.client = Minio(
            endpoint, access_key=access_key, secret_key=secret_key, secure=secure
        )

        # Create bucket if it doesn't exist
        self._ensure_bucket_exists()

    @classmethod
    def factory(cls, app, kwargs, args, options):
        args.append(kwargs["CACHE_MINIO_ENDPOINT"])
        args.append(kwargs["CACHE_MINIO_ACCESS_KEY"])
        args.append(kwargs["CACHE_MINIO_SECRET_KEY"])
        args.append(kwargs["CACHE_MINIO_BUCKET"])
        args.append(kwargs["CACHE_MINIO_SECURE"])
        args.append(kwargs["CACHE_DEFAULT_TIMEOUT"])
        args.append(kwargs["CACHE_KEY_PREFIX"])

        return cls(*args)

    def _ensure_bucket_exists(self):
        """Create the cache bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
        except S3Error as e:
            raise RuntimeError(f"Failed to create bucket: {e}")

    def _get_object_name(self, key: str) -> str:
        """Convert cache key to MinIO object name."""
        return f"{self.key_prefix}{key}"

    def _serialize_value(self, value: Any, timeout: Optional[int]) -> bytes:
        """Serialize value with expiration timestamp."""
        expires_at = None if timeout is None else time.time() + timeout
        data = {"value": value, "expires_at": expires_at}
        return pickle.dumps(data)

    def _deserialize_value(self, data: bytes) -> Optional[Any]:
        """Deserialize value and check expiration."""
        try:
            obj = pickle.loads(data)
            expires_at = obj.get("expires_at")

            # Check if expired
            if expires_at is not None and time.time() > expires_at:
                return None

            return obj["value"]
        except (pickle.PickleError, KeyError):
            return None

    def get(self, key: str) -> Optional[Any]:
        """
        Get a cached value.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found or expired
        """
        object_name = self._get_object_name(key)

        try:
            response = self.client.get_object(self.bucket, object_name)
            data = response.read()
            response.close()
            response.release_conn()

            value = self._deserialize_value(data)

            # Remove expired object
            if value is None:
                self.delete(key)

            return value
        except S3Error:
            return None

    def set(self, key: str, value: Any, timeout: Optional[int] = None) -> bool:
        """
        Set a cached value.

        Args:
            key: Cache key
            value: Value to cache
            timeout: Timeout in seconds (None for no expiration)

        Returns:
            True if successful, False otherwise
        """
        if timeout is None:
            timeout = self.default_timeout

        object_name = self._get_object_name(key)

        try:
            data = self._serialize_value(value, timeout)
            data_stream = BytesIO(data)

            self.client.put_object(
                self.bucket,
                object_name,
                data_stream,
                length=len(data),
                content_type="application/octet-stream",
            )
            return True
        except S3Error:
            return False

    def add(self, key: str, value: Any, timeout: Optional[int] = None) -> bool:
        """
        Add a value only if key doesn't exist.

        Args:
            key: Cache key
            value: Value to cache
            timeout: Timeout in seconds

        Returns:
            True if added, False if key exists
        """
        if self.has(key):
            return False
        return self.set(key, value, timeout)

    def delete(self, key: str) -> bool:
        """
        Delete a cached value.

        Args:
            key: Cache key

        Returns:
            True if successful
        """
        object_name = self._get_object_name(key)

        try:
            self.client.remove_object(self.bucket, object_name)
            return True
        except S3Error:
            return True  # Object doesn't exist is success

    def has(self, key: str) -> bool:
        """
        Check if key exists and is not expired.

        Args:
            key: Cache key

        Returns:
            True if key exists and is valid
        """
        return self.get(key) is not None

    def clear(self) -> bool:
        """
        Clear all cached values.

        Returns:
            True if successful
        """
        try:
            # List and delete all objects with the key prefix
            objects = self.client.list_objects(
                self.bucket, prefix=self.key_prefix, recursive=True
            )

            for obj in objects:
                self.client.remove_object(self.bucket, obj.object_name)

            return True
        except S3Error:
            return False

    def get_many(self, *keys: str) -> list:
        """
        Get multiple cached values.

        Args:
            keys: Cache keys

        Returns:
            List of values (None for missing/expired keys)
        """
        return [self.get(key) for key in keys]

    def set_many(self, mapping: dict, timeout: Optional[int] = None) -> list:
        """
        Set multiple cached values.

        Args:
            mapping: Dictionary of key-value pairs
            timeout: Timeout in seconds

        Returns:
            List of keys that failed to be set
        """
        failed = []
        for key, value in mapping.items():
            if not self.set(key, value, timeout):
                failed.append(key)
        return failed

    def delete_many(self, *keys: str) -> bool:
        """
        Delete multiple cached values.

        Args:
            keys: Cache keys

        Returns:
            True if all deletions successful
        """
        success = True
        for key in keys:
            if not self.delete(key):
                success = False
        return success
