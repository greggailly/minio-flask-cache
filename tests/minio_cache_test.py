import pickle
import time
from unittest.mock import patch, ANY, call, MagicMock

from minio import S3Error
from minio_cache import MinioCacheBackend


class TestMinioCacheBackend:
    def test_factory(self):
        # Arrange
        endpoint = "localhost:9000"
        access_key = "minioadmin"
        secret_key = "minioadmin"
        bucket = "flask-cache"
        secure = False
        default_timeout = 300
        key_prefix = "custom_key_prefix:"

        config = {
            "CACHE_MINIO_ENDPOINT": endpoint,
            "CACHE_MINIO_ACCESS_KEY": access_key,
            "CACHE_MINIO_SECRET_KEY": secret_key,
            "CACHE_MINIO_BUCKET": bucket,
            "CACHE_MINIO_SECURE": secure,
            "CACHE_DEFAULT_TIMEOUT": default_timeout,
            "CACHE_KEY_PREFIX": key_prefix,
        }

        app = MagicMock()
        app.config = config

        # Act
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend.factory(
                app,
                [],
                {},
            )

        # Assert
        assert type(backend) == MinioCacheBackend

    @patch("minio_cache.Minio")
    @patch("minio_cache.MinioCacheBackend._ensure_bucket_exists")
    def test_init(self, mock_ensure_bucket_exists, mock_Minio):
        # Arrange
        endpoint = "localhost:9000"
        access_key = "minioadmin"
        secret_key = "minioadmin"
        bucket = "flask-cache"
        secure = False
        default_timeout = 300
        key_prefix = "custom_key_prefix:"

        # Act
        backend = MinioCacheBackend(
            endpoint,
            access_key,
            secret_key,
            bucket,
            secure,
            default_timeout,
            key_prefix,
        )

        # Assert
        assert backend.key_prefix == key_prefix
        assert backend.bucket == bucket
        assert backend.default_timeout == default_timeout
        mock_Minio.assert_called_once_with(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        assert type(backend.client) == type(mock_Minio.return_value)
        mock_ensure_bucket_exists.assert_called_once()

    @patch("minio_cache.Minio")
    def test_ensure_bucket_exists_creates_bucket(self, mock_Minio):
        # Arrange
        mock_client = mock_Minio.return_value
        mock_client.bucket_exists.return_value = False

        # Act
        MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")

        # Assert
        mock_client.bucket_exists.assert_called_once_with("flask-cache")
        mock_client.make_bucket.assert_called_once_with("flask-cache")

    def test_get_object_name(self):
        # Arrange
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend(
                "localhost:9000", "minioadmin", "minioadmin", key_prefix="test_prefix:"
            )
        key = "sample_key"

        # Act
        object_name = backend._get_object_name(key)

        # Assert
        assert object_name == "test_prefix:sample_key"

    @patch("minio_cache.time.time", return_value=1000)
    def test_serialize_value(self, mock_time):
        # Arrange
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")
        value = {"a": 1, "b": 2}
        timeout = 600

        # Act
        serialized = backend._serialize_value(value, timeout)

        # Assert
        assert isinstance(serialized, bytes)

        # Deserialize and check the value
        deserialized = pickle.loads(serialized)
        assert deserialized["value"] == value
        assert deserialized["expires_at"] == mock_time.return_value + timeout

    def test_deserialize_value_not_expired(self):
        # Arrange
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")
        value = {"a": 1, "b": 2}
        expires_at = time.time() + 600
        data = pickle.dumps({"value": value, "expires_at": expires_at})

        # Act
        result = backend._deserialize_value(data)

        # Assert
        assert result == value

    def test_deserialize_value_expired(self):
        # Arrange
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")
        value = {"a": 1, "b": 2}
        expires_at = time.time() - 10  # Expired 10 seconds ago
        data = pickle.dumps({"value": value, "expires_at": expires_at})

        # Act
        result = backend._deserialize_value(data)

        # Assert
        assert result is None

    @patch("minio_cache.Minio")
    @patch("minio_cache.MinioCacheBackend._get_object_name")
    @patch("minio_cache.MinioCacheBackend._deserialize_value")
    def test_get(self, mock_deserialize_value, mock_get_object_name, mock_Minio):
        # Arrange
        mock_client = mock_Minio.return_value

        # Act
        backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")
        key = "sample_key"
        object = backend.get(key)

        # Assert
        mock_get_object_name.assert_called_once_with(key)
        mock_client.get_object.assert_called_once_with(
            "flask-cache", mock_get_object_name.return_value
        )
        mock_client.get_object.return_value.read.assert_called_once()
        mock_client.get_object.return_value.close.assert_called_once()
        mock_client.get_object.return_value.release_conn.assert_called_once()
        mock_deserialize_value.assert_called_once_with(
            mock_client.get_object.return_value.read.return_value
        )
        assert object == mock_deserialize_value.return_value

    @patch("minio_cache.Minio")
    @patch("minio_cache.MinioCacheBackend._get_object_name")
    @patch("minio_cache.MinioCacheBackend._deserialize_value", return_value=None)
    @patch("minio_cache.MinioCacheBackend.delete")
    def test_get_expired(
        self, mock_delete, mock_deserialize_value, mock_get_object_name, mock_Minio
    ):
        # Arrange
        mock_client = mock_Minio.return_value

        # Act
        backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")
        key = "sample_key"
        object = backend.get(key)

        # Assert
        mock_get_object_name.assert_called_once_with(key)
        mock_client.get_object.assert_called_once_with(
            "flask-cache", mock_get_object_name.return_value
        )
        mock_client.get_object.return_value.read.assert_called_once()
        mock_client.get_object.return_value.close.assert_called_once()
        mock_client.get_object.return_value.release_conn.assert_called_once()
        mock_deserialize_value.assert_called_once_with(
            mock_client.get_object.return_value.read.return_value
        )
        mock_delete.assert_called_once_with(key)
        assert object == mock_deserialize_value.return_value

    @patch("minio_cache.Minio")
    @patch("minio_cache.MinioCacheBackend._get_object_name")
    @patch("minio_cache.MinioCacheBackend._serialize_value")
    @patch("minio_cache.BytesIO")
    def test_set(
        self, mock_bytesio, mock_serialize_value, mock_get_object_name, mock_Minio
    ):
        # Arrange
        mock_client = mock_Minio.return_value

        # Act
        backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")
        key = "sample_key"
        value = {"a": 1, "b": 2}
        timeout = 600
        success = backend.set(key, value, timeout)

        # Assert
        mock_get_object_name.assert_called_once_with(key)
        mock_serialize_value.assert_called_once_with(value, timeout)
        mock_bytesio.assert_called_once_with(mock_serialize_value.return_value)
        mock_client.put_object.assert_called_once_with(
            "flask-cache",
            mock_get_object_name.return_value,
            mock_bytesio.return_value,
            length=len(mock_serialize_value.return_value),
            content_type="application/octet-stream",
        )
        assert success is True

    def test_set_error(self):
        # Arrange
        with patch("minio_cache.Minio") as mock_Minio:
            mock_client = mock_Minio.return_value
            mock_client.put_object.side_effect = S3Error(
                resource="res",
                request_id="req",
                host_id="host",
                response="resp",
                code=500,
                message="Error",
            )

            backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")
            key = "sample_key"
            value = {"a": 1, "b": 2}
            timeout = 600

            # Act
            success = backend.set(key, value, timeout)

            # Assert
            assert success is False

    @patch("minio_cache.MinioCacheBackend.has")
    @patch("minio_cache.MinioCacheBackend.set")
    def test_add_does_not_have_key(self, mock_set, mock_has):
        # Arrange
        mock_has.return_value = False

        # Act
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")
        key = "sample_key"
        value = {"a": 1, "b": 2}
        timeout = 600
        backend.add(key, value, timeout)

        # Assert
        mock_has.assert_called_once_with(key)
        mock_set.assert_called_once_with(key, value, timeout)

    @patch("minio_cache.MinioCacheBackend.has")
    @patch("minio_cache.MinioCacheBackend.set")
    def test_add_has_key(self, mock_set, mock_has):
        # Arrange
        mock_has.return_value = True

        # Act
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")
        key = "sample_key"
        value = {"a": 1, "b": 2}
        timeout = 600
        backend.add(key, value, timeout)

        # Assert
        mock_has.assert_called_once_with(key)
        assert mock_set.call_count == 0

    @patch("minio_cache.MinioCacheBackend._get_object_name")
    @patch("minio_cache.Minio")
    def test_delete(self, mock_Minio, mock_get_object_name):
        # Arrange
        mock_client = mock_Minio.return_value

        backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")

        # Act
        key = "sample_key"
        success = backend.delete(key)

        # Assert
        mock_client.remove_object.assert_called_once_with(
            "flask-cache", backend._get_object_name(key)
        )
        assert success is True

    @patch("minio_cache.MinioCacheBackend._get_object_name")
    @patch("minio_cache.Minio")
    def test_delete_object_does_not_exist(self, mock_Minio, mock_get_object_name):
        # Arrange
        mock_client = mock_Minio.return_value
        mock_client.remove_object.side_effect = S3Error(
            resource="res",
            request_id="req",
            host_id="host",
            response="resp",
            code=500,
            message="Error",
        )

        backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")

        # Act
        key = "sample_key"
        success = backend.delete(key)

        # Assert
        mock_client.remove_object.assert_called_once_with(
            "flask-cache", backend._get_object_name(key)
        )
        assert success is True

    @patch("minio_cache.MinioCacheBackend.get")
    def test_has(self, mock_get):
        # Arrange
        mock_get.return_value = {"a": 1, "b": 2}
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")

        # Act
        key = "sample_key"
        has_key = backend.has(key)

        # Assert
        mock_get.assert_called_once_with(key)
        assert has_key is True

    @patch("minio_cache.MinioCacheBackend.get")
    def test_has_not(self, mock_get):
        # Arrange
        mock_get.return_value = None
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")

        # Act
        key = "sample_key"
        has_key = backend.has(key)

        # Assert
        mock_get.assert_called_once_with(key)
        assert has_key is False

    @patch("minio_cache.Minio")
    def test_clear(self, mock_Minio):
        # Arrange
        mock_client = mock_Minio.return_value
        mock_obj1 = MagicMock()
        mock_obj1.object_name = "obj1"
        mock_obj2 = MagicMock()
        mock_obj2.object_name = "obj2"
        mock_client.list_objects.return_value = [
            mock_obj1,
            mock_obj2,
        ]
        backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")

        # Act
        backend.clear()

        # Assert
        mock_client.list_objects.assert_called_once_with(
            "flask-cache", prefix="cache:", recursive=True
        )
        mock_client.remove_object.assert_has_calls(
            [call("flask-cache", "obj1"), call("flask-cache", "obj2")]
        )

    @patch("minio_cache.Minio")
    def test_clear_error(self, mock_Minio):
        # Arrange
        mock_client = mock_Minio.return_value
        mock_client.list_objects.side_effect = S3Error(
            resource="res",
            request_id="req",
            host_id="host",
            response="resp",
            code=500,
            message="Error",
        )
        backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")

        # Act
        res = backend.clear()

        # Assert
        assert res is False

    @patch("minio_cache.MinioCacheBackend.get")
    def test_get_many(self, mock_get):
        mock_get.side_effect = [{"a": 1, "b": 2}, "error"]
        # Arrange
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")

        # Act
        values = backend.get_many("sample_key1", "sample_key2")

        # Assert
        mock_get.assert_has_calls([call("sample_key1"), call("sample_key2")])
        assert values == [{"a": 1, "b": 2}, "error"]

    @patch("minio_cache.MinioCacheBackend.set")
    def test_set_many(self, mock_set):
        mock_set.side_effect = [True, False]
        # Arrange
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")

        # Act
        values = backend.set_many(
            {"sample_key1": {"a": 1, "b": 2}, "sample_key2": "error"}
        )

        # Assert
        mock_set.assert_has_calls(
            [
                call("sample_key1", {"a": 1, "b": 2}, None),
                call("sample_key2", "error", None),
            ]
        )
        assert values == ["sample_key2"]

    @patch("minio_cache.MinioCacheBackend.delete")
    def test_delete_many_ok(self, mock_delete):
        # Arrange
        mock_delete.side_effect = [True, True]
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")

        # Act
        res = backend.delete_many("sample_key1", "sample_key2")

        # Assert
        mock_delete.assert_has_calls([call("sample_key1"), call("sample_key2")])
        assert res == True

    @patch("minio_cache.MinioCacheBackend.delete")
    def test_delete_many_ok(self, mock_delete):
        # Arrange
        mock_delete.side_effect = [True, False]
        with patch("minio_cache.Minio"):
            backend = MinioCacheBackend("localhost:9000", "minioadmin", "minioadmin")

        # Act
        res = backend.delete_many("sample_key1", "sample_key2")

        # Assert
        mock_delete.assert_has_calls([call("sample_key1"), call("sample_key2")])
        assert res == False
