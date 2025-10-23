# Minio Cache

A custom cache backend for Flask-Caching based on Minio. Usable for applications like Superset as a cache backend out of the box.

## Installation
Simply install with pip !
```
pip install minio-flask-cache
```

## Usage
### Classic flask app
```python
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
```

### Superset cache backend
See [Superset Docs](https://superset.apache.org/docs/6.0.0/configuration/cache) for more details on superset cache backends.
Edit superset_config.py to add/edit the `CACHE_CONFIG` dict.
Config can be used for all cache configurations (FILTER_STATE_CACHE_CONFIG, EXPLORE_FORM_DATA_CACHE_CONFIG, CACHE_CONFIG and DATA_CACHE_CONFIG)
```python
CACHE_CONFIG = {
    "CACHE_TYPE": "MinioCacheBackend",
    "CACHE_MINIO_ENDPOINT": "localhost:9000",
    "CACHE_MINIO_ACCESS_KEY": "minioadmin",
    "CACHE_MINIO_SECRET_KEY": "minioadmin",
    "CACHE_MINIO_BUCKET": "superset",
    "CACHE_MINIO_SECURE": False  # Set to True for HTTPS,
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_"
}
```