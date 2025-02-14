import os

# Configuration de base
SUPERSET_SECRET_KEY = 'votre_clé_secrète'
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# Configuration Redis
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': 'redis',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,
}

# Configuration Hive
SQLALCHEMY_CUSTOM_PASSWORD_STORE = {}

ADDITIONAL_DATABASE_CONFIG = {
    'HIVE': {
        'engine': 'hive',
        'host': 'hive-server',
        'port': 10000,
        'database': 'seismic_database',
        'auth': 'NOSASL'
    }
}

# Configuration supplémentaire
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
}

# Configuration de sécurité
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = []
ENABLE_PROXY_FIX = True

# Configuration des connexions autorisées
ALLOWED_HOSTS = ['*']

# Configuration des timeouts
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300