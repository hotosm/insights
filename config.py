import os

# Type defaults here
_DATABASE_HOST = 'None'
_DATABASE_PORT = '5432'
_DATABASE_NAME = 'postgres_dba'
_DATABASE_USER = 'None'
_DATABASE_PASSWORD = 'None'

# Pick from ENVVAR if available, else fallback to values above
DATABASE_HOST = os.getenv('DATABASE_HOST', _DATABASE_HOST)
DATABASE_PORT = os.getenv('DATABASE_PORT', _DATABASE_PORT)
DATABASE_NAME = os.getenv('DATABASE_NAME', _DATABASE_NAME)
DATABASE_USER = os.getenv('DATABASE_USER', _DATABASE_USER)
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD', _DATABASE_PASSWORD)

if DATABASE_PASSWORD is None:
    print("Database password is not set via ENVVARS..")

