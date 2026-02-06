#!/bin/sh
until /usr/bin/mc alias set minio http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD; do
    echo '...Esperando a MinIO...'
    sleep 1
done

/usr/bin/mc mb --ignore-existing minio/warehouse
/usr/bin/mc mb --ignore-existing minio/landing
/usr/bin/mc mb --ignore-existing minio/processed
echo 'Init completado: Buckets listos.'