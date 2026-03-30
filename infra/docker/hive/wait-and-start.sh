#!/bin/bash
set -e

echo "Aguardando PostgreSQL em postgres-lakehouse:5432..."
until nc -z postgres-lakehouse 5432; do
  echo "  PostgreSQL não disponível ainda, aguardando..."
  sleep 3
done
echo "PostgreSQL disponível. Iniciando Hive Metastore..."

# Chama o entrypoint original da imagem apache/hive
exec /bin/bash /entrypoint.sh