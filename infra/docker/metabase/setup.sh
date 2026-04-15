#!/bin/sh

echo "Aguardando Metabase ficar 100% pronto..."

# espera health OK
until curl -s http://metabase:3000/api/health | grep -q '"ok"'; do
  sleep 5
done

echo "Health OK, aguardando inicialização completa..."
sleep 60   # 🔥 ESSENCIAL (migrations internas)

# =============================
# VERIFICA SE JÁ FOI CONFIGURADO
# =============================
SETUP_TOKEN=$(curl -s http://metabase:3000/api/session/properties | sed -n 's/.*"setup-token":"\([^"]*\)".*/\1/p')

echo "SETUP TOKEN: $SETUP_TOKEN"

if [ -n "$SETUP_TOKEN" ]; then
  echo "Executando setup inicial..."

  curl -s -X POST http://metabase:3000/api/setup \
    -H "Content-Type: application/json" \
    -d "{
      \"token\": \"$SETUP_TOKEN\",
      \"user\": {
        \"email\": \"admin@admin.com\",
        \"password\": \"adminmetabase123\",
        \"first_name\": \"Admin\",
        \"last_name\": \"User\"
      },
      \"prefs\": {
        \"site_name\": \"Lakehouse\"
      },
      \"database\": null
    }"

  echo "Setup enviado, aguardando..."
  sleep 15
else
  echo "Metabase já configurado."
fi

# =============================
# LOGIN (COM RETRY)
# =============================
echo "Fazendo login..."

for i in 1 2 3 4 5
do
  LOGIN_RESPONSE=$(curl -s -X POST http://metabase:3000/api/session \
    -H "Content-Type: application/json" \
    -d '{
      "username": "admin@admin.com",
      "password": "adminmetabase123"
    }')

  SESSION_ID=$(echo $LOGIN_RESPONSE | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')

  if [ -n "$SESSION_ID" ]; then
    break
  fi

  echo "Tentativa $i falhou, retry..."
  sleep 5
done

echo "SESSION: $SESSION_ID"

if [ -z "$SESSION_ID" ]; then
  echo "ERRO: não conseguiu logar"
  exit 1
fi

# =============================
# VERIFICA SE DB JÁ EXISTE
# =============================
echo "Verificando databases existentes..."

DB_LIST=$(curl -s -X GET http://metabase:3000/api/database \
  -H "X-Metabase-Session: $SESSION_ID")

echo "$DB_LIST"

if echo "$DB_LIST" | grep -q "Lakehouse"; then
  echo "Database já existe. Pulando criação."
  exit 0
fi

# =============================
# CRIAR DATABASE (TRINO)
# =============================
echo "Criando database Lakehouse..."

CREATE_RESPONSE=$(curl -s -X POST http://metabase:3000/api/database \
  -H "Content-Type: application/json" \
  -H "X-Metabase-Session: $SESSION_ID" \
  -d '{
    "name": "Lakehouse",
    "engine": "starburst",
    "details": {
      "host": "trino",
      "port": 8085,
      "catalog": "hive",
      "schema": "gold",
      "user": "admin",
      "ssl": false
    },
    "is_full_sync": true,
    "is_on_demand": false
  }')

echo "Resposta criação DB:"
echo "$CREATE_RESPONSE"

# 🔥 valida erro
if echo "$CREATE_RESPONSE" | grep -q '"id"'; then
  echo "Database criado com sucesso!"
else
  echo "ERRO ao criar database:"
  echo "$CREATE_RESPONSE"
  exit 1
fi

echo "FINALIZADO COM SUCESSO!"