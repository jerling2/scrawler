#!/bin/bash
set -e

set -a
. .env 
set +a

if ! command -v milvus_cli ; then
    echo "Error: milvus_cli is not installed" >&2
    exit 1
fi

echo "🔴 [milvus] Create database '$SCRAWLER_MILVUS_DATABASE' ..."
milvus_cli <<-COMMANDS > /dev/null 2>&1
    connect -uri http://localhost:19530
    create database -db $SCRAWLER_MILVUS_DATABASE
COMMANDS
echo "🟢 Done!"
