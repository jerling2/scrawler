#!/bin/bash
set -e

echo "🔴 Retrieve nomic-embed-text model..."
docker exec scrawler-ollama ollama pull nomic-embed-text:latest
echo "🟢 Done!"
