#!/bin/bash
set -e

host="$1"
port="$2"
shift 2
cmd="$@"

echo "Čekam da $host:$port bude dostupan..."

until nc -z "$host" "$port"; do
  echo "Kafka još nije spreman - spavam"
  sleep 5
done

echo "Kafka je spreman - pokretam aplikaciju"
exec $cmd