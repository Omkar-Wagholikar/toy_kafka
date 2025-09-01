set -e

(
  cd "$(dirname "$0")"
  go build -o /tmp/codecrafters-build-kafka-go app/*.go
)

exec /tmp/codecrafters-build-kafka-go "$@"
