set -e

(
  cd "$(dirname "$0")"
  go build -o /tmp/toy_kafka app/*.go
)

exec /tmp/toy_kafka "$@"
