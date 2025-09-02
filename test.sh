set -e

(cd "$(dirname "$0")"

echo "Building project..."
go build -o /tmp/codecrafters-build-kafka-go app/*.go
)

(echo "Running tests..."
go test ./app/... -v)