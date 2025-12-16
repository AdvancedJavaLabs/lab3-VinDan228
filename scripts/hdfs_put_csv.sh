set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

LOCAL_DIR="${1:-$ROOT_DIR/csv}"
HDFS_DIR="${2:-/input/sales}"
SERVICE="${HADOOP_NAMENODE_SERVICE:-namenode}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found" >&2
  exit 127
fi

if [[ ! -d "$LOCAL_DIR" ]]; then
  echo "Local dir not found: $LOCAL_DIR" >&2
  exit 2
fi

shopt -s nullglob
FILES=("$LOCAL_DIR"/*.csv)
shopt -u nullglob

if (( ${#FILES[@]} == 0 )); then
  echo "No .csv files found in: $LOCAL_DIR" >&2
  exit 2
fi

for f in "${FILES[@]}"; do
  if head -n 1 "$f" | grep -q 'version https://git-lfs.github.com/spec/v1'; then
    echo "Detected Git LFS pointer file (not real CSV): $f" >&2
    echo "Run: git lfs install && git lfs pull" >&2
    exit 3
  fi
done

TMP_DIR="/tmp/csv_upload_$$"

echo "Creating HDFS dir: $HDFS_DIR"
docker compose exec -T "$SERVICE" bash -lc "'${HADOOP_HOME:-/opt/hadoop-3.2.1}/bin/hdfs' dfs -mkdir -p '$HDFS_DIR'"

echo "Uploading ${#FILES[@]} files from $LOCAL_DIR to HDFS:$HDFS_DIR"
docker compose exec -T "$SERVICE" bash -lc "rm -rf '$TMP_DIR' && mkdir -p '$TMP_DIR'"

for f in "${FILES[@]}"; do
  base="$(basename "$f")"
  docker compose cp "$f" "$SERVICE:$TMP_DIR/$base"
  docker compose exec -T "$SERVICE" bash -lc "'${HADOOP_HOME:-/opt/hadoop-3.2.1}/bin/hdfs' dfs -put -f '$TMP_DIR/$base' '$HDFS_DIR/'"
done

docker compose exec -T "$SERVICE" bash -lc "rm -rf '$TMP_DIR'"

echo "Done. HDFS listing:"
docker compose exec -T "$SERVICE" bash -lc "'${HADOOP_HOME:-/opt/hadoop-3.2.1}/bin/hdfs' dfs -ls '$HDFS_DIR' || true"



