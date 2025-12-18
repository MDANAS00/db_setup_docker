#!/bin/bash
set -euo pipefail

source .env

SQL_MODE="STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"

OFFSET_FILE="${RESUME_FILE}"
SQL_FILE="${SQL_FILE}"
CONTAINER="${CONTAINER_NAME}"
DB="${MYSQL_DATABASE}"

# Detect compressed file
if [[ "${SQL_FILE}" == *.gz ]]; then
  READ_CMD="gzip -dc"
  FILE_SIZE=$(gzip -l "${SQL_FILE}" | awk 'NR==2 {print $2}')
else
  READ_CMD="cat"
  FILE_SIZE=$(stat -c%s "${SQL_FILE}")
fi

# Resume offset
OFFSET=0
[ -f "${OFFSET_FILE}" ] && OFFSET=$(cat "${OFFSET_FILE}")

echo "🚀 MariaDB Large Dump Import"
echo "📄 File: ${SQL_FILE}"
echo "📦 Size: $((FILE_SIZE / 1024 / 1024)) MB"
echo "▶ Resume offset: ${OFFSET} bytes"
echo ""

# Initial session setup (idempotent)
docker exec -i "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB}" <<SQL
SET SESSION sql_mode='${SQL_MODE}';
SET FOREIGN_KEY_CHECKS=0;
SET UNIQUE_CHECKS=0;
SQL

# Stream with resume + progress
(
  ${READ_CMD} "${SQL_FILE}" | tail -c +$((OFFSET + 1))
) | pv -s "${FILE_SIZE}" | docker exec -i "${CONTAINER}" \
    mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB}" \
    2> >(tee /tmp/import.err >&2)

# Update offset only if successful
BYTES_SENT=$(pv -n "${SQL_FILE}" 2>&1 | tail -1)
echo "${FILE_SIZE}" > "${OFFSET_FILE}"

# Restore checks
docker exec -i "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB}" <<SQL
SET FOREIGN_KEY_CHECKS=1;
SET UNIQUE_CHECKS=1;
SQL

rm -f "${OFFSET_FILE}"

echo ""
echo "✅ Import completed successfully"