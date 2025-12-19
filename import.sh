#!/bin/bash
set -euo pipefail

source .env

SQL_MODE="STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
OFFSET_FILE="${RESUME_FILE}"
SQL_FILE="${SQL_FILE}"
CONTAINER="${CONTAINER_NAME}"
DB="${MYSQL_DATABASE}"
LOG_FILE="${LOG_FILE:-/tmp/mysql_import.log}"

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
echo "📋 Log file: ${LOG_FILE}"
echo ""

# Initial session setup (idempotent)
echo "⚙️  Setting up MySQL session..." | tee -a "${LOG_FILE}"
docker exec -i "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB}" <<SQL 2>&1 | tee -a "${LOG_FILE}"
SET SESSION sql_mode='${SQL_MODE}';
SET FOREIGN_KEY_CHECKS=0;
SET UNIQUE_CHECKS=0;
SELECT 'Session configured' AS status;
SQL

echo "" | tee -a "${LOG_FILE}"
echo "📥 Starting import..." | tee -a "${LOG_FILE}"

# Create a named pipe for progress tracking
PIPE=$(mktemp -u)
mkfifo "${PIPE}"

# Cleanup function
cleanup() {
  rm -f "${PIPE}"
}
trap cleanup EXIT

# Background process to update offset
(
  while IFS= read -r line; do
    if [[ "${line}" =~ ^[0-9]+$ ]]; then
      echo "${line}" > "${OFFSET_FILE}"
    fi
  done < "${PIPE}"
) &
OFFSET_PID=$!

# Stream with resume + progress
IMPORT_START=$(date +%s)
(
  if [ "${OFFSET}" -gt 0 ]; then
    ${READ_CMD} "${SQL_FILE}" | tail -c +$((OFFSET + 1))
  else
    ${READ_CMD} "${SQL_FILE}"
  fi
) | pv -f -s "$((FILE_SIZE - OFFSET))" -i 1 -N "Import" 2>"${PIPE}" | \
  docker exec -i "${CONTAINER}" \
    mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB}" \
    --verbose 2>&1 | tee -a "${LOG_FILE}"

IMPORT_STATUS=$?
IMPORT_END=$(date +%s)
IMPORT_DURATION=$((IMPORT_END - IMPORT_START))

# Wait for offset updater to finish
wait "${OFFSET_PID}" 2>/dev/null || true

if [ ${IMPORT_STATUS} -eq 0 ]; then
  echo "" | tee -a "${LOG_FILE}"
  echo "✅ Import pipe completed" | tee -a "${LOG_FILE}"
  
  # Update offset to full file size on success
  echo "${FILE_SIZE}" > "${OFFSET_FILE}"
  
  # Restore checks
  echo "🔧 Restoring foreign key and unique checks..." | tee -a "${LOG_FILE}"
  docker exec -i "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB}" <<SQL 2>&1 | tee -a "${LOG_FILE}"
SET FOREIGN_KEY_CHECKS=1;
SET UNIQUE_CHECKS=1;
SELECT 'Checks restored' AS status;
SQL
  
  # Remove resume file
  rm -f "${OFFSET_FILE}"
  
  # Show import statistics
  echo "" | tee -a "${LOG_FILE}"
  echo "📊 Import Statistics:" | tee -a "${LOG_FILE}"
  echo "   Duration: ${IMPORT_DURATION} seconds" | tee -a "${LOG_FILE}"
  echo "   Speed: $((FILE_SIZE / 1024 / 1024 / IMPORT_DURATION)) MB/s" | tee -a "${LOG_FILE}"
  
  # Query database statistics
  echo "" | tee -a "${LOG_FILE}"
  echo "🗄️  Database Statistics:" | tee -a "${LOG_FILE}"
  docker exec -i "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" "${DB}" <<SQL 2>&1 | tee -a "${LOG_FILE}"
SELECT 
  TABLE_NAME,
  TABLE_ROWS,
  ROUND(DATA_LENGTH / 1024 / 1024, 2) AS 'Data_MB',
  ROUND(INDEX_LENGTH / 1024 / 1024, 2) AS 'Index_MB'
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = '${DB}'
ORDER BY DATA_LENGTH DESC
LIMIT 10;
SQL
  
  echo "" | tee -a "${LOG_FILE}"
  echo "✅ Import completed successfully" | tee -a "${LOG_FILE}"
  exit 0
else
  echo "" | tee -a "${LOG_FILE}"
  echo "❌ Import failed with status: ${IMPORT_STATUS}" | tee -a "${LOG_FILE}"
  echo "💾 Resume file preserved at: ${OFFSET_FILE}" | tee -a "${LOG_FILE}"
  echo "📋 Check log file: ${LOG_FILE}" | tee -a "${LOG_FILE}"
  exit ${IMPORT_STATUS}
fi
