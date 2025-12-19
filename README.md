
# Setup

### **IMPORTANT  (REQUIRED for import.sh)**
```bash
    sudo apt install pv
```

-------------------------   

#### For best resume reliability, dumps should be created with:
    ```bash
        mysqldump \
        --single-transaction \
        --quick \
        --skip-lock-tables \
        --set-gtid-purged=OFF \
        dbname > dump.sql
    ```

#### Faster InnoDB Imports
- **Add temporarily to sql_mode.cnf or session**:
```sql
  SET GLOBAL innodb_flush_log_at_trx_commit=2;
  SET GLOBAL sync_binlog=0;

  #Do this only during import, revert after.
```