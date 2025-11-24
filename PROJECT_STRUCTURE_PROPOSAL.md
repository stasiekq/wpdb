# Project Structure Cleanup Proposal

## Current State Analysis

### Files in Root Directory

#### ✅ **Core Application Files** (Keep)
- `business_project_task_1.py` - Task 1: CSV to PostgreSQL loader
- `consumer_avro.py` - Task 3: Kafka AVRO consumer
- `spark_job.py` - Task 4: Spark streaming job
- `wipe_database.py` - Utility: Database cleanup script
- `docker-compose.yml` - Infrastructure: Docker services configuration
- `register_postgres.json` - Configuration: Debezium connector config

#### ✅ **Documentation Files** (Keep, but organize)
- `project_requirements.md` - Project requirements/specification
- `QUICK_START.md` - Quick start guide
- `REQUIREMENTS_CHECKLIST.md` - Requirements verification checklist
- `SPARK_JOB_FIX.md` - Spark job troubleshooting guide

#### ✅ **Scripts** (Keep, but organize)
- `test_setup.sh` - Testing/verification script
- `run_pipeline.sh` - Pipeline execution script (bash)
- `run_pipeline.py` - Pipeline execution script (Python) - **NEW**
- `run_spark_job.sh` - Spark job runner script

#### ❌ **Files to Remove/Archive**
- `sranie.json` - Appears to be a test/debug file (contains old schema example with "wpdb" instead of "pg")
- `notes.txt` - Personal notes/commands (useful but should be in docs)
- `old_business_project.py` - Old version (if exists, should be removed or archived)

#### 📁 **Directories**
- `data/` - CSV data files (keep)
- `venv/` - Python virtual environment (keep, but add to .gitignore)
- `__pycache__/` - Python cache (should be in .gitignore)

---

## Proposed Structure

```
wpdb/
├── README.md                          # Main project documentation
├── .env.example                       # Environment template (NEW)
├── .gitignore                         # Git ignore file (NEW)
├── docker-compose.yml                 # Docker services
│
├── src/                               # Source code directory
│   ├── task1/                         # Task 1: Business Process
│   │   └── csv_loader.py              # Renamed from business_project_task_1.py
│   ├── task3/                         # Task 3: Kafka Consumer
│   │   └── avro_consumer.py           # Renamed from consumer_avro.py
│   ├── task4/                         # Task 4: Spark Processing
│   │   └── spark_streaming.py         # Renamed from spark_job.py
│   └── utils/                         # Utility scripts
│       └── database.py                # Contains wipe_database.py functionality
│
├── config/                            # Configuration files
│   ├── debezium/                      # Debezium configurations
│   │   └── postgres_connector.json    # Renamed from register_postgres.json
│   └── spark/                         # Spark configurations (if needed later)
│
├── scripts/                            # Execution and utility scripts
│   ├── run_pipeline.sh                # Bash pipeline runner
│   ├── run_pipeline.py                # Python pipeline runner
│   ├── run_spark_job.sh               # Spark job runner
│   └── test_setup.sh                  # Test/verification script
│
├── docs/                              # Documentation
│   ├── requirements.md                # Renamed from project_requirements.md
│   ├── quick_start.md                 # Renamed from QUICK_START.md
│   ├── requirements_checklist.md      # Renamed from REQUIREMENTS_CHECKLIST.md
│   ├── spark_troubleshooting.md       # Renamed from SPARK_JOB_FIX.md
│   └── notes.md                       # Personal notes/commands from notes.txt
│
├── data/                              # Data files
│   ├── data1.csv
│   ├── data2.csv
│   └── data3.csv
│
└── .gitignore                         # Git ignore patterns
```

---

## Detailed File Organization

### 1. **Source Code (`src/`)**

**Rationale**: Separates application code from scripts and configs

- `src/task1/csv_loader.py` (from `business_project_task_1.py`)
  - Clear naming convention
  - Organized by task

- `src/task3/avro_consumer.py` (from `consumer_avro.py`)
  - Task-specific organization

- `src/task4/spark_streaming.py` (from `spark_job.py`)
  - Clear purpose

- `src/utils/database.py` (from `wipe_database.py`)
  - Utility functions grouped together
  - Can expand with more database utilities

### 2. **Configuration (`config/`)**

**Rationale**: Centralizes all configuration files

- `config/debezium/postgres_connector.json`
  - Debezium-specific configs in subdirectory
  - Easy to add more connectors later

### 3. **Scripts (`scripts/`)**

**Rationale**: All executable scripts in one place

- All `.sh` and `.py` execution scripts
- Easy to find and run
- Clear separation from source code

### 4. **Documentation (`docs/`)**

**Rationale**: All documentation centralized

- All `.md` files moved here
- Consistent naming (lowercase with underscores)
- `notes.md` preserves useful commands from `notes.txt`

### 5. **Root Directory Cleanup**

**Keep in root:**
- `README.md` - Main entry point
- `docker-compose.yml` - Standard location
- `.env.example` - Template for environment variables
- `.gitignore` - Standard location

**Remove:**
- `sranie.json` - Test/debug file (or move to `docs/examples/` if useful)
- `old_business_project.py` - Old version (archive or delete)
- `__pycache__/` - Should be in .gitignore

---

## Files to Remove

1. **`sranie.json`**
   - **Reason**: Appears to be a test/debug file with old schema example
   - **Action**: Delete (or move to `docs/examples/` if you want to keep as reference)

2. **`old_business_project.py`** (if exists)
   - **Reason**: Old version, no longer needed
   - **Action**: Delete

3. **`__pycache__/` directories**
   - **Reason**: Python cache, should be ignored by git
   - **Action**: Add to .gitignore, delete existing

---

## New Files to Create

1. **`.gitignore`**
   ```
   # Python
   __pycache__/
   *.py[cod]
   *$py.class
   *.so
   .Python
   
   # Virtual Environment
   venv/
   env/
   ENV/
   
   # Environment variables
   .env
   
   # IDE
   .vscode/
   .idea/
   *.swp
   *.swo
   
   # OS
   .DS_Store
   Thumbs.db
   ```

2. **`.env.example`**
   ```
   POSTGRES_USER=pguser
   POSTGRES_PASSWORD=admin
   POSTGRES_DB=business_db
   DB_HOST=localhost
   DB_PORT=5433
   DATA_DIR=data
   ```

3. **`README.md`** (main project readme)
   - Project overview
   - Quick start
   - Links to detailed docs

---

## Migration Steps (if you approve)

1. Create new directory structure
2. Move files to new locations
3. Update import paths in Python files
4. Update script paths in shell scripts
5. Update docker-compose.yml volume mounts if needed
6. Create .gitignore and .env.example
7. Update documentation with new paths
8. Test that everything still works
9. Remove old files

---

## Benefits of This Structure

1. **Clarity**: Easy to find what you need
2. **Scalability**: Easy to add new tasks/utilities
3. **Maintainability**: Related files grouped together
4. **Professional**: Follows common project structure patterns
5. **Git-friendly**: Clear separation of what to commit/ignore

---

## Alternative: Minimal Changes

If you prefer minimal changes, at least:

1. Create `scripts/` directory and move all `.sh` files
2. Create `docs/` directory and move all `.md` files
3. Create `config/` directory and move `register_postgres.json`
4. Delete `sranie.json` and `notes.txt` (or move notes to docs)
5. Add `.gitignore` file
6. Keep Python files in root (or create `src/` if you want)

---

## Questions for Review

1. Do you want the full reorganization or minimal changes?
2. Should `sranie.json` be deleted or kept as reference?
3. Do you want to rename Python files or keep original names?
4. Should `notes.txt` be converted to markdown in docs?
5. Any other files you want to keep/remove?

