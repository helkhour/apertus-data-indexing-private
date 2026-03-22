#!/bin/bash
# Generate SLURM job submission commands for Elasticsearch Multi-CSV search queries
# This generator processes multiple CSV files against multiple indices

# =============================================================================
# CONFIGURATION
# =============================================================================

# List of ES data directories (one per index)
ES_DATA_DIRS_FILE="${ES_DATA_DIRS_FILE:-}"

# Multiple CSV files (space-separated or use array)
# You can specify multiple CSV files in several ways:
# 1. Space-separated: CSV_FILES="file1.csv file2.csv file3.csv"
# 2. Pattern: CSV_FILES="/path/to/*.csv"
# 3. List in a file: CSV_FILES_LIST_FILE="/path/to/csv_list.txt"

CSV_FILES="${CSV_FILES:-}"
CSV_FILES_LIST_FILE="${CSV_FILES_LIST_FILE:-}"

# Script path
SCRIPT_PATH="${SCRIPT_PATH:-}"

# Dataset type for field extraction
DATASET="${DATASET:-pure_text}"  # Can be 'fineweb', 'web', 'sft', or 'pure_text'

# =============================================================================
# QUERY EXECUTION CONFIGURATION
# =============================================================================
EXECUTE_MATCH_QUERY="${EXECUTE_MATCH_QUERY:-false}"
EXECUTE_MATCH_PHRASE_QUERY="${EXECUTE_MATCH_PHRASE_QUERY:-true}"
EXECUTE_WILDCARD_QUERY="${EXECUTE_WILDCARD_QUERY:-false}"
EXECUTE_FUZZY_QUERY="${EXECUTE_FUZZY_QUERY:-false}"
EXECUTE_BOOL_MUST_QUERY="${EXECUTE_BOOL_MUST_QUERY:-false}"

# Query parameters (as JSON arrays for multiple values)
# MATCH_QUERY_OPERATOR="${MATCH_QUERY_OPERATOR:-[\"or\"]}"
# MATCH_PHRASE_SLOP="${MATCH_PHRASE_SLOP:-[0]}"

# Bool query parameters
BOOL_MUST_OPERATOR="${BOOL_MUST_OPERATOR:-or}"
BOOL_MUST_MAX_WORDS="${BOOL_MUST_MAX_WORDS:-3}"
BOOL_MUST_MINIMUM_SHOULD_MATCH="${BOOL_MUST_MINIMUM_SHOULD_MATCH:-50%}"

# =============================================================================
# OUTPUT CONFIGURATION
# =============================================================================

# Job group ID for organizing results
JOB_GROUP_ID=$(date +%Y%m%d_%H%M%S)

# Base output directory - will contain subdirectories per CSV per index
OUTPUT_DIR_BASE="${OUTPUT_DIR_BASE:-/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/MULTI_CSV_20251008_041505}"

# Log directories
OUTPUT_LOGS="${OUTPUT_LOGS:-/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output}"
ERROR_LOGS="${ERROR_LOGS:-/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err}"

# =============================================================================
# FUNCTIONS
# =============================================================================

extract_index_name() {
    local es_data_dir="$1"
    local basename=$(basename "$es_data_dir")
    
    # Extract index name from patterns like:
    # es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_035-920316
    # Should return: swissai-fineweb-edu-score-2-filterrobots-merge_part_035
    
    # Remove the 'es-data-{prefix}-' part and the trailing '-{6-digit-job-id}'
    local index_name=$(echo "$basename" | sed -E 's/^es-data-[^-]+-//; s/-[0-9]{6}$//')
    
    # Trim whitespace and return
    echo "$index_name" | xargs
}

# =============================================================================
# VALIDATION
# =============================================================================

echo "=== Elasticsearch Multi-CSV Search Job Generator ==="
echo "Script path: $SCRIPT_PATH"
echo "Dataset: $DATASET"
echo "Output results base: $OUTPUT_DIR_BASE"
echo "Output logs: $OUTPUT_LOGS"
echo "Error logs: $ERROR_LOGS"
echo

# Validate script exists
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "ERROR: Script not found: $SCRIPT_PATH"
    exit 1
fi

# Get CSV files list
CSV_FILES_ARRAY=()

if [ -n "$CSV_FILES_LIST_FILE" ] && [ -f "$CSV_FILES_LIST_FILE" ]; then
    echo "Reading CSV files from list: $CSV_FILES_LIST_FILE"
    while IFS= read -r csv_file; do
        # Trim whitespace from the line (spaces, tabs)
        csv_file=$(echo "$csv_file" | xargs)
        if [ -n "$csv_file" ] && [[ ! "$csv_file" =~ ^# ]]; then
            CSV_FILES_ARRAY+=("$csv_file")
        fi
    done < "$CSV_FILES_LIST_FILE" 
elif [ -n "$CSV_FILES" ]; then
    echo "Using CSV files from CSV_FILES variable"
    # Handle space-separated or glob patterns
    for csv_file in $CSV_FILES; do
        CSV_FILES_ARRAY+=("$csv_file")
    done
else
    echo "ERROR: No CSV files specified!"
    echo
    echo "Please specify CSV files using one of these methods:"
    echo "  1. CSV_FILES=\"file1.csv file2.csv file3.csv\" $0"
    echo "  2. CSV_FILES=\"/path/to/*.csv\" $0"
    echo "  3. CSV_FILES_LIST_FILE=\"csv_list.txt\" $0"
    echo
    echo "Example csv_list.txt format:"
    echo "  /path/to/ww_en.csv"
    echo "  /path/to/ww_ita.csv"
    echo "  /path/to/ww_deu.csv"
    exit 1
fi

# Validate CSV files exist
CSV_COUNT=0
VALID_CSV_FILES=()
echo "Validating CSV files..."
for csv_file in "${CSV_FILES_ARRAY[@]}"; do
    if [ -f "$csv_file" ]; then
        VALID_CSV_FILES+=("$csv_file")
        echo "  ✓ $csv_file"
        CSV_COUNT=$((CSV_COUNT + 1))
    else
        echo "  ✗ WARNING: CSV file not found: $csv_file (skipping)"
    fi
done

if [ $CSV_COUNT -eq 0 ]; then
    echo "ERROR: No valid CSV files found"
    exit 1
fi

echo
echo "Found $CSV_COUNT valid CSV file(s)"
echo

# Get list of ES data directories
if [ -n "$ES_DATA_DIRS_FILE" ] && [ -f "$ES_DATA_DIRS_FILE" ]; then
    echo "Reading ES data directories from: $ES_DATA_DIRS_FILE"
    mapfile -t es_dirs < "$ES_DATA_DIRS_FILE"
else
    echo "ERROR: ES_DATA_DIRS_FILE is required and must exist"
    echo "Create a file with ES data directories, one per line, e.g.:"
    echo "  /iopsstor/.../es-data-septemberv1-index_part_001-920282"
    echo "  /iopsstor/.../es-data-septemberv1-index_part_002-920283"
    exit 1
fi

TOTAL_JOBS=${#es_dirs[@]}

if [ $TOTAL_JOBS -eq 0 ]; then
    echo "ERROR: No ES data directories found"
    exit 1
fi

echo "Found $TOTAL_JOBS ES data directories"
echo

# =============================================================================
# DISPLAY CONFIGURATION SUMMARY
# =============================================================================

echo "=== Configuration Summary ==="
echo "Total indices to search: $TOTAL_JOBS"
echo "Total CSV files per index: $CSV_COUNT"
echo "Total jobs to generate: $TOTAL_JOBS"
echo
echo "CSV files to process per index:"
for csv in "${VALID_CSV_FILES[@]}"; do
    echo "  - $(basename "$csv")"
done
echo
echo "Query Configuration:"
echo "  Match Query: $EXECUTE_MATCH_QUERY"
echo "  Match Phrase Query: $EXECUTE_MATCH_PHRASE_QUERY"
echo "  Wildcard Query: $EXECUTE_WILDCARD_QUERY"
echo "  Fuzzy Query: $EXECUTE_FUZZY_QUERY"
echo "  Bool Must Query: $EXECUTE_BOOL_MUST_QUERY"
echo
echo "Advanced Parameters:"
echo "  Match Query Operator: $MATCH_QUERY_OPERATOR"
echo "  Match Phrase Slop: $MATCH_PHRASE_SLOP"
echo "  Bool Must Operator: $BOOL_MUST_OPERATOR"
echo "  Bool Must Max Words: $BOOL_MUST_MAX_WORDS"
echo "  Bool Must Min Should Match: $BOOL_MUST_MINIMUM_SHOULD_MATCH"
echo

# =============================================================================
# GENERATE JOB COMMANDS
# =============================================================================

echo "=== Generated Job Submission Commands ==="
echo

# Build CSV_FILES string for export
CSV_FILES_EXPORT=""
for csv in "${VALID_CSV_FILES[@]}"; do
    if [ -z "$CSV_FILES_EXPORT" ]; then
        CSV_FILES_EXPORT="$csv"
    else
        CSV_FILES_EXPORT="$CSV_FILES_EXPORT $csv"
    fi
done

# Generate commands for each index
for ((i=0; i<TOTAL_JOBS; i++)); do
    es_data_dir="${es_dirs[$i]}"
    INDEX_NAME=$(extract_index_name "$es_data_dir")
    JOB_NUM=$(printf "%03d" $((i+1)))

    cat << EOF
# Job $JOB_NUM: $INDEX_NAME (with $CSV_COUNT CSV files)
export PATH_DATA="$es_data_dir"
export CSV_FILES="$CSV_FILES_EXPORT"
export INDEX_NAME="$INDEX_NAME"
export OUTPUT_DIR_BASE="$OUTPUT_DIR_BASE"
export DATASET="$DATASET"
export EXECUTE_MATCH_QUERY="$EXECUTE_MATCH_QUERY"
export EXECUTE_MATCH_PHRASE_QUERY="$EXECUTE_MATCH_PHRASE_QUERY"
export EXECUTE_WILDCARD_QUERY="$EXECUTE_WILDCARD_QUERY"
export EXECUTE_FUZZY_QUERY="$EXECUTE_FUZZY_QUERY"
export EXECUTE_BOOL_MUST_QUERY="$EXECUTE_BOOL_MUST_QUERY"
export BOOL_MUST_OPERATOR="$BOOL_MUST_OPERATOR"
export BOOL_MUST_MAX_WORDS="$BOOL_MUST_MAX_WORDS"
export BOOL_MUST_MINIMUM_SHOULD_MATCH="$BOOL_MUST_MINIMUM_SHOULD_MATCH"
bash $SCRIPT_PATH

EOF
done

# =============================================================================
# SUMMARY AND INSTRUCTIONS
# =============================================================================

echo "# ==========================="
echo "# Summary:"
echo "# Total search jobs: $TOTAL_JOBS"
echo "# CSV files per job: $CSV_COUNT"
CSV_NAMES=""
for csv in "${VALID_CSV_FILES[@]}"; do
    csv_basename=$(basename "$csv" | sed 's/\.[^.]*$//')
    if [ -z "$CSV_NAMES" ]; then
        CSV_NAMES="$csv_basename"
    else
        CSV_NAMES="$CSV_NAMES, $csv_basename"
    fi
done
echo "# CSV file basenames: $CSV_NAMES"
echo "# Dataset: $DATASET"
echo "#"
echo "# Results directory structure:"
echo "# $OUTPUT_DIR_BASE/"
for csv in "${VALID_CSV_FILES[@]}"; do
    csv_basename=$(basename "$csv" | sed 's/\.[^.]*$//')
    echo "#   ├── ${csv_basename}/"
    echo "#   │   ├── ${INDEX_NAME}_001/"
    echo "#   │   │   ├── search_results_detailed_*.json"
    echo "#   │   │   ├── search_results_summary_*.json"
    echo "#   │   │   └── search_results_fulltext_*.json"
    echo "#   │   ├── ${INDEX_NAME}_002/"
    echo "#   │   │   └── ..."
done
echo "#"
echo "# Output logs: ${OUTPUT_LOGS}/search_%j.out"
echo "# Error logs: ${ERROR_LOGS}/search_%j.err"
echo "#"
echo "# NOTE: Make sure these directories exist before submitting:"
echo "# mkdir -p $OUTPUT_LOGS"
echo "# mkdir -p $ERROR_LOGS"
echo "# mkdir -p $OUTPUT_DIR_BASE"
echo "#"
echo "# To submit all jobs, run:"
echo "# bash $0 | grep -E '^(export|bash)' | bash"
echo "#"
echo "# Or save to file and review before running:"
echo "# bash $0 > jobs_to_run.sh"
echo "# # Review jobs_to_run.sh"
echo "# bash jobs_to_run.sh"
echo "# ==========================="