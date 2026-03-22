#!/bin/bash
#SBATCH --job-name=index-mp
#SBATCH --partition=normal
#SBATCH --account=a145
#SBATCH --time=08:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=10
#SBATCH --mem=256G

#SBATCH --environment=elasticsearch

# FineWeb Dataset Indexing Script with Multi-Process Support
set -e

# Resolves repository-local scripts so the wrapper works from any working directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ============================================================================
# CONFIGURATION PARAMETERS
# ============================================================================

DATA_DIR="${DATA_DIR:-}"

BATCH_SIZE="${BATCH_SIZE:-5000}"           
ES_HOST="${ES_HOST:-localhost}"
ES_PORT="${ES_PORT:-9200}"
INDEX_NAME="${INDEX_NAME:-}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
# Adds the web-indexing selector knobs without changing the legacy text defaults.
INDEX_CONFIG="${INDEX_CONFIG:-}"
DATASET_TYPE="${DATASET_TYPE:-text}"
DOCUMENT_ID_MODE="${DOCUMENT_ID_MODE:-auto}"
METADATA_FIELDS="${METADATA_FIELDS:-text}"
INDEXER_SCRIPT="${INDEXER_SCRIPT:-$SCRIPT_DIR/index_with_metadata.py}"

# File range support for parallel processing
FILE_RANGE_START="${FILE_RANGE_START:-}"
FILE_RANGE_END="${FILE_RANGE_END:-}"


NUM_WORKERS="${NUM_WORKERS:-8}"             
MAX_CHUNK_BYTES="${MAX_CHUNK_BYTES:-100}"  
THREAD_COUNT="${THREAD_COUNT:-6}"          
QUEUE_SIZE="${QUEUE_SIZE:-12}"              

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ============================================================================
# LOGGING FUNCTIONS
# ============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# ============================================================================
# ELASTICSEARCH FUNCTIONS
# ============================================================================

start_elasticsearch() {
    log_info "Starting Elasticsearch server with Java environment fix"
    
    # 1. Fix Java environment variables
    log_info "=== Configuring Java Environment ==="
    
    unset JAVA_HOME
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"
    
    log_info "Testing Java installation..."
    if $ES_JAVA_HOME/bin/java -version; then
        log_success "Java test successful"
    else
        log_error "Java test failed"
        return 1
    fi
    
    log_info "Testing Elasticsearch binary..."
    if /usr/share/elasticsearch/bin/elasticsearch --version; then
        log_success "Elasticsearch binary test successful"
    else
        log_error "Elasticsearch binary test failed"
        return 1
    fi
    
    
    # 3. Set proper heap size based on available memory
    if [ "${SLURM_MEM_PER_NODE:-0}" -ge 32768 ]; then
        # 32GB+ available, use 30GB heap
        CUSTOM_HEAP="-Xms30g -Xmx30g -XX:MaxGCPauseMillis=200"
    else
        # Less than 32GB, use conservative 8GB heap  
        CUSTOM_HEAP="-Xms8g -Xmx8g -XX:MaxGCPauseMillis=200"
    fi


    log_info "Using heap settings: $CUSTOM_HEAP"
    log_info "Workers will use remaining memory (~8GB per worker for $NUM_WORKERS workers)"
    
    CURRENT_USER="${SLURM_JOB_USER:-$USER}"
    local job_data_dir="/iopsstor/scratch/cscs/${CURRENT_USER}/es-data${INDEX_NAME}-${SLURM_JOB_ID}"
    local job_logs_dir="/iopsstor/scratch/cscs/${CURRENT_USER}/es-logs${INDEX_NAME}-${SLURM_JOB_ID}"
   

    mkdir -p "$job_data_dir"
    mkdir -p "$job_logs_dir"
    
    log_info "ES Data Directory: $job_data_dir"
    log_info "ES Logs Directory: $job_logs_dir"

    # 5. Start Elasticsearch with optimized settings for bulk indexing
    ES_JAVA_OPTS="$CUSTOM_HEAP" \
    /usr/share/elasticsearch/bin/elasticsearch \
        -E path.data="$job_data_dir" \
        -E path.logs="$job_logs_dir" \
        -E discovery.type=single-node \
        -E network.host=127.0.0.1 \
        -E http.host=127.0.0.1 \
        -E http.port=9200 \
        -E transport.host=127.0.0.1 \
        -E network.bind_host=127.0.0.1 \
        -E network.publish_host=127.0.0.1 \
        -E node.store.allow_mmap=false \
        -E xpack.security.enabled=false \
        -E cluster.routing.allocation.disk.watermark.low=85% \
        -E cluster.routing.allocation.disk.watermark.high=90% \
        -E cluster.routing.allocation.disk.watermark.flood_stage=95% \
        -E bootstrap.memory_lock=false \
        -E logger.root=INFO \
        -E http.max_content_length=200mb \
        -E thread_pool.write.queue_size=2000 \
        -E thread_pool.write.size=1 &

    ES_PID=$!
    log_info "Elasticsearch started with PID: $ES_PID"
    
    
    # Wait for startup with enhanced monitoring
    log_info "Waiting for Elasticsearch to be ready..."
    max_retries=30
    retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        # Check if process is still running
        if ! kill -0 $ES_PID 2>/dev/null; then
            log_error "Elasticsearch process died! PID $ES_PID is no longer running"
            
            # Show any logs that might have been created
            log_info "=== Checking for error logs ==="
            find /usr/share/elasticsearch/logs -name "*.log" -exec tail -20 {} \; 2>/dev/null || log_warn "No log files found"
            
            return 1
        fi
        
        # Try to connect
        if curl -s "http://127.0.0.1:9200/_cluster/health" > /dev/null 2>&1; then
            log_success "Elasticsearch is ready at 127.0.0.1:9200"
            
            # Show successful startup info
            curl -s "http://127.0.0.1:9200/" | head -10
            
            return 0
        else
            retry_count=$((retry_count + 1))
            log_info "Waiting for Elasticsearch... attempt $retry_count/$max_retries"
            sleep 10
        fi
    done
    
    log_error "Elasticsearch failed to start after $max_retries attempts"
    return 1
}

stop_elasticsearch() {
    if [ ! -z "$ES_PID" ] && kill -0 $ES_PID 2>/dev/null; then
        log_info "Stopping Elasticsearch (PID: $ES_PID)..."
        kill $ES_PID
        wait $ES_PID 2>/dev/null || true
        log_info "Elasticsearch stopped"
    fi
}

# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

validate_data_directory() {
    log_info "Validating data directory: $DATA_DIR"
    
    # Check if it's a glob pattern
    if [[ "$DATA_DIR" == *"*"* ]]; then
        log_info "Detected glob pattern in data directory"
        
        # Expand the pattern and check if any directories match
        shopt -s nullglob  # Make glob return empty if no matches
        matching_dirs=($DATA_DIR)
        shopt -u nullglob
        
        if [ ${#matching_dirs[@]} -eq 0 ]; then
            log_error "No directories found matching pattern: $DATA_DIR"
            exit 1
        fi
        
        log_success "Found ${#matching_dirs[@]} directories matching pattern"
        
        # Update DATA_DIR to space-separated list of matched directories
        # DATA_DIR="${matching_dirs[*]}"
        
        # Check for parquet files across all matching directories
        parquet_count=0
        for dir in "${matching_dirs[@]}"; do
            count=$(find "$dir" -name "*.parquet" 2>/dev/null | wc -l)
            parquet_count=$((parquet_count + count))
        done
        
        if [ $parquet_count -eq 0 ]; then
            log_error "No parquet files found in matched directories"
            exit 1
        fi
        log_success "Found $parquet_count parquet files across all directories"
        
    # Check if DATA_DIR is a file or directory (only if NOT a glob pattern)
    elif [ -f "$DATA_DIR" ]; then
        # Single parquet file
        if [[ ! "$DATA_DIR" == *.parquet ]]; then
            log_error "File is not a parquet file: $DATA_DIR"
            exit 1
        fi
        parquet_count=1
        log_success "Using single parquet file: $DATA_DIR"
        
    elif [ -d "$DATA_DIR" ]; then
        # Directory containing parquet files
        parquet_count=$(find "$DATA_DIR" -name "*.parquet" | wc -l)
        if [ $parquet_count -eq 0 ]; then
            log_error "No parquet files found in directory: $DATA_DIR"
            exit 1
        fi
        log_success "Found $parquet_count parquet files in data directory"
        
    else
        log_error "Data path does not exist: $DATA_DIR"
        exit 1
    fi
}

show_configuration() {
    log_info "=== SLURM Job Configuration ==="
    echo "Job ID: ${SLURM_JOB_ID:-'Not in SLURM'}" 
    echo "Node: ${SLURM_NODELIST:-'Unknown'}" 
    echo "CPUs: ${SLURM_CPUS_PER_TASK:-'Unknown'}"
    echo "Memory: ${SLURM_MEM_PER_NODE:-'Unknown'}MB" 
    echo "=============================" 
    
    log_info "=== Indexing Configuration ==="
    echo "Data Directory: $DATA_DIR"
    echo "Batch Size: $BATCH_SIZE" 
    echo "Elasticsearch: $ES_HOST:$ES_PORT" 
    echo "Index Name: $INDEX_NAME"
    echo "Index Config: ${INDEX_CONFIG:-'(default)'}"
    echo "Dataset Type: $DATASET_TYPE"
    echo "Document ID Mode: $DOCUMENT_ID_MODE"
    echo "Metadata Fields: $METADATA_FIELDS"
    echo "Indexer Script: $INDEXER_SCRIPT"
    echo "File Start Range: $FILE_RANGE_START"
    echo "File End Range: $FILE_RANGE_END"
    echo "Log Level: $LOG_LEVEL" 
    echo "============================"
    
    log_info "=== Multi-Process Configuration ==="
    echo "Number of Workers: $NUM_WORKERS"
    echo "Max Chunk Bytes: $MAX_CHUNK_BYTES MB"
    echo "Thread Count: $THREAD_COUNT"
    echo "Queue Size: $QUEUE_SIZE"
    echo "============================"
}

monitor_resources() {
    log_info "System resource monitoring:"
    echo "Memory usage:"
    free -h 
    echo "Disk usage:" 
    df -h | head -10
    echo "CPU info:"
    lscpu | grep -E "CPU\(s\)|Thread|Core|Socket"
}

# ============================================================================
# INDEXING FUNCTION
# ============================================================================

run_indexing() {
    log_info "Starting FineWeb dataset indexing with multi-process support..."
    
    start_time=$(date +%s)
    
    # Increase file descriptor limit for multi-process
    ulimit -n 65536
    
    # Builds the metadata-aware indexer command and passes through the web dataset options.
    base_cmd="python3 \"$INDEXER_SCRIPT\" \
        --data-dir \"$DATA_DIR\" \
        --batch-size $BATCH_SIZE \
        --chunk-size 50000 \
        --es-host $ES_HOST \
        --es-port $ES_PORT \
        --index-name $INDEX_NAME \
        --log-level $LOG_LEVEL \
        --max-chunk-bytes $MAX_CHUNK_BYTES \
        --thread-count $THREAD_COUNT \
        --queue-size $QUEUE_SIZE \
        --num-workers $NUM_WORKERS \
        --dataset-type $DATASET_TYPE \
        --document-id-mode $DOCUMENT_ID_MODE \
        --metadata-fields $METADATA_FIELDS"

    # Add file range arguments if specified
    if [[ -n "$FILE_RANGE_START" && -n "$FILE_RANGE_END" ]]; then
        base_cmd+=" --file-range-start $FILE_RANGE_START --file-range-end $FILE_RANGE_END"
        log_info "Using file range: $FILE_RANGE_START to $FILE_RANGE_END"
    else
        log_info "Processing all files (no file range specified)"
    fi

    # Attaches an explicit mapping file when the caller wants deterministic target settings.
    if [[ -n "$INDEX_CONFIG" ]]; then
        base_cmd+=" --index-config \"$INDEX_CONFIG\""
        log_info "Using index config: $INDEX_CONFIG"
    fi

    log_info "Dataset type: $DATASET_TYPE"
    log_info "Document ID mode: $DOCUMENT_ID_MODE"
    log_info "Metadata fields mode: $METADATA_FIELDS"
    log_info "Multi-process mode: $NUM_WORKERS workers will parse files in parallel"

    # Execute the command
    eval "$base_cmd" 2>&1

    indexing_exit_code=${PIPESTATUS[0]}
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    if [ $indexing_exit_code -eq 0 ]; then
        log_success "Indexing completed successfully in ${duration} seconds"
        return 0
    else
        log_error "Indexing failed with exit code $indexing_exit_code after ${duration} seconds"
        return 1
    fi
}

show_index_location() {
    log_info "=== Index Storage Information ==="
    
    if [ -d "/usr/share/elasticsearch/data" ]; then
        echo "Current index files:"
        find /usr/share/elasticsearch/data -name "*$INDEX_NAME*" -type f 2>/dev/null | head -10 || true
    fi
    echo "============================" 
}

cleanup() {
    log_info "Cleaning up..."
    stop_elasticsearch
    show_index_location
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
    log_info "=== FineWeb Elasticsearch Indexing Started (Multi-Process) ==="
    log_info "Timestamp: $(date)"
    log_info "Script: $0"
    log_info "Working directory: $(pwd)"
    
    # Set trap for cleanup
    trap cleanup EXIT
    
    # Show configuration
    show_configuration
    
    # System resource monitoring
    monitor_resources
    
    # Validate data directory
    validate_data_directory
    
    # Start Elasticsearch
    if ! start_elasticsearch; then
        log_error "Failed to start Elasticsearch"
        exit 1
    fi
    
    # Run indexing with multi-process support
    if run_indexing; then
        log_success "=== Indexing process completed successfully ==="
        
        # Show where the index is stored
        show_index_location
        
        # Final system resource check
        log_info "Final system resources:"
        monitor_resources
        
        exit 0
    else
        log_error "=== Indexing process failed ==="
        exit 1
    fi
}

# Run main function
main "$@"
