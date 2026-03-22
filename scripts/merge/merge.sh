#!/bin/bash
#SBATCH --job-name=cluster-merge-indexes
#SBATCH --partition=normal
#SBATCH --account=a145
#SBATCH --time=08:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=256G

#SBATCH --output=/capstor/scratch/cscs/inesaltemir/MERGE_logs/brouillon/output/merge_%j.out
#SBATCH --error=/capstor/scratch/cscs/inesaltemir/MERGE_logs/brouillon/err/merge_%j.err
#SBATCH --environment=es-python

# Cluster-Level Index Merge - Avoids metadata corruption by using live clusters
# This script solves the UUID/metadata problem by running each data directory
# in its own Elasticsearch cluster, then using remote reindex API for merging

set -e

# Resolves repository-local scripts so the merge wrapper works from any working directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MERGE_SCRIPT="${MERGE_SCRIPT:-$SCRIPT_DIR/merge.py}"

# Configuration - Override these via environment variables
# CRITICAL: SOURCE_DATA_DIRS should be a space-separated list of directories
# passed from the job generator via environment variables
SOURCE_DATA_DIRS="${SOURCE_DATA_DIRS:-}"
TARGET_INDEX="${TARGET_INDEX:-}"
SOURCE_INDEX_PATTERNS="${SOURCE_INDEX_PATTERNS:-}"

MERGE_CLUSTER_PORT="${MERGE_CLUSTER_PORT:-9200}"
BATCH_SIZE="${BATCH_SIZE:-10000}"
# Allows callers to force a specific target mapping so merge output preserves web fields deterministically.
INDEX_CONFIG="${INDEX_CONFIG:-}"


# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }

# CRITICAL FIX: Add proxy bypass configuration
configure_proxy_bypass() {
    log_info "Configuring proxy bypass for localhost connections..."
    
    # Save original proxy settings
    ORIGINAL_HTTP_PROXY="${http_proxy:-}"
    ORIGINAL_HTTPS_PROXY="${https_proxy:-}"
    ORIGINAL_NO_PROXY="${no_proxy:-}"
    
    # Extend no_proxy to include all localhost variants and port ranges
    export no_proxy="${no_proxy},127.0.0.1,localhost,0.0.0.0,::1,127.0.0.1:9200,127.0.0.1:9201,127.0.0.1:9202,127.0.0.1:9203,127.0.0.1:9204,127.0.0.1:9205"
    
    # Also unset proxy for localhost explicitly
    export http_proxy=""
    export https_proxy=""
    
    log_info "Original no_proxy: $ORIGINAL_NO_PROXY"
    log_info "Updated no_proxy: $no_proxy"
    log_info "Disabled HTTP/HTTPS proxy for localhost connections"
}

# Enhanced curl function with explicit proxy bypass
safe_curl() {
    curl --noproxy "127.0.0.1,localhost" --connect-timeout 10 --max-time 30 "$@"
}

# Test Elasticsearch connectivity with proxy bypass
test_elasticsearch_connection() {
    local host="$1"
    local port="$2"
    local max_retries=45  # Increased retries
    local retry_count=0
    
    log_info "Testing Elasticsearch connection to $host:$port"
    
    while [ $retry_count -lt $max_retries ]; do
        # CRITICAL: Test multiple endpoints to ensure cluster is fully ready
        if safe_curl -s "http://$host:$port/" > /dev/null 2>&1 && \
           safe_curl -s "http://$host:$port/_cluster/health" > /dev/null 2>&1 && \
           safe_curl -s "http://$host:$port/_cat/indices" > /dev/null 2>&1; then
            
            # VERIFY cluster is actually healthy
            local health_status=$(safe_curl -s "http://$host:$port/_cluster/health" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
            if [[ "$health_status" == "green" || "$health_status" == "yellow" ]]; then
                log_success "Successfully connected to healthy Elasticsearch at $host:$port (status: $health_status)"
                return 0
            else
                log_warn "Cluster is responding but not healthy: $health_status"
            fi
        fi
        
        # Check if process is still alive
        if [ ! -z "$ES_PID" ] && ! kill -0 $ES_PID 2>/dev/null; then
            log_error "Elasticsearch process died during startup!"
            return 1
        fi
        
        retry_count=$((retry_count + 1))
        log_warn "Connection attempt $retry_count/$max_retries failed, retrying..."
        sleep 15
    done
    
    log_error "Failed to connect to healthy Elasticsearch at $host:$port after $max_retries attempts"
    return 1
}

show_configuration() {
    log_info "=== Configuration ==="
    echo "Job ID: ${SLURM_JOB_ID:-'Not in SLURM'}"
    echo "Source Data Directories: $SOURCE_DATA_DIRS"
    echo "Source Index Patterns: $SOURCE_INDEX_PATTERNS"
    echo "Target Index: $TARGET_INDEX"
    echo "Target Port: $MERGE_CLUSTER_PORT"
    echo "Batch Size: $BATCH_SIZE"
    echo "Index Config: ${INDEX_CONFIG:-'(clone first source mapping)'}"
    echo "Merge Script: $MERGE_SCRIPT"
    echo "========================"
}

# Start individual Elasticsearch cluster for each data directory
start_source_clusters() {
    log_info "=== Starting Individual Source Clusters ==="
    
    # FIXED: Use SOURCE_DATA_DIRS from environment variable instead of glob pattern
    # Convert space-separated string to array
    read -ra source_dirs <<< "$SOURCE_DATA_DIRS"

    if [ ${#source_dirs[@]} -eq 0 ]; then
        log_error "No source data directories provided in SOURCE_DATA_DIRS"
        log_info "SOURCE_DATA_DIRS value: $SOURCE_DATA_DIRS"
        return 1
    fi

    log_info "Found ${#source_dirs[@]} source data directories from environment"
    
    # Validate all directories exist
    for data_dir in "${source_dirs[@]}"; do
        if [ ! -d "$data_dir" ]; then
            log_error "Source directory does not exist: $data_dir"
            return 1
        fi
        log_info "  ✓ Validated: $(basename "$data_dir")"
    done
    
    # Configure Java environment
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"
    local heap_size="8g"  # Conservative heap size for source clusters
    
    local port=9201  # Start from port 9201 for source clusters
    local cluster_pids=()
    
    # Start one cluster per data directory
    for data_dir in "${source_dirs[@]}"; do
        local cluster_name="source_cluster_$(basename "$data_dir")"
        local log_dir="/tmp/es-logs-${cluster_name}-${SLURM_JOB_ID}"
        mkdir -p "$log_dir"
        
        log_info "Starting cluster for: $(basename "$data_dir") on port $port"
        
        # Start Elasticsearch with this specific data directory
        ES_JAVA_OPTS="-Xms${heap_size} -Xmx${heap_size}" \
        /usr/share/elasticsearch/bin/elasticsearch \
            -E cluster.name="$cluster_name" \
            -E node.name="source_node_$port" \
            -E path.data="$data_dir" \
            -E path.logs="$log_dir" \
            -E discovery.type=single-node \
            -E network.host=127.0.0.1 \
            -E http.port=$port \
            -E transport.port=$((port + 100)) \
            -E node.store.allow_mmap=false \
            -E xpack.security.enabled=false \
            -E bootstrap.memory_lock=false \
            -E logger.root=INFO &
        
        local pid=$!
        cluster_pids+=("$pid:$port:$data_dir")
        
        log_info "Started cluster PID $pid on port $port"
        port=$((port + 1))
        sleep 5
    done
    
    # Wait for all source clusters to be ready
    log_info "Waiting for all source clusters to be ready..."
    for cluster_info in "${cluster_pids[@]}"; do
        local pid=$(echo "$cluster_info" | cut -d: -f1)
        local port=$(echo "$cluster_info" | cut -d: -f2)
        local data_dir=$(echo "$cluster_info" | cut -d: -f3)
        
        # Check if process is still running
        if ! kill -0 $pid 2>/dev/null; then
            log_error "Cluster for $(basename "$data_dir") (PID $pid) died during startup"
            return 1
        fi
        
        # Test connection with proxy bypass
        if ! test_elasticsearch_connection "127.0.0.1" "$port"; then
            log_error "Cluster on port $port failed to start properly"
            return 1
        fi
        
        log_success "Cluster ready on port $port for $(basename "$data_dir")"
    done
    
    # Save cluster info for later use
    echo "${cluster_pids[@]}" > /tmp/source_clusters_${SLURM_JOB_ID}.txt
    log_success "All ${#cluster_pids[@]} source clusters are ready"
    return 0
}

# Start target cluster for the merged index
start_target_cluster() {
    log_info "=== Starting Target Merge Cluster ==="
    
    local target_data_dir="/iopsstor/scratch/cscs/inesaltemir/es-data-target-${TARGET_INDEX}-${SLURM_JOB_ID}"
    mkdir -p "$target_data_dir"
    
    local target_log_dir="/tmp/es-logs-target-${TARGET_INDEX}-${SLURM_JOB_ID}"
    mkdir -p "$target_log_dir"
    
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"
    local heap_size="30g"  # DO NOT SURPASS 31GB
    
    log_info "Starting target cluster on port $MERGE_CLUSTER_PORT"
    log_info "Target data directory: $target_data_dir"
    
    # Start target cluster optimized for write operations with reindex whitelist
    ES_JAVA_OPTS="-Xms${heap_size} -Xmx${heap_size}" \
    /usr/share/elasticsearch/bin/elasticsearch \
        -E cluster.name="merge_cluster" \
        -E node.name="merge_node" \
        -E path.data="$target_data_dir" \
        -E path.logs="$target_log_dir" \
        -E discovery.type=single-node \
        -E network.host=127.0.0.1 \
        -E http.port=$MERGE_CLUSTER_PORT \
        -E transport.port=$((MERGE_CLUSTER_PORT + 100)) \
        -E node.store.allow_mmap=false \
        -E xpack.security.enabled=false \
        -E bootstrap.memory_lock=false \
        -E logger.root=INFO \
        -E indices.memory.index_buffer_size=40% \
        -E thread_pool.write.queue_size=1000 \
        -E reindex.remote.whitelist="127.0.0.1:9201,127.0.0.1:9202,127.0.0.1:9203,127.0.0.1:9204,127.0.0.1:9205,127.0.0.1:9206,127.0.0.1:9207,127.0.0.1:9208,localhost:9201,localhost:9202,localhost:9203,localhost:9204,localhost:9205,localhost:9206,localhost:9207,localhost:9208" &
    
    export TARGET_ES_PID=$!
    
    # Test connection with proxy bypass
    if ! test_elasticsearch_connection "127.0.0.1" "$MERGE_CLUSTER_PORT"; then
        log_error "Target cluster failed to start properly"
        return 1
    fi
    
    log_success "Target cluster ready on port $MERGE_CLUSTER_PORT"
    return 0
}

# Discover indexes and execute the Python merge script
discover_and_merge_indexes() {
    log_info "=== Discovering and Merging Indexes ==="
    
    local cluster_pids_file="/tmp/source_clusters_${SLURM_JOB_ID}.txt"
    if [ ! -f "$cluster_pids_file" ]; then
        log_error "Cluster info file not found"
        return 1
    fi
    
    read -ra cluster_infos < "$cluster_pids_file"
    
    local source_configs=()
    local total_expected_docs=0
    
    log_info "Scanning ${#cluster_infos[@]} source clusters for indexes..."
    
    # Discover indexes from each source cluster
    for cluster_info in "${cluster_infos[@]}"; do
        local port=$(echo "$cluster_info" | cut -d: -f2)
        local data_dir=$(echo "$cluster_info" | cut -d: -f3)
        
        log_info "Querying cluster on port $port ($(basename "$data_dir"))"
        
        # Get all indexes from this cluster with document counts
        local indexes_response=$(safe_curl -s "http://127.0.0.1:$port/_cat/indices?v&h=index,docs.count")
        
        # Parse indexes matching SOURCE_INDEX_PATTERNS
        while IFS= read -r line; do
            # Skip header and empty lines
            if [[ ! "$line" =~ ^index ]] && [[ ! "$line" =~ ^$ ]] && [[ ! "$line" =~ ^\. ]]; then  # Skip system indexes
                local index_name=$(echo "$line" | awk '{print $1}')
                local doc_count=$(echo "$line" | awk '{print $2}' | grep -o '[0-9]*' || echo "0")
                
                # Validate index has documents
                if [ "$doc_count" -gt 0 ]; then
                    for pattern in $SOURCE_INDEX_PATTERNS; do
                        if [[ "$index_name" == *"$pattern"* ]]; then
                            source_configs+=("{\"index\":\"$index_name\",\"host\":\"127.0.0.1\",\"port\":$port,\"doc_count\":$doc_count}")
                            total_expected_docs=$((total_expected_docs + doc_count))
                            log_info "  ✓ Found: $index_name ($doc_count docs)"
                            break
                        fi
                    done
                else
                    log_warn "  ⚠ Skipping empty index: $index_name"
                fi
            fi
        done <<< "$indexes_response"
    done
    
    if [ ${#source_configs[@]} -eq 0 ]; then
        log_error "No valid source indexes found with documents!"
        return 1
    fi
    
    log_info "Total expected documents to merge: $total_expected_docs"
    
    # VERIFY target cluster before starting merge
    if ! test_elasticsearch_connection "127.0.0.1" "$MERGE_CLUSTER_PORT"; then
        log_error "Target cluster is not ready for merge"
        return 1
    fi
    
    # Build JSON configuration
    local json_config="["
    for i in "${!source_configs[@]}"; do
        json_config+="${source_configs[i]}"
        if [ $i -lt $((${#source_configs[@]} - 1)) ]; then
            json_config+=","
        fi
    done
    json_config+="]"
    
    log_info "Starting merge with configuration: $json_config"
    
    # Builds the repo-local merge command and attaches the explicit target mapping when requested.
    local -a merge_cmd=(
        python3 "$MERGE_SCRIPT"
        --source-configs "$json_config"
        --target-index "$TARGET_INDEX"
        --target-host "127.0.0.1"
        --target-port "$MERGE_CLUSTER_PORT"
        --batch-size "$BATCH_SIZE"
        --log-level INFO
    )

    # Adds the optional merge target mapping so web metadata is not dropped during reindexing.
    if [ -n "$INDEX_CONFIG" ]; then
        merge_cmd+=(--index-config "$INDEX_CONFIG")
    fi

    # Executes the merge through the checked-in Python entrypoint.
    if NO_PROXY="$no_proxy" HTTP_PROXY="" HTTPS_PROXY="" "${merge_cmd[@]}"; then
        # CRITICAL: Verify merge results
        log_info "=== Verifying Merge Results ==="
        sleep 10  # Wait for final refresh
        
        local final_count=$(safe_curl -s "http://127.0.0.1:$MERGE_CLUSTER_PORT/${TARGET_INDEX}/_count" | grep -o '"count":[0-9]*' | cut -d: -f2 || echo "0")
        local index_exists=$(safe_curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$MERGE_CLUSTER_PORT/${TARGET_INDEX}")
        
        if [ "$index_exists" != "200" ]; then
            log_error "Target index was not created!"
            return 1
        fi
        
        if [ "$final_count" -eq 0 ]; then
            log_error "Target index is empty after merge!"
            return 1
        fi
        
        local percentage=$((final_count * 100 / total_expected_docs))
        log_success "Final index count: $final_count / $total_expected_docs documents ($percentage%)"
        
        if [ $percentage -lt 95 ]; then
            log_warn "Warning: Less than 95% of documents were merged"
        fi
        
        return 0
    else
        log_error "Merge failed!"
        return 1
    fi
}

# Cleanup function
cleanup() {
    local exit_code=$?
    log_info "=== Cleanup ==="
    
    # Stop target cluster
    if [ ! -z "$TARGET_ES_PID" ]; then
        log_info "Stopping target cluster (PID $TARGET_ES_PID)"
        kill $TARGET_ES_PID 2>/dev/null || true
        sleep 5
        kill -9 $TARGET_ES_PID 2>/dev/null || true
    fi
    
    # Stop source clusters
    local cluster_pids_file="/tmp/source_clusters_${SLURM_JOB_ID}.txt"
    if [ -f "$cluster_pids_file" ]; then
        while IFS= read -r cluster_info; do
            local pid=$(echo "$cluster_info" | cut -d: -f1)
            log_info "Stopping source cluster (PID $pid)"
            kill $pid 2>/dev/null || true
            sleep 2
            kill -9 $pid 2>/dev/null || true
        done < "$cluster_pids_file"
        rm -f "$cluster_pids_file"
    fi
    
    log_info "Cleanup complete"
    exit $exit_code
}

trap cleanup EXIT INT TERM

# Main execution
main() {
    log_info "=== Starting Elasticsearch Index Merge ==="
    
    configure_proxy_bypass
    show_configuration
    
    if ! start_source_clusters; then
        log_error "Failed to start source clusters"
        exit 1
    fi
    
    if ! start_target_cluster; then
        log_error "Failed to start target cluster"
        exit 1
    fi
    
    if ! discover_and_merge_indexes; then
        log_error "Merge operation failed"
        exit 1
    fi
    
    log_success "=== Merge completed successfully ==="
}

main