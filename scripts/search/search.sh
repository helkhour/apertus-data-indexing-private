#!/bin/bash

# Multi-CSV Search Script for Elasticsearch

# Processes multiple CSV files against a single index

set -e

# Resolves repository-local paths so the search wrapper works from any working directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SEARCH_SCRIPT="${SEARCH_SCRIPT:-$SCRIPT_DIR/search.py}"

# =============================================================================
# QUERY CONFIGURATION PARAMETERS
# =============================================================================
EXECUTE_MATCH_QUERY="${EXECUTE_MATCH_QUERY:-false}"
EXECUTE_MATCH_PHRASE_QUERY="${EXECUTE_MATCH_PHRASE_QUERY:-true}"
EXECUTE_WILDCARD_QUERY="${EXECUTE_WILDCARD_QUERY:-false}"
EXECUTE_FUZZY_QUERY="${EXECUTE_FUZZY_QUERY:-false}"
EXECUTE_BOOL_MUST_QUERY="${EXECUTE_BOOL_MUST_QUERY:-false}"

MATCH_QUERY_OPERATOR="${MATCH_QUERY_OPERATOR:-[\"or\"]}"
MATCH_PHRASE_SLOP="${MATCH_PHRASE_SLOP:-[0]}"

BOOL_MUST_OPERATOR="${BOOL_MUST_OPERATOR:-or}"
BOOL_MUST_MAX_WORDS="${BOOL_MUST_MAX_WORDS:-3}"
BOOL_MUST_MINIMUM_SHOULD_MATCH="${BOOL_MUST_MINIMUM_SHOULD_MATCH:-50%}"

# =============================================================================
# ELASTICSEARCH CONFIGURATION
# =============================================================================
ES_HOST="${ES_HOST:-127.0.0.1}"
ES_PORT="${ES_PORT:-9200}"

DATASET="${DATASET:-pure_text}"  # Expected values: fineweb, web, sft, pure_text
CURRENT_USER="${SLURM_JOB_USER:-$USER}"
PATH_DATA="${PATH_DATA:-/iopsstor/scratch/cscs/${CURRENT_USER}/es-data-target_brouillon}"
INPUT_STRING="${INPUT_STRING:-}"

# MULTI-CSV SUPPORT: CSV_FILES is now a space-separated list
CSV_FILES="${CSV_FILES:-}"

INDEX_NAME="${INDEX_NAME:-september_brouillon1}" 

OUTPUT_DIR_BASE="${OUTPUT_DIR_BASE:-/capstor/scratch/cscs/${CURRENT_USER}/SEARCH_RESULTS}"

ES_URL="http://${ES_HOST}:${ES_PORT}"

# Calculate heap based on available memory 
if [ "${SLURM_MEM_PER_NODE:-0}" -ge 32768 ]; then
    # 32GB+ available, use 30GB heap (conservative, leaves room for OS + caches)
    JAVA_HEAP="-Xms30g -Xmx30g"
else
    # Less than 32GB, use 8GB heap
    JAVA_HEAP="-Xms8g -Xmx8g"
fi

# Improved GC settings for large heap
JAVA_OPTS="$JAVA_HEAP -XX:MaxGCPauseMillis=500 -XX:G1HeapRegionSize=32m"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'


# Trim whitespace from critical variables
PATH_DATA=$(echo "$PATH_DATA" | xargs)
INDEX_NAME=$(echo "$INDEX_NAME" | xargs)

# Logging functions
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

# Function to build configuration JSON
build_config_json() {
    local temp_json=$(mktemp)
    
    cat > "$temp_json" << EOF
{
    "execute_match_query": $EXECUTE_MATCH_QUERY,
    "execute_match_phrase_query": $EXECUTE_MATCH_PHRASE_QUERY,
    "execute_wildcard_query": $EXECUTE_WILDCARD_QUERY,
    "execute_fuzzy_query": $EXECUTE_FUZZY_QUERY,
    "execute_bool_must_query": $EXECUTE_BOOL_MUST_QUERY,
    "match_query_operator": $MATCH_QUERY_OPERATOR,
    "match_phrase_slop": $MATCH_PHRASE_SLOP,
    "bool_must_operator": "$BOOL_MUST_OPERATOR",
    "bool_must_max_words": $BOOL_MUST_MAX_WORDS
EOF

    if [ -n "$BOOL_MUST_MINIMUM_SHOULD_MATCH" ]; then
        if [[ "$BOOL_MUST_MINIMUM_SHOULD_MATCH" =~ ^[0-9]+%?$ ]]; then
            if [[ "$BOOL_MUST_MINIMUM_SHOULD_MATCH" =~ % ]]; then
                echo "    ,\"bool_must_minimum_should_match\": \"$BOOL_MUST_MINIMUM_SHOULD_MATCH\"" >> "$temp_json"
            else
                echo "    ,\"bool_must_minimum_should_match\": $BOOL_MUST_MINIMUM_SHOULD_MATCH" >> "$temp_json"
            fi
        else
            echo "    ,\"bool_must_minimum_should_match\": \"$BOOL_MUST_MINIMUM_SHOULD_MATCH\"" >> "$temp_json"
        fi
    fi
    
    echo "}" >> "$temp_json"
    
    if command -v python3 >/dev/null 2>&1; then
        local json_content=$(python3 -c "import json, sys; print(json.dumps(json.load(open('$temp_json'))))" 2>/dev/null)
        if [ $? -eq 0 ]; then
            echo "$json_content"
        else
            cat "$temp_json"
        fi
    else
        cat "$temp_json"
    fi
    
    rm -f "$temp_json"
}

configure_proxy_bypass() {
    log_info "Configuring proxy bypass for localhost connections..."
    
    ORIGINAL_HTTP_PROXY="${http_proxy:-}"
    ORIGINAL_NO_PROXY="${no_proxy:-}"
    
    export no_proxy="${no_proxy},127.0.0.1,localhost,0.0.0.0,::1"
    
    log_info "Original no_proxy: $ORIGINAL_NO_PROXY"
    log_info "Updated no_proxy: $no_proxy"
    log_info "HTTP proxy: ${http_proxy:-'(not set)'}"
}

# Validate required parameters
if [ -z "$PATH_DATA" ]; then
    log_error "PATH_DATA is required"
    exit 1
fi


if [ -z "$CSV_FILES" ] && [ -z "$INPUT_STRING" ]; then
    log_error "Either CSV_FILES or INPUT_STRING must be provided"
    exit 1
fi



# Validate CSV files exist

CSV_COUNT=0
if [ -n "$CSV_FILES" ]; then
    for csv in $CSV_FILES; do
        if [ ! -f "$csv" ]; then
            log_error "CSV file '$csv' not found"
            exit 1
        fi
        CSV_COUNT=$((CSV_COUNT + 1))
    done
fi


if [ ! -d "$PATH_DATA" ]; then
    log_error "Data path '$PATH_DATA' not found"
    exit 1
fi

log_info "Starting Elasticsearch Multi-CSV Search Pipeline..."
log_info "========================================================================="
log_info "Configuration:"
log_info "  Data path: $PATH_DATA"
log_info "  CSV files ($CSV_COUNT):"

for csv in $CSV_FILES; do

    log_info "    - $csv"

done

log_info "  Index name: $INDEX_NAME"
log_info "  Elasticsearch: $ES_URL"
log_info "  Java heap: $JAVA_OPTS"
log_info "  Output base directory: $OUTPUT_DIR_BASE"
log_info "  Dataset chosen: $DATASET"
log_info ""
log_info "Query Configuration:"
log_info "  Match Query: $EXECUTE_MATCH_QUERY"
log_info "  Match Query Operator: $MATCH_QUERY_OPERATOR"
log_info "  Match Phrase Query: $EXECUTE_MATCH_PHRASE_QUERY"
log_info "  Match Phrase Slop: $MATCH_PHRASE_SLOP"
log_info "  Wildcard Query: $EXECUTE_WILDCARD_QUERY"
log_info "  Fuzzy Query: $EXECUTE_FUZZY_QUERY"
log_info "  Bool Must Query: $EXECUTE_BOOL_MUST_QUERY"
log_info "  Bool Must Operator: $BOOL_MUST_OPERATOR"
log_info "  Bool Must Max Words: $BOOL_MUST_MAX_WORDS"
log_info "  Bool Must Min Should Match: ${BOOL_MUST_MINIMUM_SHOULD_MATCH:-'(not set)'}"
log_info "========================================================================="

# Check disk space before starting
log_info "=== Disk Space Check ==="
df -h "$PATH_DATA" | tail -1
log_info "Index size on disk:"
du -sh "$PATH_DATA" 2>/dev/null || echo "Cannot calculate"
log_info ""

start_elasticsearch() {
    log_info "Starting Elasticsearch with large index optimizations..."
    
    unset JAVA_HOME
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"

    log_info "Testing Java installation..."
    if $ES_JAVA_HOME/bin/java -version; then
        log_success "Java test successful"
    else
        log_error "Java test failed"
        return 1
    fi
    
    log_info "Using heap settings: $JAVA_OPTS"
    CURRENT_USER="${SLURM_JOB_USER:-$USER}"
    local job_logs_dir="/iopsstor/scratch/cscs/${CURRENT_USER}/es-search-logs-${SLURM_JOB_ID:-$$}"
    mkdir -p "$job_logs_dir"

    local job_data_dir="$PATH_DATA"

    log_info "=== Starting Elasticsearch ==="

    ES_JAVA_OPTS="$JAVA_OPTS" \
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
        -E cluster.routing.allocation.disk.watermark.low=98% \
        -E cluster.routing.allocation.disk.watermark.high=99% \
        -E cluster.routing.allocation.disk.watermark.flood_stage=99.5% \
        -E cluster.routing.allocation.disk.threshold_enabled=true \
        -E cluster.routing.allocation.node_concurrent_recoveries=4 \
        -E cluster.routing.allocation.node_initial_primaries_recoveries=8 \
        -E indices.recovery.max_bytes_per_sec=200mb \
        -E cluster.routing.allocation.enable=all \
        -E bootstrap.memory_lock=false \
        -E logger.root=INFO \
        -E http.max_content_length=200mb \
        -E gateway.recover_after_time=5m \
        -E gateway.expected_data_nodes=1 \
        -E gateway.recover_after_data_nodes=1 \
        -E indices.recovery.max_concurrent_file_chunks=2 \
        -E indices.recovery.max_concurrent_operations=1 \
        > "$job_logs_dir/elasticsearch.out" 2>&1 &
    
    ES_PID=$!
    log_info "Elasticsearch started with PID: $ES_PID (optimized for large 600GB+ index)"
    
    log_info "Waiting for Elasticsearch process to stabilize (60 seconds)..."
    sleep 60
    
    if ! kill -0 $ES_PID 2>/dev/null; then
        log_error "Elasticsearch process died during startup!"
        log_error "Check logs at: $job_logs_dir/elasticsearch.out"
        tail -50 "$job_logs_dir/elasticsearch.out"
        return 1
    fi
    
    log_info "Waiting for large index to load (this may take 30-60 minutes)..."
    max_retries=1800
    retry_count=0
    
    local stage=1
    local basic_connectivity_established=false
    local cluster_responding=false
    
    while [ $retry_count -lt $max_retries ]; do
        if ! kill -0 $ES_PID 2>/dev/null; then
            log_error "Elasticsearch process died! PID $ES_PID is no longer running"
            log_error "Last 50 lines of log:"
            tail -50 "$job_logs_dir/elasticsearch.out"
            return 1
        fi
        
        # Stage 1: Basic connectivity
        if [ "$basic_connectivity_established" = false ]; then
            if curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/" > /dev/null 2>&1; then
                basic_connectivity_established=true
                log_success "Stage 1: Basic connectivity established!"
                stage=2
            fi
        fi
        
        # Stage 2: Cluster responding
        if [ "$basic_connectivity_established" = true ] && [ "$cluster_responding" = false ]; then
            local health_check=$(curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cluster/health" 2>/dev/null)
            if [ -n "$health_check" ]; then
                cluster_responding=true
                log_success "Stage 2: Cluster responding!"
                stage=3
            fi
        fi
        
        # Stage 3: Shard recovery monitoring
        if [ "$cluster_responding" = true ]; then
            HEALTH_RESPONSE=$(curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cluster/health" 2>/dev/null)
            
            # More flexible grep patterns that handle variable spacing
            STATUS=$(echo "$HEALTH_RESPONSE" | grep -o '"status"[[:space:]]*:[[:space:]]*"[^"]*"' | grep -o '"[^"]*"$' | tr -d '"')
            INITIALIZING=$(echo "$HEALTH_RESPONSE" | grep -o '"initializing_shards"[[:space:]]*:[[:space:]]*[0-9]*' | grep -o '[0-9]*$')
            UNASSIGNED=$(echo "$HEALTH_RESPONSE" | grep -o '"unassigned_shards"[[:space:]]*:[[:space:]]*[0-9]*' | grep -o '[0-9]*$')
            RELOCATING=$(echo "$HEALTH_RESPONSE" | grep -o '"relocating_shards"[[:space:]]*:[[:space:]]*[0-9]*' | grep -o '[0-9]*$')
            ACTIVE_SHARDS=$(echo "$HEALTH_RESPONSE" | grep -o '"active_shards"[[:space:]]*:[[:space:]]*[0-9]*' | grep -o '[0-9]*$')
            ACTIVE_PRIMARY=$(echo "$HEALTH_RESPONSE" | grep -o '"active_primary_shards"[[:space:]]*:[[:space:]]*[0-9]*' | grep -o '[0-9]*$')
            
            # Validate we got values (fallback if grep fails)
            if [ -z "$STATUS" ]; then
                if [ $((retry_count % 30)) -eq 0 ]; then
                    log_warn "Failed to parse cluster health, retrying..."
                fi
                retry_count=$((retry_count + 1))
                sleep 1
                continue
            fi
            
            # Progress logging every 30 seconds
            if [ $((retry_count % 30)) -eq 0 ]; then
                log_info "Stage 3: Shard recovery in progress..."
                log_info "  Status: $STATUS | Active: $ACTIVE_SHARDS/${ACTIVE_PRIMARY:-?} | Initializing: $INITIALIZING | Unassigned: $UNASSIGNED | Relocating: $RELOCATING"
                
                # Show recovery progress
                local recovery_info=$(curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cat/recovery/$INDEX_NAME?v&h=shard,stage,type,bytes_percent" 2>/dev/null | head -10)
                if [ -n "$recovery_info" ]; then
                    log_info "  Recovery progress (first 10 shards):"
                    echo "$recovery_info" | while IFS= read -r line; do
                        log_info "    $line"
                    done
                fi
            fi
            
            # Check for success: green or yellow status with no initializing/relocating shards
            if [ "$STATUS" = "green" ] || [ "$STATUS" = "yellow" ]; then
                if [ "$INITIALIZING" = "0" ] && [ "$RELOCATING" = "0" ]; then
                    # FIXED: Robust check for unassigned primary shards
                    # Get shard info, grep for unassigned primaries, count lines, clean output
                    local shard_output=$(curl --noproxy "127.0.0.1" -s \
                        "http://127.0.0.1:9200/_cat/shards/$INDEX_NAME?h=prirep,state" 2>/dev/null)
                    
                    local unassigned_primaries=0
                    if [ -n "$shard_output" ]; then
                        # Count lines containing "p UNASSIGNED", ensure we get a clean number
                        unassigned_primaries=$(echo "$shard_output" | grep "p UNASSIGNED" | wc -l | tr -d ' ')
                        # If grep finds nothing, wc returns 0, but ensure it's a valid number
                        if ! [[ "$unassigned_primaries" =~ ^[0-9]+$ ]]; then
                            unassigned_primaries=0
                        fi
                    fi
                    
                    if [ "$unassigned_primaries" -eq 0 ]; then
                        log_success "Elasticsearch is ready!"
                        log_info "Final cluster status: $STATUS (Active shards: $ACTIVE_SHARDS/${ACTIVE_PRIMARY:-?}, Unassigned replicas: $UNASSIGNED)"
                        
                        log_info "=== Final Cluster Health ==="
                        echo "$HEALTH_RESPONSE"
                        
                        log_info "=== Node Stats (Memory) ==="
                        curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_nodes/stats/jvm,indices?pretty" 2>/dev/null | head -50
                        
                        sleep 5
                        log_info "=== Configuring Index Settings for Query Performance ==="
                        curl --noproxy "127.0.0.1" -s -X PUT "http://127.0.0.1:9200/$INDEX_NAME/_settings" \
                            -H 'Content-Type: application/json' -d'
                        {
                          "index": {
                            "refresh_interval": "-1",
                            "number_of_replicas": 0,
                            "translog.durability": "async",
                            "translog.sync_interval": "30s"
                          }
                        }' 2>/dev/null
                        log_success "Index settings configured for query performance"
                        
                        return 0
                    else
                        log_warn "Still have $unassigned_primaries unassigned primary shards, waiting..."
                    fi
                fi
            fi
            
            # Check for stuck recovery
            if [ "$STATUS" = "red" ] && [ $retry_count -gt 600 ]; then
                log_warn "=== Investigating RED status after 10 minutes ==="
                
                local allocation_explain=$(curl --noproxy "127.0.0.1" -s -X GET \
                    "http://127.0.0.1:9200/_cluster/allocation/explain?pretty" 2>/dev/null)
                log_warn "Allocation explanation:"
                echo "$allocation_explain" | head -50
                
                log_warn "=== Disk Space Status ==="
                df -h "$job_data_dir" | tail -1
                
                if [ $retry_count -gt 1200 ]; then
                    log_error "Cluster has been RED for over 20 minutes - likely a serious issue"
                    log_error "Check logs at: $job_logs_dir/elasticsearch.out"
                    tail -100 "$job_logs_dir/elasticsearch.out"
                    return 1
                fi
            fi
        fi
        
        retry_count=$((retry_count + 1))
        
        if [ $((retry_count % 60)) -eq 0 ]; then
            case $stage in
                1)
                    log_info "Stage 1: Waiting for basic connectivity... (${retry_count}s elapsed)"
                    ;;
                2)
                    log_info "Stage 2: Waiting for cluster to respond... (${retry_count}s elapsed)"
                    ;;
                3)
                    log_info "Stage 3: Monitoring shard recovery... (${retry_count}s elapsed / max ${max_retries}s)"
                    ;;
            esac
        fi
        
        sleep 1
    done
    
    log_error "Elasticsearch startup timeout after $max_retries seconds"
    log_error "Last cluster health check:"
    curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cluster/health?pretty" 2>/dev/null
    log_error "Check logs at: $job_logs_dir/elasticsearch.out"
    tail -100 "$job_logs_dir/elasticsearch.out"
    return 1
}

check_index_exists() {
    local index_name="$1"
    local http_code
    
    http_code=$(curl -s -o /dev/null -w "%{http_code}" "$ES_URL/$index_name" 2>/dev/null)
    local curl_exit_code=$?
    
    if [ $curl_exit_code -ne 0 ]; then
        log_warn "Failed to check index existence (curl error)"
        return 1
    fi
    
    case "$http_code" in
        200)
            return 0
            ;;
        404)
            return 1
            ;;
        *)
            log_warn "Unexpected HTTP code when checking index: $http_code"
            return 1
            ;;
    esac
}

list_indices() {
    log_info "Available indices:"
    
    local response
    response=$(curl -s "$ES_URL/_cat/indices?v" 2>&1)
    local curl_exit_code=$?
    
    if [ $curl_exit_code -eq 0 ] && [ -n "$response" ]; then
        if echo "$response" | grep -qi "<!DOCTYPE\|<html"; then
            log_warn "Received HTML response instead of indices list"
            log_warn "This usually means Elasticsearch returned an error page"
            log_info "Trying JSON format..."
            
            local json_response
            json_response=$(curl -s "$ES_URL/_cat/indices?format=json" 2>&1)
            if [ $? -eq 0 ] && echo "$json_response" | grep -q "^\["; then
                echo "$json_response"
            else
                log_error "Both formats failed. Raw response:"
                echo "$response" | head -10
                return 1
            fi
        else
            echo "$response"
        fi
    else
        log_error "Failed to fetch indices (exit code: $curl_exit_code)"
        if [ -n "$response" ]; then
            log_error "Response: $response"
        fi
        return 1
    fi
}

cleanup() {
    if [ ! -z "$ES_PID" ] && kill -0 $ES_PID 2>/dev/null; then
        log_info "Stopping Elasticsearch (PID: $ES_PID)..."
        kill $ES_PID
        wait $ES_PID 2>/dev/null || true
    fi
}

trap cleanup EXIT

main() {
    configure_proxy_bypass

    if ! start_elasticsearch; then
        log_error "Failed to start Elasticsearch"
        exit 1
    fi
    
    log_info "=== Testing connection after proxy fix ==="
    log_info "Testing root endpoint without proxy:"
    curl -v "http://127.0.0.1:9200/" 2>&1 | head -10
    log_info "=== End connection test ==="
    
    log_info "Checking available indices..."
    if ! list_indices; then
        log_error "Failed to list indices, but continuing anyway..."
        log_info "You may need to check the index name manually"
    fi
    
    log_info "Checking if index '$INDEX_NAME' exists..."
    if ! check_index_exists "$INDEX_NAME"; then
        log_warn ""
        log_warn "Index '$INDEX_NAME' not found or couldn't verify!"
        log_info "Attempting to list indices again..."
        list_indices
        log_info ""
        log_info "If the index exists but isn't showing, there might be a connectivity issue."
        log_info "The search pipeline will attempt to continue anyway."
        log_warn "If searches fail, verify the index name and Elasticsearch health."
    else
        log_success ""
        log_success "Index '$INDEX_NAME' found. Proceeding with search pipeline..."
    fi
    
    log_info "Building query configuration..."
    CONFIG_JSON=$(build_config_json)
    
    if [ -z "$CONFIG_JSON" ]; then
        log_error "Failed to build configuration JSON"
        exit 1
    fi

       log_info ""

    log_info "========================================================================="
    log_info "Starting MULTI-CSV search queries execution..."
    log_info "This index will be queried with $CSV_COUNT different CSV files"
    log_info "Each CSV will have its own output subdirectory"
    log_info "========================================================================="
    log_info ""


   if [ -n "$INPUT_STRING" ]; then
    log_info "Running search on direct text input..."
    # Executes the repo-local search entrypoint for direct input mode.
    if python3 "$SEARCH_SCRIPT" \
        --input-string "$INPUT_STRING" \
        --index-name "$INDEX_NAME" \
        --es-url "$ES_URL" \
        --output-dir-base "$OUTPUT_DIR_BASE" \
        --dataset "$DATASET" \
        --config "$CONFIG_JSON"; then

        log_success ""
        log_success "Search pipeline completed successfully (input string mode)!"
        log_info "Output files saved to: $OUTPUT_DIR_BASE"

        log_info ""
        log_info "Query configuration used:"
        echo "$CONFIG_JSON" | python3 -m json.tool 2>/dev/null || echo "$CONFIG_JSON"
	exit 0

    else
        log_error "Search pipeline failed (input string mode)!"
        exit 1
    fi
   fi

   # Convert CSV_FILES to array for Python
    CSV_FILES_ARRAY=""
    for csv in $CSV_FILES; do
        if [ -z "$CSV_FILES_ARRAY" ]; then
            CSV_FILES_ARRAY="$csv"
        else
            CSV_FILES_ARRAY="$CSV_FILES_ARRAY $csv"
        fi
    done

    # Executes the repo-local search entrypoint for CSV batch mode.
    if python3 "$SEARCH_SCRIPT" \
        --csv-files $CSV_FILES_ARRAY \
        --index-name "$INDEX_NAME" \
        --es-url "$ES_URL" \
        --output-dir-base "$OUTPUT_DIR_BASE" \
        --dataset "$DATASET" \
        --config "$CONFIG_JSON"; then
        log_success ""
        log_success "Multi-CSV search pipeline completed successfully!"
        log_info "Output files saved to: $OUTPUT_DIR_BASE"
        log_info ""
        log_info "Directory structure:"
        log_info "  $OUTPUT_DIR_BASE/"

        for csv in $CSV_FILES; do
            csv_basename=$(basename "$csv" | sed 's/\.[^.]*$//')
            log_info "    ├── ${INDEX_NAME}_${csv_basename}/"
            log_info "    │   ├── search_results_detailed_*.json"
            log_info "    │   ├── search_results_summary_*.json"
            log_info "    │   └── search_results_fulltext_*.json"
        done

        log_info ""
        log_info "Query configuration used:"
        echo "$CONFIG_JSON" | python3 -m json.tool 2>/dev/null || echo "$CONFIG_JSON"
    else
        log_error "Search pipeline failed!"
        exit 1
    fi
}

main
