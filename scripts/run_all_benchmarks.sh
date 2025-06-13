
#!/bin/bash

set -euo pipefail

# Configuration
RESULTS_DIR="benchmark_results"
BENCHMARK_TIMEOUT=600  # 10 minutes per benchmark

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

mkdir -p "$RESULTS_DIR"

echo -e "${BLUE}🚀 Running Complete RS2 Benchmark Suite${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Results will be saved to: ${YELLOW}$RESULTS_DIR/${NC}"
echo ""

# Function to check if benchmark actually has results
has_benchmark_results() {
    local output_file="$1"

    if [ ! -f "$output_file" ]; then
        return 1
    fi

    # Check for actual benchmark timing data
    if grep -q "time:" "$output_file" 2>/dev/null; then
        return 0
    fi

    return 1
}

# Function to check if benchmark failed due to compilation
has_compilation_errors() {
    local output_file="$1"

    if [ ! -f "$output_file" ]; then
        return 1
    fi

    # Check for compilation errors
    if grep -q "error\[E[0-9]" "$output_file" 2>/dev/null; then
        return 0
    fi

    return 1
}

# Function to detect test output instead of benchmark output
has_test_output() {
    local output_file="$1"

    if [ ! -f "$output_file" ]; then
        return 1
    fi

    # Check for test runner output (indicates benchmark wasn't properly configured)
    if grep -q "running.*tests" "$output_file" 2>/dev/null; then
        return 0
    fi

    return 1
}

# Function to move HTML reports to results directory
move_html_reports() {
    echo -e "${BLUE}📁 Moving HTML reports to $RESULTS_DIR/...${NC}"

    # Find and move all criterion HTML reports
    if [ -d "target/criterion" ]; then
        # Copy the entire criterion directory structure to maintain functionality
        cp -r target/criterion "$RESULTS_DIR/" 2>/dev/null || true
        echo -e "${GREEN}✅ HTML reports moved to $RESULTS_DIR/criterion/${NC}"

        # Also create direct links to index files for easy access
        find "$RESULTS_DIR/criterion" -name "index.html" -type f | while read -r html_file; do
            local benchmark_name=$(basename "$(dirname "$html_file")")
            local link_name="$RESULTS_DIR/${benchmark_name}_report.html"

            # Create a relative link or copy
            cp "$html_file" "$link_name" 2>/dev/null || true
        done

        echo -e "${BLUE}📊 HTML reports available:${NC}"
        find "$RESULTS_DIR" -name "*.html" -type f | sed 's|^|  - |'
    else
        echo -e "${YELLOW}⚠️ No HTML reports found in target/criterion${NC}"
    fi
}

# Enhanced benchmark runner
run_benchmark_with_clean_output() {
    local bench_name="$1"
    local output_file="$2"
    local display_name="$3"
    local description="$4"

    echo -e "${BLUE}📊 Running $display_name...${NC}"
    echo -e "${BLUE}   $description${NC}"

    # Run benchmark with timeout protection
    local exit_code=0
    local temp_output="$output_file.tmp"

    if command -v timeout >/dev/null 2>&1; then
        if timeout $BENCHMARK_TIMEOUT bash -c "RUSTFLAGS='-A warnings' cargo bench --bench '$bench_name' 2>/dev/null" > "$temp_output"; then
            exit_code=0
        else
            exit_code=$?
        fi
    else
        # Bash-based timeout fallback
        (
            sleep $BENCHMARK_TIMEOUT && pkill -f "cargo bench --bench $bench_name" &
            TIMEOUT_PID=$!

            if RUSTFLAGS="-A warnings" cargo bench --bench "$bench_name" 2>/dev/null > "$temp_output"; then
                kill $TIMEOUT_PID 2>/dev/null || true
                exit_code=0
            else
                kill $TIMEOUT_PID 2>/dev/null || true
                exit_code=1
            fi
        )
    fi

    mv "$temp_output" "$output_file" 2>/dev/null || true

    # Process results
    if [ $exit_code -eq 0 ]; then
        if has_test_output "$output_file"; then
            echo -e "${RED}❌ $display_name ran as test instead of benchmark${NC}"
            echo -e "${YELLOW}   Add this to Cargo.toml:${NC}"
            echo -e "${BLUE}   [[bench]]${NC}"
            echo -e "${BLUE}   name = \"$bench_name\"${NC}"
            echo -e "${BLUE}   harness = false${NC}"
        elif has_benchmark_results "$output_file"; then
            echo -e "${GREEN}✅ $display_name completed successfully${NC}"
            local benchmark_count=$(grep -c "time:" "$output_file" 2>/dev/null || echo "0")
            echo -e "${GREEN}   📊 $benchmark_count benchmarks completed${NC}"
        else
            echo -e "${YELLOW}⚠️ $display_name completed but no timing data found${NC}"
        fi
    else
        if has_compilation_errors "$output_file"; then
            echo -e "${RED}❌ $display_name failed to compile${NC}"
        else
            echo -e "${RED}❌ $display_name failed to run${NC}"
        fi
    fi
    echo ""
}

# Pre-flight checks
echo -e "${BLUE}🔧 Pre-flight checks...${NC}"

# Check compilation
if ! RUSTFLAGS="-A warnings" cargo check --benches >/dev/null 2>&1; then
    echo -e "${RED}❌ Benchmarks don't compile. Please fix compilation errors first.${NC}"
    exit 1
fi
echo -e "${GREEN}✅ All benchmarks compile successfully${NC}"
echo ""

# List available benchmarks
echo -e "${BLUE}📋 Available benchmarks:${NC}"
ls benches/*.rs 2>/dev/null | sed 's/benches\///; s/\.rs$//' | sed 's/^/  - /' || echo "  No benchmarks found"
echo ""

# Run benchmarks
echo -e "${BLUE}Running benchmarks (this may take a while)...${NC}"
echo ""

# Core stream operations
run_benchmark_with_clean_output "stream_operations" "$RESULTS_DIR/stream_operations.txt" \
    "Stream Operations Benchmarks" \
    "Core stream operations including map, filter, fold, chunk, and async operations."

# Backpressure strategies
run_benchmark_with_clean_output "backpressure_comparison" "$RESULTS_DIR/backpressure_comparison.txt" \
    "Backpressure Comparison" \
    "Evaluation of different backpressure strategies under various load conditions."

# Ecosystem comparison
run_benchmark_with_clean_output "ecosystem_comparison" "$RESULTS_DIR/ecosystem_comparison.txt" \
    "Ecosystem Comparison" \
    "Performance comparison with other Rust streaming libraries and frameworks."

# Memory usage analysis
run_benchmark_with_clean_output "memory_usage" "$RESULTS_DIR/memory_usage.txt" \
    "Memory Usage Analysis" \
    "Memory consumption analysis for different stream operations and configurations."

# Parallel processing (longer timeout)
BENCHMARK_TIMEOUT=1800  # 30 minutes for parallel processing
run_benchmark_with_clean_output "parallel_processing" "$RESULTS_DIR/parallel_processing.txt" \
    "Parallel Processing Benchmarks" \
    "Comprehensive parallel processing evaluation including scaling behavior and optimal concurrency levels."

# Reset timeout
BENCHMARK_TIMEOUT=600

# Move HTML reports after all benchmarks complete
move_html_reports

echo ""
echo -e "${GREEN}🎉 Benchmark suite completed!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}📂 Text outputs saved to:${NC}"
echo -e "   - ${YELLOW}$RESULTS_DIR/stream_operations.txt${NC}"
echo -e "   - ${YELLOW}$RESULTS_DIR/backpressure_comparison.txt${NC}"
echo -e "   - ${YELLOW}$RESULTS_DIR/ecosystem_comparison.txt${NC}"
echo -e "   - ${YELLOW}$RESULTS_DIR/memory_usage.txt${NC}"
echo -e "   - ${YELLOW}$RESULTS_DIR/parallel_processing.txt${NC}"
echo ""
echo -e "${GREEN}📊 HTML reports available at:${NC}"
echo -e "   - ${YELLOW}$RESULTS_DIR/criterion/*/index.html${NC}"
echo -e "   - ${YELLOW}$RESULTS_DIR/*_report.html${NC}"
echo ""
echo -e "${BLUE}Quick commands:${NC}"
echo -e "   ${YELLOW}# View all text results${NC}"
echo -e "   ${YELLOW}ls $RESULTS_DIR/*.txt${NC}"
echo ""
echo -e "   ${YELLOW}# Open HTML reports${NC}"
echo -e "   ${YELLOW}open $RESULTS_DIR/criterion/*/index.html${NC}"
echo ""
echo -e "${GREEN}✨ All results organized in $RESULTS_DIR/ directory${NC}"