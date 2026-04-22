#!/bin/bash
# Demo script for Scherlok — record with:
#   asciinema rec demo.cast -c "bash examples/demo.sh"
#
# Then convert to GIF:
#   npm install -g svg-term-cli
#   svg-term --in demo.cast --out demo.svg --window --width 80 --height 24
#
# Or upload to asciinema.org:
#   asciinema upload demo.cast

set -e

# Simulate typing effect
type_cmd() {
    echo ""
    echo -n "$ "
    echo "$1" | pv -qL 30
    sleep 0.5
    eval "$1"
    sleep 1.5
}

# Check if pv is installed (for typing effect), fallback to plain echo
if ! command -v pv &>/dev/null; then
    type_cmd() {
        echo ""
        echo "$ $1"
        sleep 0.3
        eval "$1"
        sleep 1
    }
fi

clear
echo "  ╔══════════════════════════════════════╗"
echo "  ║     🔍 Scherlok — Quick Demo         ║"
echo "  ║     Zero-config data quality          ║"
echo "  ╚══════════════════════════════════════╝"
sleep 2

# Step 1: Install
type_cmd "pip install scherlok -q"

# Step 2: Connect
type_cmd "scherlok connect postgres://scherlok:scherlok@localhost:5433/demo"

# Step 3: Investigate
type_cmd "scherlok investigate"

# Step 4: Status
type_cmd "scherlok status"

# Step 5: Watch (should be clean)
type_cmd "scherlok watch"

sleep 1
echo ""
echo "  💥 Simulating a data pipeline failure..."
echo "  (deleting half the orders)"
sleep 1.5

# Step 6: Break things
type_cmd "docker exec examples-postgres-1 psql -U scherlok demo -c 'DELETE FROM orders WHERE id > 7'"

sleep 1
echo ""
echo "  🔍 Running Scherlok again..."
sleep 1

# Step 7: Detect anomaly
type_cmd "scherlok watch"

sleep 2
echo ""
echo "  ✅ Scherlok detected the volume drop!"
echo "  📦 pip install scherlok"
echo "  🐙 github.com/rbmuller/scherlok"
sleep 3
