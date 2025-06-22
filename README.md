# PebbleDB Benchmark Tool

This project provides a configurable CLI benchmarking tool for evaluating the performance of [PebbleDB](https://github.com/cockroachdb/pebble) under workloads resembling the Polygon PoS state access patterns. It supports concurrent read and write operations, reproducible workloads, and integration with pre-existing datasets.

## Features

* Concurrent write and read benchmarking
* Configurable key/value sizes and operation ratios
* Deterministic runs using seed
* Optional input of key file (binary format)
* Structured logs with zerolog (JSON)

## Use Cases

* Profiling raw throughput of PebbleDB with realistic state trie-like key structures
* Evaluating cache hit/miss behavior under various concurrency levels
* Measuring read latency with known key sets

---

## 🏗 Project Structure

* `main.go` — CLI entrypoint using Cobra
* `cmd/run.go` — CLI flag parsing and command handler
* `benchmark/runner.go` — Benchmark execution and statistics
* `pebbledb/` — PebbleDB integration wrapper
* `sample/keys.dat` — Optional binary file with pre-encoded keys

---

## 🚀 CLI Usage

### Build

```bash
go build -o pebble-bench
```

### Help

```bash
./pebble-bench run --help
```

---

## 📦 Binary Key File Format

The binary key file must follow this format:

```
[size][keybytes][size][keybytes]...
```

Where `size` is a varint indicating the byte length of the next key.

---

## 💻 Examples

### 1. Read-only benchmark with preloaded keys

```bash
go run main.go run \
  --db-path /tmp/pebble-bench \
  --keys-file ./sample/keys.dat \
  --read-ratio 1.0 \
  --benchmark-id read-only-test
```

### 2. Concurrent read-only benchmark (4 workers)

```bash
go run main.go run \
  --db-path /tmp/pebble-bench \
  --keys-file ./sample/keys.dat \
  --read-ratio 1.0 \
  --concurrency 4 \
  --benchmark-id parallel-read-only
```

### 3. Full write + read (from generated keys)

```bash
go run main.go run \
  --write \
  --db-path /tmp/pebble-bench \
  --key-count 10000 \
  --value-size 256 \
  --read-ratio 1.0 \
  --seed 42 \
  --benchmark-id write-read-test
```

### 4. Full write + read with concurrency

```bash
go run main.go run \
  --write \
  --db-path /tmp/pebble-bench \
  --key-count 10000 \
  --value-size 256 \
  --read-ratio 1.0 \
  --seed 42 \
  --concurrency 4 \
  --benchmark-id parallel-write-read
```

---

## 🛠 Dependencies

* Go 1.20+
* [PebbleDB](https://github.com/cockroachdb/pebble)
* [Zerolog](https://github.com/rs/zerolog)
* [Cobra CLI](https://github.com/spf13/cobra)
