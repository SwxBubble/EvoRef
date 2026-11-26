# EvoRef

EvoRef is a version-aware delta-compression system for multi-version datasets.
It combines Evolution-Aware Reference Propagation (ERP), a Adaptive Conditional 
Reference Search (ACRS), and Online Ancestor-Reuse Scheduler (OARS). 

## Requirements

- Linux with a C++17 compiler
- CMake 3.10 or newer

On Ubuntu or Debian:

```bash
sudo apt-get install build-essential cmake libgoogle-glog-dev \
  libgflags-dev libgtest-dev
```

Xdelta3 and cpptoml are included under `3party/`; no separate installation is
required for them.

## Build

```bash
git clone <repository-url> EvoRef
cd EvoRef
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```


## Dataset layout

Place versions in one directory, with one regular file per version. EvoRef
recursively discovers files below `task_data_dir`. For version archives, names
such as the following work with the default `natural_version` order:

```text
dataset/
├── project-1.0.tar
├── project-1.1.tar
└── project-1.10.tar
```

## Compress

Edit `task_data_dir` and, if needed, the artifact paths in
`configs/compress.toml`, then run:

```bash
./build/delta --config=configs/compress.toml
```

The default configuration writes:

- `artifacts/evoref.data`: Base and Delta payloads;
- `artifacts/evoref.meta`: fixed-size chunk metadata;
- `artifacts/evoref.file`: source file paths and logical chunk ranges.

## Recover

Materialize every file with the LRU cache:

```bash
./build/delta --config=configs/recover.toml
```

Recovered files are written below `artifacts/recovered`. The configuration
records output files by basename, so dataset version filenames should be
unique.

## OARS reconstruction benchmark

OARS currently runs in reconstruction-only mode so filesystem output does not
contaminate reconstruction throughput:

```bash
./build/delta --config=configs/recover_oars.toml
```

## Repository structure

```text
3party/    bundled cpptoml and Xdelta3 sources and licenses
chunk/     FastCDC and chunk representation
configs/   portable compression, recovery, OARS, and verification examples
dedup/     exact SHA-1 chunk deduplication
encoder/   Xdelta wrapper and round-trip test
feature/   EvoRef ODESS subfeature extraction and regression test
index/     ACRS
storage/   persistent chunk storage, LRU cache, and OARS cache primitives
utils/     Gear hash table, memory mapping, and SHA-1
```

