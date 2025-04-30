# FLDAS and NLDAS Cloud Storage Benchmarking

## Overview

### Reprocess Data Files

The `reprocess` script implements cloud optimization strategies:

- Enlarge "chunks" to a size better for cloud object stores
- Move the internal data on file structure into distinct "pages"
- Copy the internal data on file structure to external "sidecar" files

### Benchmark Zonal Statistics against Local Storage

The `benchmark` script opens original and reprocessed files to calculate a zonal statistic.

## Contributing

The `CONTRIBUTING.md` file provides setup instructions and shell commands to reproduce the data included in the associated publication.

## Acknowledgments

