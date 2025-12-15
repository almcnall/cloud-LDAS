---
jupytext:
  formats: md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.17.3
kernelspec:
  name: bash
  display_name: Bash
  language: bash
---

# scripts/README

As noted in the README, this guide is [MyST Markdown], with cells you can run interatively using the bash kernel when the [Jupytext] extension is available.

To reproduce the published results, follow the [](#setup) instructions,
and then run cells below (as needed) within the same session using the activated "workspace".

[MyST Markdown]: https://mystmd.org/
[Jupytext]: https://jupytext.readthedocs.io/

## Setup

```{code-cell}
conda-lock install --name workspace ../conda-lock.yml
```

```{code-cell}
eval "$(conda shell.bash hook)"
```

```{code-cell}
conda activate workspace
```

## (WIP) Reprocess Earthdata Cloud Granules

The `reprocess` script implements cloud optimization strategies:

- Enlarge "chunks" to a size better for cloud object stores
- Move the internal data on file structure into distinct "pages"
- Copy the internal data on file structure to external "sidecar" files

Execute file reprocessing on https://openscapes.2i2c.cloud, using "~28 GB RAM, ~4 CPUs"

```{code-cell}
python reprocess.py --count=-1
```

Execute file reprocessing on an HPC.

```{code-cell}
python reprocess.py --storage=/mnt/mfs/${USER} --count=-1
```

## (WIP) Benchmark Zonal Statistics against Local Storage

The `benchmark` script opens original and reprocessed files to calculate a zonal statistic.

```{code-cell}
python benchmark.py --help # \
  # --remote=s3 \
  # --prefix=$SCRATCH_BUCKET \
  # --tempdir \
  # --count=2
```
