---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.17.2
kernelspec:
  name: bash
  display_name: Bash
  language: bash
---

# Contributing Guide

+++

This guide is an executable MyST Notebook with a Bash kernel.
To interatively execute the code cells below:
  1. use JupyterLab with the Jupytext extension, and
  1. open the file with right-click > "Open With" > "Notebook".

+++

## Steup

+++

### Develop in Notebooks

+++

The code repository does not store notebooks (.ipynb files).
Instead, we store Python scripts (.py files) that the Jupytext extension keeps automatically synced with notebooks.
Those scripts are what we run "operationally" (i.e. to reprocess files and perform benchmarking).
While developing, however, we prefer to use notebooks.

When working in JupyterLab, Jupytext performs synchronization on "save" and on opening a paired .ipynb notebook.

To create .ipynb notebooks after a cloning this repo, or when pulling a new script, run the cell below to manually sync.

```{code-cell}
jupytext --sync $(git ls-files scripts)
```

### Prepare Isolated Environment

+++

We need a Conda environment to get the `h5repack` and `nccopy` command line utilities that
come with `hdf5` and `libnetcdf`.
We also include the `uv` utility, which is the only Python installer currently implementing [PEP-751].

[PEP-751]: https://peps.python.org/pep-0751/

```{code-cell}
conda env create --name ${PWD##*/}
```

The Python packages we can get from PyPI and install with `uv` into the `conda` environment.

```{code-cell}
conda run --live-stream --name ${PWD##*/} uv pip sync pylock.toml
```

### Update Python Packages

+++

In case new packages are added to the dependencies included in `pyproject.toml`, update the `pylock.toml` using `uv export`.
Adding the `--upgrade` flag to `uv lock` would also update all packages.

```{code-cell}
conda run --live-stream --name ${PWD##*/} uv lock
conda run --live-stream --name ${PWD##*/} uv export --quiet --output-file pylock.toml
```

## Reprocess and Benchmark

+++

### Execute as Scripts

+++

Running with no arguments uses the default arguments that apply to notebooks.
The result should be the same, but outputs will be in the current working directory rather
than the `notebooks` directory.

```{code-cell}
conda run --live-stream --name ${PWD##*/} python scripts/reprocess.py
```

```{code-cell}
conda run --name ${PWD##*/} python scripts/reprocess.py \
    --remote=s3 \
    --prefix=$SCRATCH_BUCKET \
    --count=-1
```
