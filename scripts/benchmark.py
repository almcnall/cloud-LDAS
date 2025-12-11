# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.3
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Performance for Zonal Statistics

# %% [markdown]
# ## Setup

# %%
import os
import argparse
from pathlib import Path

import fsspec
import earthaccess
import xarray as xr

# %%
# FIXME: core code

# %%
in_cloud = "SCRATCH_BUCKET" in os.environ
in_cloud

# %%
parser = argparse.ArgumentParser()
parser.add_argument(
    "--remote",
    default="s3" if in_cloud else "local",
    help="type of storage used for reprocessed files and copies of the original",
)
parser.add_argument(
    "--prefix",
    default=os.environ["SCRATCH_BUCKET"].removeprefix("s3://") if in_cloud else "data",
    help="the prefix to prepend to 'cloud_ldas' for remote storage",
)
parser.add_argument(
    "--tmpdir",
    default=in_cloud,
    action="store_true",
    help="whether to use a transient temporary directory for downloads (original files)",
)
parser.add_argument(
    "--count",
    default=2,
    help="the number of files to reprocess, use '-1' for all",
)
args, _ = parser.parse_known_args()
args

# %%
if args.remote == "local":
    storage = fsspec.filesystem(args.remote, auto_mkdir=True)
else:
    storage = fsspec.filesystem(args.remote)
storage

# %%
prefix = Path(args.prefix, "cloud_ldas")
prefix

# %% [markdown]
# ## Read Reprocess data

# %%
dataset = xr.open_dataset("reprocess.nc")
dataset


# %%
def storage_path(name):
    path = prefix
    for item in exp_dims:
        path = prefix / item / int(getattr(args, item)) # FIXME
    path = path / name
    return path


# %%
def open_dataset(dataset, name):
    # need to time "xr.open_[mf]dataset"
    array = dataset["time"]
    paths = storage_path(array)
    return zs    


# %%
storage.glob(str(prefix / "C*"))

# %% [markdown]
# ## FLDAS

# %% [markdown]
# ### File List

# %%
fs = fsspec.filesystem("local")
fs.glob("*.ipynb")

# %%
storage.glob('/openscapeshub-scratch/itcarroll/cloud_ldas/C2773858780-GES_DISC/**/G*')

# %%
str(prefix)

# %%
storage.glob(str(prefix) + "/**/G*")

# %%
paths = [storage_path(i["meta"]["concept-id"]) for i in granules]

# %% [markdown]
# load function that accepts coords for exp_dims

# %% [markdown]
# ## Scratch ...

# %% [markdown]
# 1. use fsspec to open from storage
# 2. use reference filesystem when needed for kerchunk
# 3. do zonal statistics
# 4. write output to dataset that can be combined by coords with open_mfdataset (replicates?)

# %%
storage, prefix

# %%
path = prefix / "rechunk/0/repack/0/kerchunk/0/G2777011867-GES_DISC"
with storage.open(path) as fo:
    ds = xr.open_dataset(fo, engine="h5netcdf")

# %%
path = prefix / "rechunk/0/repack/0/kerchunk/0/G2777011867-GES_DISC"
path = prefix / "rechunk/1/repack/1/kerchunk/1/G2777011879-GES_DISC"
storage.cat(str(prefix / path))

# %%
path = prefix / "rechunk/0/repack/0/kerchunk/1/G2777011879-GES_DISC"
ds = xr.open_dataset(
    "reference://",
    engine="zarr",
    backend_kwargs={
        "consolidated": False,
        "storage_options": {
            "fo": str(path),
            "target_protocol": args.remote,
            "remote_protocol": args.remote,
        },
    },
    chunks={},
)

# %% slideshow={"slide_type": ""} editable=true
with storage.open(path) as fo:
    ds = xr.open_dataset(
        "reference://",
        engine="zarr",
        backend_kwargs={
            "consolidated": False,
            "storage_options": {
                "fo": fo, # HERE a str but give target_options?
                "remote_protocol": args.remote,
            },
        },
        chunks={},
    )

# %%
# %debug

# %%
