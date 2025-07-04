# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.2
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
if "SCRATCH_BUCKET" in os.environ:
    remote = "s3"
    prefix = os.environ["SCRATCH_BUCKET"]
    tempdir = True
else:
    remote = "local"
    prefix = "data"
    tempdir = False

# %%
parser = argparse.ArgumentParser()
parser.add_argument(
    "--remote",
    default=remote,
    help="the type of filesystem used to store the original and reprocessed files",
)
parser.add_argument(
    "--prefix",
    default=prefix,
    help="the [bucket and] prefix to prepend to 'cloud_ldas' for remote storage"
)
args, _ = parser.parse_known_args()

# %%
if args.remote == "local":
    storage = fsspec.filesystem(args.remote, auto_mkdir=True)
else:
    storage = fsspec.filesystem(args.remote)

# %%
prefix = Path(args.prefix.removeprefix(f"{args.remote}:/"), "cloud_ldas")

# %% [markdown]
# The `exp_dims` constant determins the literal dimensions of the xarray.Dataset
# created to hold timing results for different levels in the three factors of our experiment.

# %%
exp_dims = ("rechunk", "repack", "kerchunk")


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


# %% [markdown]
# ## FLDAS

# %% [markdown]
# ### File List

# %%
product = {
    "short_name": "FLDAS_NOAHMP001_G_CA_D",
    "version": "001",
    "temporal": ("2023-02-01", "2023-02-28"),
}
granules = earthaccess.search_data(**product, cloud_hosted=True)

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

# %% editable=true slideshow={"slide_type": ""}
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
