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
# # Reprocess FLDAS and NLDAS for Cloud Optimization

# %% [markdown]
# ## Setup

# %%
import os
import argparse
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory


import earthaccess
import fsspec
import xarray as xr
from dask.diagnostics import ProgressBar
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr
from msgspec import json

# %% [markdown]
# ### Define Arguments

# %% [markdown]
# The default values set for the `argparse.ArgumentParser` are intended for development work,
# and should be set explicitly when running scripts for the full experiment.
#
# The defaults depend on the existence of the `SCRATCH_BUCKET` environment variable, which
# is defined on the 2i2c JupyterHub and probably not defined locally.
# "Operational" values are supplied when running the scripts (see `CONTRIBUTING.md`).

# %%
parser = argparse.ArgumentParser()
parser.add_argument(
    "--remote",
    default="s3" if "SCRATCH_BUCKET" in os.environ else "local",
    help="type of storage used for reprocessed files and copies of the original",
)
parser.add_argument(
    "--prefix",
    default=os.environ.get("SCRATCH_BUCKET", "data").removeprefix("s3://"),
    help="the prefix to prepend to 'cloud_ldas' for remote storage",
)
parser.add_argument(
    "--tempdir",
    default="SCRATCH_BUCKET" in os.environ,
    action="store_true",
    help="whether to use a transient temporary directory for downloads (original files)",
)
parser.add_argument(
    "--count",
    default=2,
    help="the number of files to reprocess, use '-1' for all",
)
args, _ = parser.parse_known_args()

# %% [markdown]
# ### File handling

# %% [markdown]
# Make an `fsspec.filesystem` for storage, depending on the `remote` argument.

# %%
if args.remote == "local":
    storage = fsspec.filesystem(args.remote, auto_mkdir=True)
else:
    storage = fsspec.filesystem(args.remote)
storage

# %% [markdown]
# Set a path to use as the root of the `storage` filesystem, based on the `prefix` argument.

# %%
prefix = Path(args.prefix, "cloud_ldas")
prefix

# %% [markdown]
# A `TemporaryDirectory()` will be deleted when Python exists, which we don't want
# while developing.

# %%
if args.tempdir:
    tempdir = TemporaryDirectory().name
else:
    tempdir = Path("tmp")
tempdir


# %%
def storage_path(array, **kwargs):
    """Build a path, starting from `prefix`, for outputs.

    The array, which must have a size of one, will also have coordinates used to
    construct the output path for the processed granule. The resulting path looks
    like "prefix/rechunk/[0|1]/repack/[0|1]/kerchunk/[0|1]/concept-id"

    Parameters
    ----------
    array :
        the size-one chunk of the array used to distribute processing
    kwargs :
        override the array's coordinate value for the given keyword
    """
    granule = array["files"].item()
    meta = granule["meta"]
    path = prefix / meta["collection-concept-id"]
    for item in ("rechunk", "repack", "kerchunk"):
        index = kwargs.get(item)
        if index is None:
            index = array[item].item()
        path = path / item / str(index)
    return str(path / meta["concept-id"])


# %% [markdown]
# ### Core Functions

# %% [markdown]
# #### get

# %%
def process_get(dataset):
    """Copy a file from Earthdata Cloud to storage.

    Download URLs are taken from the "files" coordinate in the input array,
    and the time it takes to download and push each file is returned in the
    corresponding value of the output array having the same coordinates.

    Parameters
    ----------
    dataset :
        the size-one chunked dataset used to distribute processing
    """
    array = dataset["time"]
    start = datetime.now()
    paths = earthaccess.download([array["files"].item()], tempdir, show_progress=False)
    storage.put(paths, [storage_path(array)])
    stop = datetime.now()
    dataset["time"][...] = stop - start
    return dataset


# %% [markdown]
# #### rechunk

# %%
def process_rechunk(dataset):
    """Fetch a granule from storage, rechunk, and push back.

    Downloads a granule onto the local filesystem, executes rechunking
    with `nccopy`, and pushes the result to storage.

    Parameters
    ----------
    dataset :
        see `process_get`
    """
    start = datetime.now()
    array = dataset["time"]
    # HERE
    if array["rechunk"] and not array["repack"] and not array["kerchunk"]:
        chunks = dataset["chunks"].item()
        with NamedTemporaryFile(suffix=".nc") as src:
            with NamedTemporaryFile(suffix=".nc") as dst:
                storage.get(storage_path(array, rechunk=0), src.name)
                subprocess.run(["nccopy", "-w", "-c", chunks, src.name, dst.name])
                storage.put(dst.name, storage_path(array))
    stop = datetime.now()
    dataset["time"] = xr.DataArray(stop - start, coords=array.coords)
    return dataset


# %% [markdown]
# #### repack

# %%
def process_repack(dataset):
    """Fetch a granule from storage, repack, and push back.

    Downloads a granule onto the local filesystem, executes repacking
    with `h5repack`, and pushes the result to storage.

    Parameters
    ----------
    dataset :
        see `process_get`
    """
    start = datetime.now()
    array = dataset["time"]
    if array["repack"] and not array["kerchunk"]:
        page_size = dataset["page_size"].item()
        with NamedTemporaryFile(suffix=".nc") as src:
            with NamedTemporaryFile(suffix=".nc") as dst:
                storage.get(storage_path(array, repack=0), src.name)
                subprocess.run(
                    ["h5repack", "-S", "PAGE", "-G", page_size, src.name, dst.name],
                )
                storage.put(dst.name, storage_path(array))
                stop = datetime.now()
                dataset["time"] = xr.DataArray(stop - start, coords=array.coords)
    return dataset


# %% [markdown]
# #### kerchunk

# %%
def process_single_kerchunk(dataset):
    """Fetch a granule from storage, repack, and push back.

    Executes kerchunking on a single granule in storage and writes the result to storage.

    Parameters
    ----------
    dataset :
        see `process_get`
    """
    start = datetime.now()
    array = dataset["time"]
    if array["kerchunk"]:
        path = storage_path(array, kerchunk=0)
        with storage.open(path) as src:
            reference = SingleHdf5ToZarr(src, path)
            reference = reference.translate()
            with storage.open(storage_path(array), "wb") as dst:
                dst.write(json.encode(reference))
        stop = datetime.now()
        dataset["time"] = xr.DataArray(stop - start, coords=array.coords)
    return dataset


# %%
def process_multi_kerchunk(dataset, name):
    """Fetch a granule from storage, repack, and push back.

    Executes kerchunking on a single granule in storage and writes the result to storage.

    Parameters
    ----------
    dataset :
        see `process_get`
    """
    start = datetime.now()
    stop = start
    array = dataset["time"]
    if array["kerchunk"]:
        dicts = []
        for item in array:
            path = storage_path(item)
            reference = storage.cat(path)
            dicts.append(json.decode(reference))
        reference = MultiZarrToZarr(
            dicts,
            remote_protocol=args.remote,
            concat_dims="time",
        )
        reference = reference.translate()
        path = str(Path(path).parent / name)
        with storage.open(path, "wb") as dst:
            dst.write(json.encode(reference))
        stop = datetime.now()
    return xr.DataArray(stop - start, coords=dataset.drop_dims("files").coords)


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
results = earthaccess.search_data(**product, count=args.count)
ds = xr.Dataset(
    {
        "time": xr.DataArray(
            float('nan'),
            coords=[
                ("files", results),
                ("rechunk", [0, 1]),
                ("repack", [0, 1]),
                ("kerchunk", [0, 1]),
            ],
        ),
        "chunks": ((), "time/1,lat/350,lon/700"),
        "page_size": ("rechunk", ["6291456", "524288"]),
    }
)
ds

# %% [markdown]
# ### Execute Reprocessing

# %%
levels = {
    "rechunk": [0],
    "repack": [0],
    "kerchunk": [0],
}
out = ds.sel(levels).chunk(1)
with ProgressBar():
    out = out.map_blocks(process_get, template=out).compute()
ds = xr.merge((ds, out), join="outer", compat="no_conflicts")

# %%

# %%

# %%
ds = ds.chunk(1)
ds = ds.map_blocks(process_rechunk, template=ds).compute()

# %%
ds = ds.chunk(1)
ds = ds.map_blocks(process_repack, template=ds).compute()

# %%
ds = ds.chunk(1)
ds = ds.map_blocks(process_single_kerchunk, template=ds).compute()

# %%
time = ds["time"].sum("files")

# %%
ds = ds.chunk(1).chunk({"files": ds.sizes["files"]})
time = (
    time
    + ds.map_blocks(
        process_multi_kerchunk,
        (product["short_name"],),
        template=time.chunk(1),
    ).compute()
)

# %% [markdown]
# ### Results

# %%
time.to_netcdf(f'data/{product["short_name"]}-reprocess-{datetime.now()}.nc')

# %%
time.astype("timedelta64[s]").astype(int).to_dataframe()

# %% [markdown]
# ## NLDAS

# %% [markdown]
# ### File List

# %%
product = {
    "short_name": "NLDAS_NOAH0125_H",
    "version": "2.0",
    "temporal": ("2023-02-01", "2023-02-28"),
}
results = earthaccess.search_data(**product, count=args.count)
ds = xr.Dataset(
    {
        "time": xr.DataArray(
            float('nan'),
            coords=[
                ("files", results),
                ("rechunk", [0]),
                ("repack", [0, 1]),
                ("kerchunk", [0, 1]),
            ],
        ),
        "page_size": ("rechunk", ["419430"]),
    }
)
ds

# %% [markdown]
# ### Execute Reprocessing

# %%
ds = ds.chunk(1)
ds = ds.map_blocks(process_get, template=ds).compute()

# %%
ds = ds.chunk(1)
ds = ds.map_blocks(process_repack, template=ds).compute()

# %%
ds = ds.chunk(1)
ds = ds.map_blocks(process_single_kerchunk, template=ds).compute()

# %%
time = ds["time"].sum("files")

# %%
ds = ds.chunk(1).chunk({"files": ds.sizes["files"]})
time = (
    time
    + ds.map_blocks(
        process_multi_kerchunk,
        (product["short_name"],),
        template=time.chunk(1),
    ).compute()
)

# %% [markdown]
# ### Results

# %%
time.to_netcdf(f'data/{product["short_name"]}-reprocess-{datetime.now()}.nc')

# %%
time.astype("timedelta64[s]").astype(int).to_dataframe()
