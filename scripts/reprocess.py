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
import requests
from dask.diagnostics import ProgressBar
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr
from msgspec import json

# %% [markdown]
# ### Command Line Arguments

# %% [markdown]
# Operational values can be supplied as needed when running the scripts (see "scripts/README.md").
#
# Default settings are intended for development and testing,
# and depend on the existence of the `SCRATCH_BUCKET` environment variable,
# which is defined on the 2i2c JupyterHub and probably not defined locally.

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


# %%
def storage_path(dataset, **kwargs):
    """Build a path, starting from `prefix`, for outputs.

    The array, which must have a size of one, will also have coordinates used to
    construct the output path for the processed granule. The resulting path looks
    like "prefix/<product>/<rechunk>/<repack>/<kerchunk>/<file>".

    Parameters
    ----------
    dataset : xarray.Dataset
        the size-one chunk of the dataset used to distribute processing
    kwargs
        override the array's coordinate value for the given keyword

    Returns
    -------
    str
        a absolute path to use for storage of outputs
    """
    path = prefix
    for item in ("product", "rechunk", "repack", "kerchunk", "file"):
        index = kwargs.get(item)
        if index is None:
            index = dataset[item].item()
        path = path / str(index)
    return path


# %% [markdown]
# ### Reprocessing Functions

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
    dataset : xarray.Dataset
        the size-one chunk of the dataset used to distribute processing

    Returns
    -------
    xarray.Dataset
        the processing time, with coordinates as for `dataset`
    """
    dst_path = storage_path(dataset)
    with TemporaryDirectory() as tmpdir:
        tmpdir = tmpdir if args.tmpdir else "tmp"
        start = datetime.now()
        paths = earthaccess.download(
            [dataset["results"].item()],
            tmpdir,
            show_progress=False,
        )
        stop = datetime.now()
        storage.put_file(paths[0], dst_path)
    dataset["time"][...] = stop - start
    return dataset


# %% [markdown]
# #### rechunk

# %%
def process_rechunk(dataset):
    """Fetch a granule from storage, rechunk, and push back.

    Copies an object from storage into a temporary file,
    executes rechunking with `nccopy`, and moves the result to storage.


    Parameters
    ----------
    dataset : xarray.Dataset
        see `process_get`

    Returns
    -------
    xarray.Dataset
        see `process_get`
    """
    dst_path = storage_path(dataset)
    src_path = storage_path(dataset, rechunk=0)
    if not storage.exists(src_path):
        return dataset
    chunk_size = dataset["chunk_size"].item()
    with NamedTemporaryFile(suffix=".nc") as src:
        with NamedTemporaryFile(suffix=".nc") as dst:
            storage.get_file(src_path, src.name)
            start = datetime.now()
            subprocess.run(["nccopy", "-w", "-c", chunk_size, src.name, dst.name])
            stop = datetime.now()
            storage.put_file(dst.name, dst_path)
    dataset["time"][...] = stop - start
    return dataset


# %% [markdown]
# #### repack

# %%
def process_repack(dataset):
    """Fetch a granule from storage, repack, and push back.

    Copies an object from storage into a temporary file,
    executes repacking with `h5repack`, and moves the result to storage.

    Parameters
    ----------
    dataset : xarray.Dataset
        see `process_get`

    Returns
    -------
    xarray.Dataset
        see `process_get`
    """
    dst_path = storage_path(dataset)
    src_path = storage_path(dataset, repack=0)
    if not storage.exists(src_path):
        return dataset
    page_size = dataset["page_size"].item()
    with NamedTemporaryFile(suffix=".nc") as src:
        with NamedTemporaryFile(suffix=".nc") as dst:
            storage.get_file(src_path, src.name)
            start = datetime.now()
            subprocess.run(
                ["h5repack", "-S", "PAGE", "-G", page_size, src.name, dst.name],
            )
            stop = datetime.now()
            storage.put_file(dst.name, dst_path)
    dataset["time"][...] = stop - start
    return dataset


# %% [markdown]
# #### kerchunk

# %%
def process_single_kerchunk(dataset):
    """Kerchunk a single file in storage and push sidecar to storage.

    Execute kerchunking on a single object in storage,
    and write the resulting sidecar file to storage.

    Parameters
    ----------
    dataset : xarray.Dataset
        see `process_get`

    Returns
    -------
    xarray.Dataset
        see `process_get`
    """
    dst_path = storage_path(dataset)
    src_path = storage_path(dataset, kerchunk=0)
    if not storage.exists(src_path):
        return dataset
    start = datetime.now()
    with storage.open(src_path) as src:
        reference = SingleHdf5ToZarr(src, str(src_path)).translate()
    with storage.open(dst_path, "wb") as dst:
        dst.write(json.encode(reference))
    stop = datetime.now()
    dataset["time"][...] = stop - start
    return dataset


# %%
def process_multi_kerchunk(dataset):
    """Combine sidecar files, and push merged sidecar to storage.

    Load the sidecar files for each file from storage, merge them,
    and write the resulting sidecar file into storage.

    Parameters
    ----------
    dataset : xarray.Dataset
        the chunk of the dataset used to distribute processing with the entire
        "file" dimension in one chunk

    Returns
    -------
    xarray.Dataset
        see `process_get`
    """
    dst_path = storage_path(dataset, file=dataset["product"].item())
    src_path = storage.glob(str(dst_path.parent / "G*"))
    if not src_path:
        return dataset
    start = datetime.now()
    reference = MultiZarrToZarr(
        [json.decode(storage.cat(i)) for i in src_path],
        remote_protocol=args.remote,
        concat_dims="time",
    )
    reference = reference.translate()
    with storage.open(dst_path, "wb") as dst:
        dst.write(json.encode(reference))
    stop = datetime.now()
    dataset["time"][...] = stop - start
    return dataset


# %% [markdown]
# ## Reprocessing

# %% [markdown]
# ### Data & Parameters

# %% [markdown]
# Define the datat to be reprocessed, and the needed reprocessing parameters, in a dictionary.

# %%
products = {
    "FLDAS_NOAHMP001_G_CA_D": {
        "query": {
            "version": "001",
            "temporal": ("2023-02-01", "2023-02-28"),
        },
        "chunk_size": ((), "time/1,lat/350,lon/700"),
        "page_size": ("rechunk", ["6291456", "524288"]),
    },
    "NLDAS_NOAH0125_H": {
        "query": {
            "version": "2.0",
            "temporal": ("2023-02-01", "2023-02-28"),
        },
        "page_size": ((), "419430"),
    },
}

# %% [markdown]
# Build an xarray.Dataset that embeds the experimental design in its dimensions (factors) and coordinates (levels).
# The `dataset` will hold timing results for the reprocessing steps.

# %%
earthaccess.login()
dataset = []
for key, value in products.items():
    results = earthaccess.search_data(
        count=int(args.count),
        short_name=key,
        **value["query"],
    )
    ds = xr.Dataset(
        {
            "results": ("file", results),
            "page_size": value["page_size"],
        },
        coords={
            "file": ("file", [i["meta"]["concept-id"] for i in results]),
            "product": ("file", [key] * len(results)),
        },
    )
    da = xr.DataArray(
        float('nan'),
        coords=[
            ("repack", [0, 1]),
            ("kerchunk", [0, 1]),
        ],
    )
    da = da.astype("timedelta64[ns]")
    if "chunk_size" in value:
        da = da.expand_dims({"rechunk": [0, 1]})
        ds["chunk_size"] = value["chunk_size"]
    ds["time"] = da
    dataset.append(ds)
dataset = xr.concat(dataset, dim="file", data_vars="all")
dataset

# %% [markdown]
# ### Execute

# %%
# TODO: workaround for https://github.com/nsidc/earthaccess/issues/1136
auth = earthaccess.login()
endpoint = "https://data.gesdisc.earthdata.nasa.gov/s3credentials"
earthaccess.__store__._s3_credentials[(None, None, endpoint)] = (
    datetime.now(),
    auth.get_s3_credentials(endpoint=endpoint)
)

# %% [markdown]
# In each cell, a selection of the dataset is created and chunked before submitting to Dask workers for a reprocessing step.

# %%
levels = {
    "rechunk": [0],
    "repack": [0],
    "kerchunk": [0],
}
ds = dataset.sel(levels).chunk(1)
print("process_get")
with ProgressBar():
    ds = ds.map_blocks(process_get, template=ds).compute()
dataset = xr.merge((dataset, ds), join="outer", compat="no_conflicts")

# %%
levels = {
    "rechunk": [1],
    "repack": [0],
    "kerchunk": [0],
}
ds = dataset.sel(levels)
ds = ds.where(~ds["chunk_size"].isnull(), drop=True).chunk(1)
print("process_rechunk")
with ProgressBar():
    ds = ds.map_blocks(process_rechunk, template=ds).compute()
dataset = xr.merge((dataset, ds), join="outer", compat="no_conflicts")

# %%
levels = {
    "repack": [1],
    "kerchunk": [0],
}
ds = dataset.sel(levels).chunk(1)
print("process_repack")
with ProgressBar():
    ds = ds.map_blocks(process_repack, template=ds).compute()
dataset = xr.merge((dataset, ds), join="outer", compat="no_conflicts")

# %%
levels = {
    "kerchunk": [1],
}
ds = dataset.sel(levels).chunk(1)
print("process_single_kerchunk")
with ProgressBar():
    ds = ds.map_blocks(process_single_kerchunk, template=ds).compute()
dataset = xr.merge((dataset, ds), join="outer", compat="no_conflicts")
ds = dataset.groupby("product").first().sel(levels).chunk(1)
print("process_multi_kerchunk")
with ProgressBar():
    ds = ds.map_blocks(process_multi_kerchunk, template=ds).compute()
dataset = dataset.rename({"product": "_product"})
dataset["time_multi_kerchunk"] = ds["time"]

# %% [markdown]
# ### View & Save Timing

# %% [markdown]
# Save the dataset with all timing information, but not the `earthdata.search_data` results, to a netCDF file.

# %%
dataset.drop_vars("results").to_netcdf("reprocess.nc")

# %% [markdown]
# View the timing results as tables.

# %%
ds = xr.load_dataset("reprocess.nc")

# %%
df = ds["time_multi_kerchunk"].to_dataframe()
df.dropna()

# %%
df = ds["time"].groupby("_product").mean().to_dataframe()
df.dropna()
