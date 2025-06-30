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
# # Reprocess FLDAS and NLDAS for Cloud Optimization

# %% [markdown]
# ## Setup

# %%
import argparse
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import dask
import earthaccess
import fsspec
import xarray as xr
from dask.distributed import Client
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr
from msgspec import json

# %% [markdown]
# The default values in the `parser` are set for "debugging" while working in the notebook.
# They will be supplied when running the scripts, see `CONTRIBUTING.md`.

# %%
parser = argparse.ArgumentParser()
parser.add_argument(
    "--remote",
    default="local",
    help="the type of filesystem used to store the original and reprocessed files",
)
parser.add_argument(
    "--prefix",
    default="data",
    help="the [bucket and] prefix to prepend to 'cloud_ldas' for remote storage",
)
parser.add_argument(
    "--tempdir",
    action="store_true",
    help="whether to use a transient temporary directory for downloaded originals",
)
parser.add_argument(
    "--count",
    default=2,
    help="the number of granules to process",
)
args, _ = parser.parse_known_args()

# %%
storage = fsspec.filesystem(args.remote)
prefix = Path(args.prefix, "cloud_ldas")
tempdir = TemporaryDirectory().name if args.tempdir else Path("data", "granules")
exp_dims = ("rechunk", "repack", "kerchunk")


# %%
def storage_path(array, **kwargs):
    """Build a path, starting from `prefix`, for outputs.

    The array, a single item, has coordinates that are used to construct
    the output path for the processed granule. The resulting path looks
    like "prefix/rechunk/[0|1]/repack/[0|1]/kerchunk/[0|1]/concept-id"

    Parameters
    ----------
    array :
        the single item chunk of the array used to distribute processing
    kwargs :
        override the array's coordinate value for the given keyword
    """
    path = prefix
    granule = array["granules"].item()
    for item in exp_dims:
        index = kwargs.get(item)
        if index is None:
            index = array[item].item()
        path = path / item / str(index)
    path.mkdir(exist_ok=True, parents=True)
    return str(path / granule["meta"]["concept-id"])


# %%
def process_get(dataset):
    """Copy granule from Earthdata Cloud to storage.

    Download URLs are taken from the "granules" coordinate in the input array,
    and the time it takes to download and push each file is returned in the
    corresponding value of the output array having the same coordinates.

    Parameters
    ----------
    dataset :
        the single item chunk of the dataset used to distribute processing
    """
    array = dataset["time"]
    if not array["rechunk"] and not array["repack"] and not array["kerchunk"]:
        start = datetime.now()
        paths = earthaccess.download([array["granules"].item()], tempdir)
        storage.put(paths[0], storage_path(array))
        stop = datetime.now()
        dataset["time"] = xr.DataArray(stop - start, coords=array.coords)
    return dataset


# %% [markdown]
# ### rechunk

# %%
def process_rechunk(dataset):
    """Fetch a granule from storage, rechunk, and push back.

    Downloads a granule onto the local file system, executes rechunking
    with `nccopy`, and pushes the result to storage.

    Parameters
    ----------
    dataset :
        see `process_get`
    """
    start = datetime.now()
    array = dataset["time"]
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
# ### repack

# %%
def process_repack(dataset):
    """Fetch a granule from storage, repack, and push back.

    Downloads a granule onto the local file system, executes repacking
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
# ### kerchunk

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
    return xr.DataArray(stop - start, coords=dataset.drop_dims("granules").coords)


# %% [markdown]
# ## Dask Workers

# %%
# import coiled

# cluster = coiled.Cluster(n_workers=15, region='us-west-2', worker_cpu=2, name='lsterzinger-cloud-optimized-data-tests') #, software='lsterzinger-env')
# client = cluster.get_client()

# add process bars

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
granules = earthaccess.search_data(**product, count=args.count, cloud_hosted=True)
ds = xr.Dataset(
    {
        "time": xr.DataArray(
            None,
            coords=[
                ("granules", granules),
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
ds = ds.chunk(1)
ds = ds.map_blocks(process_get, template=ds).compute()

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
time = ds["time"].sum("granules")

# %%
ds = ds.chunk(1).chunk({"granules": ds.sizes["granules"]})
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
granules = earthaccess.search_data(**product, count=args.count, cloud_hosted=True)
ds = xr.Dataset(
    {
        "time": xr.DataArray(
            None,
            coords=[
                ("granules", granules),
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
time = ds["time"].sum("granules")

# %%
ds = ds.chunk(1).chunk({"granules": ds.sizes["granules"]})
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
