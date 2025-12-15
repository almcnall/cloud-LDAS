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
# # Performance for Spatial Means

# %% [markdown]
# ## Setup

# %%
import sys
from datetime import datetime

# %%
import earthaccess
import xarray as xr

# %%
sys.path.append("../scripts")

# %%
from core import args, prefix, storage, storage_path


# %% [markdown]
# ## Functions

# %%
def time_mfdataset_steps(array):
    
    pattern = str(storage_path(array, file="G*"))
    paths = storage.glob(pattern)
    paths = [storage.open(i) for i in paths]
    
    start = datetime.now()
    ds = xr.open_mfdataset(paths, engine="h5netcdf")
    stop = datetime.now()
    
    yield stop - start, ds
    
    for item in paths:
        item.close()

    yield


# %%
def time_reference(array):

    path = storage_path(array, file=array["product"].item() + ".json")
    start = datetime.now()
    ds = xr.open_dataset(
        "reference://",
        chunks={},  
        engine="zarr",
        backend_kwargs={
            "consolidated": False,
            "storage_options": {
                "fo": str(path),
                "target_protocol": storage.protocol[0],
                "remote_protocol": storage.protocol[0],
                "remote_options": {"asynchronous": True},
            },
        },
    )
    stop = datetime.now()
    
    return stop - start, ds


# %%
def time_area_mean(dataset):

    west = 95
    south = 32
    east = 100
    north = 37
    
    start = datetime.now()
    ds = dataset.sel({
        "lat": slice(32, 37),
        "lon": slice(-100, -95),
    })
    ds = ds[["Rainf"]].mean(["lat", "lon"]).compute()
    stop = datetime.now()
    
    return stop - start, ds


# %%
def time_zonal_mean(dataset):

    start = datetime.now()
    ds
    stop = datetime.now()
    
    return stop - start, ds


# %% [markdown]
# ## Prepare DataArray

# %%
ds = xr.load_dataset("reprocess.nc")
array = (
    ds["time"]
    .rename({"_product": "product"})
    .groupby("product")
    .first()
    .stack({"case": [...]})
    .dropna("case")
)
array[...] = float('nan')
array

# %% [markdown]
# ## Run benchmarks

# %%
time_open = []
time_calc = []
for item in array:
    if item["product"] == "FLDAS_NOAHMP001_G_CA_D":
        # TODO: zonal mean for FLDAS
        continue

    # open all times as a single dataset
    if item["kerchunk"] == 0:
        time_mfdataset = time_mfdataset_steps(item)
        timedelta, ds = next(time_mfdataset)
    else:
        timedelta, ds = time_reference(item)
    item[...] = timedelta
    time_open.append(item.copy())

    # calculate a spatial average
    if item["product"] == "FLDAS_NOAHMP001_G_CA_D":
        timedelta, ds = time_zonal_mean(ds)
    else:
        timedelta, ds = time_area_mean(ds)
    item[...] = timedelta
    time_calc.append(item.copy())

    # close files that remain open
    if item["kerchunk"] == 0:
        next(time_mfdataset)


# %%
dataset = xr.Dataset({
    "time_open": xr.concat(time_open, dim="case", coords="all"),
    "time_calc": xr.concat(time_calc, dim="case", coords="all"),
})
dataset = dataset.set_index({"case": ["rechunk", "repack", "kerchunk", "product"]}).unstack()
dataset.to_netcdf("benchmark.nc")


# %% [markdown]
# ## Display Results

# %%
ds = xr.open_dataset("benchmark.nc")
ds.to_dataframe()

# %% [markdown]
# ## Scratch

# %%
