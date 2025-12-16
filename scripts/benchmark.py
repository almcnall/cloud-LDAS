# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Time Data Access for Spatial Averaging

# %% [markdown]
# ## Setup

# %%
import os
try:
    del os.environ["PROJ_DATA"]
except KeyError:
    pass

# %%
from datetime import datetime

from dask.diagnostics import ProgressBar
import earthaccess
import geopandas as gpd
import rasterio.features
import rioxarray
import xarray as xr

from core import args, prefix, storage, storage_path

# %%
pbar = ProgressBar()
pbar.register()

# %%
url = "https://services5.arcgis.com/sjP4Ugu5s0dZWLjd/arcgis/rest/services/Afghanistan_water_basin_boundaries/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
geom = gpd.read_file(url)
geom

# %%
paths = storage.find(prefix / "FLDAS_NOAHMP001_G_CA_D")
with storage.open(paths[0]) as f:
    coords = xr.open_dataset(f).coords
list(coords)

# %%
epsg = 4087
basins = xr.DataArray(0, [coords["lat"], coords["lon"]], name="OBJECTID")
basins = basins.rio.write_crs(epsg)
basins[...] = rasterio.features.rasterize(
    geom[["geometry", "OBJECTID"]].itertuples(index=False),
    out_shape=basins.rio.shape,
    transform=basins.rio.transform(),
)
basins = basins.where(basins > 0)
#im = basins.plot.imshow()

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
    ds = (
        ds[["Rainf"]]
        .mean(["lat", "lon"])
        .compute(scheduler="threads", num_workers=2)
    )
    stop = datetime.now()
    
    return stop - start, ds


# %%
def time_zonal_mean(dataset):

    dataset = dataset.rio.write_crs(4087)
    start = datetime.now()
    ds = (
        dataset["SWE_inst"]
        .groupby(basins)
        .mean()
        .compute(scheduler="threads", num_workers=2)
    )
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
# ## Run benchmarking

# %%
time_open = []
time_calc = []
for item in array:
    
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
df = xr.open_dataset("benchmark.nc").to_dataframe()

# %%
df["time_open"].dropna().dt.total_seconds().reset_index()

# %%
df["time_calc"].dropna().dt.total_seconds().reset_index()
