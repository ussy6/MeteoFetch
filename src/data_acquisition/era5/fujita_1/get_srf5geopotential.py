import cdsapi
dataset = "reanalysis-era5-single-levels-monthly-means"
request = {
    "product_type": ["monthly_averaged_reanalysis"],
    "variable": ["geopotential"],
    "year": ["2020"],
    "month": ["01"],
    "time": ["00:00"],
    "data_format": "netcdf",
    "download_format": "unarchived"
}
target = 'surface_geopotential.nc'
client = cdsapi.Client(delete = True)
client.retrieve(dataset, request, target)
