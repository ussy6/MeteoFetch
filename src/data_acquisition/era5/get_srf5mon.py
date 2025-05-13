import cdsapi
import os

odir = 'nc'
site = 'srf'
syr, eyr = 2010, 2020
latn, lats, lonw, lone = 80, 70, -80, -60

os.makedirs(odir, exist_ok=True)
for iyr in range(syr,eyr+1,1):
    yr=str(iyr)
    print(yr)
    dataset = "reanalysis-era5-single-levels-monthly-means"
    request = {
        "product_type": ["monthly_averaged_reanalysis"],
        "variable": [
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "2m_dewpoint_temperature",
            "2m_temperature",
            "surface_pressure",
            "total_precipitation",
            "surface_solar_radiation_downwards",
            "surface_thermal_radiation_downwards"
        ],
        "year": yr,
        "month": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12"
        ],
        "time": ["00:00"],
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": [latn, lonw, lats, lone]
    }
    target = f'{odir}/{site}{yr}mon.nc'
    client = cdsapi.Client(delete = True)
    client.retrieve(dataset, request, target)
