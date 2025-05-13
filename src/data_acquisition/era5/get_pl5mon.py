import cdsapi
import os

odir = 'nc'
site = 'pl'
syr, eyr = 2010, 2020
latn, lats, lonw, lone = 80, 70, -80, -60

os.makedirs(odir, exist_ok=True)
for iyr in range(syr,eyr+1,1):
    yr=str(iyr)
    print(yr)
    dataset = "reanalysis-era5-pressure-levels-monthly-means"
    request = {
        "product_type": ["monthly_averaged_reanalysis"],
        "variable": [
            "geopotential",
            "temperature"
        ],
        "pressure_level": [
            "200", "225", "250",
            "300", "350", "400",
            "450", "500", "550",
            "600", "650", "700",
            "750", "775", "800",
            "825", "850", "875",
            "900", "925", "950",
            "975", "1000"
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
