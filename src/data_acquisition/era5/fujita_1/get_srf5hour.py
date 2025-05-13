import cdsapi
import os

odir = 'nc'
site = 'harutori_srf_'
syr, eyr = 2010, 2020
latn, lats, lonw, lone = 43, 42.75, 144, 144.5

os.makedirs(odir, exist_ok=True)
for iy in range(syr,eyr+1,1):
    yr=str(iy)
    for im in range(1,13,1):
        mn = f'{im:02d}'
        print(yr,mn)
        dataset = "reanalysis-era5-single-levels"
        request = {
            "product_type": ["reanalysis"],
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
            "month": mn,
            "day": [
                "01", "02", "03",
                "04", "05", "06",
                "07", "08", "09",
                "10", "11", "12",
                "13", "14", "15",
                "16", "17", "18",
                "19", "20", "21",
                "22", "23", "24",
                "25", "26", "27",
                "28", "29", "30",
                "31"
            ],
            "time": [
                "00:00", "01:00", "02:00",
                "03:00", "04:00", "05:00",
                "06:00", "07:00", "08:00",
                "09:00", "10:00", "11:00",
                "12:00", "13:00", "14:00",
                "15:00", "16:00", "17:00",
                "18:00", "19:00", "20:00",
                "21:00", "22:00", "23:00"
            ],
            "data_format": "netcdf",
            "download_format": "unarchived",
            "area": [latn, lonw, lats, lone]
        }
        target = f'{odir}/{site}{yr}{mn}hour.nc'
        client = cdsapi.Client(delete = True)
        client.retrieve(dataset, request, target)
