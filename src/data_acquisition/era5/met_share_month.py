import pandas as pd
import numpy as np
import netCDF4
import math
import sys
import csv

odir = 'nc'
site = 'arctic'

df = pd.read_csv('site.ini') # site info. read
ns = len(df['site']) # how many sites?
slon = df['lon'] # site lon
slat = df['lat'] # site lat
selv = df['elv'] # site elv
fname = np.array([None] * ns, dtype=object) # set monthly trd array
header = ['year','month','at','pr','rh','ws','sp','srd','lrd','t2m','d2m','u','v'] # set header for met output
for ist in range(0,ns,1): # loop for site
	fname[ist] = 'era5mon_' + str(df['site'][ist]) + '.csv' # set output file & header
	with open(fname[ist], 'w', newline='') as f:
		writer = csv.writer(f); writer.writerow(header)

nc = netCDF4.Dataset('surface_geopotential.nc') # geopotential data
# global data lon:0->360
dlon = nc['longitude'] # era5 lot
dlat = nc['latitude'] # era5 lat
delv = np.zeros((ns)) # era5 elv array
nx = len(dlon) # x length
ny = len(dlat) # y length
dgx = abs(dlon[0]-dlon[1]) # x interval
dgy = abs(dlat[0]-dlat[1]) # y interval
lx = float(dlon[0]) - 0.5 * dgx # starting lon west -> east
uy = float(dlat[0]) + 0.5 * dgy # starting lat north -> south
header = ['site_name','lon_site','lon_era5','lat_site','lat_era5','z_site','z_era5'] # header for the site file
with open('era5site_summary.csv', 'w', newline='') as f:
	writer = csv.writer(f); writer.writerow(header)
	for ist in range(0,ns,1):
		xx = slon[ist]
		if slon[ist] < 0: xx = slon[ist] + 360 # replace west lon by east lon
		ix = int((xx - lx) / dgx)
		if ix >= nx: ix = ix - nx
		iy = int((uy - slat[ist]) / dgy) # north -> southh
		delv[ist] = nc['z'][iy,ix] / 9.80665 # geopotential to height
		print(df['site'][ist],slon[ist],dlon[ix],slat[ist],dlat[iy],selv[ist],delv[ist])
		var = np.array((df['site'][ist],slon[ist],dlon[ix],slat[ist],dlat[iy],selv[ist],delv[ist]))
		writer.writerow(var)

syr = 2010 # starting year for existing data
eyr = 2020 # end year for existing data
lap = -0.0065 # tentative temperature lapse rate
for iyr in range(syr,eyr+1,1):
	nc = netCDF4.Dataset(odir + '/' + site + str(iyr) + 'mon.nc')
	# regional data lon:-180->180
	nt = len(nc['date']) # t length
	nx = len(nc['longitude']) # x length
	ny = len(nc['latitude']) # y length
	#date = nc['date'] # set lon array
	dlon = nc['longitude'] # set lon array
	dlat = nc['latitude'] # set lon array
	dgx = abs(dlon[0]-dlon[1]) # x interval
	dgy = abs(dlat[0]-dlat[1]) # y interval
	lx = float(dlon[0]) - 0.5 * dgx # starting lon
	uy = float(dlat[0]) + 0.5 * dgy # starting lat

	for ist in range(0,ns,1):
		if iyr >= df['syr'][ist] and iyr <= df['eyr'][ist]: # calculate if iyr within the duration in the site file
			ix = int((slon[ist] - lx) / dgx)
			iy = int((uy - slat[ist]) / dgy)
			if ix < 0 or ix >= nx: sys.exit('out of domain for longitude') # stop if the target point does not exist in the era5 file
			if iy < 0 or iy >= ny: sys.exit('out of domain for latitude') # stop if the target point does not exist in the era5 file
			print(fname[ist],slon[ist],dlon[ix],slat[ist],dlat[iy])
			with open(fname[ist], 'a', newline='') as f:
				for it in range(0,nt,1):
					u = nc['u10'][it,iy,ix]
					v = nc['v10'][it,iy,ix]
					ws = math.sqrt(u**2 + v**2) # wind speed
					d = nc['d2m'][it,iy,ix] - 273.15 # from K to degC
					t = nc['t2m'][it,iy,ix] - 273.15 # from K to degC
					at = t + lap * (selv[ist] - delv[ist]) # calibrating elevation bias
					# calculate rh #
					if t >= 0: a = 7.5;b = 237.3
					else: a = 9.5;b = 265.5
					eat = 6.11 * 10**((a * t) / (b + t))
					if d >= 0: a = 7.5;b = 237.3
					else: a = 9.5;b = 265.5
					edt = 6.11 * 10**((a * d) / (b + d))
					rh = edt / eat * 100
					sp = nc['sp'][it,iy,ix] / 100 # from Pa to hPa
					im = it + 1 # month
					mdy = 31 # days of month
					if im == 4 or im == 6 or im == 9 or im ==11: mdy = 30 # Apr, Jun, Sep, Nov
					if im == 2: mdy = 28 # Feb (no Olympic year consideration)
					pr = nc['tp'][it,iy,ix] * 1000 * mdy # from m to mm, and monthly sum
					srd = nc['ssrd'][it,iy,ix] / 86400 # from J m^-2 to W m^-2
					lrd = nc['strd'][it,iy,ix] / 86400 # from J m^-2 to W m^-2
					var = np.array((iyr,im,at,pr,rh,ws,sp,srd,lrd,t,d,u,v))
					writer = csv.writer(f); writer.writerow(var)
