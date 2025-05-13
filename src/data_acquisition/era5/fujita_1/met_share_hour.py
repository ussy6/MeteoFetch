import pandas as pd
import numpy as np
import netCDF4
import math
import sys
import csv

odir = 'nc'
site = 'arctic'
nh = 24
nd = 366
nm = 12

df = pd.read_csv('site.ini') # site info. read
ns = len(df['site']) # how many sites?
slon = df['lon'] # site lon
slat = df['lat'] # site lat
selv = df['elv'] # site elv
fname = np.array([None] * ns, dtype=object) # set hourly output file
dname = np.array([None] * ns, dtype=object) # set daily output file
header = ['year','month','day','hour','doy','at','pr','rh','ws','sp','srd','lrd','t2m'] # set header for met output
for ist in range(0,ns,1): # loop for site
	fname[ist] = 'era5hour_' + str(df['site'][ist]) + '.csv' # set hourly output file & header
	dname[ist] = 'era5daily_' + str(df['site'][ist]) + '.csv' # set daily output file & header
	with open(fname[ist], 'w', newline='') as f:
		writer = csv.writer(f); writer.writerow(header)
	with open(dname[ist], 'w', newline='') as f:
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
header = ['site_name','lon_site','lon_era5','lat_site','lat_era5','z_site','z_era5']
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

syr = 2011
eyr = 2012
lap = -0.0065 # tentative temperature lapse rate
for iyr in range(syr,eyr+1,1):
	dye = 0
	for im in range(1,nm+1,1):
		mn=format(im,'02')
		nc = netCDF4.Dataset(odir + '/' + site + str(iyr) + str(mn) + 'hour.nc')
		# regional data lon:-180->180
		nt = len(nc['valid_time']) # t length month-day x hour
		mdy = int(nt / nh) # days of a month
		nx = len(nc['longitude']) # x length
		ny = len(nc['latitude']) # y length
		dlon = nc['longitude'] # set lon array
		dlat = nc['latitude'] # set lon array
		dgx = abs(dlon[0]-dlon[1])
		dgy = abs(dlat[0]-dlat[1])
		lx = float(dlon[0]) - 0.5 * dgx # starting lon
		uy = float(dlat[0]) + 0.5 * dgy # starting lat

		for ist in range(0,ns,1):
			if iyr >= df['syr'][ist] and iyr <= df['eyr'][ist]:
				ix = int((slon[ist] - lx) / dgx)
				iy = int((uy - slat[ist]) / dgy)
				if ix < 0 or ix >= nx: sys.exit('out of domain for longitude')
				if iy < 0 or iy >= ny: sys.exit('out of domain for latitude')
				print(fname[ist],iyr,mn,slon[ist],dlon[ix],slat[ist],dlat[iy])
				dws = np.zeros((mdy)); dat = np.zeros((mdy)); dt = np.zeros((mdy)) # prepare daily arrays
				drh = np.zeros((mdy)); dsp = np.zeros((mdy)); dpr = np.zeros((mdy)) # prepare daily arrays
				dsr = np.zeros((mdy)); dlr = np.zeros((mdy)) # prepare daily arrays
				with open(fname[ist], 'a', newline='') as f: # hourly data
					it = -1
					for md in range(0,mdy,1):
						for ih in range(0,nh,1):
							it = it + 1
							u = nc['u10'][it,iy,ix]
							v = nc['v10'][it,iy,ix]
							ws = math.sqrt(u**2 + v**2); dws[md] = dws[md] + ws / nh # wind speed
							t = nc['t2m'][it,iy,ix] - 273.15; dt[md] = dt[md] + t / nh # from K to degC
							d = nc['d2m'][it,iy,ix] - 273.15 # from K to degC
							at = t + lap * (selv[ist] - delv[ist]); dat[md] = dat[md] + at / nh # calibrating elevation bias
							# calculate rh #
							if t >= 0: a = 7.5;b = 237.3
							else: a = 9.5;b = 265.5
							eat = 6.11 * 10**((a * t) / (b + t))
							if d >= 0: a = 7.5;b = 237.3
							else: a = 9.5;b = 265.5
							edt = 6.11 * 10**((a * d) / (b + d))
							rh = edt / eat * 100; drh[md] = drh[md] + rh / nh
							sp = nc['sp'][it,iy,ix] / 100; dsp[md] = dsp[md] + sp / nh # from Pa to hPa
							pr = nc['tp'][it,iy,ix] * 1000; dpr[md] = dpr[md] + pr # from m to mm, and monthly sum
							srd = nc['ssrd'][it,iy,ix] / 3600; dsr[md] = dsr[md] + srd / nh # from J m^-2 to W m^-2
							lrd = nc['strd'][it,iy,ix] / 3600; dlr[md] = dlr[md] + lrd / nh # from J m^-2 to W m^-2
							var = np.array((iyr,im,md+1,ih+1,dye+md+1,at,pr,rh,ws,sp,srd,lrd,t))
							writer = csv.writer(f); writer.writerow(var)
				with open(dname[ist], 'a', newline='') as f: # daily means
					for md in range(0,mdy,1):
						var = np.array((iyr,im,md+1,0,dye+md+1,dat[md],dpr[md],drh[md],dws[md],dsp[md],dsr[md],dlr[md],dt[md]))
						writer = csv.writer(f); writer.writerow(var)
		dye = dye + mdy # doy at end of a month
