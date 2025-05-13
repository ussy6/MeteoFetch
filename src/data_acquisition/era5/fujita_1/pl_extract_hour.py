import pandas as pd
import numpy as np
import netCDF4
import math
import sys
import csv

odir = 'nc'
site = 'pl'
nh = 24
nm = 12

df = pd.read_csv('site.ini') # site info. read
ns = len(df['site']) # how many sites?
slon = df['lon'] # site lon
slat = df['lat'] # site lat
selv = df['elv'] # site elv
fname = np.array([None] * ns, dtype=object) # set hourly output file
dname = np.array([None] * ns, dtype=object) # set daily output file
# set header for met output
header = ['year','month','day','hour','doy','pt','lap','tl','zl','pl','tu','zu','pu']
for ist in range(0,ns,1): # loop for site
	fname[ist] = 'pl5hour_' + str(df['site'][ist]) + '.csv' # set output file & header
	dname[ist] = 'pl5daily_' + str(df['site'][ist]) + '.csv' # set output file & header
	with open(fname[ist], 'w', newline='') as f:
		writer = csv.writer(f); writer.writerow(header)
	with open(dname[ist], 'w', newline='') as f:
		writer = csv.writer(f); writer.writerow(header)

syr = 2010
eyr = 2012
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
		nz = len(nc['pressure_level']) # z length
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
				dpt = np.zeros((mdy)); dlap = np.zeros((mdy))
				dtl = np.zeros((mdy)); dzl = np.zeros((mdy)); dpl = np.zeros((mdy))
				dtu = np.zeros((mdy)); dzu = np.zeros((mdy)); dpu = np.zeros((mdy))
				with open(fname[ist], 'a', newline='') as f: # hourly data
					iz = 1
					it = -1
					for md in range(0,mdy,1):
						for ih in range(0,nh,1):
							it = it + 1
							izl = nz
							while izl == nz:
								if selv[ist] < nc['z'][it,iz,iy,ix] / 9.80665:
									izu = iz
									izl = izu - 1
									if izl > 0 and selv[ist] < nc['z'][it,izl,iy,ix] / 9.80665:
										print('level setting not enough low')
									iz = iz - 2
									if iz < 1: iz = 1
								else: iz = iz + 1
								if iz > nz and selv[ist] > nc['z'][it,iz,iy,ix] / 9.80665:
									print('warning level not enough')
									izu = nz
									izl = izu - 1
									iz = 1
							tu = nc['t'][it,izu,iy,ix] - 273.15; dtu[md] = dtu[md] + tu / nh
							zu = nc['z'][it,izu,iy,ix] / 9.80665; dzu[md] = dzu[md] + zu / nh
							pu = nc['pressure_level'][izu]; dpu[md] = dpu[md] + pu / nh
							tl = nc['t'][it,izl,iy,ix] - 273.15; dtl[md] = dtl[md] + tl / nh
							zl = nc['z'][it,izl,iy,ix] / 9.80665; dzl[md] = dzl[md] + zl / nh
							pl = nc['pressure_level'][izl]; dpl[md] = dpl[md] + pl / nh
							lap = (tu - tl) / (zu - zl); dlap[md] = dlap[md] + lap / nh
							pt = lap * (selv[ist] - zl) + tl; dpt[md] = dpt[md] + pt / nh
							var = np.array((iyr,im,md+1,ih+1,dye+md+1,pt,lap,tl,zl,pl,tu,zu,pu))
							writer = csv.writer(f); writer.writerow(var)
				with open(dname[ist], 'a', newline='') as f: # daily means
					for md in range(0,mdy,1):
						var = np.array((iyr,im,md+1,0,dye+md+1,dpt[md],dlap[md],dtl[md],dzl[md],dpl[md],dtu[md],dzu[md],dpu[md]))
						writer = csv.writer(f); writer.writerow(var)
		dye = dye + mdy # doy at end of a month
