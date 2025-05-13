import pandas as pd
import numpy as np
import netCDF4
import math
import sys
import csv

odir = 'nc'
site = 'pl'

df = pd.read_csv('site.ini') # site info. read
ns = len(df['site']) # how many sites?
slon = df['lon'] # site lon
slat = df['lat'] # site lat
selv = df['elv'] # site elv
fname = np.array([None] * ns, dtype=object) # set monthly trd array
# set header for met output
header = ['year','month','pt','lap','tl','zl','pl','tu','zu','pu']
for ist in range(0,ns,1): # loop for site
	fname[ist] = 'pl5mon_' + str(df['site'][ist]) + '.csv' # set output file & header
	with open(fname[ist], 'w', newline='') as f:
		writer = csv.writer(f); writer.writerow(header)

syr = 2010
eyr = 2020
for iyr in range(syr,eyr+1,1):
	nc = netCDF4.Dataset(odir + '/' + site + str(iyr) + 'mon.nc')
	# regional data lon:-180->180
	nt = len(nc['date']) # t length
	nx = len(nc['longitude']) # x length
	ny = len(nc['latitude']) # y length
	nz = len(nc['pressure_level']) # z length
	dlon = nc['longitude'] # set lon array
	dlat = nc['latitude'] # set lon array
	dpl = nc['pressure_level']
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
			print(fname[ist],iyr,slon[ist],dlon[ix],slat[ist],dlat[iy],selv[ist])
			with open(fname[ist], 'a', newline='') as f:
				iz = 1
				for it in range(0,nt,1):
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
					tu = nc['t'][it,izu,iy,ix] - 273.15
					zu = nc['z'][it,izu,iy,ix] / 9.80665
					pu = nc['pressure_level'][izu]
					tl = nc['t'][it,izl,iy,ix] - 273.15
					zl = nc['z'][it,izl,iy,ix] / 9.80665
					pl = nc['pressure_level'][izl]
					lap = (tu - tl) / (zu - zl)
					pt = lap * (selv[ist] - zl) + tl
					var = np.array((iyr,it+1,pt,lap,tl,zl,pl,tu,zu,pu))
					writer = csv.writer(f); writer.writerow(var)
