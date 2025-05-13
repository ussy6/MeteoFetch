import os
import sys
from def_hour import *

site = 'Harutori'
syr, eyr = 2000, 2025
tm = 1 # restart month
YY, MM = 1999, 3 # for single month retrieval

os.makedirs(site, exist_ok=True)
iy = syr
for im in range(tm,13,1):
    sfc(site,iy,im)

for iy in range(syr+1,eyr+1,1):
    for im in range(1,13,1):
        sfc(site,iy,im)


sys.exit() # stop the script

# to get data for one month
iy, im = YY, MM
sfc(site,iy,im)
