import subprocess
import os
import sys

# Intput parameters
# 1. Runnable file path
# 2. Give name of input file -> Specify settings file
# 3. Give number of processors -> Specify 1 for running directly, 2 or high
def start_process_single():

    logfile = open('logfile_process', 'w')
    dir_path = os.path.dirname(os.path.realpath(__file__))+"/TSF_process.x64"
    args = ([dir_path, "west_afrika.set", "1"])
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # log
    for line in proc.stdout:
        sys.stdout.write(line.decode('utf-8'))
        logfile.write(line.decode('utf-8'))

    proc.wait()

#How to run .exe file under Linux
def gdal_restarise_shapefile():

    logfile = open('logfile_restarize', 'w')

    dir_path =  os.path.dirname(os.path.realpath (__file__))+"/apps/gdal_rasterize.exe"

    #Here just call wine first
    args = (["wine", dir_path, "-a","SHORT_ID1", "/home/vik/Documents/mproject/Spirits/TUTORIAL/DATA/SEN/NDVI/REF/regions.shp", "regions.img"])
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # log
    for line in proc.stdout:
        sys.stdout.write(line.decode('utf-8'))
        logfile.write(line.decode('utf-8'))

    proc.wait()


gdal_restarise_shapefile()
# start_process_single()