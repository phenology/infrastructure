
# coding: utf-8

# # Plot GeoTiffs
# 
# 
# In this NoteBook the reader finds code to read a GeoTiff file, single- or multi-band, from HDFS. It reads the GeoTiff as a **ByteArray** and then stores the GeoTiff in memory using **MemFile** from **RasterIO** python package. The same package is then used to plot a GeoTiff's band, or multiple bands using sub-plots, histograms, etc. The idea is also to show that **%matplotlib inline** and **%matplotlib notebook** is also possible.
# 
# With this example the user can load GeoTiffs from HDFS and then explore all the features of Python packages such as [rasterio](https://github.com/mapbox/rasterio).

# ## Dependencies

# In[1]:


#Add all dependencies to PYTHON_PATH
import sys
sys.path.append("/usr/lib/spark/python")
sys.path.append("/usr/lib/spark/python/lib/py4j-0.10.4-src.zip")
sys.path.append("/usr/lib/python3/dist-packages")

#Define environment variables
import os
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "ipython"

#Load PySpark to connect to a Spark cluster
from pyspark import SparkConf, SparkContext

#from osgeo import gdal
#To read GeoTiffs as a ByteArray
from io import BytesIO
from rasterio.io import MemoryFile


# ## Connect to Spark

# In[2]:


appName = "plot_GeoTiff"
masterURL="spark://pheno0.phenovari-utwente.surf-hosted.nl:7077"

#A context needs to be created if it does not already exist
try:
    sc.stop()
except NameError:
    print("A  new Spark Context will be created.")
    
sc = SparkContext(conf = SparkConf().setAppName(appName).setMaster(masterURL))


# ## Read a GeoTiff file

# ## Visualization
# 
# It is inspired in the **rasterio** [data visualization example](https://github.com/mapbox/rasterio/blob/master/examples/Data%20visualization.ipynb).

# ### Static plot

# In[13]:


file_path = "hdfs:///user/pheno/correlation/LeafFinal_BloomFinal.tif"
data = sc.binaryFiles(file_path).take(1)

dataByteArray = bytearray(data[0][1])



# In[14]:


import matplotlib.pyplot as plt
import rasterio
from rasterio import plot

get_ipython().magic('matplotlib inline')
with MemoryFile(dataByteArray) as memfile:
    with memfile.open() as dataset:
        plt = plot.get_plt()
        plt.figure(figsize=(20,20))
        plot.show((dataset,1))


# In[11]:


file_path = "hdfs:///user/pheno/correlation/SOST_BloomFinal.tif"
data = sc.binaryFiles(file_path).take(1)

dataByteArray = bytearray(data[0][1])

#If it is needed to convert to a numpy array
#import numpy as np
#file_bytes = np.asarray(dataByteArray, dtype=np.uint8)


# In[12]:


import matplotlib.pyplot as plt
import rasterio
from rasterio import plot

get_ipython().magic('matplotlib inline')
with MemoryFile(dataByteArray) as memfile:
    with memfile.open() as dataset:
        plt = plot.get_plt()
        plt.figure(figsize=(20,20))
        plot.show((dataset,1))


# In[8]:


import matplotlib.pyplot as plt
import rasterio
from rasterio import plot

get_ipython().magic('matplotlib inline')
with MemoryFile(dataByteArray) as memfile:
    with memfile.open() as dataset:
        plt = plot.get_plt()
        plt.figure(figsize=(20,20))
        plot.show((dataset,1))


# In[9]:


file_path = "hdfs:///user/pheno/correlation/SOST_LeafFinal.tif"
data = sc.binaryFiles(file_path).take(1)

dataByteArray = bytearray(data[0][1])

#If it is needed to convert to a numpy array
#import numpy as np
#file_bytes = np.asarray(dataByteArray, dtype=np.uint8)


# In[10]:


import matplotlib.pyplot as plt
import rasterio
from rasterio import plot

get_ipython().magic('matplotlib inline')
with MemoryFile(dataByteArray) as memfile:
    with memfile.open() as dataset:
        plt = plot.get_plt()
        plt.figure(figsize=(20,20))
        plot.show((dataset,1))


# In[15]:


file_path = "hdfs:///user/pheno/correlation/neg_SOST_BloomFinal.tif"
data = sc.binaryFiles(file_path).take(1)

dataByteArray = bytearray(data[0][1])


# In[16]:


import matplotlib.pyplot as plt
import rasterio
from rasterio import plot

get_ipython().magic('matplotlib inline')
with MemoryFile(dataByteArray) as memfile:
    with memfile.open() as dataset:
        plt = plot.get_plt()
        plt.figure(figsize=(20,20))
        plot.show((dataset,1))


# In[7]:


file_path = "hdfs:///user/pheno/correlation/neg_SOST_LeafFinal.tif"
data = sc.binaryFiles(file_path).take(1)

dataByteArray = bytearray(data[0][1])


# In[8]:


import matplotlib.pyplot as plt
import rasterio
from rasterio import plot

get_ipython().magic('matplotlib inline')
with MemoryFile(dataByteArray) as memfile:
    with memfile.open() as dataset:
        plt = plot.get_plt()
        plt.figure(figsize=(20,20))
        plot.show((dataset,1))


# ### Interactive visualization
# 
# It should be run on a Chrome browser, otherwise, it might have some rendering issues.

# In[ ]:


#Define variables so they can be re-used.
memfile = MemoryFile(dataByteArray)
dataset = memfile.open()

get_ipython().magic('matplotlib notebook')
fig, (axr, axg, axb) = plt.subplots(1,3, figsize=(12, 4), sharex=True, sharey=True)
plot.show((dataset, 1), title='band 1', ax=axr)
#plot.show((dataset, 2), title='band 2', ax=axg)
#plot.show((dataset, 3), title='band 3', ax=axb)


# ### Histogram

# In[ ]:


get_ipython().magic('matplotlib inline')
plot.show_hist(dataset)#, bins=250)


# In[ ]:




