import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

#SETUP
cd = os.path.join(os.path.expanduser("~"),r'Documents',r'projects',r'temps')
cd_dotdot = os.path.join(os.path.expanduser("~"),r'Documents',r'projects')

#LOAD WEATHER
extreme_df_path = os.path.join(cd,'extremes.csv')
extremes = pd.read_csv(extreme_df_path)
precip_path = os.path.join(cd,'precip.csv')
precip = pd.read_csv(precip_path)
tmax_path = os.path.join(cd,'tmax.csv')
tmax = pd.read_csv(tmax_path)
tmin_path = os.path.join(cd,'tmin.csv')
tmin = pd.read_csv(tmin_path)
tavg_path = os.path.join(cd,'tavg.csv')
tavg = pd.read_csv(tavg_path)

#LOAD MAPS
county_shapefile_path = os.path.join(cd_dotdot,r'cfs_cz_shapefile_and_distances',r'fips',r'fips.shp')
county_map = gpd.read_file(county_shapefile_path)

#SIMPLE MAPS
#January Average
jan_avg_data = tavg[['fips','jan']].copy()
jan_avg_data['fips'] = jan_avg_data['fips'].astype(str)
jan_avg_data['fips'] = jan_avg_data['fips'].str.zfill(5)
jan_avg_df = pd.merge(county_map,jan_avg_data,on='fips',how='inner')
fig, ax = plt.subplots(1, figsize=(8.5,6.5))
ax.axis('off')
cmap = plt.get_cmap('bwr')
jan_avg_df.plot(ax=ax, column='jan',legend=True,linewidth=0.2,edgecolor='gray',cmap=cmap)
plt.title('Average January Temperature (1970-2000)')
plt.show()

#July Average
jul_avg_data = tavg[['fips','jul']].copy()
jul_avg_data['fips'] = jul_avg_data['fips'].astype(str)
jul_avg_data['fips'] = jul_avg_data['fips'].str.zfill(5)
jul_avg_df = pd.merge(county_map,jul_avg_data,on='fips',how='inner')
fig, ax = plt.subplots(1, figsize=(8.5,6.5))
ax.axis('off')
cmap = plt.get_cmap('bwr')
jul_avg_df.plot(ax=ax,column='jul',legend=True,linewidth=0.2,edgecolor='gray',cmap=cmap)
plt.title('Average July Temperature (1970-2000)')
plt.show()



