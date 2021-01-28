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

#Days above 25
above_25_data = extremes[['fips','avg_daily_max_air_temp_25']].copy()
above_25_data['fips'] = above_25_data['fips'].astype(str)
above_25_data['fips'] = above_25_data['fips'].str.zfill(5)
above_25_df = pd.merge(county_map,above_25_data,on='fips',how='inner')
fig, ax = plt.subplots(1, figsize=(8.5,6.5))
ax.axis('off')
cmap = plt.get_cmap('bwr')
above_25_df.plot(ax=ax,column='avg_daily_max_air_temp_25',legend=True,linewidth=0.2,edgecolor='gray',cmap=cmap)
plt.title('Average # of Days w/ a  Max. Air Temp. Above 25 (1979-2011)')
plt.show()


#Days above 30
above_30_data = extremes[['fips','avg_daily_max_air_temp_30']].copy()
above_30_data['fips'] = above_30_data['fips'].astype(str)
above_30_data['fips'] = above_30_data['fips'].str.zfill(5)
above_30_df = pd.merge(county_map,above_30_data,on='fips',how='inner')
fig, ax = plt.subplots(1, figsize=(8.5,6.5))
ax.axis('off')
cmap = plt.get_cmap('bwr')
above_30_df.plot(ax=ax,column='avg_daily_max_air_temp_30',legend=True,linewidth=0.2,edgecolor='gray',cmap=cmap)
plt.title('Average # of Days w/ a  Max. Air Temp. Above 30 (1979-2011)')
plt.show()

#Days below 35
above_35_data = extremes[['fips','avg_daily_max_air_temp_35']].copy()
above_35_data['fips'] = above_35_data['fips'].astype(str)
above_35_data['fips'] = above_35_data['fips'].str.zfill(5)
above_35_df = pd.merge(county_map,above_35_data,on='fips',how='inner')
fig, ax = plt.subplots(1, figsize=(8.5,6.5))
ax.axis('off')
cmap = plt.get_cmap('bwr')
above_35_df.plot(ax=ax,column='avg_daily_max_air_temp_35',legend=True,linewidth=0.2,edgecolor='gray',cmap=cmap)
plt.title('Average # of Days w/ a  Max. Air Temp. Above 35 (1979-2011)')
plt.show()

#Days below 45
above_45_data = extremes[['fips','avg_daily_max_air_temp_45']].copy()
above_45_data['fips'] = above_45_data['fips'].astype(str)
above_45_data['fips'] = above_45_data['fips'].str.zfill(5)
above_45_df = pd.merge(county_map,above_45_data,on='fips',how='inner')
fig, ax = plt.subplots(1, figsize=(8.5,6.5))
ax.axis('off')
cmap = plt.get_cmap('bwr')
above_45_df.plot(ax=ax,column='avg_daily_max_air_temp_45',legend=True,linewidth=0.2,edgecolor='gray',cmap=cmap)
plt.title('Average # of Days w/ a  Max. Air Temp. Above 45 (1979-2011)')
plt.show()


#Days over 80
above_80_data = extremes[['fips','avg_daily_max_air_temp_80']].copy()
above_80_data['fips'] = above_80_data['fips'].astype(str)
above_80_data['fips'] = above_80_data['fips'].str.zfill(5)
above_80_df = pd.merge(county_map,above_80_data,on='fips',how='inner')
fig, ax = plt.subplots(1, figsize=(8.5,6.5))
ax.axis('off')
cmap = plt.get_cmap('bwr')
above_80_df.plot(ax=ax,column='avg_daily_max_air_temp_80',legend=True,linewidth=0.2,edgecolor='gray',cmap=cmap)
plt.title('Average # of Days w/ a  Max. Air Temp. Above 80 (1979-2011)')
plt.show()

#Days over 85
above_85_data = extremes[['fips','avg_daily_max_air_temp_85']].copy()
above_85_data['fips'] = above_85_data['fips'].astype(str)
above_85_data['fips'] = above_85_data['fips'].str.zfill(5)
above_85_df = pd.merge(county_map,above_85_data,on='fips',how='inner')
fig, ax = plt.subplots(1, figsize=(8.5,6.5))
ax.axis('off')
cmap = plt.get_cmap('bwr')
above_85_df.plot(ax=ax,column='avg_daily_max_air_temp_85',legend=True,linewidth=0.2,edgecolor='gray',cmap=cmap)
plt.title('Average # of Days w/ a  Max. Air Temp. Above 85 (1979-2011)')
plt.show()

#Days over 90
above_90_data = extremes[['fips','avg_daily_max_air_temp_90']].copy()
above_90_data['fips'] = above_90_data['fips'].astype(str)
above_90_data['fips'] = above_90_data['fips'].str.zfill(5)
above_90_df = pd.merge(county_map,above_90_data,on='fips',how='inner')
fig, ax = plt.subplots(1, figsize=(8.5,6.5))
ax.axis('off')
cmap = plt.get_cmap('bwr')
above_90_df.plot(ax=ax,column='avg_daily_max_air_temp_90',legend=True,linewidth=0.2,edgecolor='gray',cmap=cmap)
plt.title('Average # of Days w/ a  Max. Air Temp. Above 90 (1979-2011)')
plt.show()

#Days over 95
above_95_data = extremes[['fips','avg_daily_max_air_temp_95']].copy()
above_95_data['fips'] = above_95_data['fips'].astype(str)
above_95_data['fips'] = above_95_data['fips'].str.zfill(5)
above_95_df = pd.merge(county_map,above_95_data,on='fips',how='inner')
fig, ax = plt.subplots(1, figsize=(8.5,6.5))
ax.axis('off')
cmap = plt.get_cmap('bwr')
above_95_df.plot(ax=ax,column='avg_daily_max_air_temp_95',legend=True,linewidth=0.2,edgecolor='gray',cmap=cmap)
plt.title('Average # of Days w/ a  Max. Air Temp. Above 95 (1979-2011)')
plt.show()


