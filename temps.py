import os
import pandas as pd

#SETUP
cd = os.path.join(os.path.expanduser("~"),r'Documents',r'projects',r'temps')
cd_dotdot = os.path.join(os.path.expanduser("~"),r'Documents',r'projects')
if not os.path.exists(os.path.join(cd,r'temps_by_state')):
    os.makedirs(os.path.join(cd,r'temps_by_state'))

#COUNTY LAND AREA
land_area_dtypes = {'fips':'str','land_area':'float'}
land_area = pd.read_csv('land_area_2010.csv',dtype=land_area_dtypes)
land_area['land_area'] = land_area['land_area'].round(1)
land_area_add = [['12025','51560','51780'], [1897.7,3.1,13.2]]
land_area_add_df = pd.DataFrame(land_area_add, index=['fips', 'land_area']).T
land_area = land_area.append(land_area_add_df)
land_area_mdf = land_area[~(land_area['fips'] == '12025') & ~(land_area['fips'] == '51560') & \
                          ~(land_area['fips'] == '51580') & ~(land_area['fips'] == '51515') & \
                          ~(land_area['fips'] == '51780') & ~(land_area['fips'] == '51530') & \
                          ~(land_area['fips'] == '51678')]
land_area = land_area[land_area['fips'] != '12086']

#DAILY WEATHER DATA
#load as .csv
#normal states
states = (['01','04','05','06','08','09','10','11','12','13','16','17','18','19','20'
           ,'21','22','23','24','25','26','27','28','29','30','31','32','33','34','35'
           ,'36','37','38','39','40','41','42','44','45','46','47','49','50','51','53'
           ,'54','55','56'])
for st in states:
    raw_filename = st + r'.txt'  
    raw_data_path = os.path.join(cd,r'North America Land Data Assimilation System (NLDAS) Daily Air Temperatures and Heat Index (1979-2011)',raw_filename)
    state_df = pd.read_csv(raw_data_path,sep='\t',dtype=str,index_col=False)
    del state_df['Notes']
    state_df = state_df.dropna()
    filename =  st + r'.csv'  
    temp_path = os.path.join(cd,r'temps_by_state',filename)
    state_df.to_csv(temp_path,index=False)
#texas
raw_filename1 = r'48_1.txt'  
raw_filename2 = r'48_2.txt'  
raw_data_path1 = os.path.join(cd,r'North America Land Data Assimilation System (NLDAS) Daily Air Temperatures and Heat Index (1979-2011)',raw_filename1)
raw_data_path2 = os.path.join(cd,r'North America Land Data Assimilation System (NLDAS) Daily Air Temperatures and Heat Index (1979-2011)',raw_filename2)
state_df1 = pd.read_csv(raw_data_path1,sep='\t',dtype=str,index_col=False)
state_df2 = pd.read_csv(raw_data_path2,sep='\t',dtype=str,index_col=False)
del state_df1['Notes']
del state_df2['Notes']
state_df = state_df1.append(state_df2)
state_df = state_df.dropna()
filename =  r'48.csv'
temp_path = os.path.join(cd,r'temps_by_state',filename)
state_df.to_csv(temp_path,index=False)
#combine
extreme_df = state_df
for st in states:
    filename =  st + r'.csv'  
    temp_in_path = os.path.join(cd,r'temps_by_state',filename)
    state_df = pd.read_csv(temp_in_path,dtype=str,index_col=False)
    extreme_df = extreme_df.append(state_df)
extreme_df = extreme_df.sort_values(by=['County Code'])
extreme_df = extreme_df[['County', 'County Code', 'Day of Year', \
                   'Avg Daily Max Air Temperature (F)','Min Temp for Daily Max Air Temp (F)', \
                   'Max Temp for Daily Max Air Temp (F)','Avg Daily Min Air Temperature (F)', \
                   'Min Temp for Daily Min Air Temp (F)','Max Temp for Daily Min Air Temp (F)']]
extreme_df.columns = ['county', 'fips', 'day', 'avg_daily_max_air_temp','min_daily_max_air_temp', \
                   'max_daily_max_air_temp','avg_daily_min_air_temp','min_daily_min_air_temp', \
                   'max_daily_min_air_temp']
comb_path = os.path.join(cd,'temps.csv')
extreme_df.to_csv(comb_path,index=False)

#MONTHLY WEATHER DATA
filetype = ['pcpncy','tmaxcy','tmincy','tmpccy']
dataframes = {}
mean_dataframes = {}
#state_codes to state_fips
weather_state_codes = ['01','02','03','04','05','06','07','08','09','10','11','12', \
                       '13','14','15','16','17','18','19','20','21','22','23','24', \
                       '25','26','27','28','29','30','31','32','33','34','35','36', \
                       '37','38','39','40','41','42','43','44','45','46','47','48']
fips_state_codes = ['01','04','05','06','08','09','10','12','13','16','17','18','19', \
                    '20','21','22','23','24','25','26','27','28','29','30','31','32', \
                    '33','34','35','36','37','38','39','40','41','42','44','45','46', \
                    '47','48','49','50','51','53','54','55','56']
state_code_map = pd.DataFrame(
    {'state_code': weather_state_codes,
     'state_fips': fips_state_codes
    })
#loop over type of data
for ftype in filetype:
    filename = r'climdiv-' + ftype + r'-v1.0.0-20210106.txt'
    raw_path = os.path.join(cd,filename)
    raw_data = pd.read_fwf(raw_path,dtype=str,index_col=False,header=None)
    raw_data['state_code'] = raw_data[0].str[:2]
    raw_data['county_fips'] = raw_data[0].str[2:5]
    raw_data['element_code'] = raw_data[0].str[5:7]
    raw_data['year'] = raw_data[0].str[7:11]
    del raw_data[0]
    new_order = ['state_code','county_fips','element_code','year',1,2,3,4,5,6,7,8,9,10,11,12]
    raw_data = raw_data[new_order]
    new_labels = ['state_code','county_fips','element_code','year','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    raw_data.columns = new_labels
    raw_data = raw_data[~(raw_data['state_code'] == '50')]
    #50 is Alaska?
    raw_data = pd.merge(raw_data,state_code_map,on=['state_code'],how='inner')
    del raw_data['state_code']
    raw_data['fips'] = raw_data['state_fips'] + raw_data['county_fips'] 
    new_order =  ['fips','state_fips','county_fips','element_code','year','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    raw_data = raw_data[new_order]
    dataframes[ftype] = raw_data
    filename = ftype + r'.csv'
    dataframes[ftype].to_csv(filename,index=False)
    dataframes[ftype] = dataframes[ftype][(dataframes[ftype]['year'].astype(int) < 2001) & (dataframes[ftype]['year'].astype(int) > 1969)].copy()
    data_cols = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    dataframes[ftype][data_cols] = dataframes[ftype][data_cols].apply(pd.to_numeric)
    mean_dataframes[ftype] = dataframes[ftype].groupby(['fips'],as_index=False)[data_cols].mean().round(1)
    #Shannon County, SD
    sd_data = mean_dataframes[ftype][(mean_dataframes[ftype]['fips'] == '46007') \
                     | (mean_dataframes[ftype]['fips'] == '46033') \
                     | (mean_dataframes[ftype]['fips'] == '46047') \
                     | (mean_dataframes[ftype]['fips'] == '46071')]
    sd_add = list(sd_data.mean()[1:].round(1))
    sd_add.insert(0,'46113')
    add_cols = data_cols.copy()
    add_cols.insert(0,'fips')
    sd_add_df = pd.DataFrame(sd_add,index=add_cols).T
    mean_dataframes[ftype] = mean_dataframes[ftype].append(sd_add_df)
    #Washington, D.C.
    dc_data = mean_dataframes[ftype][(mean_dataframes[ftype]['fips'] == '51013') \
                     | (mean_dataframes[ftype]['fips'] == '51510') \
                     | (mean_dataframes[ftype]['fips'] == '24031') \
                     | (mean_dataframes[ftype]['fips'] == '24033')]
    dc_add = list(sd_data.mean()[1:].round(1))
    dc_add.insert(0,'11001')
    dc_add_df = pd.DataFrame(dc_add,index=add_cols).T
    mean_dataframes[ftype] = mean_dataframes[ftype].append(dc_add_df)

#APPLY FIPS MAPPING (AVERAGE WITHIN A NEW_FIPS IF MULTIPLE)
fips_crosswalk_filepath = os.path.join(cd_dotdot,r'uniform_counties',r'fips_crosswalk.csv')
fips_crosswalk = pd.read_csv(fips_crosswalk_filepath)
fips_crosswalk['fips'] = fips_crosswalk['fips'].apply(str)
fips_crosswalk['fips'] = fips_crosswalk['fips'].str.zfill(5)
fips_crosswalk['new_fips'] = fips_crosswalk['new_fips'].apply(str)
fips_crosswalk['new_fips'] = fips_crosswalk['new_fips'].str.zfill(5)
del fips_crosswalk['year']
fips_crosswalk = fips_crosswalk.drop_duplicates(subset=['fips','new_fips'])
#land_area
land_area_mapped = pd.merge(land_area,fips_crosswalk,on=['fips'],how='right')
land_area_mapped['new_land_area'] = land_area_mapped['land_area'].groupby(land_area_mapped['new_fips']).transform('sum').round(1)
land_area_mapped_mdf = pd.merge(land_area_mdf,fips_crosswalk,on=['fips'],how='right')
land_area_mapped_mdf['new_land_area'] = land_area_mapped_mdf['land_area'].groupby(land_area_mapped_mdf['new_fips']).transform('sum').round(1)

#extreme
extreme_df = pd.merge(extreme_df,fips_crosswalk,on=['fips'],how='inner') #don't need Yellowstone national park (30113)
extreme_df = pd.merge(extreme_df,land_area_mapped,on=['fips','new_fips'],how='inner')
extreme_df['wght'] = extreme_df['land_area']/extreme_df['new_land_area']
variables1 = ['min_daily_max_air_temp','max_daily_max_air_temp', \
             'min_daily_min_air_temp','max_daily_min_air_temp']
variables2 = ['avg_daily_max_air_temp','avg_daily_min_air_temp']
extreme_df[variables1] = extreme_df[variables1].apply(pd.to_numeric)
extreme_df[variables2] = extreme_df[variables2].apply(pd.to_numeric)
extreme_df_summed = {}
for var in variables1:
    new_var_name_temp = var + r'temporary'
    extreme_df[new_var_name_temp] = extreme_df[var]*extreme_df['wght']
    del extreme_df[var]
    extreme_df_summed[var] = extreme_df.groupby(['day','new_fips'],as_index=False).agg({new_var_name_temp: 'sum'}).round(1)
    extreme_df_summed[var] = extreme_df_summed[var].sort_values(by=['new_fips','day'])
    extreme_df_summed[var].columns = ['day_of_year','fips',var]
    del extreme_df[new_var_name_temp]
for var in variables2:
    new_var_name_temp = var + r'temporary'
    extreme_df[new_var_name_temp] = extreme_df[var]*extreme_df['wght']
    del extreme_df[var]
    extreme_df_summed[var] = extreme_df.groupby(['day','new_fips'],as_index=False).agg({new_var_name_temp: 'sum'}).round(2)
    extreme_df_summed[var] = extreme_df_summed[var].sort_values(by=['new_fips','day'])
    extreme_df_summed[var].columns = ['day_of_year','fips',var]
    del extreme_df[new_var_name_temp]
del extreme_df['fips']
del extreme_df['land_area']
del extreme_df['county']
del extreme_df['wght']
extreme_df = extreme_df.drop_duplicates(subset=['new_fips','day'])
extreme_df.columns = ['day_of_year','fips','land_area']
extreme_df = extreme_df.sort_values(by=['fips','day_of_year'])
variables = variables2 + variables1
for var in variables:
    extreme_df = pd.merge(extreme_df,extreme_df_summed[var],on=['day_of_year','fips'],how='inner')
#save extreme_df detailed
extreme_df.to_csv('extremes_detailed.csv',index=False)
extreme_df_ind = extreme_df[['fips','day_of_year']].copy()
#monthly
temp_5_deg_bounds = [5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100,105] #above 0 is all, and above 110 is none
for temp in temp_5_deg_bounds:
    var_name = r'avg_daily_max_air_temp_' + str(temp)
    temp_var_name = r'temp_avg_daily_max_air_temp_' + str(temp)
    extreme_df_ind[temp_var_name] = (extreme_df['avg_daily_max_air_temp'] > temp)
    extreme_df_ind[temp_var_name] = extreme_df_ind[temp_var_name].astype(int)
    extreme_df_ind[var_name] = extreme_df_ind[temp_var_name].groupby(extreme_df_ind['fips']).transform('sum')
    del extreme_df_ind[temp_var_name]
del extreme_df_ind['day_of_year']
extreme_df_ind = extreme_df_ind.drop_duplicates()
extreme_df_ind.to_csv('extremes.csv',index=False)
mean_dataframes_summed = {}

for ftype in filetype:
    mean_dataframes[ftype] = pd.merge(mean_dataframes[ftype],fips_crosswalk,on=['fips'],how='inner')
    mean_dataframes[ftype] = pd.merge(mean_dataframes[ftype],land_area_mapped_mdf,on=['fips','new_fips'],how='inner')
    mean_dataframes[ftype]['wght'] = mean_dataframes[ftype]['land_area']/mean_dataframes[ftype]['new_land_area']
    variables = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    mean_dataframes_summed[ftype] = {'jan':None,'feb':None,'mar':None,'apr':None,'may':None,'jun':None, \
                                     'jul':None,'aug':None,'sep':None,'oct':None,'nov':None,'dec':None}
    for var in variables:
        new_var_name_temp = var + r'_temporary'
        mean_dataframes[ftype][new_var_name_temp] = mean_dataframes[ftype][var]*mean_dataframes[ftype]['wght']
        del mean_dataframes[ftype][var]
        mean_dataframes_summed[ftype][var] = mean_dataframes[ftype].groupby(['new_fips'],as_index=False).agg({new_var_name_temp: 'sum'}).round(1)
        mean_dataframes_summed[ftype][var] = mean_dataframes_summed[ftype][var].sort_values(by=['new_fips'])
        mean_dataframes_summed[ftype][var].columns = ['fips',var]
        del mean_dataframes[ftype][new_var_name_temp]
    del mean_dataframes[ftype]['fips']
    del mean_dataframes[ftype]['land_area']
    del mean_dataframes[ftype]['wght']
    mean_dataframes[ftype] = mean_dataframes[ftype].drop_duplicates(subset=['new_fips'])
    mean_dataframes[ftype].columns = ['fips','land_area']
    for var in variables:
        mean_dataframes[ftype] = pd.merge(mean_dataframes[ftype],mean_dataframes_summed[ftype][var],on=['fips'],how='inner')
precip = mean_dataframes['pcpncy']
precip.to_csv('precip.csv',index=False)
tmax = mean_dataframes['tmaxcy']
tmax.to_csv('tmax.csv',index=False)
tmin = mean_dataframes['tmincy']
tmin.to_csv('tmin.csv',index=False)
tavg = mean_dataframes['tmpccy']
tavg.to_csv('tavg.csv',index=False)

