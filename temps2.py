import os
import pandas as pd

#SETUP
cd = os.path.join(os.path.expanduser("~"),r'Documents',r'projects',r'temps')
cd_dotdot = os.path.join(os.path.expanduser("~"),r'Documents',r'projects')

#LOAD
extreme_df_path = os.path.join(cd,'extremes_detailed.csv')
extreme = pd.read_csv(extreme_df_path)
precip_path = os.path.join(cd,'precip.csv')
precip = pd.read_csv(precip_path)
tmax_path = os.path.join(cd,'tmax.csv')
tmax = pd.read_csv(tmax_path)
tmin_path = os.path.join(cd,'tmin.csv')
tmin = pd.read_csv(tmin_path)
tavg_path = os.path.join(cd,'tavg.csv')
tavg = pd.read_csv(tavg_path)

