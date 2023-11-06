import pandas as pd
import os
import numpy as np
import datetime as dt

#parameters
munich_center = [48.137154, 11.576124]
# 1 degree latitude = 111.139 km
# 1 degree longitude = 111.139*cos(latitude)
radius = 40 # km
lat_factor = 111.139
long_factor = np.cos(munich_center[0]*np.pi/180)*111.139
stations = pd.read_csv('../Thesis input/tankerkoenig-data/stations/stations.csv')
stations = stations[(lat_factor*(stations['latitude']-munich_center[0]))**2 + (long_factor*(stations['longitude']-munich_center[1]))**2 < radius**2]
price_dir = '../Thesis input/tankerkoenig-data/prices/2022'
def concat_csvs(path, price_column): # multithread this if area expands
    df = pd.DataFrame() # create empty final dataframe
    df['time'] = pd.period_range(\
        start = dt.datetime(year = 2022, month = 1, day = 1, hour = 0, minute = 0, second = 0, tzinfo = dt.timezone(dt.timedelta(hours = 2), 'Berlin')),\
        end = dt.datetime(year = 2022, month = 12, day = 31, hour = 23, minute = 59, second = 0, tzinfo = dt.timezone(dt.timedelta(hours = 2), 'Berlin')),\
        freq='T')
    df['time'] = df['time'].apply(lambda x: x.to_timestamp())
    df['time'] = df['time'].apply(lambda x: x.replace(tzinfo = dt.timezone(dt.timedelta(hours = 2), 'Berlin')))
    df = df.copy()
    for uuid in stations['uuid'].unique():
        df[uuid] = np.nan
    df = df.set_index('time')
    
    for foldername in os.listdir(path): # load data and fill dataframe
        for filename in os.listdir(path + '/' + foldername):
            if filename.endswith(".csv"):
                print(foldername + '/' + filename)
                df1 = pd.DataFrame()
                df1 = pd.read_csv(path + '/' + foldername + '/' + filename)
                # if df1.shape[0] == 0:
                #     del df1
                #     continue
                df1 = df1[df1['station_uuid'].isin(stations['uuid'])]
                df1 = df1[['station_uuid','date',price_column]]
                df1['date'] = pd.to_datetime(df1['date'], utc = True)
                df1['date'] = df1['date'].apply(lambda x: x.tz_convert(dt.timezone(dt.timedelta(hours = 2), 'Berlin')))
                df1 = df1.pivot_table(index = 'date', columns = 'station_uuid', values = price_column)
                
                df1 = df1.resample('T').last()
                
                df.update(df1)
                
                del df1
            else:
                continue
    df = df.copy()
    df.to_csv('Thesis Data/Working Data/price_changes.csv')
    print('Filling started')
    df = df.ffill()
    print('Filling done')
    return df

    

    
def main():
    df = concat_csvs(price_dir, 'e5')
    df.to_csv('Thesis Data/Working Data/price_series.csv')
    stations.to_csv('Thesis Data/stations.csv')
if __name__ == '__main__':
    main()
    
#legacy
# def process_huge_dataframe_date(df, partitions): # avoid insane memory usage, could also be done in parallel
#     size = df.shape[0]//partitions
#     ref_day = dt.datetime(2022,1,1,0,0, tzinfo=dt.timezone(dt.timedelta(hours = 1))) # reference day for time since 2022-01-01 00:00:00
#     for i in range(partitions):
#         print('partition ' + str(i+1) +'/' + str(partitions) +  ' started')
#         df_part = pd.DataFrame()
#         df_part = df[i*size:(i+1)*size].copy()
        
#         print('partition shape: ' + str(df_part.shape))
#         df_part['datetime'] = df_part.apply(lambda x: dt.datetime.fromisoformat(x['date']),axis=1)
#         df_part['time'] = df_part['datetime'].apply(lambda x: x-ref_day)
#         df_part.drop(['date'],axis=1)
        
        
#         df_part.to_csv('Thesis Data/prices' + str(i) + '.csv')
#         #print(df_part.columns)
#         del df_part
#         print('partition ' + str(i+1) +'/' + str(partitions) +  ' finished')
    
#     for i in range(partitions):
#         df_part = pd.read_csv('Thesis Data/prices' + str(i) + '.csv')
#         if i == 0:
#             df_final = df_part.copy()
#         else:
#             df_final = pd.concat([df_final,df_part.copy()])
#         del df_part
#         os.remove('Thesis Data/prices' + str(i) + '.csv')
#     df_final = df_final[['station_uuid','diesel','e5','e10','datetime','time']]
#     print('concatenation done')
#     print('shape of final data frame: ' + str(df_final.shape))
#     return df_final

# def fill_huge_dataframe(df, partitions):
#     df_final = pd.DataFrame()
#     df['datetime'] = pd.to_datetime(df['datetime'])
#     df['time'] = pd.to_timedelta(df['time'])
#     partition_size = (max(df['time'])-min(df['time']))/partitions
#     print('partition size: ' + str(partition_size))
#     print('filling dataframe:')
#     for i in range(partitions):
#         print('partition ' + str(i+1) + '/' + str(partitions) + ' started')
#         df_part = df[(df['time'] < partition_size*(i+1)) & (df['time'] > partition_size*i)]
#         #df_part = df_part.set_index('time')
#         #print(df_part.head(5))
#         df_part.groupby('station_uuid', sort = False, group_keys = True)\
#             .apply(lambda x: x.resample('1T', on = 'time')).ffill()
#         #print(df_part.head(20))
#         print('partition size: ' + str(df_part.shape))
#         df_part.to_csv('Thesis Data/prices' + str(i) + '.csv')
#     for i in range(partitions):
#         df_part = pd.read_csv('Thesis Data/prices' + str(i) + '.csv')
#         if i == 0:
#             df_final = df_part.copy()
#         else:
#             df_final = pd.concat([df_final,df_part.copy()])
#         del df_part
#         os.remove('Thesis Data/prices' + str(i) + '.csv')
#     df_final = df_final[['station_uuid','diesel','e5','e10','datetime','time']]
#     print('filling done')
#     return df_final