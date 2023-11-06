import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt
import Data_Loading as dl
import datetime as dt

#parameters
munich_center = [48.137154, 11.576124]
# 1 degree latitude = 111.139 km
# 1 degree longitude = 111.139*cos(latitude)
radius = 40 # km
lat_factor = 111.139
long_factor = np.cos(munich_center[0]*np.pi/180)*111.139
stations = pd.read_csv('../Thesis data/tankerkoenig-data/stations/stations.csv')
stations = stations[(lat_factor*(stations['latitude']-munich_center[0]))**2 + (long_factor*(stations['longitude']-munich_center[1]))**2 < radius**2]
price_dir = '../Thesis data/tankerkoenig-data/prices/2022'

def test_load_stations():
    assert dl.stations.shape >= (100,9)
    
def test_load_prices():
    prices_path = '../Thesis data/tankerkoenig-data/prices/2023/08/2023-08-13-prices.csv'
    prices = pd.read_csv(prices_path)
    assert prices.shape >= (100,8)

def test_update():
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    
    df1['time'] = pd.Series(pd.period_range('2022-08-13', periods = 10, freq = 'T'))
    df2['time'] = pd.Series(pd.period_range('2022-08-13 00:03:00', periods = 5, freq = 'T'))
    df1['time'] = df1['time'].apply(lambda x: x.to_timestamp()).apply(lambda x: x.replace(tzinfo = dt.timezone(dt.timedelta(hours = 2), 'Berlin')))
    df2['time'] = df2['time'].apply(lambda x: x.to_timestamp()).apply(lambda x: x.replace(tzinfo = dt.timezone(dt.timedelta(hours = 2), 'Berlin')))
    df1 = df1.set_index('time')
    df2 = df2.set_index('time')
    df1[['a','b']] = np.nan
    df2['a'] = 5
    df2['b'] = 10
    df1.update(df2)
    print(df1.head(10))
  
def test_update_main_df(price_dir, column = 'e5'):
    df1 = pd.read_csv(price_dir + '/08/2022-08-13-prices.csv')
    df1 = df1[df1['station_uuid'].isin(stations['uuid'])]
    df1 = df1[['station_uuid','date',column]]
    df1['date'] = pd.to_datetime(df1['date'])
    df1 = df1.pivot_table(index = 'date', columns = 'station_uuid', values = column)
    df2 = df1.resample('T').last()

    shape = df2[df2['fb79c457-543a-4ff6-ba70-cd270ac2110a'].notna()].shape
    print(shape)
    
    main_df = pd.DataFrame()
    main_df['time'] = pd.period_range(\
        start= dt.datetime(year = 2022, month = 8, day = 13, hour = 0, minute = 0, second = 0, tzinfo = dt.timezone(dt.timedelta(hours = 2), 'Berlin')),\
        end=dt.datetime(year = 2022, month = 8, day = 13, hour = 23, minute = 59, second = 0, tzinfo = dt.timezone(dt.timedelta(hours = 2), 'Berlin')),\
        freq='T')
    main_df['time'] = main_df['time'].apply(lambda x: x.to_timestamp())
    main_df['time'] = main_df['time'].apply(lambda x: x.replace(tzinfo = dt.timezone(dt.timedelta(hours = 2), 'Berlin')))
    
    for uuid in stations['uuid'].unique():
        main_df[uuid] = np.nan
    
    
    main_df = main_df.set_index('time')
    main_df.update(df2)

    main_df.to_csv('Thesis Data/Test Data/main_update_test.csv')
    df2.to_csv('Thesis Data/Test Data/to_update_test.csv')
    return main_df

def test_ffill(df):
    df_final = df.ffill()
    df_final.to_csv('Thesis Data/ffill_test.csv')
    return df_final

def test_day(price_dir, test_day, test_month, column = 'e5'): # I now hate daylight savings, use Berlin +2 for datetimes
    print('Test day: ' + str(test_day))
    
    main_df = pd.DataFrame()
    main_df['time'] = pd.period_range(\
        start= dt.datetime(year = 2022, month = int(test_month), day = int(test_day), hour = 0, minute = 0, second = 0, tzinfo = dt.timezone(dt.timedelta(hours = 2), 'Berlin')),\
        end=dt.datetime(year = 2022, month = int(test_month), day = int(test_day), hour = 23, minute = 59, second = 0, tzinfo = dt.timezone(dt.timedelta(hours = 2), 'Berlin')),\
        freq='T')
    main_df['time'] = main_df['time'].apply(lambda x: x.to_timestamp())
    main_df['time'] = main_df['time'].apply(lambda x: x.replace(tzinfo = dt.timezone(dt.timedelta(hours = 2), 'Berlin')))
    main_df = main_df.copy()
    for uuid in stations['uuid'].unique():
        main_df[uuid] = np.nan
    main_df = main_df.set_index('time')
    
    df1 = pd.read_csv(price_dir + '/'+ test_month + '/2022-'+ test_month + '-' + test_day + '-prices.csv')
    df1 = df1[df1['station_uuid'].isin(stations['uuid'])]
    df1['date'] = pd.to_datetime(df1['date'])
    df1['date'] = df1['date'].apply(lambda x: x.tz_convert(dt.timezone(dt.timedelta(hours = 2), 'Berlin')))
    df1 = df1[['station_uuid','date',column]]
    df1 = df1.pivot_table(index = 'date', columns = 'station_uuid', values = column)
    #df1 = df1.set_index('date')
    df2 = df1.resample('T').last()
    main_df.update(df2)
    main_df = main_df.ffill()
    
    df1.to_csv('Thesis Data/Test Data/'+ test_day + '-'+ test_month + '_test.csv')
    main_df.to_csv('Thesis Data/Test Data/' + test_day + '-'+ test_month + '_main_update_test.csv')
    df2.to_csv('Thesis Data/Test Data/' + test_day + '-'+ test_month + '_to_update_test.csv')
    return main_df

def test_groupby():
    test_cha_melt = pd.DataFrame({'variable': ['a','b','c','a','b','c'],\
                            'value1': [30,23,18,24,43,21],\
                            'value2': [36,43,4,48,31,9],\
                            'value3': [5,15,20,22,28,25],\
                            'value4': [47,48,1,18,14,5]})
    test_delta_melt = test_cha_melt.groupby('variable').apply(lambda grp: (grp - grp.shift(1).bfill())/grp.shift(1).bfill())
    print(test_delta_melt)
if __name__ == '__main__':
    #test_load_stations()
    #test_load_prices()
    #test_update()
    #test_df = test_update_main_df(price_dir)
    #test_df = test_ffill(test_df)
    #test_march_27_df = test_day(price_dir,test_day = '27', test_month = '03')
    #test_jan_1_df = test_day(price_dir,test_day = '01', test_month = '01')
    #test_sep_1_df = test_day(price_dir,test_day = '01', test_month = '09')
    #test_nov_1_df = test_day(price_dir,test_day = '01', test_month = '11')
    print('Tests done')