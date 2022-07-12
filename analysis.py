import jrlta as ta
import pandas as pd
import numpy as np
""" download daily data """
ta.download_files()

# Help pick date
date_pick = input("Do you want to analyse todays date?[y/n]\n(if no, will analyse latest downloaded date)\n")
if(date_pick == 'y'):
    # get today's date
    date_select = pd.to_datetime('today')
    # remove timestamp from the date
    date_select = pd.to_datetime(str(date_select).split()[0])
elif(date_pick == 'n'):
    # get latest downloaded file's date using the index
    date_select = ta.get_dl_filenames().index[-1]
    # remove timestamp 
    date_select = str(date_select).split()[0]
else:
    # get latest downloaded file's date using the index
    date_select = ta.get_dl_filenames().index[-1]
    # remove timestamp 
    date_select = str(date_select).split()[0]



# """ CALCULATE DAILY CHANGE AND SORT ACCORDINGLY """
print("Analysing date {}".format(date_select))
df = ta.daily_change(date=date_select,sortOrder='des',writeToFile=True)
# print(df.head())
df_select = df.iloc[:15]
for i,r in df_select.iterrows():
    print(r['SC_NAME'],":\t", r['CHANGE'],"%\t", r['CLOSE'])

# FINDING THE WEEKLY GAINERS AND LOOSERS

# Get weekly data
week_status = input("\n\nDo you want to analyse weekly data?[y/n]\n\n")
if(week_status == 'y'):
    success = False
    while not success:
        # get a week number for analysis
        week_number = int(input("\nPlease input a week number to analyse (1 to 52): "))
        # validate week number, if not valid get a new entry
        if(week_number > 52 or week_number < 1):
            print("\nPlease input a valid week number!!!.\n")
            success = False
        else:
            success = True
    # weekly data analysis
    df = ta.get_weekly_data(week_number,writeToFile=True,sortOrder='asc')
    print(df.head())
else:
    print("You opted not to analyse weekly data")


# # """ GET ALL STOCKS ON A PARTICULAR DAY WITH CLOSE == HIGH AND CLOSE-OPEN != 0 """


""" load data to ram """
# define starting date and ending date
start = pd.to_datetime('1-Jan-2019')
end = pd.to_datetime(date_select)
# end = today

print(end)
data = ta.load_data(start,end)

print(type(data))
d_keys = data.keys()
# print(type(d_keys))
# for k in d_keys:
#     print(k)
""" from the loaded data get list of SC_CODE """
# get data of the last trading date
temp_df = data[end]
# load all securities of type Q
sc_code_list = temp_df[temp_df['SC_TYPE']=='Q']
temp_df = None


""" fetch all companies with high == close and close > open """
sc_code_list = sc_code_list[sc_code_list['CLOSE']==sc_code_list['HIGH']]
sc_code_list = sc_code_list[sc_code_list['CLOSE']>sc_code_list['OPEN']]
sc_code_list = sc_code_list['SC_CODE']

""" calculate targeted technical indicators (bollinger band, ema21, ema9) for each of the sc_code"""
for sc_code in sc_code_list:
    # get data for the desired equity
    security_data = ta.get_security_data(data,int(sc_code),start,end)
    # calculate ema200
    ema200 = ta.ema(security_data,200)
    # # calculate ema9
    # ema50 = ta.ema(security_data,50)
    # calculate bollinger bands
    bollinger_band = ta.bollinger_band(security_data)
    # estimate daily change
    # daily_returns = ta.daily_returns(security_data)
    # join all the technical indicators to a single dataframe
    df = ema200.join(bollinger_band)
    # df = df.join()
    # df = df.join(security_data['CLOSE'])
    # df = df.join(daily_returns)
    # print(df)
    # # ta.plot_linegraph(df.tail())
    # last_close = df['CLOSE'][-1]
    last_bb_mean = df['BB_MEAN'][-1]
    last_ema_200 = df['EMA_200'][-1]
    # last_ema50 = df['EMA_50'][-1]

    # if last_close<last_bb_mean:
    #     if last_ema50<last_ema100:
    #         print(security_data['SC_NAME'][0],security_data['CLOSE'][-1])
    # # last_daily_returns = df['DAILY_RETURNS'][-3:]
    # # status = False
    # count = 0
    # for dr in last_daily_returns:
    #     if dr>=2:
    #         count += 1
    # if(count == 3):
    #     print('\n\n',security_data['SC_NAME'][0])
    #     print(last_daily_returns.values)
    
    """
    BELOW IS AN ANALYSIS METHOD USED EARLIER
    """
    # if last_bb_mean > last_ema_200:
    #     bb_arr = list(df['BB_MEAN'][-10:])
    #     ema_arr = list(df['EMA_200'][-10:])
    #     # print("\n\n",security_data['SC_NAME'][0])
    #     # print(bb_arr)
    #     # print(ema_arr) 
    #     # print(type(bb_arr),type(ema_arr))
    #     for i in range(len(bb_arr)-1,0,-1):
    #         if bb_arr[i]<ema_arr[i]:
    #             print("\n\n",security_data['SC_NAME'][0])
    #             break

    """
    GET ALL STOCKS THAT ARE TRADING BELOW LOWER BOLLINGER BAND
    """
    # print("Finding stocks price less than LOWER BOLLINGER BAND")
    last_bb_lower = df['BB_LOWER'][-1]
    close_price = security_data['CLOSE'][-1]
    low_price = security_data['LOW'][-1]
    if(low_price < last_bb_lower):
        print(security_data['SC_NAME'][0], "\t CLOSE: {}, LOW_PRICE: {}, BB_LOWER: {}\n".format(close_price, low_price, last_bb_lower))


""" Use ema_200 and bollinger mean
    if bollinger mean is above present ema_200 select it
    look for last 10 days window - if bollinger_mean - ema_200 becomes -ve select it"""
    



""" Calculate difference between ema_long and ema_short for the last 10 days
    If there is change in trend then select it  """


