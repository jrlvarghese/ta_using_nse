"""
Calculate daily returns
"""
# Import all modules
import jrlta as ta
import pandas as pd
import numpy as np

# file_names = ta.get_dl_filenames()
# print(file_names)
# print(type(file_names))

# df_A = ta.mean_daily_return(2)
# df_B = ta.mean_daily_return(5)


# print("\n\nAnalysing mean daily return for 5 and 10 days,\nIf there is change in trend pick the stock")
# for i in df_A.index:
#     if not df_A['SECURITY_NAME'][i] == df_B['SECURITY_NAME'][i]:
#         print("\nNot matching!!\n")
#     else:
#         mdr_a = df_A['MEAN_DAILY_RETURN'][i]
#         mdr_b = df_B['MEAN_DAILY_RETURN'][i]
#         if(mdr_a<=0 and mdr_b>0):
#             print("{}, A: {}, B: {}".format(df_A['SECURITY_NAME'][i], mdr_a, mdr_b))
#         else:
#             print("----")

file_names = ta.get_dl_filenames()
nDays = 10
start_date = file_names.index[-nDays]
end_date = file_names.index[-1]
data = ta.load_data(start_date, end_date)
# Get list of all securities in the latest date
sc_namelist = ta.get_security_names(data,end_date)
# Select a single security to analyse
sc_select = sc_namelist[29]
print("Selected security "+sc_select)
# Get daily returns data for particular security
sc_data = ta.get_security_data(data,sc_select,start_date,end_date)
# Calculate daily returns in percentage
sc_daily_returns = ta.daily_returns(sc_data)
print(sc_daily_returns)