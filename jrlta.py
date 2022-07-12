import datetime
# from calendar import monthrange
import requests 
import pandas as pd
import numpy as np
import os
from os import listdir
from zipfile import ZipFile as zf
# import re
import sys
import matplotlib.pyplot as plt 

""" list of holidays """
holidays_2022 = ['260122','010322','180322','140422','150422','030522','090822','150822','310822','051022','241022','261022','081122']
holidays_2021 = ['260121','110321','290321','020421','140421','210421','130521','210721','190821','100921','151021','041121','051121','191121']
holidays_2020 = ['210220','100320','020420','060420','100420','140420','010520','250520','021020','161120','301120','251220']
holidays_2019 = ['260119','040319','210319','130419','140419','170419','190419','290419','010519','050619','120819','150819','020919',
                '100919','021019','081019','211019','271019','281019','121119','251219']
bse_holidays = ['210220','100320','020420','060420','100420','140420','010520','250520','021020','161120','301120','251120']
holidays_2018 = ['260118','130218','020318','290318','300318','010518','150818','220818','130918','200918','021018','181018','071118','081118','231118','251218']
MONTH_DICT = {'01':'Jan','02':'Feb','03':'Mar','04':'Apr','05':'May','06':'Jun',
            '07':'Jul','08':'Aug','09':'Sep','10':'Oct','11':'Nov','12':'Dec'}
holidays = []
holidays.extend(holidays_2018)
holidays.extend(holidays_2019)
holidays.extend(holidays_2020)
holidays.extend(holidays_2021)
holidays.extend(holidays_2022)
holidays_formated = map(lambda x:"{}-{}-{}".format(x[0:2],MONTH_DICT[x[-4:-2]],x[-2:]),holidays)
HOLIDAYS = list(map(lambda x:pd.Timestamp(x), holidays_formated))


""" url for nse and bse """
# nse_data url https://www1.nseindia.com/content/historical/EQUITIES/2020/SEP/cm25SEP2020bhav.csv.zip
NSE_URL = 'https://www1.nseindia.com/content/historical/EQUITIES/'
BSE_URL = 'https://www.bseindia.com/download/BhavCopy/Equity/'
# reference file can be downloaded from this link https://www.bseindia.com/corporates/List_Scrips.aspx 

BSE_SCRIP = "https://www.bseindia.com/members/index.aspx?expandable=6/SCRIP.zip"

""" paths for different directories """
BSE_REF_PATH = 'bse/ref/'
BSE_DL_PATH = 'bse/downloads/'
BSE_DAILY_DATA_PATH = 'bse/downloads/daily_report_isin/'
BSE_PARSE_PATH = 'bse/downloads/parsed_files/'
BSE_PARSE_LOG = 'bse/log/parse_log.csv'

BSE_REF_FILE = 'bse/ref/Equity.csv'
BSE_ISIN_REF_FILE = 'bse/ref/bse_ref.csv'

ANALYSIS_OUT_PATH = 'analysis_out/'

BSE_DATA_COLUMNS = ['SC_CODE','SC_NAME','SC_GROUP','SC_TYPE',
                    'OPEN','HIGH','LOW','CLOSE','LAST','PREVCLOSE','NO_TRADES','NO_OF_SHRS',
                    'NET_TURNOV','TDCLOINDI','ISIN_CODE','TRADING_DATE']
BSE_TARGET_COLUMNS = ['SC_CODE','SC_NAME','SC_TYPE','ISIN_CODE','OPEN','CLOSE','HIGH','LOW','PREVCLOSE',
                    'NO_TRADES','NO_OF_SHRS','TRADING_DATE']

PARSE_FILE_LIST = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o']

EICHER_OLD_ISIN = 'INE066A01013'
EICHER_NEW_ISIN = 'INE066A01021'

LAURUSLAB_OLD_ISIN = 'INE947Q01010'
LAURUSLAB_NEW_ISIN = 'INE947Q01028'

""" FUNCTIONS """
def format_date(date):
    year = date[-2:]
    month = MONTH_DICT[date[-4:-2]]
    dt = date[0:2]
    # print(dt,' ',month,' ',year)
    date_string = "{}-{}-{}".format(dt,month,year)
    return date_string

def date_to_filename(dateList,exchange='bse',fileType='csv'):
    file_name_list = []
    for date in dateList:
        # get date, month, year from the given date
        d = date.day
        m = date.month
        y = str(date.year)[2:]
        file_name = ''
        # add '0' before the dates whenever needed
        if(d<10):
            file_name = file_name + '0' + str(d)
        else:
            file_name = file_name + str(d)
        
        if(m<10):
            file_name = file_name + '0' + str(m)
        else:
            file_name = file_name + str(m)
        file_name = file_name + y
        # # EQ_ISINCODE_240920.CSV
        # if the exchange is bse
        if(exchange == 'bse' and fileType == 'csv'):
            file_name = 'EQ_ISINCODE_{}.CSV'.format(file_name)
        elif(exchange == 'bse' and fileType == 'zip'):
            file_name = 'EQ_ISINCODE_{}.zip'.format(file_name)
        file_name_list.append(file_name)

    return file_name_list

def filename_to_date(filenameList):
    # fName = "EQ_ISINCODE_040219.CSV"
    """
    Given the filename in the said format, return date
    """
    dateList = []
    for f in filenameList:
        date = f.split('_')
        date = date[-1].split('.')
        year = date[0][-2:]
        month = MONTH_DICT[date[0][-4:-2]]
        dt = date[0][0:2]
        # print(dt,' ',month,' ',year)
        date_string = "{}-{}-{}".format(dt,month,year)
        # print(date_string)
        dateList.append(pd.Timestamp(date_string))
    return dateList

def download_url(url, save_path, chunk_size=128):
    headers = {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0'}
    r = requests.get(url, stream=True, headers=headers)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

def check_today(date):
    today = pd.to_datetime('today')
    if(str(date).split()[0] == str(pd.to_datetime('today')).split()[0]):
        return True
    else:
        return False

def get_dates(start='1/1/2018',end='today'):
    # first date of the data is on 1 jan 2018
    lowest_date = pd.to_datetime('1/1/2018')
    # maximum possible date will be today
    highest_date = pd.to_datetime(str(pd.to_datetime('today')).split()[0])
    start_date = pd.to_datetime(start)
    end_date = pd.to_datetime(str(pd.to_datetime(end)).split()[0])
    # if end date is after today convert to today
    if(end_date > highest_date):
        print(end_date," is greater than ",highest_date)
        end = 'today'
    # Ensure that the input dates are in a valid range
    # print("start date: {}, lowest date: {}".format(start_date,lowest_date))
    status = True
    while status:
        status = True
        if(start_date < lowest_date):
            print("\nCannot access data before {} ".format(lowest_date))
            # status = True
            start = input("Please input start date (m/d/y): ")
            start_date = pd.to_datetime(start)
        else:
            status = False
        status = True
        if(end_date > highest_date):
            print("\nEnd date {} is not valid, should be greater than start date.".format(highest_date))
            # status = True
            end = input("Please input end date (m/d/y): ")
            end_date = pd.to_datetime(str(pd.to_datetime(end)).split()[0])
        else:
            status = False
        
        if not status:
            break            
    # Generate dates from 2018-Jan-1 to today
    dt = pd.bdate_range(start=start,end=end,freq='C',holidays=HOLIDAYS)
    today = pd.to_datetime('today')
    if(check_today(dt[-1]) and today.hour<18):
        dt = dt[:-1]
    return dt

def download_files():
    # Generate dates from 2018 - jan to today
    dt = get_dates()
    # get files present in the data directory
    dl_file_df = get_dl_filenames()
    
    target_dates = []
    for d in dt:
        if(d not in dl_file_df.index):
            target_dates.append(d)
    
    if(target_dates == []):
        print("Files are upto date till {}.".format(str(dt[-1]).split()[0]))
    else:
        print("{} file/s to be downloaed.".format(len(target_dates)))
        
    for i,d in enumerate(target_dates):
        z_name = date_to_filename([d],fileType='zip')[0]
        z_file = BSE_DL_PATH + z_name
        filename = BSE_DAILY_DATA_PATH + date_to_filename([d])[0]
        url = BSE_URL + z_name
        print("\n\nDownloading {} out of {} files.".format(i+1,len(target_dates)))
        # download data from the site using the function download_url
        download_url(url,z_file)
        print("Completed downloading from {}".format(url))

        # Extract file after downloading
        print("Extracting file ..")
        with zf(z_file,'r') as zip:
            # zip.printdir()
            zip.extractall(BSE_DAILY_DATA_PATH)
        
        # Delet zip file after extracting
        if os.path.exists(z_file):
            print("Deleting {} after extraction.".format(z_file))
            os.remove(z_file)
        else:
            print("Couldn't find {}.".format(z_file))
"""
Function to get list of all file names
"""
def get_dl_filenames():
    # dl_file_list = listdir(BSE_DAILY_DATA_PATH)
    dl_file_list = list(f for f in listdir(BSE_DAILY_DATA_PATH) if f.endswith('.CSV'))
    # convert downloaed file names to date so that it can be sorted later on
    dl_file_date = filename_to_date(dl_file_list)
    # check all files are uptodate or not
    dl_file_df = pd.DataFrame(dl_file_list,index=dl_file_date,columns=['FILENAME'])
    dl_file_df = dl_file_df.sort_index()
    return dl_file_df

def bse_ref_update():
    columns = ['Security Code','Security Id','Security Name','ISIN No','Industry','Instrument']
    df = pd.read_csv(BSE_REF_FILE,na_values='nan',usecols=columns,index_col=False)
    df = df[df['Instrument']=='Equity']
    df = df[df['ISIN No']!='nan'].reset_index(drop=True)
    
    parse_file_list = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o']
    df_len = len(df)
    indexes = np.arange(df_len)[0::400]
    if(indexes[-1] < df_len):
        indexes = np.append(indexes,df_len)
    isin_file_dict = {}
    for i in range(0,len(indexes)-1):
        # print(indexes[i],' : ',indexes[i+1])
        df.loc[indexes[i]:indexes[i+1],'File'] = parse_file_list[i]
    
    # Check if the file is present
    if(os.path.exists(BSE_ISIN_REF_FILE)):
        # print("File exists")
        # fetch list of isin no from the existing file
        ref_df = pd.read_csv(BSE_ISIN_REF_FILE,na_values='nan',usecols=columns.append('File'),index_col=False)
        # compare isin numbers in isin_df and df
        ref_isin = list(ref_df['ISIN No'])
        new_isin = list(df['ISIN No'])
        target_isin = []
        # Loop through new isin list, if isin is not there append to target isin
        for isin in new_isin:
            if isin not in ref_isin:
                target_isin.append(isin)
        # Get dataframe rows based on the target isin
        target_df = df[df['ISIN No'].isin(target_isin)]
        if target_df.empty:
            print('Reference file with bse isin is upto date.')
        else:
            print('Following securities are added to the list')
            print(target_df)
            target_df.to_csv(BSE_ISIN_REF_FILE,mode='a',header=False)
    else:
        df.to_csv(BSE_ISIN_REF_FILE,columns=columns.append('File'))
    

# def init_parse_df_list():
#     for c in BSE_DATA_COLUMNS[4:12]:


def parse_files():
    # open files to be parsed from daily data
    dl_files = get_dl_filenames()
    # get isin ref from the reference file
    isin_df = pd.read_csv(BSE_ISIN_REF_FILE,usecols=['ISIN No','File'])
    ohlc_df_list = {}
    for i,c in enumerate(BSE_DATA_COLUMNS[4:12]):
        path = os.path.join(BSE_PARSE_PATH,c)
        if(not os.path.exists(path)):
            print("Creating {} .".format(path))
            os.mkdir(path)
        else:
            print("{} alredy exists".format(path))
    # check log file
    files_to_parse = []
    if(os.path.exists(BSE_PARSE_LOG)):
        print("\nOpening log file")
        log_df = pd.read_csv(BSE_PARSE_LOG)['FILENAME']
        for f in dl_files['FILENAME']:
            if f not in list(log_df):
                files_to_parse.append(f)
    else:
        print("\nFiles have not been parsed yet")
        # # write a log file
        # log_df = pd.DataFrame(columns=['FILNAME'])
        # log_df.to_csv(mode='w')
        # since there is no log file parse all files
        files_to_parse = list(dl_files['FILENAME'])
    
    isin_df = pd.read_csv(BSE_ISIN_REF_FILE,usecols=['ISIN No','File'])
    parse_files = isin_df['File'].drop_duplicates().reset_index(drop=True)

    # Open a data file and get as a dataframe
    # files_to_parse = list(map(lambda x:BSE_DAILY_DATA_PATH+x,files_to_parse))
    total_files_to_parse = len(files_to_parse)
    
    for i,ftp in enumerate(files_to_parse):
        print("\n\nParsing {} out of {}. ::{}.".format(i+1, total_files_to_parse, ftp))
        # Get data to be parsed
        df = get_data((BSE_DAILY_DATA_PATH+ftp))
        
        file_date = filename_to_date([ftp])[0]
        parse_folders = listdir(BSE_PARSE_PATH)
        
        for p_folder in parse_folders:
            for f in parse_files:
                isin_list = list(isin_df[isin_df['File']==f]['ISIN No'])
                # for isin in isin_list:
                #     isin_data = get_isin_data(df,isin)
                    # if(not isin_data.empty):
                    #     print(isin_data['CLOSE'])
                # data = list(df[df['ISIN_CODE'].isin(isin_list)][p_folder])
                print("now parsing {}, in {}".format(f,p_folder))
                print("Length of isin list is: ",len(isin_list))
                print("length of isin_code column: ",len(df[df['ISIN_CODE'].isin(isin_list)]))
                data = df[df['ISIN_CODE'].isin(isin_list)][p_folder]
                print("length of data(before list): ",len(data))
                data = list(data)

                print("isin list length: {}, data_length: {}".format(len(isin_list),len(data)))
                data_df = pd.DataFrame([data],index=[file_date],columns=isin_list)
                # print("isin list length: {}, data_length: {}, data_df_length: {}".format(len(isin_list),len(data),len(data_df)))
                
                # print(data_df.head())
                target_path = os.path.join(BSE_PARSE_PATH,p_folder,f+'.csv')
                if(os.path.exists(target_path)):
                    # print('Appending data {}.'.format(target_path))
                    data_df.to_csv(target_path,mode='a',header=False)
                else:
                    # print('Writing data {}'.format(target_path))
                    data_df.to_csv(target_path,mode='w',header=True)     

        
        # write to log file
        print("Writing to log file.")
        log_df = pd.DataFrame([ftp],index=[file_date],columns=['FILENAME'])
        if(os.path.exists(BSE_PARSE_LOG)):
            log_df.to_csv(BSE_PARSE_LOG,mode='a',header=False)
        else:
            log_df.to_csv(BSE_PARSE_LOG,mode='w',header=True)        
  
def get_isin_data(df,isin):
    """
    Input: Dataframe containing single day's data
            isin
    """
    df_select = df[df['ISIN_CODE']==isin]
        
    # if it's not empty append value to the list
    if(not df_select.empty):
        return df_select
    else:
        # check whether it's of eicher motors
        if(isin==EICHER_OLD_ISIN):
            df_select = df[df['ISIN_CODE']==EICHER_NEW_ISIN]
            # check whether we are getting data with eicher motors new isin code
            if(not df_select.empty):
                return df_select
            else:
                return pd.DataFrame(columns=BSE_DATA_COLUMNS)
        else:
            return pd.DataFrame(columns=BSE_DATA_COLUMNS)

def optimise_df(df):
    isin_df = pd.read_csv(BSE_ISIN_REF_FILE,usecols=['ISIN No'])
    # df = get_data(BSE_DAILY_DATA_PATH + dl_files['FILENAME'][0])
    isin_df = isin_df['ISIN No']
    isin_list = []
    for isin in isin_df:
        if isin not in df['ISIN_CODE']:
            isin_list.append(isin)
    
    # print(list(df['ISIN_CODE']))
    # print(isin_list,': ',len(isin_list))
    # print(df[df['ISIN_CODE'].isin(isin_list)])
    
    if EICHER_NEW_ISIN in isin_list:
        print("Removing ",EICHER_NEW_ISIN)
        isin_list.remove(EICHER_NEW_ISIN)
    
    if EICHER_NEW_ISIN in isin_list:
        print("Removing ",EICHER_NEW_ISIN)
        isin_list.remove(EICHER_NEW_ISIN)
    
    # dictionary with missing values
    missing_dict = {}
    # Create a dictionary with all the columns
    for col in BSE_DATA_COLUMNS:
        missing_dict[col] = np.zeros(len(isin_list))
    missing_dict['ISIN_CODE'] = isin_list
    # create a new dataframe using missing isin codes
    new_df = pd.DataFrame(missing_dict,columns=BSE_DATA_COLUMNS)
    # print(new_df.head())
    # print(new_df.tail())
    # print(new_df.columns)
    # append new_df with old df
    print("LENGTH OF ISIN_CODE COL BEFORE OPTIMISATION:",len(df['ISIN_CODE']))
    df = df.append(new_df).reset_index(drop=True)
    print("LENGTH OF ISIN_CODE COL:",len(df['ISIN_CODE']))
    print("LENGTH OF ISIN LIST: ",len(isin_df))
    return df

def get_ref_data():
    # update reference data 
    ref_file = BSE_REF_PATH + 'reference.csv'
    columns = ['SC_CODE','SC_NAME']
    if(os.path.exists(ref_file)):
        ref_data = pd.read_csv(ref_file)
        # ref_data = list(ref_data['SC_NAME'])
    else:
        ref_data = pd.DataFrame(columns=columns)
    return ref_data

def get_data(fileName,index_col='TRADING_DATE',parse_dates=True,columns=BSE_DATA_COLUMNS):
    return pd.read_csv(fileName,index_col=index_col,parse_dates=parse_dates,usecols=columns,na_values=['nan'])


def load_data(start='1/1/2018',end='today',columns=BSE_TARGET_COLUMNS):
    # convert start and end to pandas dates
    start = pd.to_datetime(start)
    end = pd.to_datetime(str(pd.to_datetime(end)).split()[0])
    # get list of all files that are downloaded
    dl_files = get_dl_filenames()
    
    # select files with specified date range
    file_list = dl_files.loc[start:end]
    # convert file_list dataframe to a list
    file_list = list(file_list['FILENAME'])
    # append path with the file list
    file_path_list = list(map(lambda x:BSE_DAILY_DATA_PATH+x,file_list))

    # initialise a dictionary for loading data with dates as keys
    data_dict = {}
    # loop through all the files
    total_files = len(file_path_list)
    for i,f in enumerate(file_path_list):
        # print("Loading data for: ",f)
        df = get_data(f,columns=columns)
        # print(df.index)
        # date = pd.Timestamp(df['TRADING_DATE'][0])
        date = pd.to_datetime(df.index[0])
        # print("Date in load data {} :: {}".format((date), pd.to_datetime(date)))
        # print("Loading data for {}   [{} of {}]".format(date,i,total_files))
        progress = i*100/total_files
        sys.stdout.write("Loading data: %d%%   \r" % (progress) )
        sys.stdout.flush()
        # sc_name_list = list(df['SC_NAME'].values)
        # ref_data.extend(x for x in sc_name_list if x not in ref_data)
        # print(len(ref_data))
        data_dict[date] = df
        # print("{}: size of dictionary now is: {} kb\n".format(date,sys.getsizeof(data_dict)/1000.0))   
    print("\nFinished loading data.")
    return data_dict   

def get_security_data(df_dict,sc_name,start,end='today'):
    # Get dates 
    dates = get_dates(start,end)
    # Create an empty dataframe with index as dates
    df = pd.DataFrame(index=dates)
    temp_df = pd.DataFrame()
    # Now loop through the data dictionary which contains data for all dates
    for date in dates:
        # print(date)
        date_df = df_dict[date]
        if type(sc_name) == str:
            temp_df = temp_df.append(date_df[date_df['SC_NAME']==sc_name])
        elif type(sc_name) == int:
            temp_df = temp_df.append(date_df[date_df['SC_CODE']==sc_name])
    # join df 
    df = df.join(temp_df)
    return df



def modify_data(old_isin,new_isin,fv_factor):
    # get list files 
    file_list = get_dl_filenames()
    # add path with the filenames
    files_path_list = list(map(lambda x:BSE_DAILY_DATA_PATH+x,file_list['FILENAME'].values))
    
    for f in files_path_list:
        df = get_data(f,index_col=None)
        target_columns = ['OPEN','HIGH','LOW','CLOSE','PREVCLOSE']
        # df = df[df['ISIN_CODE']==EICHER_OLD_ISIN][target_columns].div(10).round(2)
        # df.loc[[df['ISIN_CODE']==EICHER_OLD_ISIN]['ISIN_CODE']] = EICHER_NEW_ISIN
        # df = df[df['ISIN_CODE']==EICHER_OLD_ISIN]
        # temp['ISIN_CODE'] = EICHER_NEW_ISIN
        if(df[df['ISIN_CODE']==old_isin].empty):
            print("Data uptodate")
        else:
            index = df.index[df['ISIN_CODE']==old_isin][0]
            sc_name = df[df['ISIN_CODE']==old_isin]['SC_NAME'].values[0]
            df.loc[index,'ISIN_CODE'] = new_isin
            temp = df[df['ISIN_CODE']==new_isin][target_columns].div(fv_factor).round(2)
            for c in target_columns:
                df.loc[index,c] = temp[c].values
            # print(index)
            # print(df[df['ISIN_CODE']==EICHER_NEW_ISIN])
            # print(df.head())
            print("Wrting {} with new isin.".format(sc_name))
            df.to_csv(f)
            # temp_df = df[df['ISIN_CODE']==EICHER_NEW_ISIN]
            # # temp_df_target = temp_df[]
            # print(temp_df)

def update_ref_data(data_dict,start,end):
    # Get desired dates from start and end values
    dates = get_dates(start,end)
    # all dates from the given data_dictionary
    data_dates = list(data_dict.keys())
    if(dates[0]<data_dates[0]):
        print("Start date {} is out bounds".format(dates[0].date()))
    if(dates[-1]>data_dates[-1]):
        print("End date {} should be within range.".format(dates[-1].date()))

def get_security_names(data_dict,date):
    date = pd.to_datetime(date)
    #get data for a particular date
    df = data_dict[date]
    return list(df[df['SC_TYPE']=='Q']['SC_NAME'])

def get_weekwise_dates(year=2020,weekNum=1):
    # # initial week number
    # week_n = 0
    # # initialise a dictionary to store list of dates with week num as key
    # week_dates = {}
    dates = get_dates()
    date_list = []
    # loop through all given dates
    for date in dates:
        if date.year == year:
            if date.week == weekNum:
                # append dates to the date list
                date_list.append(date)
    # if the date_list is empty notify that there is no dates belonging to particulr week
    if len(date_list)==0:
        print("Dates for {} the week couldn't found.".format(weekNum))
        return
    # return the list of dates with corresponding weekNum
    return date_list

# get weekly data
def get_weekly_data(weekNum,writeToFile=False,sortOrder='asc'):
    week_dates = get_weekwise_dates(weekNum=weekNum)
    # load data 
    data = load_data(week_dates[0],week_dates[-1])

    # get list of security names from the last day of the week
    df = data[week_dates[-1]]
    sc_name_list = list(df[df['SC_TYPE']=='Q']['SC_NAME'])

    temp_dic = {}
    # now loop through the sc_name_list and fetch data
    for i,s in enumerate(sc_name_list):
        # intialise an array to hold data corresponding to a security
        temp = []
        # get data for the security for the first day and the last day
        s_data_first = get_security_data(data,s,week_dates[0],week_dates[0])
        s_data_last = get_security_data(data,s,week_dates[-1],week_dates[-1])
        d_first = s_data_first['CLOSE'][0]
        d_last = s_data_last['CLOSE'][0]
        # calculate the percentage change that occured
        change = round((d_last - d_first)*100/d_first,2)
        # store data in a list
        temp.append(s)
        temp.append(change)
        # copy the list to a dictionary
        temp_dic[i] = temp

    # convert the dictionary to a dataframe
    df = pd.DataFrame.from_dict(temp_dic,orient='index',columns=['SC_NAME','CHANGE'])
    if sortOrder=='asc':
        sortOrder = True
    elif sortOrder=='des':
        sortOrder = False
    else:
        print("Please input a valid sort order")
        return
    # sort the dataframe based on percentage change
    df = df.sort_values(by='CHANGE',ascending=sortOrder).reset_index(drop=True)
    # print(df.head())

    # write to a file
    if writeToFile:
        file_name = ANALYSIS_OUT_PATH + str(weekNum)+'_weekly_change.csv'
        print("writing to file: ",file_name)
        df.to_csv(file_name)
    # return the dataframe
    return df

def daily_change(date='today',writeToFile=False,sortOrder='asc'):
    data = load_data(date)
    dates = list(data.keys())

    # print(data[dates[0]].head())
    df = data[dates[0]]
    temp_dict = {}
    i = 1
    for index,row in df.iterrows():
        temp = []
        # print(row['SC_NAME'],row['CLOSE'],row['PREVCLOSE'])
        sc_name = row['SC_NAME']
        close = row['CLOSE']
        prevclose = row['PREVCLOSE']
        try:
            day_change = round(100*(close - prevclose)/prevclose,2)
        except ZeroDivisionError:
            day_change = 0
        # print("{}: {}".format(sc_name,day_change))
        temp.append(sc_name)
        temp.append(day_change)
        temp.append(close)
        temp_dict[i] = temp
        i += 1

    # for key in temp_dict:
    #     print(temp_dict[key])
    if sortOrder=='asc':
        sortOrder = True
    elif sortOrder=='des':
        sortOrder = False
    else:
        print("Please input a valid sort order")
        return
    df = pd.DataFrame.from_dict(temp_dict, orient='index',columns=['SC_NAME','CHANGE','CLOSE'])
    df = df.sort_values(by='CHANGE',ascending=sortOrder).reset_index(drop=True)
    # print(df)

    if writeToFile:
        file_name = ANALYSIS_OUT_PATH + str(dates[0]).split()[0]+'_daily_change.csv'
        print("writing to file: ",file_name)
        df.to_csv(file_name)
    
    return df

def sma(df,window=10,column='CLOSE'):
    df = df[column]
    column_name = df.name
    # calculate the simple moving average
    df = df.rolling(window).mean()
    # convert series into dataframe
    df = pd.DataFrame(df).round(2)
    new_column_name = 'SMA_'+str(window)
    df = df.rename(columns={column_name:new_column_name})
    return df

def bollinger_band(df,window=20,column='CLOSE'):
    # get the desired column from the passed data
    df = df[column]
    # get the column name 
    column_name = df.name
    # calculate simple moving average of the window
    sma = df.rolling(window).mean()
    std = df.rolling(window).std()
    # calculate upperband and lower band
    upper_band = sma + 2*std
    lower_band = sma - 2*std
    # convert to dataframe
    sma = pd.DataFrame(sma).round(2)
    upper_band = pd.DataFrame(upper_band).round(2)
    lower_band = pd.DataFrame(lower_band).round(2)
    # rename columns of the dataframe
    sma = sma.rename(columns={column_name:'BB_MEAN'})
    upper_band = upper_band.rename(columns={column_name:'BB_UPPER'})
    lower_band = lower_band.rename(columns={column_name:'BB_LOWER'})
    # now join the bands
    df = sma.join(upper_band)
    df = df.join(lower_band)
    return df


def ema(df,window=21,column='CLOSE'):
    # From the passed dataframe get the prefered column for calculating ema
    df = df[column]
    # get the column name of the series passed 
    column_name = df.name
    # calculate the exponential moving average
    df = df.ewm(span=window, adjust=False).mean()
    # convert series into dataframe
    df = pd.DataFrame(df).round(2)
    # assign a new column name
    new_column_name = 'EMA_'+str(window)
    # rename the dataframe
    df = df.rename(columns={column_name:new_column_name})
    return df

def daily_returns(df):
    close = df['CLOSE']
    prevclose = df['PREVCLOSE']
    # calculate daily returns
    dr = ((close/prevclose)-1)*100
    dr = dr.round(2)
    # convert to a dataframe
    dr = pd.DataFrame(dr,columns=['DAILY_RETURNS'])
    return dr

def plot_linegraph(df,title="Price variation",xlabel="Date",ylabel="Price"):
    ax = df.plot(title=title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend(loc='upper left')
    plt.show()

def plot_histogram(df,title="Histogram"):
    df.hist(bins=20)
    plt.axvline(dr_mean, color='k',linestyle='dashed',linewidth=2)
    plt.axvline(-dr_std, color='r',linestyle='dashed',linewidth=2)
    plt.axvline(dr_std, color='r',linestyle='dashed',linewidth=2)
    plt.show()

"""
Calculate mean daily returns for given number of days, based on the downloaded files
"""
def mean_daily_return(nDays):
    # GET TARGET FILES
    file_names = get_dl_filenames()
    start_date = file_names.index[-nDays]
    end_date = file_names.index[-1]
    print("\n\nStart date: {}, End date: {}".format(start_date,end_date))
    # LOAD DATAFRAME FOR DESIRED TIME PERIOD
    data = load_data(start_date, end_date)
    # GET ALL SECURITY NAMES FOR A PARTICULAR DATE
    sc_namelist = get_security_names(data,end_date)
    dr_mean_list = []
    # LOOP THROUGH ALL SECURITY NAMES
    for sc_name in sc_namelist:
        # print(sc_name)
        # GET SECURITY DATA
        security_data = get_security_data(data,sc_name,start_date,end_date)
        # print(security_data)
        sc_daily_returns = daily_returns(security_data)
        mean_dr = round(sc_daily_returns['DAILY_RETURNS'].mean(),3)
        # print("{}: {}".format(sc_name, mean_dr))
        row_list = []
        row_list.append(sc_name)
        row_list.append(mean_dr)
        dr_mean_list.append(row_list)
        dr_df = pd.DataFrame(dr_mean_list,columns=['SECURITY_NAME','MEAN_DAILY_RETURN'])
    
    return dr_df

""" END OF FUNCTIONS """

if __name__ == "__main__":
    print('test run')
    # df = pd.read_csv(BSE_REF_FILE)
    # print(df.head())
    # dt = pd.bdate_range(start='1/1/2018',end='today',freq='C',holidays=HOLIDAYS)
    # print(type(dt),len(dt))
    # print(dt[0])

    dl_files = get_dl_filenames()
    # start='1/1/2018'
    start = '15-Nov-2020'
    end='today'
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    print(dl_files.loc[start:end])
    
    
    
    # download_files()
    

    # sc = 'ASTERDM     '
    # # sc = 'STATE BANK  '
    # # sc = 'STER TECH   '
    # # sc = 'CAMS        '
    # # sc = 543232
    
    # # print(get_security_data(data_dict,sc,start_date))
    # """ MODIFY DATA FOR EICHER MOTORS AND LAURUS LABS """
    # # modify_data(LAURUSLAB_OLD_ISIN, LAURUSLAB_NEW_ISIN, 10)
    # # modify_data(EICHER_OLD_ISIN, EICHER_NEW_ISIN, 10)
    # # m/d/y
    # start = '9/1/2020'
    # end = '10/23/2020'
    
    # # load data 
    # data = load_data(start,end)
    
    # # get a single security data
    # sc_data = get_security_data(data,sc,start,end)
    # # first_value = sc_data['CLOSE'][0]
    # # print(sc_data['CLOSE']/first_value)
    # # make a copy of the given dataframe
    # # close = sc_data['CLOSE']
    # # print(close)
    # # sma = sma(close,10)
    
    # # ema21 = ema(sc_data,21)
    # # ema9 = ema(sc_data,9)
    
    # # df = ema21.join(ema9)
    # df = bollinger_band(sc_data)
    # df = df.join(sc_data['CLOSE'])
    # print(df)
    # # start_date = pd.to_datetime('1-Sep-2020')
    # # end_date = pd.to_datetime('23-Oct-2020')
    # # sc_data = sc_data[start_date:end_date]
    