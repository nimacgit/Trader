
# !pip3 install xlrd
import pandas as pd
import numpy as np
from pathlib import Path
import os
from threading import Thread
from queue import Queue
import multiprocessing
# !pip3 install khayyam
from khayyam import JalaliDate, JalaliDatetime

xcelLocation = "../xcels/"
!export xcelLocation="../xcels/"
HEADER = ["symbol", "name", "amount", "volume", "value", "lastday", "open", "close",
 "last-change", "last-percent", "ending", "ending-change", "ending-percent",
 "min", "max",]
HEADER_extra = HEADER + ["year", "month", "day", "date"]

!ls $xcelLocation | grep ".xlsx" > xlFiles
tmp = !cat xlFiles
names = [name[:-5] for name in tmp]


def cleaner():
    for name in names:
        if os.path.getsize(xcelLocation + name + '.xlsx') < 10000:
            os.remove(xcelLocation + name + '.xlsx')


def convert(xcelLocation, xlFileName, returnAllMode=False):
    xl = None
    try:
        if (not Path(xcelLocation + xlFileName + '.csv').is_file()) or returnAllMode:
            xl = pd.read_excel(xcelLocation + xlFileName + ".xlsx",
                               header=[0], skiprows=[0,1], convert_float=False)
            xl.columns = HEADER
            xl.to_csv(xcelLocation + xlFileName + '.csv', encoding='utf-8', index=False, header=HEADER)
    except:
        xl = str(xlFileName)
    finally:
        return xl
    
def convertThread(threadname, q, qDFs, qErrors):
    while not q.empty():
        fileNames = q.get()
        q.task_done()
        for name in fileNames:
            tmp = convert(xcelLocation=xcelLocation, xlFileName=name, returnAllMode=True)
            if isinstance(tmp, str):
                qErrors.put(tmp)
            else:
                qDFs.put((tmp.copy(), name))
    print(str(threadname) + " done")

def convert_all(batchSize=10, numThread=16):
    all_dfs = []
    all_df_names = []
    i = 0
    workers = []

    pool = multiprocessing.Pool(processes=numThread)
    m = multiprocessing.Manager()
    queue = m.Queue()
    qDFs = m.Queue()
    qErrors = m.Queue()

    while i*batchSize < len(names):
        if (i+1)*batchSize < len(names):
            queue.put(names[i*batchSize:(i+1)*batchSize])
        else:
            queue.put(names[i*10:])
        i+=1
    print(len(names))
    print(queue.qsize())

    for i in range(numThread):
    #     workers.append(Thread(target=readThread, args=("Thread-" + str(i), queue, qsum, qcount)))
    #     workers.append(pool.apply_async(readThread, ("Thread-" + str(i), queue, qsum, qcount,)))
        workers.append(multiprocessing.Process(target=convertThread, args=("Thread-" + str(i),
                                                                        queue, qDFs, qErrors)))
        workers[i].start()

    for i in range(numThread):
        workers[i].join()

    while not qDFs.empty():
        dftmp, nametmp = qDFs.get()
        all_dfs.append(dftmp)
        all_df_names.append(nametmp)

    errors = []
    while not qErrors.empty():
       errors.append(qErrors.get())
    print(len(all_dfs))
    return all_dfs, all_df_names, errors

def makeMasterTable(all_dfs, all_df_names, chunkSize):
    for index, df in enumerate(all_dfs):
        year, month, day = all_df_names[index].split("-")
        date = JalaliDate(year, month, day).todate()
        yearlist = np.full(len(df), year).tolist()
        monthlist = np.full(len(df), month).tolist()
        daylist = np.full(len(df), day).tolist()
        datelist = np.full(len(df), date).tolist()
        df["year"] = yearlist
        df["month"] = monthlist
        df["day"] = daylist
        df["date"] = datelist
    xl = pd.concat(all_dfs, keys=all_df_names, ignore_index=True)
    xl.columns = HEADER_extra
    xl = xl.astype({"year": int, "month": int, "day": int})
    xl['date'] = pd.to_datetime(xl['date'])
    print(xl.dtypes)
    xl.sort_values(by=['date'], inplace=True)
    xl.reset_index(drop=True, inplace=True)
    i = 0
    while i*chunkSize < len(xl):
        if (i+1)*chunkSize < len(xl):
            df_i = xl.iloc[i*chunkSize:(i+1)*chunkSize]
        else:
            df_i = xl.iloc[i*chunkSize:]
        df_i.to_csv('{xcelLocation}master{i}.csv'.format(i=i, xcelLocation=xcelLocation),
                    header=HEADER_extra, encoding='utf-8', index=False)
        i += 1
    return xl

def write_errors(errors):
    with open("errors", 'w') as error_file:
    for error in errors:
        error_file.write(str(error))
        error_file.write("\n")

        
def error_cleaner():
    for name in errors:
        os.remove(xcelLocation + name + '.xlsx')
