# Import statements
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandarallel import pandarallel
import time as tm

# Reads a stored CSV of pname, LI, company name, website
df = pd.read_csv('webTitles.csv')
pandarallel.initialize()

'''
Method to extract a website title given a website string of the form: http://***
    @param: website: a String for a website to search
    @return: a String for the title text for the website
'''
def getWebsiteTitle(website):
    # Check to make sure website starts with http
    if website[:4] != "http":
        website = "http://" + website
    # Extracts web page and returns title text if found. Timeout parameters.
    try:
        s_ = BeautifulSoup(requests.get(website, timeout=(1.5, 3.0)).text).title.text
        return s_
    except:
        return "N/A"

# Populates a dataframe (df) with the column 'webtitle' which represents the title text
# for the website. Print statements are every 100 websites to keep track of progress.
# This function is parallelized in batches of 100.
num_buckets = df.shape[0]
num_buckets = int(num_buckets / 100)
for i in range(9,num_buckets):
    print(i)
    tm.sleep(2)
    start = 100 * i ; end = 100 * (i+1)
    df['webtitle'] = df['website'][start:end].parallel_apply(lambda x: getWebsiteTitle(x))
    df[start:end].to_csv('webtitles_final.csv', mode='a', index=False, header=False)

# After the previous cell, we re-load the saved CSV file and drop any duplicates.
# Then, the new file is saved and should be converted to a .xlsx file and put through
# an online converted back to a CSV (newfile.csv). This is because some of the spacing
# for the dataframe CSV is a bit off.
df2 = pd.read_csv('webtitles_final.csv', names=['pname', 'LI', 'name', 'website', 'webtitle'])

inds = []
for i in range(df.shape[0]):
    try:
        if df2.loc[i].pname != df.loc[i].pname:
            inds.append(i)
    except:
        print(i)

df2.drop_duplicates(subset=df2.columns[0:2], keep=False)
df2.to_csv('webtitles_final.csv', index=False)

# From the newly converted CSV (newfile.csv), columns 5-10 are condensed and re-saved.
df = pd.read_csv('newfile.csv', names=['pname', 'LI', 'name', 'website',
                                       'webtitle','5','6','7','8','9','10'])
df.fillna("", inplace=True)
df["webtitle"] = df[["webtitle", "5", "6", "7", "8", "9", "10"]].agg(' '.join, axis=1)
df = df.drop(columns=['5', '6', '7', '8', '9', '10'])
df.to_csv('myfile.csv', index=False)
