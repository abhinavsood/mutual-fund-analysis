# pip install beautifulsoup4
# pip install lxml
# pip install requests


#######################################################################################################################
# import the required python modules
#######################################################################################################################

from bs4 import BeautifulSoup
import requests
import sys, csv
from datetime import date
from dateutil.rrule import rrule, DAILY


####################################################################################################################
# function to verify whether the data is of float type or not
####################################################################################################################

def is_float(x):
    try:
        f=float(x)
    except:
        return 0
    return 1

#######################################################################################################################
# Generating the date_list
#######################################################################################################################

a = date(2016, 01, 01)  # set start_date
b = date(2016, 03, 24)  # set end_date
date_list=[]
for dt in rrule(DAILY, dtstart=a, until=b):
    date_list.append(dt.strftime("%d-%b-%Y"))


#######################################################################################################################
# Generating the URL
#######################################################################################################################
# page_home = 'http://portal.amfiindia.com/NavHistoryReport_Rpt_Po.aspx?rpt=0&frmdate='
# url_list=[]
# for d in date_list:
#     url_list.append(page_home+d)


#######################################################################################################################
# initialize the page home url and output path
#######################################################################################################################

page_home = 'http://portal.amfiindia.com/NavHistoryReport_Rpt_Po.aspx?rpt=0&frmdate='

outputPath ='F:/Semester 2/Bigdata Lab/Project/mutual-fund-analysis-master/data/NAV_hist_data/'

#######################################################################################################################
# Scrap the web pages and fetch the mutual fund NAV historical data for  the given date range
#######################################################################################################################

for d in date_list:
    # download the webpage
    response = requests.get(page_home+d).text
    # make the soup tree
    soup = BeautifulSoup(response, 'lxml')


    # fetching the rows containing table data
    form = soup.find("form", id = "aspnetForm")
    div = form.find("div",id="adBodyBlank")
    tbl = div.find("table")
    rows = tbl.findAll('tr')

    dict=[]
    for tr in rows:
        cols = tr.findAll('td')
        for td in cols:
            try:
                data = unicode(td.find(text=True))
            except AttributeError:
                data = u'None'
            dict.append(data)

    data = dict[9:-1]



    ####################################################################################################################
    #Declare and initialize the required variables
    ####################################################################################################################

    fund_type_list = [u'Axis Liquid Fund',u'BOI AXA Liquid Fund',u'Canara Robeco Liquid',u'Indiabulls Liquid Fund',u'Sahara Liquid Fund',u'SBI DEBT FUND SERIES - 16 MONTHS - 1',u'SBI DEBT FUND SERIES - 16 MONTHS - 2',u'SBI DEBT FUND SERIES - 17 MONTHS - 1',u'SBI DEBT FUND SERIES - 18 MONTHS - 12',u'SBI DEBT FUND SERIES - 18 MONTHS - 13',u'SBI DEBT FUND SERIES - 36 MONTHS - 4',u'SBI DEBT FUND SERIES - 36 MONTHS - 5',u'SBI DEBT FUND SERIES - 36 MONTHS - 6',u'SBI DEBT FUND SERIES - 36 MONTHS - 7',u'SBI DEBT FUND SERIES - 366 DAYS - 34',u'SBI DEBT FUND SERIES - 366 DAYS - 35',u'SBI DEBT FUND SERIES - 366 DAYS - 36',u'SBI DEBT FUND SERIES - 366 DAYS - 37',u'SBI DEBT FUND SERIES - 366 DAYS - 38',u'SBI DEBT FUND SERIES - 366 DAYS - 39',u'SBI DEBT FUND SERIES - 366 DAYS - 40',u'SBI DEBT FUND SERIES - 366 DAYS - 41',u'SBI DEBT FUND SERIES - 366 DAYS - 42',u'SBI DEBT FUND SERIES - 366 DAYS - 44',u'SBI DEBT FUND SERIES - 366 DAYS - 45',u'SBI DEBT FUND SERIES - 366 DAYS - 46',u'SBI DEBT FUND SERIES - 366 DAYS - 47',u'SBI DEBT FUND SERIES - 366 DAYS - 48',u'SBI DEBT FUND SERIES - 366 DAYS - 49',u'SBI DEBT FUND SERIES - 366 DAYS - 51',u'SBI DEBT FUND SERIES - 366 DAYS - 52',u'SBI DEBT FUND SERIES - 366 DAYS - 53',u'SBI DEBT FUND SERIES - 366 DAYS - 54',u'SBI DEBT FUND SERIES - 60 MONTHS - 2',u'SBI DEBT FUND SERIES - 60 MONTHS - 3',u'SBI DEBT FUND SERIES A - 1',u'SBI DEBT FUND SERIES A - 10 (400 days)',u'SBI DEBT FUND SERIES A - 11 (385 days)',u'SBI DEBT FUND SERIES A - 13 (366 days)',u'SBI DEBT FUND SERIES A - 14 (380 days)',u'SBI DEBT FUND SERIES A - 15 (745 days)',u'SBI DEBT FUND SERIES A - 16 (366 days)',u'SBI DEBT FUND SERIES A - 17 (366 days)',u'SBI DEBT FUND SERIES A - 18 (366 days)',u'SBI DEBT FUND SERIES A - 19 (366 days)',u'SBI DEBT FUND SERIES A - 2 (15 Months)',u'SBI Debt Fund Series A - 20 (366 days)',u'SBI DEBT FUND SERIES A - 21 (366 days)',u'SBI DEBT FUND SERIES A - 22 (366 days)',u'SBI DEBT FUND SERIES A - 23 (36 Months)',u'SBI DEBT FUND SERIES A - 24 (366 Days)',u'SBI DEBT FUND SERIES A - 25 (366 Days)',u'SBI DEBT FUND SERIES A - 26 (682 Days)',u'SBI DEBT FUND SERIES A - 27 (366 Days)',u'SBI DEBT FUND SERIES A - 28 (367 DAYS)',u'SBI DEBT FUND SERIES A - 3 (420 days)',u'SBI DEBT FUND SERIES A - 31 (367 Days)',u'SBI DEBT FUND SERIES A - 32 (367 Days)',u'SBI DEBT FUND SERIES A - 33 (36 Months)',u'SBI DEBT FUND SERIES A - 34 (367 Days)',u'SBI DEBT FUND SERIES A - 35 (369 Days)',u'SBI DEBT FUND SERIES A - 36 (36 Months)',u'SBI DEBT FUND SERIES A - 38 (1100 Days)',u'SBI DEBT FUND SERIES A - 39 (1100 DAYS)',u'SBI DEBT FUND SERIES A - 4 (786 days)',u'SBI DEBT FUND SERIES A - 40 (1100 DAYS)',u'SBI DEBT FUND SERIES A - 42 (1111 DAYS)',u'SBI DEBT FUND SERIES A - 43 (1111 DAYS)',u'SBI DEBT FUND SERIES A - 44 (1111 DAYS)',u'SBI DEBT FUND SERIES A - 5 (411 DAYS)',u'SBI DEBT FUND SERIES A - 6 (760 DAYS)',u'SBI DEBT FUND SERIES A - 7 (3 Years)',u'SBI DEBT FUND SERIES A - 9 (366 days)',u'SBI DEBT FUND SERIES B - 1 (1111 DAYS)',u'SBI DEBT FUND SERIES B - 16 (1100 Days)',u'SBI DEBT FUND SERIES B - 17 (1100 DAYS)',u'SBI DEBT FUND SERIES B - 18 (1100 DAYS)',u'SBI DEBT FUND SERIES B - 19 (1100 Days)',u'SBI DEBT FUND SERIES B - 2 (1111 DAYS)',u'SBI DEBT FUND SERIES B - 20 (1100 DAYS)',u'SBI DEBT FUND SERIES B - 22 (1100 DAYS)',u'SBI DEBT FUND SERIES B - 23 (1100 DAYS)',u'SBI DEBT FUND SERIES B - 25 (1100 DAYS)',u'SBI DEBT FUND SERIES B - 26 (1100 DAYS)',u'SBI DEBT FUND SERIES B - 27 (1100 DAYS)',u'SBI DEBT FUND SERIES B - 28 (1100 DAYS)',u'SBI DEBT FUND SERIES B - 29 (1200 DAYS)',u'SBI DEBT FUND SERIES B - 3 (1111 DAYS)',u'SBI DEBT FUND SERIES B - 31 (1200 DAYS)',u'SBI Debt Fund Series B - 32 (60 Days)',u'SBI Debt Fund Series B - 33 (1131 Days)',u'SBI Debt Fund Series B - 34 (1131 Days)',u'SBI DEBT FUND SERIES B - 4 (1111 days)',u'SBI DEBT FUND SERIES B - 6 (1111 DAYS)',u'SBI DEBT FUND SERIES B - 7 (38 MONTHS)',u'SBI DEBT FUND SERIES B - 8 (1105 DAYS)',u'SBI DEBT FUND SERIES B - 9 (1105 DAYS)',u'SBI Dual Advantage Fund - Series I',u'SBI Dual Advantage Fund - Series II',u'SBI Dual Advantage Fund - Series III',u'SBI DUAL ADVANTAGE FUND - SERIES IV',u'SBI DUAL ADVANTAGE FUND - SERIES IX',u'SBI DUAL ADVANTAGE FUND - SERIES V',u'SBI DUAL ADVANTAGE FUND - SERIES VI',u'SBI DUAL ADVANTAGE FUND - SERIES VII',u'SBI DUAL ADVANTAGE FUND - SERIES VIII',u'SBI DUAL ADVANTAGE FUND - SERIES X',u'SBI DUAL ADVANTAGE FUND - SERIES XI',u'SBI DUAL ADVANTAGE FUND - SERIES XII',u'SBI DUAL ADVANTAGE FUND - SERIES XIII',u'SBI DUAL ADVANTAGE FUND -SERIES-XIV',u'SBI EQUITY OPPORTUNITIES FUND - SERIES I',u'SBI EQUITY OPPORTUNITIES FUND - SERIES II',u'SBI EQUITY OPPORTUNITIES FUND - SERIES IV',u'SBI MAGNUM INSTA CASH FUND - WEEKLY DIVIDEND',u'SBI MAGNUM INSTA CASH FUND - DAILY DIVIDEND',u'SBI MAGNUM INSTA CASH FUND - GROWTH PLAN',u'SBI MAGNUM INSTA CASH FUND - LIQUID FLOATER - DIVIDEND',u'SBI MAGNUM INSTA CASH FUND - LIQUID FLOATER - GROWTH',u'SBI PREMIER LIQUID FUND - DAILY DIVIDEND',u'SBI PREMIER LIQUID FUND - FORTNIGHTLY DIVIDEND',u'SBI PREMIER LIQUID FUND - GROWTH',u'SBI Premier Liquid Fund - Institutional - Fortnightly Dividend',u'SBI Premier Liquid Fund - Institutional - Weekly Dividend',u'SBI Premier Liquid Fund - Institutional - Daily Dividend',u'SBI Premier Liquid Fund - Institutional - Growth',u'SBI PREMIER LIQUID FUND - WEEKLY DIVIDEND']
    fund_family_List = [u'Axis Mutual Fund',u'BOI AXA Mutual Fund',u'Canara Robeco Mutual Fund',u'Indiabulls Mutual Fund',u'Sahara Mutual Fund',u'SBI Mutual Fund',u'Sundaram Mutual Fund',u'Tata Mutual Fund',u'Union KBC Mutual Fund',u'UTI Mutual Fund']

    # flag & counters
    flag = 1
    index =0

    #initialize fund data
    fund_type=u''
    fund_family=u''
    scheme_name=u''
    NAV=u''
    Repurchase_Price=u''
    Sale_Price=u''
    DateNtime=u''
    fund_data = []
    fund_dict={}

    for item in data:
        if (item in fund_family_List) or (item in fund_type_list):
            if (item in fund_family_List):
                fund_family = item
            elif(item in fund_type_list):
                fund_type = item
        elif item == u'\xa0':
            continue
        else:
            flag = is_float(item)
            if(flag):
                index+=1
                if(index%3==1):
                    NAV = item
                elif(index%3==2):
                    Repurchase_Price = item
                elif(index==3):
                    Sale_Price = item
                    fund_dict = {
                        'Scheme Name'     : scheme_name,
                        #'Fund Family'     : fund_family,
                        #'Fund Type'       : fund_type,
                        'NAV'             : NAV,
                        'Repurchase_Price': Repurchase_Price,
                        'Sale_Price'      : Sale_Price,
                        'Date'            : d
                    }
                    fund_data.append(fund_dict)
                    index = 0
            else:
                scheme_name = item


    ####################################################################################################################
    # Save the data to a csv file
    ####################################################################################################################

    resultFile = open(outputPath + d +'.csv','wb')
    fieldnames = ['Scheme Name','NAV','Repurchase_Price','Sale_Price','Date']
    wr = csv.DictWriter(resultFile, fieldnames=fieldnames)
    wr.writeheader()
    for item in fund_data:
        wr.writerow(item)
    fund_data = []
