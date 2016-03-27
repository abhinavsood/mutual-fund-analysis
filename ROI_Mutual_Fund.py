__author__ = 'ndeep'

# pip install beautifulsoup4
# pip install lxml
# pip install requests


#######################################################################################################################
# import the required python modules
#######################################################################################################################

from bs4 import BeautifulSoup
import requests
import sys, csv

#######################################################################################################################
# Fetch the Scheme url
#######################################################################################################################

input = 'F:/Semester 2/Bigdata Lab/Project/mutual-fund-analysis-master/data/fund_scheme_data.csv'
outputPath ='F:/Semester 2/Bigdata Lab/Project/mutual-fund-analysis-master/data/fund_return_data/'

scheme_url = []
scheme_names = []

with open(input, 'r') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',')
    for row in spamreader:
        scheme_url.append(row[5])
        scheme_names.append(row[4])


#######################################################################################################################
# Scrape the web pages and fetch the mutual fund NAV historical data for  the given date range
#######################################################################################################################

ind = 0
sch_name =''
for url in scheme_url:

    # Get the scheme name
    sch_name = scheme_names[ind]
    ind += 1

    # download the webpage
    response = requests.get(url).text

    # make the soup tree
    soup = BeautifulSoup(response, 'lxml')

    # fetching the rows containing table data
    div1 = soup.find("div",id="mmcnt")
    div2 = div1.findAll("td")

    # Fetch all the data available into an array
    arr_data=[]
    for td in div2:
        try:
            data = unicode(td.find(text=True))
        except AttributeError:
            data = u'None'
        arr_data.append(data)

    # get the required data
    data = arr_data[48:104]

    ####################################################################################################################
    # function to verify whether the data is of float type or not
    ####################################################################################################################

    def is_float(x):
        if(x == u'' or x == u'--' or x == u' '):
            return 2
        else:
            try:
                f=float(x)
            except:
                return 0
        return 1


    ####################################################################################################################
    #Declare and initialize the required variables
    ####################################################################################################################

    # flag & counters
    flag = 1
    index =0

    #initialize fund data

    Category = u''
    One_mth = u''
    Three_mth = u''
    Six_mth = u''
    One_year= u''
    Two_year= u''
    Three_year= u''
    Five_year= u''

    fund_data = []
    fund_dict={}

    for item in data:
        flag = is_float(item)
        if(flag>0):
            index+=1
            if(flag==2):
                item=u'NA'
            if(index%7==1):
                One_mth = unicode(item)
            elif(index%7==2):
                Three_mth = unicode(item)
            elif(index%7==3):
                Six_mth = unicode(item)
            elif(index%7==4):
                One_year = unicode(item)
            elif(index%7==5):
                Two_year = unicode(item)
            elif(index%7==6):
                Three_year = unicode(item)
            elif(index==7):
                Five_year = unicode(item)
                fund_dict = {
                    'Category' : Category,
                    '1_mth' : One_mth,
                    '3_mth' : Three_mth,
                    '6_mth' : Six_mth,
                    '1_year': One_year,
                    '2_year': Two_year,
                    '3_year': Three_year,
                    '5_year': Five_year
                }
                fund_data.append(fund_dict)
                index = 0
        else:
            Category = unicode(item)


    ####################################################################################################################
    # Save the data to a csv file
    ####################################################################################################################

    resultFile = open(outputPath+sch_name+'.csv','wb')
    fieldnames = ['Category','1_mth','3_mth','6_mth','1_year','2_year','3_year','5_year']
    wr = csv.DictWriter(resultFile, fieldnames=fieldnames)
    wr.writeheader()
    for item in fund_data:
        wr.writerow(item)


