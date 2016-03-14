# pip install beautifulsoup4    # Download and install beautiful soup 4
# pip install lxml              # Download and install lxml for its XML and HTML parser
# pip install requests          # Download and install Python requests module


from bs4 import BeautifulSoup
import requests
import sys

# from pyspark import SparkConf, SparkContext, SQLContext
# from pyspark.sql.types import *
#
# conf = SparkConf().setAppName('Project')
# sc = SparkContext(conf=conf)
# sqlContext = SQLContext(sc)

# Get 10 mutual fund families with the highest Assets under Management from Money Control
money_control_root = 'http://www.moneycontrol.com'

markup = requests.get(money_control_root + '/mutual-funds/amc-assets-monitor').text

# make the soup
soup = BeautifulSoup(markup, "lxml")

# the table that contains the required data
table = soup.find_all('table', attrs = {"class": "tblfund1"})[0]

# get the first ten rows in this table, excluding
# the first row as it has only header information
rows = table.find_all('tr')[1:11]


# fund_families_schema = StructType([
#     StructField("fund_family", StringType(), True),
#     StructField("fund_family_url", StringType(), True),
#     StructField("fund_family_aum", StringType(), True)
# ])

# Fund Family and Assets under Management (Rs. Cr.) for the top 10 mutual fund families
fund_families = []
for r in rows:
    ff_dict = {
        'fund_family_name': unicode( r.contents[1].a.string ),
        'fund_family_url' : unicode( money_control_root + r.contents[1].a.attrs['href'] ),
        'fund_family_aum' : unicode( r.contents[5].string ),
        'fund_family_shortcode' : unicode( money_control_root + r.contents[1].a.attrs['href'] ).split('/')[-1]
    }
    
    fund_families.append( ff_dict )





# For each fund family, get a list of all fund schemes along with other details
fund_schemes = []
for fund in fund_families:
    
    markup = requests.get( fund['fund_family_url'] ).text

    soup = BeautifulSoup(markup, "lxml")

    rows = soup.select('.FL.MT10.boxBg table tr')[1:-1]
    
    for r in rows:
        data_elems      = r.find_all('td')
    
        category_name   = ''
        scheme_aum      = ''
        category_url    = ''
        
        try:
            category_name   = unicode( data_elems[2].a.string )
            category_url    = money_control_root + data_elems[2].a.attrs['href']
            
        except AttributeError:
            category_name   = u'None'
            category_url    = u'None'
    
        try:
            scheme_aum = unicode( data_elems[5].string )
        except AttributeError:
            scheme_aum = u'None'

        fscheme_dict    = {
            'fund_family_name'  : fund['fund_family_name'],
            'fund_family_url'   : fund['fund_family_url' ],
            'fund_family_aum'   : fund['fund_family_aum' ],
            'fund_family_shortcode' fund['fund_family_shortcode'],
            'scheme_name'       : unicode( data_elems[0].a.string ),
            'scheme_url'        : money_control_root + data_elems[0].a.attrs['href'],
            'crisil_rating'     : unicode( data_elems[1].a.string ),
            'category'          : category_name,
            'category_url'      : category_url,
            'latest_nav'        : unicode( data_elems[3].string ),
            '1yr_return'        : unicode( data_elems[4].string ),
            'scheme_aum'        : scheme_aum
        }

        fund_schemes.append( fscheme_dict )
    

print( len( fund_schemes ) )




# ^\s*$\n replace all empty lines