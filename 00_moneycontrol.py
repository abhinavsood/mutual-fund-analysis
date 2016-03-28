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

money_control_root = 'http://www.moneycontrol.com'

# Get 10 mutual fund families with the highest Assets under Management from Money Control
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
            'fund_family_name'      : fund['fund_family_name'],
            'fund_family_url'       : fund['fund_family_url' ],
            'fund_family_aum'       : fund['fund_family_aum' ],
            'fund_family_shortcode' : fund['fund_family_shortcode'],
            'scheme_name'           : unicode( data_elems[0].a.string ),
            'scheme_url'            : money_control_root + data_elems[0].a.attrs['href'],
            'crisil_rating'         : unicode( data_elems[1].a.string ),
            'category'              : category_name,
            'category_url'          : category_url,
            'latest_nav'            : unicode( data_elems[3].string ),
            '1yr_return'            : u'None' if unicode( data_elems[4].string ) == u'--' else unicode( data_elems[4].string ),
            'scheme_aum'            : scheme_aum
        }

        fund_schemes.append( fscheme_dict )


# Scrape useful data such as Riskometer Rating (Risk Classification),
# relative and absolute returns and benchmark information for each scheme
for idx, scheme in enumerate(fund_schemes):
    # Read the page at the URL for each scheme
    markup = requests.get( scheme['scheme_url'] ).text
    soup = BeautifulSoup(markup, "lxml")


    # Riskometer (Risk Rating)
    scheme['scheme_risk_text'] = unicode(soup.select('.header .MT10 .toplft_cl3 p.avgbgtit')[0].string )

    # Scheme Plan and Scheme Option
    scheme_plan_option_data    = [unicode( x.string ).strip() for x in soup.select('#planname_frm .FL span')]
    
    [scheme['scheme_plan'],
     scheme['scheme_option'] ] = scheme_plan_option_data if scheme_plan_option_data else [u'None', u'None']


    # From the Investment Info section, collect scheme fund type,
    # benchmark name, minimum investment required for this scheme,
    # last dividend or bonus, if paid else NA
    sub_soup = soup.select('.mainCont .tog_cont .MT20 .FL td')
    [scheme['scheme_fund_type'],
     scheme['scheme_benchmark'],
     scheme['scheme_min_investment'],
     scheme['scheme_last_dividend'],
     scheme['scheme_bonus'] ] = [
        unicode(x.string).strip() if( x.string and unicode(x.string).strip() != u'N.A.' ) else u'None' for x in [
            sub_soup[0],
            sub_soup[3],
            sub_soup[5],
            sub_soup[6],
            sub_soup[7]
        ]
    ]



    # From the performance section, gather
    # Fund Returns, Category Avg, Difference of Fund Returns and Category Returns
    # Best of category and worst of category
    sub_soup = soup.select('.mainCont .tog_cont table')[0]

    # Get the relevant table rows containing this information
    rows = [row for row in sub_soup if not row.string and unicode(row).strip()][1:]

    for row in rows:
        row_attrs = [x for x in row.children if unicode(x).strip()]

        row_name  = unicode(row_attrs[0].string).strip().lower()

        # fund returns
        if row_name == 'fund returns':
            scheme['fund_ret_1m']      = u'None' if unicode( row_attrs[1].string ) == u'--' else unicode( row_attrs[1].string )
            scheme['fund_ret_3m']      = u'None' if unicode( row_attrs[2].string ) == u'--' else unicode( row_attrs[2].string )
            scheme['fund_ret_6m']      = u'None' if unicode( row_attrs[3].string ) == u'--' else unicode( row_attrs[3].string )
            scheme['fund_ret_1y']      = u'None' if unicode( row_attrs[4].string ) == u'--' else unicode( row_attrs[4].string )
            scheme['fund_ret_2y']      = u'None' if unicode( row_attrs[5].string ) == u'--' else unicode( row_attrs[5].string )
            scheme['fund_ret_3y']      = u'None' if unicode( row_attrs[6].string ) == u'--' else unicode( row_attrs[6].string )
            scheme['fund_ret_5y']      = u'None' if unicode( row_attrs[7].string ) == u'--' else unicode( row_attrs[7].string )

        # category avg
        if row_name == 'category avg':
            scheme['cat_avg_ret_1m']   = u'None' if unicode( row_attrs[1].string ) == u'--' else unicode( row_attrs[1].string )
            scheme['cat_avg_ret_3m']   = u'None' if unicode( row_attrs[2].string ) == u'--' else unicode( row_attrs[2].string )
            scheme['cat_avg_ret_6m']   = u'None' if unicode( row_attrs[3].string ) == u'--' else unicode( row_attrs[3].string )
            scheme['cat_avg_ret_1y']   = u'None' if unicode( row_attrs[4].string ) == u'--' else unicode( row_attrs[4].string )
            scheme['cat_avg_ret_2y']   = u'None' if unicode( row_attrs[5].string ) == u'--' else unicode( row_attrs[5].string )
            scheme['cat_avg_ret_3y']   = u'None' if unicode( row_attrs[6].string ) == u'--' else unicode( row_attrs[6].string )
            scheme['cat_avg_ret_5y']   = u'None' if unicode( row_attrs[7].string ) == u'--' else unicode( row_attrs[7].string )

        # difference of fund returns and category returns
        if row_name == 'difference of fund returns and category returns':
            scheme['diff_fund_cat_1m'] = u'None' if unicode( row_attrs[1].string ) == u'--' else unicode( row_attrs[1].string )
            scheme['diff_fund_cat_3m'] = u'None' if unicode( row_attrs[2].string ) == u'--' else unicode( row_attrs[2].string )
            scheme['diff_fund_cat_6m'] = u'None' if unicode( row_attrs[3].string ) == u'--' else unicode( row_attrs[3].string )
            scheme['diff_fund_cat_1y'] = u'None' if unicode( row_attrs[4].string ) == u'--' else unicode( row_attrs[4].string )
            scheme['diff_fund_cat_2y'] = u'None' if unicode( row_attrs[5].string ) == u'--' else unicode( row_attrs[5].string )
            scheme['diff_fund_cat_3y'] = u'None' if unicode( row_attrs[6].string ) == u'--' else unicode( row_attrs[6].string )
            scheme['diff_fund_cat_5y'] = u'None' if unicode( row_attrs[7].string ) == u'--' else unicode( row_attrs[7].string )

        # best of category
        if row_name == 'best of category':
            scheme['cat_best_1m']      = u'None' if unicode( row_attrs[1].string ) == u'--' else unicode( row_attrs[1].string )
            scheme['cat_best_3m']      = u'None' if unicode( row_attrs[2].string ) == u'--' else unicode( row_attrs[2].string )
            scheme['cat_best_6m']      = u'None' if unicode( row_attrs[3].string ) == u'--' else unicode( row_attrs[3].string )
            scheme['cat_best_1y']      = u'None' if unicode( row_attrs[4].string ) == u'--' else unicode( row_attrs[4].string )
            scheme['cat_best_2y']      = u'None' if unicode( row_attrs[5].string ) == u'--' else unicode( row_attrs[5].string )
            scheme['cat_best_3y']      = u'None' if unicode( row_attrs[6].string ) == u'--' else unicode( row_attrs[6].string )
            scheme['cat_best_5y']      = u'None' if unicode( row_attrs[7].string ) == u'--' else unicode( row_attrs[7].string )

        # worst of category
        if row_name == 'worst of category':
            scheme['cat_worst_1m']     = u'None' if unicode( row_attrs[1].string ) == u'--' else unicode( row_attrs[1].string )
            scheme['cat_worst_3m']     = u'None' if unicode( row_attrs[2].string ) == u'--' else unicode( row_attrs[2].string )
            scheme['cat_worst_6m']     = u'None' if unicode( row_attrs[3].string ) == u'--' else unicode( row_attrs[3].string )
            scheme['cat_worst_1y']     = u'None' if unicode( row_attrs[4].string ) == u'--' else unicode( row_attrs[4].string )
            scheme['cat_worst_2y']     = u'None' if unicode( row_attrs[5].string ) == u'--' else unicode( row_attrs[5].string )
            scheme['cat_worst_3y']     = u'None' if unicode( row_attrs[6].string ) == u'--' else unicode( row_attrs[6].string )
            scheme['cat_worst_5y']     = u'None' if unicode( row_attrs[7].string ) == u'--' else unicode( row_attrs[7].string )

    # Print every 100th scheme to verify things are running smoothly
    if idx % 100 == 0:
        print( 'Scheme # {0}\n{1}\n\n\n'.format(idx, scheme) )
