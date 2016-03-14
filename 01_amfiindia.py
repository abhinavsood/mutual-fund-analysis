from bs4 import BeautifulSoup
import requests, re, itertools, csv

#
# Build a dictionary of Mutual Fund families mapped to their AMFI IDs
#
response        = requests.get('https://www.amfiindia.com/research-information/other-data/scheme-details').text
soup            = BeautifulSoup(response, 'lxml')
options         = soup.select('select#MFName > option')
fund_id_dict    = {}

for option in options:
    if option['value'].strip():
        fund_id_dict[ unicode(option.string) ]  = option['value']


#
# Get latest NAV and details for all mutual fund schemes published by AMFI
#
response    = requests.get('http://portal.amfiindia.com/spages/NAV0.txt').text
raw_data    = [line for line in response.split('\n') if line.strip()]

# Headings come from the first line of the data
headings    = [heading.strip() for heading in raw_data[0].split(';')]

# Add scheme classification, fund family name and fund family ID as headings
headings.extend([
    u'Scheme Classification',
    u'Scheme Type',
    u'Scheme Category',
    u'Fund Family',
    u'Fund ID',
    u'Scheme Short Name'
])


final_data      = []
check_next      = False
mf_scheme_type  = u''
mf_family       = u''


for line in raw_data[1:]:
    if line.find(';') == -1:
        
        if check_next:
            mf_scheme_type  = mf_family.strip()
            check_next      = False
        else:
            check_next      = True

        mf_family   = line.strip()
        
    else:
        check_next  = False

        row = [element.strip() for element in line.split(';') ]
        row.extend( [
            mf_scheme_type,
            re.search( r'(^.*)\(',  mf_scheme_type).group(1),
            re.search( r'\((.*)\)', mf_scheme_type).group(1),
            mf_family,
            fund_id_dict[mf_family],
            ' '.join( row[3].split() ).split('-')[0].strip()
        ] )
        
        final_data.append( dict( itertools.izip( headings, row ) ) )
        
# Clean the data
for idx, row in enumerate(final_data):
    # Step 1: In the scheme name,
    # Convert multiple spaces into one
    final_data[idx]['Scheme Name']  = ' '.join( row['Scheme Name'].split() )
    

# Write the final data as CSV delimited by ;
with open('amfi.csv', 'wb') as f:  # Just use 'w' mode in 3.x
    w = csv.DictWriter(f, headings, delimiter = ';')
    w.writeheader()
    w.writerows(final_data)