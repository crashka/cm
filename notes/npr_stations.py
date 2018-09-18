
from bs4 import BeautifulSoup

infile  = 'npr_stations.html'
outfile = 'npr_stations.tsv'

with open(infile) as f:
    soup = BeautifulSoup(f)

"""
stnlist = soup.html.find(class_="mainlist", id="streamItems")
stn = stnlist.find('li', class_='streamItem')

info = stn.find('span', class_='stninfo')
info.find('span', class_='lnk').string
info.find('span', class_='stntag').string
info.find('span', class_='stncity').string
stn.find('a', class_='streamslnk')['href']

desc = stn.find('div', id='descriptionTxt4')
det = desc.find('div', class_='details')
name = det.find('span', class_='name').string
loc = det.find('span', class_='location').string
tagline = det.find('p', class_='tagline').string
"""

with open(outfile, 'w') as f:
    stnlist = soup.html.find(class_="mainlist", id="streamItems")
    for stn in stnlist.find_all(class_='streamItem'):
        info        = stn.find(class_='stninfo')
        stn_id      = info.find(class_='lnk').string
        stn_tag     = info.find(class_='stntag').string
        stn_city    = info.find(class_='stncity').string
        streamslnk  = stn.find(class_='streamslnk')['href'].rstrip()

        desc        = stn.find('div', id='descriptionTxt4')
        det         = desc.find('div', class_='details')
        stn_name    = det.find('span', class_='name').string
        stn_loc     = det.find('span', class_='location').string
        stn_tagline = det.find('p', class_='tagline').string

        f.write("%s\t%s\t%s\t%s\t%s\n" % (stn_id, stn_name, stn_loc, stn_tagline, streamslnk))
