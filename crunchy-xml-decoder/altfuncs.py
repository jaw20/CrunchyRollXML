#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
import sys
from time import sleep
import urlparse
from ConfigParser import ConfigParser
import pickle

import requests


def config():
    global video_format
    global resolution
    configr = ConfigParser()
    configr.read('settings.ini')
    quality = configr.get('SETTINGS', 'video_quality')
    qualities = {'android': ['107', '71'], '360p': ['106', '60'], '480p': ['106', '61'],
                 '720p': ['106', '62'], '1080p': ['108', '80'], 'highest': ['0', '0']}
    video_format = qualities[quality][0]
    resolution = qualities[quality][1]
    global lang
    global lang2
    lang = configr.get('SETTINGS', 'language')
    lang2 = configr.get('SETTINGS', 'language2')
    langd = {'Espanol_Espana': u'Español (Espana)', 'Francais': u'Français (France)', 'Portugues': u'Português (Brasil)',
            'English': u'English', 'Espanol': u'Español', 'Turkce': u'Türkçe', 'Italiano': u'Italiano',
            'Arabic': u'العربية', 'Deutsch': u'Deutsch'}
    lang = langd[lang]
    lang2 = langd[lang2]
    forcesub = configr.getboolean('SETTINGS', 'forcesubtitle')
    global forceusa
    forceusa = configr.getboolean('SETTINGS', 'forceusa')
    global localizecookies
    localizecookies = configr.getboolean('SETTINGS', 'localizecookies')
    onlymainsub = configr.getboolean('SETTINGS', 'onlymainsub')
    return [lang, lang2, forcesub, forceusa, localizecookies, quality, onlymainsub]


#def playerrev(url):
#    global player_revision 
#
#    revision_regex = r"swfobject.embedSWF\(\"(?:.+)'(?P<revision>[\d.]+)'(?:.+)\)"
#    try:
#        player_revision = re.search(revision_regex, gethtml(url)).group("revision")
#    except IndexError:
#        try:
#            url += '?skip_wall=1'  # perv
#            html = gethtml(url)
#            player_revision = re.search(revision_regex, html).group("revision")
#        except IndexError:
#            open('debug.html', 'w').write(html.encode('utf-8'))
#            sys.exit('Sorry, but it looks like something went wrong with accessing the Crunchyroll page. Please make an issue on GitHub and attach debug.html which should be in the folder.')
#    return player_revision


def gethtml(url):
    with open('cookies') as f:
        cookies = requests.utils.cookiejar_from_dict(pickle.load(f))
        session = requests.session()
        session.cookies = cookies
        del session.cookies['c_visitor']
        if not forceusa and localizecookies:
            session.cookies['c_locale']={u'Español (Espana)' : 'esES', u'Français (France)' : 'frFR', u'Português (Brasil)' : 'ptBR',
                                        u'English' : 'enUS', u'Español' : 'esLA', u'Türkçe' : 'enUS', u'Italiano' : 'itIT',
                                        u'العربية' : 'arME' , u'Deutsch' : 'deDE'}[lang]
        if forceusa:
            try:
                session.cookies['sess_id'] = requests.get('http://www.crunblocker.com/sess_id.php').text
            except:
                sleep(10)  # sleep so we don't overload crunblocker
                session.cookies['sess_id'] = requests.get('http://www.crunblocker.com/sess_id.php').text
    parts = urlparse.urlsplit(url)
    if not parts.scheme or not parts.netloc:
        print 'Apparently not a URL'
        sys.exit()
    data = {'Referer': 'http://crunchyroll.com/', 'Host': 'www.crunchyroll.com',
            'User-Agent': 'Mozilla/5.0  Windows NT 6.1; rv:26.0 Gecko/20100101 Firefox/26.0'}
    res = session.get(url, params=data)
    res.encoding = 'UTF-8'
    return res.text


def getxml(req, med_id):
    url = 'http://www.crunchyroll.com/xml/'
    if req == 'RpcApiSubtitle_GetXml':
        payload = {'req': 'RpcApiSubtitle_GetXml', 'subtitle_script_id': med_id}
    elif req == 'RpcApiVideoPlayer_GetStandardConfig':
        payload = {'req': 'RpcApiVideoPlayer_GetStandardConfig', 'media_id': med_id, 'video_format': video_format,
                   'video_quality': resolution, 'auto_play': '1', 'show_pop_out_controls': '1',
                   'current_page': 'http://www.crunchyroll.com/'}
    else:
        payload = {'req': req, 'media_id': med_id, 'video_format': video_format, 'video_encode_quality': resolution}
    with open('cookies') as f:
        cookies = requests.utils.cookiejar_from_dict(pickle.load(f))
        session = requests.session()
        session.cookies = cookies
        del session.cookies['c_visitor']
        if not forceusa and localizecookies:
            session.cookies['c_locale']={u'Español (Espana)' : 'esES', u'Français (France)' : 'frFR', u'Português (Brasil)' : 'ptBR',
                                        u'English' : 'enUS', u'Español' : 'esLA', u'Türkçe' : 'enUS', u'Italiano' : 'itIT',
                                        u'العربية' : 'arME' , u'Deutsch' : 'deDE'}[lang]
        if forceusa:
            try:
                session.cookies['sess_id'] = requests.get('http://www.crunblocker.com/sess_id.php').text
            except:
                sleep(10)  # sleep so we don't overload crunblocker
                session.cookies['sess_id'] = requests.get('http://www.crunblocker.com/sess_id.php').text
    headers = {'Referer': 'http://static.ak.crunchyroll.com/versioned_assets/ChromelessPlayerApp.17821a0e.swf',
               'Host': 'www.crunchyroll.com', 'Content-type': 'application/x-www-form-urlencoded',
               'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:26.0) Gecko/20100101 Firefox/26.0)'}
    res = session.post(url, params=payload, headers=headers)
    res.encoding = 'UTF-8'
    return res.text


def vidurl(url, season, ep):  # experimental, although it does help if you only know the program page.
    res = gethtml(url)
    try:
        print re.findall('<img id=\"footer_country_flag\".+?title=\"(.+?)\"', res, re.DOTALL)[0]
    except:
        pass
    # open('video.html', 'w').write(res.encode('utf-8'))
    slist = re.findall('<a href="#" class="season-dropdown content-menu block text-link strong(?: open| ) '
                       'small-margin-bottom" title="(.+?)"', res)
    if slist:  # multiple seasons
        if len(re.findall('<a href=".+episode-(01|1)-(.+?)"', res)) > 1:  # dirty hack, I know
            # print list(reversed(slist))
            # season = int(raw_input('Season number: '))
            # season = sys.argv[3]
            # ep = raw_input('Episode number: ')
            # ep = sys.argv[2]
            season = slist[int(season)]
            # import pdb
            # pdb.set_trace()
            return 'http://www.crunchyroll.com' + re.findall(
                '<a href="(.+episode-0?' + ep + '-(?:.+-)?[0-9]{6})"', res)[slist.index(season)]
        else:
            # print list(reversed(re.findall('<a href=".+episode-(.+?)-',res)))
            # ep = raw_input('Episode number: ')
            # ep = sys.argv[2]
            return 'http://www.crunchyroll.com' + re.findall('<a href="(.+episode-0?' + ep + '-(?:.+-)?[0-9]{6})"',
                                                             res).pop()
    else:
        # 'http://www.crunchyroll.com/media-'
        # print re.findall('<a href=\"(.+?)\" title=\"(.+?)\"
        #  class=\"portrait-element block-link titlefix episode\"', res)
        # epnum = raw_input('Episode number: ')
        # epnum = sys.argv[2]
        return 'http://www.crunchyroll.com' + \
               re.findall('<a href=\"(.+?)\" .+ class=\"portrait-element block-link titlefix episode\"', res)[int(ep)]
