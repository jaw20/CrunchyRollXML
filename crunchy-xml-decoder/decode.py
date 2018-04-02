#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Subtitle Decoder
Uses some library files from
http://xbmc-addon-repository.googlecode.com
Thanks!
"""

# import lxml
import os.path
import re
import shutil
import sys
#import HTMLParser

import altfuncs
from bs4 import BeautifulSoup
from crunchyDec import CrunchyDec
from unidecode import unidecode

# ----------

def decode(page_url):
    print '''
--------------------------
---- Start New Export ----
--------------------------

CrunchyRoll Downloader Toolkit DX v0.98

Crunchyroll hasn't changed anything.

If you don't have a premium account, go and sign up for one now. It's well worthit, and supports the animators.

----------
Booting up...
'''
    if page_url == '':
        page_url = raw_input('Please enter Crunchyroll video URL:\n')

    lang1, lang2, forcesub, forceusa, localizecookies, vquality, onlymainsub, connection_n_ = altfuncs.config()
    #player_revision = altfuncs.playerrev(page_url)
    html = altfuncs.gethtml(page_url)

    #h = HTMLParser.HTMLParser()
    title = re.findall('<title>(.+?)</title>', html)[0].replace('Crunchyroll - Watch ', '')
    if len(os.path.join('export', title+'.ass')) > 255:
        title = re.findall('^(.+?) \- ', title)[0]

    ### Taken from http://stackoverflow.com/questions/6116978/python-replace-multiple-strings ###
    rep = {' / ': ' - ', '/': ' - ', ':': '-', '?': '.', '"': "''", '|': '-', '&quot;': "''", 'a*G':'a G', '*': '#', u'\u2026': '...'}

    rep = dict((re.escape(k), v) for k, v in rep.iteritems())
    pattern = re.compile("|".join(rep.keys()))
    title = unidecode(pattern.sub(lambda m: rep[re.escape(m.group(0))], title))

    ### End stolen code ###

    media_id = page_url[-6:]
    xmlconfig = BeautifulSoup(altfuncs.getxml('RpcApiVideoPlayer_GetStandardConfig', media_id), 'xml')

    try:
        if '4' in xmlconfig.find_all('code')[0]:
            print xmlconfig.find_all('msg')[0].text
            sys.exit()
    except IndexError:
        pass

    xmllist = altfuncs.getxml('RpcApiSubtitle_GetListing', media_id)
    xmllist = unidecode(xmllist).replace('><', '>\n<')



    if '<media_id>None</media_id>' in xmllist:
        print 'The video has hardcoded subtitles.'
        hardcoded = True
        sub_id = False
    else:
        try:
            sub_id2 = re.findall("id=([0-9]+)", xmllist)
            sub_id3 = re.findall("title='(\[.+\]) ", xmllist)
            sub_id4 = re.findall("title='(\[.+\]) ", xmllist)
            hardcoded = False
        except IndexError:
            print "The video's subtitles cannot be found, or are region-locked."
            hardcoded = True
            sub_id = False
    sub_id3 = [word.replace('[English (US)]','eng') for word in sub_id3]
    sub_id3 = [word.replace('[Deutsch]','deu') for word in sub_id3]
    sub_id3 = [word.replace('[Portugues (Brasil)]','por') for word in sub_id3]
    sub_id3 = [word.replace('[Francais (France)]','fre') for word in sub_id3]
    sub_id3 = [word.replace('[Espanol (Espana)]','spa') for word in sub_id3]
    sub_id3 = [word.replace('[Espanol]','spa') for word in sub_id3]
    sub_id3 = [word.replace('[Italiano]','ita') for word in sub_id3]
    sub_id3 = [word.replace('[l`rby@]','ara') for word in sub_id3]
    #sub_id4 = [word.replace('[l`rby@]',u'[العربية]') for word in sub_id4]
    sub_id4 = [word.replace('[l`rby@]',u'[Arabic]') for word in sub_id4]#else:
    #	try:
    #		sub_id = re.findall("id=([0-9]+)' title='\["+re.escape(unidecode(lang1)), xmllist)[0]
    #		hardcoded = False
    #		lang = lang1
    #	except IndexError:
    #		try:
    #			sub_id = re.findall("id=([0-9]+)' title='\["+re.escape(unidecode(lang2)), xmllist)[0]
    #			print 'Language not found, reverting to ' + lang2 + '.'
    #			hardcoded = False
    #			lang = lang2
    #		except IndexError:
    #			try:
    #				sub_id = re.findall("id=([0-9]+)' title='\[English", xmllist)[0]  # default back to English
    #				print 'Backup language not found, reverting to English.'
    #				hardcoded = False
    #				lang = 'English'
    #			except IndexError:
    #				print "The video's subtitles cannot be found, or are region-locked."
    #				hardcoded = True
    #				sub_id = False
    if not hardcoded:
        for i in sub_id2:
            #xmlsub = altfuncs.getxml('RpcApiSubtitle_GetXml', sub_id)
            xmlsub = altfuncs.getxml('RpcApiSubtitle_GetXml', i)
            formattedsubs = CrunchyDec().returnsubs(xmlsub)
            if formattedsubs is None:
                continue
            #subfile = open(eptitle + '.ass', 'wb')
            subfile = open(os.path.join('export', title+'['+sub_id3.pop(0)+']'+sub_id4.pop(0)+'.ass'), 'wb')
            subfile.write(formattedsubs.encode('utf-8-sig'))
            subfile.close()
        #shutil.move(title + '.ass', os.path.join(os.getcwd(), 'export', ''))

    print 'Subtitles for '+title+' have been downloaded'

if __name__ == '__main__':
    try:
        page_url = sys.argv[1]
    except IndexError:
        page_url = ''

    decode(page_url)
