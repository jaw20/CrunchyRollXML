#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Crunchyroll Export Script DX - Last Updated 2014/07/16
Removes need for rtmpExplorer
ORIGINAL SOURCE:
  http://www.darkztar.com/forum/showthread.php?219034-Ripping-videos-amp-subtitles-from-Crunchyroll-%28noob-friendly%29
"""

# import lxml
import os.path
import re
import shutil
import subprocess
import sys
#import HTMLParser

import altfuncs
from bs4 import BeautifulSoup
from crunchyDec import CrunchyDec
from unidecode import unidecode
from hls import video_hls
# ----------

#onlymainsub=False

def video():
    print 'Downloading video...'
    cmd = [os.path.join('video-engine', 'rtmpdump.exe'),
           '-r', url1, '-a', url2,
           '-f', 'WIN 11,8,800,50',
           '-m', '15',
           '-W', 'http://www.crunchyroll.com/vendor/ChromelessPlayerApp-c0d121b.swf',
           '-p', page_url2,
           '-y', filen,
           '-o', os.path.join('export', '{}.flv'.format(title))]
    if os.name != 'nt':
        cmd.insert(0, 'wine')
    error = subprocess.call(cmd)
    # error = 0

    num = 1
    while error != 0 and num < 4:
        if error == 1:
            print '\nVideo failed to download, trying again. (' + str(num) + '/3)'
            error = subprocess.call(cmd)
            num += 1
        if error == 2:
            print '\nVideo download is incomplete, resuming. (' + str(num) + '/3)'
            error = subprocess.call(cmd + ' -e')
            num += 1

    if error != 0:
        print '\nVideo failed to download. Writing error...'
        if os.path.exists('error.log'):
            log = open('error.log', 'a')
        else:
            log = open('error.log', 'w')
        log.write(page_url2 + '\n')
        log.close()
        os.remove(title + '.flv"')
        sys.exit()

# ----------


def subtitles(eptitle):
    global sub_id
    global sub_id2
    global sub_id3
    global sub_id4
    global sub_id5
    global sub_id6
    global lang

    xmllist = altfuncs.getxml('RpcApiSubtitle_GetListing', media_id)
    xmllist = unidecode(xmllist).replace('><', '>\n<')

    global hardcoded
    if '<media_id>None</media_id>' in xmllist:
        print 'The video has hardcoded subtitles.'
        hardcoded = True
        sub_id = False
    else:
		try:
			sub_id2 = re.findall("id=([0-9]+)", xmllist)
			sub_id3 = re.findall("title='(\[.+\]) ", xmllist)
			sub_id4 = re.findall("title='(\[.+\]) ", xmllist)
			sub_id5 = re.findall("title='(\[.+\]) ", xmllist)
			sub_id6 = re.findall("title='(\[.+\]) ", xmllist)
			hardcoded = False
#			try:
#				sub_id = re.findall("id=([0-9]+)' title='\["+re.escape(unidecode(lang1)), xmllist)[0]
#				lang = lang1
#			except IndexError:
#				try:
#					sub_id = re.findall("id=([0-9]+)' title='\["+re.escape(unidecode(lang2)), xmllist)[0]
#					lang = lang2
		except IndexError:
			print "The video's subtitles cannot be found, or are region-locked."
			hardcoded = True
			sub_id = False
		try:
			sub_id = re.findall("id=([0-9]+)' title='\["+re.escape(unidecode(lang1)), xmllist)[0]
			lang = lang1
		except IndexError:
			try:
				sub_id = re.findall("id=([0-9]+)' title='\["+re.escape(unidecode(lang2)), xmllist)[0]
				lang = lang2
			except IndexError:
				lang ='English'
    sub_id3 = [word.replace('[English (US)]','eng') for word in sub_id3]
    sub_id3 = [word.replace('[Deutsch]','deu') for word in sub_id3]
    sub_id3 = [word.replace('[Portugues (Brasil)]','por') for word in sub_id3]
    sub_id3 = [word.replace('[Francais (France)]','fre') for word in sub_id3]
    sub_id3 = [word.replace('[Espanol (Espana)]','spa_spa') for word in sub_id3]
    sub_id3 = [word.replace('[Espanol]','spa') for word in sub_id3]
    sub_id3 = [word.replace('[Italiano]','ita') for word in sub_id3]
    sub_id3 = [word.replace('[l`rby@]','ara') for word in sub_id3]
#    sub_id4 = [word.replace('[l`rby@]',u'[العربية]') for word in sub_id4]
    sub_id4 = [word.replace('[l`rby@]',u'[Arabic]') for word in sub_id4]
    sub_id5 = [word.replace('[English (US)]','eng') for word in sub_id5]
    sub_id5 = [word.replace('[Deutsch]','deu') for word in sub_id5]
    sub_id5 = [word.replace('[Portugues (Brasil)]','por') for word in sub_id5]
    sub_id5 = [word.replace('[Francais (France)]','fre') for word in sub_id5]
    sub_id5 = [word.replace('[Espanol (Espana)]','spa_spa') for word in sub_id5]
    sub_id5 = [word.replace('[Espanol]','spa') for word in sub_id5]
    sub_id5 = [word.replace('[Italiano]','ita') for word in sub_id5]
    sub_id5 = [word.replace('[l`rby@]','ara') for word in sub_id5]
#    sub_id6 = [word.replace('[l`rby@]',u'[العربية]') for word in sub_id6]
    sub_id6 = [word.replace('[l`rby@]',u'[Arabic]') for word in sub_id6]
#    else:
#        try:
#            sub_id = re.findall("id=([0-9]+)' title='\["+re.escape(unidecode(lang1)), xmllist)[0]
#            hardcoded = False
#            lang = lang1
#        except IndexError:
#            try:
#                sub_id = re.findall("id=([0-9]+)' title='\["+re.escape(unidecode(lang2)), xmllist)[0]
#                print 'Language not found, reverting to ' + lang2 + '.'
#                hardcoded = False
#                lang = lang2
#            except IndexError:
#                try:
#                    sub_id = re.findall("id=([0-9]+)' title='\[English", xmllist)[0]  # default back to English
#                    print 'Backup language not found, reverting to English.'
#                    hardcoded = False
#                    lang = 'English'
#                except IndexError:
#                    print "The video's subtitles cannot be found, or are region-locked."
#                    hardcoded = True
#                    sub_id = False

    if not hardcoded:
        
        sublang = {u'Español (Espana)': 'spa_spa', u'Français (France)': 'fre', u'Português (Brasil)': 'por',
                   u'English': 'eng', u'Español': 'spa', u'Türkçe': 'tur', u'Italiano': 'ita',
                   u'العربية': 'ara', u'Deutsch': 'deu'}[lang]

        for i in sub_id2:
            sublangc = sub_id3.pop(0)
            sublangn = sub_id4.pop(0)
            if onlymainsub and sublangc != sublang:
                continue
            
	    #xmlsub = altfuncs.getxml('RpcApiSubtitle_GetXml', sub_id)
	    xmlsub = altfuncs.getxml('RpcApiSubtitle_GetXml', i)
	    formattedsubs = CrunchyDec().returnsubs(xmlsub)
	    if formattedsubs is None:
		continue
	    #subfile = open(eptitle + '.###', 'wb')
	    subfile = open(os.path.join('export', title + '['+ sublangc + ']' + sublangn + '.###'), 'wb')
            subfile.write(formattedsubs.encode('utf-8-sig'))
            subfile.close()
        #shutil.move(eptitle + '.###', os.path.join(os.getcwd(), 'export', ''))
# ----------

def ultimate(page_url, seasonnum, epnum):
    global url1, url2, filen, title, media_id, lang1, lang2, hardcoded, forceusa, page_url2, onlymainsub
    #global player_revision

    print '''
--------------------------
---- Start New Export ----
--------------------------

CrunchyRoll Downloader Toolkit DX v0.98b 

Crunchyroll hasn't changed anything.

If you don't have a premium account, go and sign up for one now. It's well worth it, and supports the animators.

----------
Booting up...
'''
    if page_url == '':
        page_url = raw_input('Please enter Crunchyroll video URL:\n')
	
    try:
        int(page_url)
        page_url = 'http://www.crunchyroll.com/media-' + page_url
    except ValueError:
        if not page_url.startswith('http://') and not page_url.startswith('https://'):
            page_url = 'http://' + page_url
        try:
            int(page_url[-6:])
        except ValueError:
            if bool(seasonnum) and bool(epnum):
                page_url = altfuncs.vidurl(page_url, seasonnum, epnum)
            elif bool(epnum):
                page_url = altfuncs.vidurl(page_url, 1, epnum)
            else:
                page_url = altfuncs.vidurl(page_url, False, False)

    #subprocess.call('title ' + page_url.replace('http://www.crunchyroll.com/', ''), shell=True)

    # ----------

    #lang1, lang2 = altfuncs.config()
    #lang1, lang2, forcesub = altfuncs.config()
    lang1, lang2, forcesub, forceusa, localizecookies, vquality, onlymainsub = altfuncs.config()
    #player_revision = altfuncs.playerrev(page_url)
    html = altfuncs.gethtml(page_url)

    #h = HTMLParser.HTMLParser()
    title = re.findall('<title>(.+?)</title>', html)[0].replace('Crunchyroll - Watch ', '')
    if len(os.path.join('export', title+'.flv')) > 255:
        title = re.findall('^(.+?) \- ', title)[0]

    # title = h.unescape(unidecode(title)).replace('/', ' - ').replace(':', '-').
    # replace('?', '.').replace('"', "''").replace('|', '-').replace('&quot;',"''").strip()
    
    ### Taken from http://stackoverflow.com/questions/6116978/python-replace-multiple-strings and improved to include the backslash###
    rep = {' / ': ' - ', '/': ' - ', ':': '-', '?': '.', '"': "''", '|': '-', '&quot;': "''", 'a*G':'a G', '*': '#', u'\u2026': '...', ' \ ': ' - '}

    rep = dict((re.escape(k), v) for k, v in rep.iteritems())
    pattern = re.compile("|".join(rep.keys()))
    title = unidecode(pattern.sub(lambda m: rep[re.escape(m.group(0))], title))

    ### End stolen code ###

    #subprocess.call('title ' + title.replace('&', '^&'), shell=True)

    # ----------

    media_id = page_url[-6:]
    xmlconfig = BeautifulSoup(altfuncs.getxml('RpcApiVideoPlayer_GetStandardConfig', media_id), 'xml')

    try:
        if '4' in xmlconfig.find_all('code')[0]:
            print xmlconfig.find_all('msg')[0].text
            sys.exit()
    except IndexError:
        pass

    vid_id = xmlconfig.find('media_id').string

    # ----------

    host = xmlconfig.find('host')
    if host:
        host = host.string

    filen = xmlconfig.find('file')
    if filen:
        filen = filen.string

    if not host and not filen:
        print 'Downloading 2 minute preview.'
        media_id = xmlconfig.find('media_id').string
        xmlconfig = BeautifulSoup(altfuncs.getxml('RpcApiVideoEncode_GetStreamInfo', media_id), 'xml')
        host = xmlconfig.find('host').string


    # ----------
    if 'subs' in sys.argv:
        subtitles(title)
        subs_only = True
        hardcoded = True  # bleh
    else:
        page_url2 = page_url
        if host:
            if re.search('fplive\.net', host):
                url1 = re.findall('.+/c[0-9]+', host).pop()
                url2 = re.findall('c[0-9]+\?.+', host).pop()
            else:
                url1 = re.findall('.+/ondemand/', host).pop()
                url2 = re.findall('ondemand/.+', host).pop()
            video()
            video_input = os.path.join("export", title + '.flv')
        else:
            video_input = os.path.join("export", title + '.ts')
            video_hls(filen, video_input)

        heightp = '360p' if xmlconfig.height.string == '368' else '{0}p'.format(xmlconfig.height.string)  # This is less likely to fail
        subtitles(title)

        print 'Starting mkv merge'
        mkvmerge = os.path.join("video-engine", "mkvmerge.exe")
        filename_output = os.path.join("export", title + '[' + heightp.strip() +'].mkv')
        subtitle_input = []
        if os.path.isfile(mkvmerge):
            with_wine = os.name != 'nt'
        else:
            mkvmerge = "mkvmerge"
            with_wine = False
        cmd = [mkvmerge, "-o", filename_output, '--language', '0:jpn', '--language', '1:jpn', '-a', '1', '-d', '0', video_input, '--title', title]
        if with_wine:
            cmd.insert(0, 'wine')
        if not hardcoded:
            sublang = {u'Español (Espana)': 'spa_spa', u'Français (France)': 'fre', u'Português (Brasil)': 'por',
                       u'English': 'eng', u'Español': 'spa', u'Türkçe': 'tur', u'Italiano': 'ita',
                       u'العربية': 'ara', u'Deutsch': 'deu'}[lang]
            for i in sub_id2:
                sublangc=sub_id5.pop(0)
                sublangn=sub_id6.pop(0)

                if onlymainsub and sublangc != sublang:
                    continue

                filename_subtitle = os.path.join("export", title+'['+sublangc+']'+sublangn+'.###')
                if not os.path.isfile(filename_subtitle):
                    continue

                cmd.extend(['--language', '0:' + sublangc.replace('spa_spa','spa')])

                if sublangc == sublang:
                    cmd.extend(['--default-track', '0:yes'])
                else:
                    cmd.extend(['--default-track', '0:no'])
                if forcesub:
                    cmd.extend(['--forced-track', '0:yes'])
                else:
                    cmd.extend(['--forced-track', '0:no'])

                cmd.extend(['--track-name', '0:' + sublangn])
                cmd.extend(['-s', '0'])
                cmd.append(filename_subtitle)
                subtitle_input.append(filename_subtitle)
        subprocess.call(cmd)
        print 'Merge process complete'
        subs_only = False

    print
    print '----------'
    print

    print 'Starting Final Cleanup'
    if not subs_only:
        os.remove(video_input)
    if not hardcoded or not subs_only:
        #os.remove(os.path.join(os.getcwd(), 'export', '') + title + '.###')
        for f in subtitle_input:
            os.remove(f)
    print 'Cleanup Complete'

# ----------


if __name__ == '__main__':
    try:
        page_url = sys.argv[1]
    except IndexError:
        page_url = ''

    try:
        seasonnum, epnum = sys.argv[2:4]
    except ValueError:
        try:
            epnum = str(int(sys.argv[2]))
            seasonnum = ''
        except IndexError:
            # sys.exit('No season or episode numbers.')
            seasonnum, epnum = '', ''
            pass
    ultimate(page_url, seasonnum, epnum)
