#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import argparse
import os
import subprocess
from getpass import getpass
sys.path.append('crunchy-xml-decoder')
import functtest
import ultimate
import login
import decode
import altfuncs
import re, urllib2
from collections import deque

import time

#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#(autocatch)#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
def autocatch():
    print 'indicate the url : '
    url=raw_input()
    mykey = urllib2.urlopen(url)
    take = open("queue_.txt", "w")

    for text in mykey:
        match = re.search('<a href="/(.+?)" title=', text)
        if match:
            print >> take, 'http://www.crunchyroll.com/'+match.group(1)

    take.close()

    with open('queue_.txt') as f,  open('queue.txt', 'w') as fout:
        fout.writelines(reversed(f.readlines()))
    os.remove('queue_.txt')
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#(CHECKING)#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
if not os.path.exists("export"):
    os.makedirs("export")

iquality = 'highest'
ilang1 = 'English'
ilang2 = 'English'
iforcesub = False
iforceusa = False
ilocalizecookies = False
ionlymainsub=False
def defaultsettings(vvquality, vlang1, vlang2, vforcesub, vforceusa, vlocalizecookies, onlymainsub):
    dsettings='''[SETTINGS]
# Set this to the preferred quality. Possible values are: "android" (hard-subbed), "360p", "480p", "720p", "1080p", or "highest" for highest available.
# Note that any quality higher than 360p still requires premium, unless it's available that way for free (some first episodes).
# We're not miracle workers.
video_quality = '''+vvquality+'''
# Set this to the desired subtitle language. If the subtitles aren't available in that language, it reverts to the second language option (below).
# Available languages: English, Espanol, Espanol_Espana, Francais, Portugues, Turkce, Italiano, Arabic, Deutsch
language = '''+vlang1+'''
# If the first language isn't available, what language would you like as a backup? Only if then they aren't found, then it goes to English as default
language2 = '''+vlang2+'''
# Set this if you want to use --forced-track rather than --default-track for subtitle
forcesubtitle = '''+str(vforcesub)+'''
# Set this if you want to use a US session ID
forceusa = '''+str(vforceusa)+'''
# Set this if you want to Localize the cookies (this option is under testing and may generate some problem and it willnot work with -forceusa- option)
localizecookies = '''+str(vlocalizecookies)+'''
# Set this if you only want to mux one subtitle only (this so make easy for some devices like TVs to play subtitle)
onlymainsub='''+str(onlymainsub)+'''
'''
    open('.\\settings.ini', 'w').write(dsettings.encode('utf-8'))

if not os.path.exists(".\\settings.ini"):
    defaultsettings(iquality, ilang1, ilang2, iforcesub, iforceusa, ilocalizecookies, ionlymainsub)
	
if not os.path.exists(".\\cookies"):
    if raw_input(u'Do you have an account [Y/N]?').lower() == 'y':
        username = raw_input(u'Username: ')
        password = getpass('Password(don\'t worry the password are typing but hidden:')
        login.login(username, password)
    else:
        login.login('', '')
else:
    userstatus = login.getuserstatus()
    print 'User Name='+userstatus[1]
    print 'Membership Type='+userstatus[0]

#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#(Argument Parser)#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
parser = argparse.ArgumentParser()
parser.add_argument("-u","--url", type=str,help="Crunchyroll Anime Link. if you get an error, try using double quotation marks (\")")
parser.add_argument("-sn","--season-number", metavar='#', type=int, nargs = 1, help="Crunchyroll Anime Season Number,it's optional option you can ignore")
parser.add_argument("-en","--episode-number", metavar='#', type=int, nargs = 1, help="Crunchyroll Anime Episode Number,it's optional option you can ignore")
parser.add_argument("-l","--login", metavar=('Username','Password'), nargs = 2, help="Crunchyroll login: -l User password. if your password has a blank, use double quotation marks (\"). Example: \"This is a password.\"")
parser.add_argument("-g","--guest", action='store_true', help="Crunchyroll login as guest")
parser.add_argument("-s","--subs-only", action='store_true', help="Download Crunchyroll Anime Subtitle only. if you get an error, try using double quotation marks (\")")
parser.add_argument("-q","--queue", type=str, nargs = '?', metavar='Queue Directory', const='.\\queue.txt', help="Run List of Crunchyroll Anime Link in queue file")
parser.add_argument("-d","--debug", action='store_true', help="Run crunchy-xml-decoder in Debug Mode")
parser.add_argument("-ds","--default-settings", action='store_true', help="Restore default settings")
arg = parser.parse_args()
sys.argv=[]
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#(    )#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
def queueu(queuepath):
    if not os.path.exists(queuepath):
        open(queuepath, 'w').write(u'#the any line that has hash before the link will be skiped\n')
        subprocess.call('notepad.exe '+queuepath)
    lines = open(queuepath).readlines()
    for line in lines:
        if line.rstrip('\n') ==''.join(line.rstrip('\n').split('#', 1)):
            #print ''.join(line.rstrip('\n').split('#', 1))
            ultimate.ultimate(line.rstrip('\n'), '', '')
            for i in range(0, len(lines)):
                if lines[i]== line:
                    lines[i]='#'+lines[i]
                    new_line_2=''
                    for new_line_ in lines:
                        try:
                            new_line_2=new_line_2+new_line_
                        except:
                            new_line_2=new_line_
                    open(queuepath, 'w').write(new_line_2)

def Languages_(Varname_):
    seleccion = 0
    if Varname_ == 'slang1':
        print '''Set this to the desired subtitle language. If the subtitles aren\'t available in that language, it reverts to the Secondary language option'''
    if Varname_ == 'slang2':
        print '''If the Primary language isn't available, what language would you like as a backup? Only if then they aren't found, then it goes to English as default'''
    print '''Available Languages:
0.- English
1.- Espanol
2.- Espanol (Espana)
3.- Francais
4.- Portugues
5.- Turkce
6.- Italiano
7.- Arabic
8.- Deutsch
'''
    try:
        seleccion = int(input("> "))
    except:
        print "ERROR: Invalid option."
        Languages_()
    if seleccion == 1 :
        return 'Espanol'
    elif seleccion == 2 :
        return 'Espanol_Espana'
    elif seleccion == 3 :
        return 'Francais'
    elif seleccion == 4 :
        return 'Portugues'
    elif seleccion == 5 :
        return 'Turkce'
    elif seleccion == 6 :
        return 'Italiano'
    elif seleccion == 7 :
        return 'Arabic'
    elif seleccion == 8 :
        return 'Deutsch'
    elif seleccion == 0 :
        return 'English'
    else:
        print "ERROR: Invalid option."
        Languages_()

def videoquality_():
    slang1, slang2, sforcesub, sforceusa, slocalizecookies, vquality, vonlymainsub = altfuncs.config()
    seleccion = 5
    print '''Set This To The Preferred Quality:
0.- android (hard-subbed)
1.- 360p
2.- 480p
3.- 720p
4.- 1080p
5.- highest
Note: Any Quality Higher Than 360p Still Requires Premium, Unless It's Available That Way For Free (Some First Episodes).
We're Not Miracle Workers.
'''
    try:
        seleccion = int(input("> "))
    except:
        print "ERROR: Invalid option."
        videoquality_()
    if seleccion == 0 :
        return 'android'
    elif seleccion == 1 :
        return '360p'
    elif seleccion == 2 :
        return '480p'
    elif seleccion == 3 :
        return '720p'
    elif seleccion == 4 :
        return '1080p'
    elif seleccion == 5 :
        return 'highest'
    else:
        print "ERROR: Invalid option."
        videoquality_()
def settings_():
    slang1, slang2, sforcesub, sforceusa, slocalizecookies, vquality, vonlymainsub = altfuncs.config()
    slang1 = {u'Español (Espana)' : 'Espanol_Espana', u'Français (France)' : 'Francais', u'Português (Brasil)' : 'Portugues',
            u'English' : 'English', u'Español' : 'Espanol', u'Türkçe' : 'Turkce', u'Italiano' : 'Italiano',
            u'العربية' : 'Arabic', u'Deutsch' : 'Deutsch'}[slang1]
    slang2 = {u'Español (Espana)' : 'Espanol_Espana', u'Français (France)' : 'Francais', u'Português (Brasil)' : 'Portugues',
            u'English' : 'English', u'Español' : 'Espanol', u'Türkçe' : 'Turkce', u'Italiano' : 'Italiano',
            u'العربية' : 'Arabic', u'Deutsch' : 'Deutsch'}[slang2]
    if slang1 == 'Espanol_Espana':
        slang1_ = 'Espanol (Espana)'
    else:
        slang1_ = slang1
    if slang2 == 'Espanol_Espana':
        slang2_ = 'Espanol (Espana)'
    else:
        slang2_ = slang2
    seleccion = 0
    print '''Options:
0.- Exit
1.- Video Quality = '''+vquality+'''
2.- Primary Language = '''+slang1_+'''
3.- Secondary Language = '''+slang2_+'''
4.- Force Subtitle = '''+str(sforcesub)+'''		#Use --forced-track in Subtitle
5.- USA Proxy = '''+str(sforceusa)+'''			#use a US session ID
6.- Localize cookies = '''+str(slocalizecookies)+'''		#Localize the cookies (Experiment)
7.- Only One Subtitle = '''+str(vonlymainsub)+'''		#Only download Primary Language
8.- Restore Default Settings
'''
    try:
        seleccion = int(input("> "))
    except:
        print "ERROR: Invalid option."
        settings_()
    if seleccion == 1 :
        vquality = videoquality_()
        defaultsettings(vquality, slang1, slang2, sforcesub, sforceusa, slocalizecookies, vonlymainsub)
        settings_()
    elif seleccion == 2 :
        slang1 = Languages_('slang1')
        defaultsettings(vquality, slang1, slang2, sforcesub, sforceusa, slocalizecookies, vonlymainsub)
        settings_()
    elif seleccion == 3 :
        slang2 = Languages_('slang2')
        defaultsettings(vquality, slang1, slang2, sforcesub, sforceusa, slocalizecookies, vonlymainsub)
        settings_()
    elif seleccion == 4 :
        if sforcesub:
            sforcesub = False
        else:
            sforcesub = True
        defaultsettings(vquality, slang1, slang2, sforcesub, sforceusa, slocalizecookies, vonlymainsub)
        settings_()
    elif seleccion == 5 :
        if sforceusa:
            sforceusa = False
        else:
            sforceusa = True
        defaultsettings(vquality, slang1, slang2, sforcesub, sforceusa, slocalizecookies, vonlymainsub)
        settings_()
    elif seleccion == 6 :
        if slocalizecookies:
            slocalizecookies = False
        else:
            slocalizecookies = True
        defaultsettings(vquality, slang1, slang2, sforcesub, sforceusa, slocalizecookies, vonlymainsub)
        settings_()
    elif seleccion == 7 :
        if vonlymainsub:
            vonlymainsub = False
        else:
            vonlymainsub = True
        defaultsettings(vquality, slang1, slang2, sforcesub, sforceusa, slocalizecookies, vonlymainsub)
        settings_()
    elif seleccion == 8 :
        defaultsettings(iquality, ilang1, ilang2, iforcesub, iforceusa, ilocalizecookies, ionlymainsub)
        settings_()
    elif seleccion == 0 :
        pass
    else:
        print "ERROR: Invalid option."
        settings_()

def makechoise():
    seleccion = 0
    print '''Options:
0.- Exit
1.- Download Anime 
2.- Download Subtitle only
3.- Login
4.- Login As Guest
5.- Download an entire Anime(Autocatch links)
6.- Run Queue
7.- Settings
'''
    try:
        seleccion = int(input("> "))
    except:
        try:
            os.system('cls')
        except:
            try:
                os.system('clear')
            except:
                pass
        print "ERROR: Invalid option."
        makechoise()
    if seleccion == 1 :
        ultimate.ultimate(raw_input('Please enter Crunchyroll video URL:\n'), '', '')
    elif seleccion == 2 :
        decode.decode(raw_input('Please enter Crunchyroll video URL:\n'))
    elif seleccion == 3 :
        username = raw_input(u'Username: ')
        password = getpass('Password(don\'t worry the password are typing but hidden:')
        login.login(username, password)
        makechoise()
    elif seleccion == 4 :
        login.login('', '')
        makechoise()
    elif seleccion == 5 :
        autocatch()
        queueu('.\\queue.txt')
    elif seleccion == 6 :
        queueu('.\\queue.txt')
    elif seleccion == 7 :
        settings_()
        makechoise()
    elif seleccion == 8 :
        import debug
    elif seleccion == 0 :
        sys.exit()
    else:
        try:
            os.system('cls')
        except:
            try:
                os.system('clear')
            except:
                pass
        print "ERROR: Invalid option."
        makechoise()
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#(    )#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
if arg.url:
    page_url = arg.url
if arg.season_number:
    seasonnum = arg.season_number[0]
else:
    seasonnum = ''
if arg.episode_number:
    epnum = arg.episode_number[0]
else:
    epnum = ''
if arg.guest:
    login.login('', '')
if arg.login:
    username = arg.login[0]
    password = arg.login[1]
    login.login(username, password)
if arg.debug:
    import debug
    sys.exit()
if arg.subs_only:
    if arg.url:
        decode.decode(page_url)
    else:
        decode.decode(raw_input('Please enter Crunchyroll video URL:\n'))
    sys.exit()
if arg.default_settings:
    defaultsettings(iquality, ilang1, ilang2, iforcesub, iforceusa, ilocalizecookies)
    sys.exit()
if arg.queue:
    queueu(arg.queue)
if arg.url and not arg.subs_only:
    ultimate.ultimate(page_url, seasonnum, epnum)
else:
    makechoise()


#print 'username'
#print 'password'
#print 'page_url'
#print 'seasonnum'
#import ultimate
