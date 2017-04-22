import sys
import re
import requests
import pickle
import cfscrape
from getpass import getpass

def getuserstatus(session=''):
    status = 'Guest'
    user1 = 'Guest'
    if session == '':
        session = cfscrape.create_scraper()
        with open('cookies') as f:
            cookies = requests.utils.cookiejar_from_dict(pickle.load(f))
            session = requests.session()
            session.cookies = cookies
            del session.cookies['c_visitor']
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
               'Connection': 'keep-alive'}
    site = session.get('https://www.crunchyroll.com/acct/membership', headers=headers, verify=True).text
    #open('tempfile','w').write(site.encode('UTF-8'))
    if re.search(re.escape('      ga(\'set\', \'dimension5\', \'registered\');'), site):
        status = 'Free Member'
    elif re.search(re.escape('      ga(\'set\', \'dimension6\', \'premium\');'), site):
        status = 'Premium Member'
    elif re.search(re.escape('      ga(\'set\', \'dimension6\', \'premiumplus\');'), site):
        status = 'Premium+ Member'
    if status != 'Guest':
        user1 = re.findall('<a href=\"/user/(.+)\" ', site).pop()
    return [status,user1]

def login(username, password):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
               'Connection': 'keep-alive'}
    session = cfscrape.create_scraper()
    res_get = session.get('https://www.crunchyroll.com/login', headers=headers)

    s = re.search('name="login_form\\[_token\\]" value="([^"]*)"', res_get.text)
    if s is None:
       print 'CSRF token not found'
       sys.exit()
    token = s.group(1)

    payload = {'login_form[redirect_url]': '/',
               'login_form[name]': username,
               'login_form[password]': password,
               'login_form[_token]': token}

    res_post = session.post('https://www.crunchyroll.com/login', data=payload, headers=headers, allow_redirects = False)
    if not (res_post.status_code == 302 or (res_post.status_code == 200 and username == '')):
      print 'Login failed'
      sys.exit()

    for c in session.cookies:
        c.expires = 9999999999  # Saturday, November 20th 2286, 17:46:39 (GMT)

    del session.cookies['c_visitor']

    userstatus = getuserstatus(session)
    if username != '' and userstatus[1] == 'Guest':
        print 'Login failed.'
        sys.exit()
    else:
        print 'Login as '+userstatus[0]+' successfully.'
        pickle.dump(requests.utils.dict_from_cookiejar(session.cookies), open('cookies', 'w'))
        with open('cookies', 'w') as f:
            pickle.dump(requests.utils.dict_from_cookiejar(session.cookies), f)

if __name__ == '__main__':
    try:
        if sys.argv[1][0] == 'y':
            username = raw_input(u'Username: ')
            password = getpass('Password(don\'t worry the password are typing but hidden:')
    except IndexError:
        username = ''
        password = ''
    login(username, password)
