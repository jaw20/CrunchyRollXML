crunchy-xml-decoder
===================
ORIGINALLY BY @einstein95

Requires Python modules:
- PyCrypto (http://www.voidspace.org.uk/python/modules.shtml#pycrypto)
- lxml (https://pypi.python.org/pypi/lxml/3.2.5)
- m3u8 (https://pypi.python.org/pypi/m3u8/)
- cfscrape (https://pypi.python.org/pypi/cfscrape/)

crunchy-xml-decoder will try to install PyCrypto and lxml automatically,
if they are missing. m3u8 and crscrape can be installed using PIP.


This is a composite of various scripts required to download video files from CrunchyRoll 
that have been automated with a batch file.


INSTRUCTIONS:

    Pre-Setup (Only need to do these once.):
    1.  Install Python 2.7.9.
    2.  Run pip install m3u8.
    3.  Run crunchy-xml-decoder.bat or crunchy-xml-decoder.py to generate necessary files (settings.ini and cookies)
    4.  choices	from the option 

    Per-Video Process:
    1.  Copy the URL of the CrunchyRoll video you want to download from your web browser
    2.  Run crunchy-xml-decoder.bat or crunchy-xml-decoder.py choice 1 and paste link
    3.  Download will start automatically. Everything is automated.
    4. Browse to the 'export' folder to view the completed file.

    SPECIAL NOTE: There is another batch file in the _run folder..
        Run crunchy-xml-decoder.bat or crunchy-xml-decoder.py choice 2 and paste link
            Just want the subtitles to an episode? OK.. fair 'nuff. Use this.


WHAT IS THE POINT OF THIS SCRIPT? WHAT IS IT ACTUALLY DOING?:

    The process of getting a working download from CrunchyRoll is effectively doing the following:
        - Downloading and decrypting subtitles
        - Downloading the video as FLV or MPEG-TS
        - Splitting the FLV/TS file into 264 video and aac audio
        - Merging video, audio, and subtitles into a mkv file
        - Naming the new video something other than 'video.mkv'


NOTES FROM THE AUTHORS:
    From the DX author:
        Yeah, I wrote the basis for this "new 'n' improved version". Basically, I monitored the traffic
        to and from Crunchyroll while a video was loading, found a few (read: a lot of) similarities, and
        basically wrote the script to do the same thing, but parse the file and call upon RTMPdump to
        dump the video (RTMPexplorer was doing the same thing basically).

    From the anonymous original author:
        I did not write these programs, and I didn't even come up with this method. All I have done is 
        created a few little bat files to bring them together. Original instructions on how this is 
        done can be found here: 
        http://www.darkztar.com/forum/showthread.php?219034-Ripping-videos-amp-subtitles-from-Crunchyroll-%28noob-friendly%29
