@echo off
RD "%USERPROFILE%\Documents\Crunchyroll-XML-Decoder_link" 1>NUL 2>NUL
mklink /j "%USERPROFILE%\Documents\Crunchyroll-XML-Decoder_link" %cd% 1>NUL 2>NUL
cd "%USERPROFILE%\Documents\Crunchyroll-XML-Decoder_link"
:sratre
crunchy-xml-decoder.py %1 %2 %3 %4 %5 %6 %7 %8 %9
RD "%USERPROFILE%\Documents\Crunchyroll-XML-Decoder_link" 1>NUL 2>NUL
pause

