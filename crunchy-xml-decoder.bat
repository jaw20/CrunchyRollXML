@echo off
RD "%PUBLIC%\Crunchyroll-XML-Decoder_link" 1>NUL 2>NUL
mklink /j "%PUBLIC%\Crunchyroll-XML-Decoder_link" %cd% 1>NUL 2>NUL
cd "%PUBLIC%\Crunchyroll-XML-Decoder_link"
:sratre
crunchy-xml-decoder.py %1 %2 %3 %4 %5 %6 %7 %8 %9
RD "%PUBLIC%\Crunchyroll-XML-Decoder_link" 1>NUL 2>NUL
pause

