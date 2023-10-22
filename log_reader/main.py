from logReader import logReader

import os

LOGFILE_PATH = "log_reader/data/access_short.log"

def main():

    reader = logReader(LOGFILE_PATH)

    print(reader._get_hour(r'147.96.46.52 - - [10/Oct/2023:12:55:47 +0200] "GET /favicon.ico HTTP/1.1" 404 519 "https://antacres.sip.ucm.es/" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0"'))



    print(reader.histbyhour())
    print(reader.ipaddresses())

if __name__ == '__main__':

    print(os.getcwd)
    main()