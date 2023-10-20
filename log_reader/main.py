from logReader import logReader

LOGFILE_PATH = "data/access.log"

def main():

    reader = logReader(LOGFILE_PATH)
    print(reader.histbyhour())
    print(reader.ipaddresses())

if __name__ == '__main__':
    main()