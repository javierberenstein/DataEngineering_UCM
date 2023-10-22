from logReader import logReader

LOGFILE_PATH = "log_reader/data/access.log"

def main():

    reader = logReader(LOGFILE_PATH)

if __name__ == '__main__':

    main()
