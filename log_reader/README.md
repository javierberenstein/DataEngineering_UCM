# Instructions

logReader is a Class that implements the functionality described in the exercise statement.
You can instantiate it by providing the path to the log file you want to analyze.

```python
from logReader import logReader

# Example of instance
my_reader = logReader(path/to/log.log)
``` 

logReader accepts several optional keyword arguments to customize its behavior:

- date_format (default: "DD/MMM/YYYY:HH:mm:ss ZZ"): You can specify a custom date and time format for parsing log entries.
- user_separator (default: "-"): The character that separates user agent information in log entries.
- ip_separator (default: "- -"): The character that separates IP address information in log entries.
- date_rounding_chars (default: []): Characters used for rounding the timestamp in log entries.

Once you've instantiated logReader with your log file and optional arguments, you can use its methods to perform various log analysis tasks. The class provides methods like ipaddresses and histbyhour for extracting IP addresses of not-bot users and creating histograms based on log entries. There are two logfiles examples in the data folder. 

Executing the logReader.py file will run the tests written in the methods docstrings. Is as easy as going to the root and calling the following command:

```bash
python logReader.py
```


