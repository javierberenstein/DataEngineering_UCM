from typing import List
import pendulum
import logging

from exceptions import (
    SplitStringException,
    DateParseException,
    NotFoundRoundingChars
)


class logReader(object):
    """
    A class for reading and analyzing log files.
    """

    def __init__(self, logfile: str, **kwargs):
        """
        Initialize the logReader.

        Args:
            logfile (str): The path to the log file.
        """
        self.loglines = self._load(logfile)
        self.set_user_separator(kwargs.get('user_separator', '"-"'))
        self.set_ip_separator(kwargs.get('ip_separator', "- -"))
        self.set_date_format(kwargs.get('date_format', "DD/MMM/YYYY:HH:mm:ss ZZ"))
        self.set_rounding_chars(kwargs.get('date_rounding_chars', '[]'))

    def _load(self, logfile: str) -> List[str]:
        """
        Load log lines from a file.

        Args:
            logfile (str): The path to the log file.

        Returns:
            List[str]: A list of log lines.
        """
        with open(logfile, 'r') as f:
            return f.readlines()
        
    def set_user_separator(self, separator: str) -> None:
        """
        Set the separator for user agent extraction.

        Args:
            separator (str): The separator character.

        Example
        -------
        >>> r.set_user_separator('hola')
        >>> assert r.user_separator == 'hola'
        """
        self.user_separator = separator

    def set_ip_separator(self, separator: str) -> None:
        """
        Set the separator for IP address extraction.

        Args:
            separator (str): The separator character.

        Example
        -------
        >>> r.set_ip_separator('hola')
        >>> assert r.ip_separator == 'hola'
        """
        self.ip_separator = separator

    def set_date_format(self, date_format: str) -> None:
        """
        Sets the date format of the log files.

        Args:
            date_format (str): The format of the date.
        
        Example
        --------
        >>> r.set_date_format('MM/YYYY/DD')
        >>> assert r.date_format == 'MM/YYYY/DD'

        """
        self.date_format = date_format

    def set_rounding_chars(self, rounding_chars: str) -> None:
        """
        Sets the rounding chars of the date.

        Args:
            rounding_chars (str): The chars enclosing the date in log.

        Example
        -------
        >>> r.set_rounding_chars('+k')
        >>> assert r.rounding_chars == '+k'
        """
        self.rounding_chars = rounding_chars

    def _split_line(self, line: str, separator: str) -> List[str]:
        """
        Split a log line into a list of values using a separator.

        Args:
            line (str): The log line to split.

        Returns:
            List[str]: A list of values from the split line.
        """
        try:
            return [line.strip().strip('"') for line in line.split(separator)]
        except Exception as e:
            raise SplitStringException(e)

    def _get_user_agent(self, line: str) -> str:
        """
        Extract the user agent from a log line.

        Args:
            line (str): The log line.

        Returns:
            str: The user agent.

        Examples
        --------
        >>> r._get_user_agent('66.249.66.35 - - [15/Sep/2023:00:18:46 +0200] "GET /~luis/sw05-06/libre_m2_baja.pdf HTTP/1.1" 200 5940849 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"')
        'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'

        >>> r._get_user_agent('147.96.46.52 - - [10/Oct/2023:12:55:47 +0200] "GET /favicon.ico HTTP/1.1" 404 519 "https://antares.sip.ucm.es/" "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0"')
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0'

        """
        separator = self.user_separator
        return self._split_line(line, separator)[-1]

    def _is_bot(self, line: str) -> bool:
        """
        Check if a log line represents a bot request.

        Args:
            line (str): The log line.

        Returns:
            bool: True if it's a bot request, False otherwise.
        
        Examples
        --------
        >>> r._is_bot('147.96.46.52 - - [10/Oct/2023:12:55:47 +0200] "GET /favicon.ico HTTP/1.1" 404 519 "https://antares.sip.ucm.es/" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0"')
        False

        >>> r._is_bot('66.249.66.35 - - [15/Sep/2023:00:18:46 +0200] "GET /~luis/sw05-06/libre_m2_baja.pdf HTTP/1.1" 200 5940849 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"')
        True

        >>> r._is_bot('213.180.203.109 - - [15/Sep/2023:00:12:18 +0200] "GET /robots.txt HTTP/1.1" 302 567 "-" "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)"')
        True
        """

        return 'bot' in self._get_user_agent(line).lower()

    def _get_ipaddr(self, line: str) -> str:
        """
        Extract the IP address from a log line.

        Args:
            line (str): The log line.

        Returns:
            str: The IP address.

        Examples
        --------
        >>> r._get_ipaddr('213.180.203.109 - - [15/Sep/2023:00:12:18 +0200] "GET /robots.txt HTTP/1.1" 302 567 "-" "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)"')
        '213.180.203.109'

        >>> r._get_ipaddr('147.96.46.52 - - [10/Oct/2023:12:55:47 +0200] "GET /favicon.ico HTTP/1.1" 404 519 "https://antares.sip.ucm.es/" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0"')
        '147.96.46.52'
        """
        separator = self.ip_separator

        try:
            return self._split_line(line, separator)[0]
        except SplitStringException as e:
            logging.error(e.message)

    def _get_hour(self, line: str) -> int:
        """
        Get the hour from a log line.

        Args:
            line (str): The log line.

        Returns:
            int: The hour.

        Examples
        ---------
        >>> r._get_hour('66.249.66.35 - - [15/Sep/2023:00:18:46 +0200] "GET /~luis/sw05-06/libre_m2_baja.pdf HTTP/1.1" 200 5940849 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"')
        0

        >>> r._get_hour('147.96.46.52 - - [10/Oct/2023:12:55:47 +0200] "GET /favicon.ico HTTP/1.1" 404 519 "https://antacres.sip.ucm.es/" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0"')
        12
        """
        try:
            index_1 = line.find(self.rounding_chars[0]) + 1
            index_2 = line.find(self.rounding_chars[1])
        except Exception as e:
            raise NotFoundRoundingChars(e)

        date_string = line[index_1:index_2]
        try:
            return pendulum.from_format(date_string, self.date_format).hour
        except Exception as e:
            raise DateParseException(e)

    def ipaddresses(self) -> set[str]:
        """
        Get a set of IP addresses from log lines, always that 
        the user agent is not a bot.

        Returns:
            set[str]: A set of IP addresses.

        >>> assert r.ipaddresses() == {'34.105.93.183', '39.103.168.88'}
        """
        ip_addresses = set()
        for line in self.loglines:
            if not self._is_bot(line):
                ip_addresses.add(self._get_ipaddr(line))
        return ip_addresses

    def histbyhour(self) -> dict[int, int]:
        """
        Create a histogram of events by hour from log lines-

        Returns:
            dict[int, int]: A dictionary where keys are hours and values are event counts.

        Example
        -------
        >>> hist = r.histbyhour()
        >>> assert hist == {5: 3, 7: 2, 23: 1}
        """
        histogram = {}
        for line in self.loglines:
            try:
                hour = self._get_hour(line)
                if hour in histogram:
                    histogram[hour] += 1
                else:
                    histogram[hour] = 1
            except (DateParseException, NotFoundRoundingChars) as e:
                logging.error(e.message)
        return histogram

if __name__ == "__main__":
    import doctest

    obj = logReader('access_short.log')

    doctest.testmod(globs={'r': obj}, verbose=True)