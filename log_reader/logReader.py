from typing import List
import pendulum
import logging
import re

from deprecated import deprecated

from exceptions import (
    SplitStringException,
    DateParseException,
    NotFoundRoundingChars,
    GroupNotFoundException
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

        self.set_pattern(kwargs.get('pattern', r"(?P<ip>\d+\.\d+\.\d+\.\d+)\s\-\s\-\s\[(?P<date>.+)]\s+\"(?P<command>\w+)\s+(?P<page>\S+)\s+(?P<protocol>\S+)\"\s(?P<status>\d+)\s(?P<bytes>\d+)\s\"\-\"\s\"(?P<user_agent>.+)\""))

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
    
    @deprecated("The class uses the pattern functionality now")
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

    @deprecated("The class uses the pattern functionality now")
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

    @deprecated("The class uses the pattern functionality now")
    def set_pattern(self, pattern: str) -> None:

        self.pattern = pattern

    @deprecated("The class uses the pattern functionality now")
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

    @deprecated("The class uses the pattern functionality now")
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

    @deprecated("The class uses the pattern functionality now")
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
        
    def _get_line_object(self, line: str, object: str) -> str:
        """
        Get an object given a pattern with defined objects in a log line.

        Args:
            line (str): The log line to search for matching groups.
            object (str): The name of the object group to extract from the line.

        Returns:
            str: The extracted object value.

        Raises:
            GroupNotFoundException: If the specified object group is not found in the line.

        Examples
        --------
            >>> line = '66.249.66.135 - - [09/Oct/2023:23:29:50 +0200] "GET /sic/investigacion/publicaciones/pdfs/SIC-8-11.pdf HTTP/1.1" 302 648 "-" "Googlebot/2.1 (+http://www.google.com/bot.html"'
            >>> date = r._get_line_object(line, 'date')
            >>> assert date == "09/Oct/2023:23:29:50 +0200"
            >>> user = r._get_line_object(line, 'user')
            Traceback (most recent call last):
            exceptions.GroupNotFoundException: Object user not found in the log line.
            """
        match = re.search(self.pattern, line)

        if match:
            try:
                return match.group(object)
            except IndexError:
                raise GroupNotFoundException(group=object)
        else:
            logging.error(f'No match with pattern\n{self.pattern}\nin line\n{line}')

    def _get_user_agent(self, line: str) -> str:
        """
        Extract the user agent from a log line.

        Args:
            line (str): The log line.

        Returns:
            str: The user agent.

        Examples
        --------
        >>> r._get_user_agent('66.249.66.135 - - [09/Oct/2023:23:29:50 +0200] "GET /sic/investigacion/publicaciones/pdfs/SIC-8-11.pdf HTTP/1.1" 302 648 "-" "Googlebot/2.1 (+http://www.google.com/bot.html)"')
        'Googlebot/2.1 (+http://www.google.com/bot.html)'
        >>> r._get_user_agent('39.103.168.88 - - [09/Oct/2023:05:22:59 +0200] "GET /dist/images/mask/guide/cn/step1.jpg HTTP/1.1" 404 498 "-" "python-requests/2.25.1"')
        'python-requests/2.25.1'

        """
        separator = self.user_separator
        try:
            return self._get_line_object(line, 'user_agent')
        except GroupNotFoundException as e:
            logging.error(e.message)

    def _is_bot(self, line: str) -> bool:
        """
        Check if a log line represents a bot request.

        Args:
            line (str): The log line.

        Returns:
            bool: True if it's a bot request, False otherwise.
        
        Examples
        --------
        >>> r._is_bot('66.249.66.135 - - [09/Oct/2023:23:29:50 +0200] "GET /sic/investigacion/publicaciones/pdfs/SIC-8-11.pdf HTTP/1.1" 302 648 "-" "Googlebot/2.1 (+http://www.google.com/bot.html")')
        True

        >>> r._is_bot('39.103.168.88 - - [09/Oct/2023:05:22:59 +0200] "GET /dist/images/mask/guide/cn/step1.jpg HTTP/1.1" 404 498 "-" "python-requests/2.25.1"')
        False
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
        >>> r._get_ipaddr('66.249.66.135 - - [09/Oct/2023:23:29:50 +0200] "GET /sic/investigacion/publicaciones/pdfs/SIC-8-11.pdf HTTP/1.1" 302 648 "-" "Googlebot/2.1 (+http://www.google.com/bot.html)"')
        '66.249.66.135'

        >>> r._get_ipaddr('39.103.168.88 - - [09/Oct/2023:05:22:59 +0200] "GET /dist/images/mask/guide/cn/step1.jpg HTTP/1.1" 404 498 "-" "python-requests/2.25.1"')
        '39.103.168.88'
        """
        separator = self.ip_separator

        try:
            return self._get_line_object(line, 'ip')
        except GroupNotFoundException as e:
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
        >>> r._get_hour('66.249.66.135 - - [09/Oct/2023:23:29:50 +0200] "GET /sic/investigacion/publicaciones/pdfs/SIC-8-11.pdf HTTP/1.1" 302 648 "-" "Googlebot/2.1 (+http://www.google.com/bot.html)"')
        23

        >>> r._get_hour('39.103.168.88 - - [09/Oct/2023:05:22:59 +0200] "GET /dist/images/mask/guide/cn/step1.jpg HTTP/1.1" 404 498 "-" "python-requests/2.25.1"')
        5
        """

        try:
            date_string = self._get_line_object(line, 'date')
        except GroupNotFoundException as e:
            logging.error(e.message)
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

    obj = logReader('log_reader/data/access_short.log')

    doctest.testmod(globs={'r': obj}, verbose=True)