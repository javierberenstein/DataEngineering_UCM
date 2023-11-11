import requests
import pandas as pd
import re
from typing import List
from io import StringIO

from EMT.helper import csv_from_zip, get_links


class UrlEMT():
    """
    A class for interacting with the EMT Madrid Open Data API.

    This class provides methods to fetch URLs from the EMT API and retrieve CSV data
    based on specified month and year.

    Attributes:
        urls (list): A list of valid URLs obtained from the EMT API.

    Methods:
        select_valid_urls(): Static method to fetch and return valid URLs from the EMT API.
        get_url(month, year): Retrieve a specific URL based on the given month and year.
        get_csv(month, year): Download and return CSV data for the given month and year.
    """  # noqa:E501
    EMT = "https://opendata.emtmadrid.es"
    GENERAL = "/Datos-estaticos/Datos-generales-(1)"

    PATTERN = r"/getattachment/[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}/trips_[1-9]+[0-9]+_[0-9]+_[a-zA-Z]+-csv.aspx"

    def __init__(self) -> None:
        self.urls = UrlEMT.select_valid_urls()

    @staticmethod
    def select_valid_urls():
        """
        Fetch and return a list of valid URLs from the EMT API.

        Raises:
            ConnectionError: If there's an issue with the network connection or the API response. # noqa:E501

        Returns:
            list: A list of valid URLs.
        """

        response = requests.get(UrlEMT.EMT + UrlEMT.GENERAL)

        if response.status_code != 200:
            raise ConnectionError('Some error happened when requesting data')

        return get_links(response.content.decode('utf-8'), pattern=UrlEMT.PATTERN)

    def get_url(self, month: int, year: int) -> str:
        """
        Retrieve a specific URL from the stored URLs based on the given month and year. # noqa:E501

        Args:
            month (int): The month for which data URL is required.
            year (int): The year for which data URL is required.

        Raises:
            ValueError: If the month or year is out of the expected range.

        Returns:
            str: A URL matching the given month and year.
        """

        if not 1 <= month <= 12 and not 21 <= year <= 23:
            raise ValueError('Both month and year values are incorrect')
        elif not 1 <= month <= 12:
            raise ValueError('Month value not correct')
        elif not 21 <= year <= 23:
            raise ValueError('Year value not correct')

        pattern = re.compile(rf"trips_{year}_{month:02}")

        for url in self.urls:
            if pattern.search(url):
                return url

        raise ValueError("Check input, match not found")

    def get_csv(self, month: int, year: int, **kwargs) -> StringIO:
        """
        Download and return CSV data from the EMT API for the given month and year. # noqa: E501

        Args:
            month (int): The month for which CSV data is required.
            year (int): The year for which CSV data is required.

        Returns:
            StringIO: A StringIO object containing the CSV data.
        """

        url = UrlEMT.EMT + self.get_url(month, year)

        return csv_from_zip(url, **kwargs)[0]


class BiciMad():
    """
    A class for handling and analyzing BiciMad bike use data.

    This class provides functionalities to load, clean, and summarize data
    related to bike trips from the BiciMad service.

    Attributes:
        COLUMNS (List[str]): Columns to be used when reading the bike trip data. # noqa: E501
        INDEX_COL (str): Column to be set as the index in the data frame.
        _data (pd.DataFrame): Private. A DataFrame holding the bike trip data.

    Methods:
        __init__(month: int, year: int): Initializes the BiciMad instance with data retrieved for the specified month and year.
        get_data(month: int, year: int) -> pd.DataFrame: Static method to retrieve bike trip data as a Pandas DataFrame.
        data() -> pd.DataFrame: Property that returns the bike trip data.
        __str__() -> str: Strin representation of the Pandas DataFrame loaded as the data.
        clean(columns: List[str]) -> None: Drop NaN values and convert the specified columns to strings.
        resume() -> pd.Series: Provides a summary of the bike usage statistics, including total uses and most popular stations.
        day_time() -> pd.Series: Calculates the total bike usage time per day.
        weekday_time() -> pd.Series: Calculates the total bike usage time by day of the week.
        total_usage_day() -> pd.Series: Computes the total number of uses per day.
    """

    COLUMNS = [
        'fecha',
        'idBike',
        'fleet',
        'trip_minutes',
        'geolocation_unlock',
        'address_unlock',
        'unlock_date',
        'locktype',
        'unlocktype',
        'geolocation_lock',
        'address_lock',
        'lock_date',
        'station_unlock',
        'unlock_station_name',
        'station_lock',
        'lock_station_name'
    ]

    CLEAN_COLUMNS = [
        'fleet',
        'idBike',
        'station_lock',
        'station_unlock'
    ]

    INDEX_COL = 'fecha'

    DAYS = ['L', 'M', 'X', 'J', 'V', 'S', 'D']

    def __init__(self, month: int, year: int) -> None:
        self._data = BiciMad.get_data(month, year)

    @staticmethod
    def get_data(month: int, year: int) -> pd.DataFrame:
        """
        Static method to retrieve bike sharing data for a specified month and year.

        This method fetches data from the UrlEMT service and loads it into a pandas DataFrame.

        Args:
            month (int): The month for which data is to be retrieved.
            year (int): The year for which data is to be retrieved.

        Returns:
            pd.DataFrame: A DataFrame containing the bike use data.  # noqa: E501
        """

        emt_instance = UrlEMT()

        csv_content = emt_instance.get_csv(month, year)

        return pd.read_csv(
            filepath_or_buffer=csv_content,
            sep=';',
            header=0,
            usecols=BiciMad.COLUMNS,
            index_col=BiciMad.INDEX_COL,
            skip_blank_lines=True,
            parse_dates=True)

    @property
    def data(self) -> pd.DataFrame:
        """
        Property that returns the bike use data.

        Returns:
            pd.DataFrame: The DataFrame containing the bike use data.
        """
        return self._data

    def __str__(self) -> str:
        """
        Provide a string representation of the bike use data.

        Returns:
            str: A string representation of the DataFrame.
        """
        return self._data.__str__()

    def clean(
            self,
            columns: List[str] = CLEAN_COLUMNS) -> None:
        """
        Clean the specified columns in the data.

        This method drops rows where all elements are NaN
        and converts the specified columns to strings.

        Args:
            columns (List[str]): A list of column names to be cleaned.
        """
        self.data.dropna(how='all', inplace=True)

        self.data[columns] = self.data[columns].astype(str)

    def resume(self) -> pd.Series:
        """
        Provide a summary of bike usage statistics.

        Calculates total uses, total time, most popular stations, and usage from the most popular stations.

        Returns:
            pd.Series: A pandas Series containing summarized bike usage statistics. # noqa:E501
        """
        total_uses = self.data.shape[0]

        total_time = self.data['trip_minutes'].sum() / 60

        most_popular_stations = self.data['address_unlock'].mode()

        uses_most_popular = self.data[self.data['address_unlock']
                                      .isin(most_popular_stations)].shape[0]

        summary = pd.Series(
            [
                self.data.index.year[0],
                self.data.index.month[0],
                total_uses,
                total_time,
                most_popular_stations.to_list(),
                uses_most_popular
            ],
            index=[
                'year',
                'month',
                'total_uses',
                'total_time',
                'most_popular_station',
                'uses_from_most_popular'
            ]
        )

        return summary

    def day_time(self) -> pd.Series:
        """
        Calculate total bike usage time per day.

        Returns:
            pd.Series: A pandas Series with the sum of trip minutes for each day. # noqa:E501
        """

        return self.data.groupby(['fecha']).trip_minutes.sum() / 60

    def weekday_time(self) -> pd.Series:
        """
        Calculate total bike usage time by day of the week.

        Returns:
            pd.Series: A pandas Series with the sum of trip minutes grouped by day of the week. # noqa:E501
        """

        return (
            self.data.groupby(
                self.data.index.weekday.map(
                    lambda x: BiciMad.DAYS[int(x)]
                )
            ).trip_minutes.sum() / 60
        )

    def total_usage_day(self) -> pd.Series:
        """
        Compute the total number of bike uses per day.

        Returns:
            pd.Series: A pandas Series with the count of uses for each day.
        """

        return self.data.groupby(['fecha']).size()

    def weekly_group(self) -> pd.Series:
        """
        Compute theuse of byke grouped by date and lock station.

        Returns:
            pd.Series: A pandas Series with the count of use for each day and station.    
        """

        return self.data.groupby(
            [pd.Grouper(freq="1D"), 'station_unlock']
        ).size()
