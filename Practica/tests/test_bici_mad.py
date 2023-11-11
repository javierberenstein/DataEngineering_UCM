import pandas as pd
from pathlib import Path
from io import StringIO
from unittest.mock import patch

import pytest

from EMT.objects import BiciMad

FIXTURE_DIR = Path(__file__).parent.resolve() / 'data'


# FIXTURE
@pytest.fixture
def sample_df():
    with open(FIXTURE_DIR / 'sample_data.csv', 'r') as file:
        data = StringIO(file.read())

        return pd.read_csv(
            filepath_or_buffer=data,
            sep=';',
            header=0,
            usecols=BiciMad.COLUMNS,
            index_col=BiciMad.INDEX_COL,
            skip_blank_lines=True,
            parse_dates=True
            )

@pytest.fixture
def mock_get_csv():
    with patch('EMT.objects.UrlEMT.get_csv') as mock_get_csv:
        mock_get_csv.return_value = FIXTURE_DIR / 'sample_data.csv'
        yield mock_get_csv

@pytest.fixture
def bici_mad():

    bici_mad_instance = BiciMad(year=23, month=2)

    return bici_mad_instance

# HELPER FUNCTIONS
def drop_dfs_na(sample_df, bici_mad):

    bici_mad.clean()
    sample_df.dropna(how='all', inplace=True)

    return sample_df, bici_mad

# TESTS
def test_equality_of_data(mock_get_csv, bici_mad, sample_df):

    assert bici_mad.data.equals(sample_df)


def test_data_is_dataframe(mock_get_csv, bici_mad):

    data = bici_mad.data

    assert isinstance(data, pd.DataFrame)

def test_columns_are_as_expected(bici_mad):

    expected_columns = [
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

    assert bici_mad.data.columns.tolist() == expected_columns

@patch('EMT.objects.UrlEMT.get_csv')
def test_invalid_csv_input(mock_get_invalid_csv):

    mock_get_invalid_csv = FIXTURE_DIR / 'invalid_data.csv'

    with pytest.raises(Exception):

        BiciMad(year=23, month=2)


def test_get_data_failure():

    with pytest.raises(ValueError) as e:
        BiciMad(year=12, month=2)
    
    assert "Year value not correct" in str(e.value)

    with pytest.raises(ValueError) as e:
        BiciMad(year=22, month=13)

    assert "Month value not correct" in str(e.value)

    with pytest.raises(ValueError) as e:
        BiciMad(year=12, month=13)

    assert "Both month and year values are incorrect" in str(e.value)


def test_bicimad_str_method(mock_get_csv, bici_mad, sample_df):

    df = bici_mad.data

    assert str(df) == str(sample_df)


def test_clean_csv(mock_get_csv, bici_mad):

    bici_mad.clean()

    assert bici_mad.data.isnull().all(axis=1).sum() == 0

    for column in BiciMad.CLEAN_COLUMNS:
        assert bici_mad.data[column].dtype == object


def test_resume(mock_get_csv, bici_mad, sample_df):

    sample_df, bici_mad = drop_dfs_na(sample_df, bici_mad)

    expected_total_uses = sample_df.shape[0]

    expected_total_time = sample_df['trip_minutes'].sum() / 60

    expected_most_popular_stations = sample_df['address_unlock'].mode()

    expected_uses_most_popular = sample_df[
        sample_df['address_unlock'].isin(expected_most_popular_stations)
        ].shape[0]

    summary = bici_mad.resume()

    assert summary['total_uses'] == expected_total_uses
    assert summary['total_time'] == expected_total_time
    assert summary['most_popular_station'][0] == expected_most_popular_stations[0] # noqa:E501
    assert summary['uses_from_most_popular'] == expected_uses_most_popular


def test_day_time(mock_get_csv, bici_mad, sample_df):

    expected_day_time_usage = (
        sample_df.groupby(['fecha'])
        .trip_minutes.sum() / 60)

    assert bici_mad.day_time().all() == expected_day_time_usage.all()


def test_weekday_time(mock_get_csv, bici_mad, sample_df):

    sample_df, bici_mad = drop_dfs_na(sample_df, bici_mad)

    expected_weekday_time = (
            sample_df.groupby(
                sample_df.index.weekday.map(
                    lambda x: BiciMad.DAYS[int(x)]
                )
            ).trip_minutes.sum() / 60
        )

    assert bici_mad.weekday_time().all() == expected_weekday_time.all()


def test_total_usage_day(mock_get_csv, bici_mad, sample_df):

    sample_df, bici_mad = drop_dfs_na(sample_df, bici_mad)

    expected_total_usage_day = sample_df.groupby(['fecha']).size()

    assert bici_mad.total_usage_day().all() == expected_total_usage_day.all()
