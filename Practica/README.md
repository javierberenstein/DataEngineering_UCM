# EMT Data Fetching and Analyzing Package

## Overview

The EMT Data Handling package provides users a set of tools to interact with the EMT Madrid Open Data API, specially focused on the data relative to the use of public e-bikes of Madrid. It offers functionalities for fetching URLs of available data, downloading and treat the CSV files and analyzing the bike use from BiciMAD public service. 

## Table of Contents
1. [Features](#features)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Test run](#tests)
5. [License](#license)

## Features

The package is composed by two classes found in the objects directory:

- UrlEMT: The class is responsible for fetching the URLs from EMT API and retrieving CSV data based on a given month and year.

- BiciMad: Handles and analyzes the BiciMAD e-bike use data, including loading, cleaning, summarizing and grouping data. Provides a set of methods that offers insights about the use. 

## Installation

To install the EMT packaga, follow this steps.

1. Clone the repository or download the source code.
2. Navigate to the project's root directory.
3. Build the package using the following command:

`python -m build`

4. Install the package using pip:

`pip install dist/EMT-1.0.0-py3-none-any.whl`

Replace the name with the actual name of the current version.

## Usage
### UrlEMT Class

```python
from EMT.objects import UrlEMT

# Initialize the UrlEMT object
emt = UrlEMT()

# Retrieve a specific URL for a given month and year
url = emt.get_url(5, 2023)

# Download and return CSV data for the specified month and year
csv_data = emt.get_csv(5, 2023)
```

### BiciMad Class

```python
from EMT.objects import BiciMad

# Initialize the BiciMad object with specific month and year
bicimad = BiciMad(month=5, year=2023)

# Access the bike use data
data = bicimad.data

# Clean the data
bicimad.clean(columns=['fleet', 'idBike', 'station_lock', 'station_unlock'])

# Get summarized bike usage statistics
summary = bicimad.resume()

# Calculate total bike usage time by day of the week
weekday_usage = bicimad.weekday_time()

# Compute the total number of bike uses per day
daily_usage = bicimad.total_usage_day()
```

## Tests
This package, designed for academic purposes, includes a comprehensive test suite to ensure the functionality and reliability of the code. The test suite is an integral part of the repository and can be executed to verify the correct behavior of the package components.

`python testes/run_tests.py`

This command will initiate the test suite, which will automatically run all the tests included in the tests directory.

## License

This project is licensed under GPL-3.0. For more information, see the LICENSE file.