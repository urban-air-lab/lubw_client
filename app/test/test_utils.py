from unittest import mock
from unittest.mock import Mock

import pytest

from app.src.utils import *


def test_timestamp_with_offset_types():
    actual = get_timestamps_with_offset()
    assert isinstance(actual, tuple)
    assert isinstance(actual[0], str)
    assert isinstance(actual[1], str)


def test_timestamp_with_offset_format():
    actual = get_timestamps_with_offset()
    datetime.strptime(actual[0], "%Y-%m-%dT%H:%M:%S")
    datetime.strptime(actual[1], "%Y-%m-%dT%H:%M:%S")


def test_UTF8BasicAuth():
    response = requests.Request("GET", url="https://www.google.de", auth=UTF8BasicAuth("user", "password"))
    request = response.prepare()
    assert request.headers == {'Authorization': 'Basic dXNlcjpwYXNzd29yZA=='}


def test_get_config():
    actual = get_config("./test.yaml")
    assert actual["test"] == "value"

def test_fetch_station_data_wrong_time():
    with pytest.raises(ValueError) as e:
        fetch_station_data("teststation",
                                ["PM10"],
                                '2025-10-18T17:00:00+01:00',
                                '2025-05-18T18:00:00+01:00')
    assert "Start time must be before end time: 2025-10-18T17:00:00+01:00" in str(e)


@mock.patch("app.test.test_utils.requests.sessions.Session.get")
def test_fetch_station_data(response_mock):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'station': 'Heilbronn',
        'komponente': 'Schwebstaub PM10 kontinuierlich',
        'einheit': 'µg/m³', 'messwerte': [
            {'startZeit': '2025-05-18T17:00:00+01:00',
             'endZeit': '2025-05-18T18:00:00+01:00',
             'wert': 14.14}
        ]
    }
    response_mock.return_value = mock_response

    actual = fetch_station_data("teststation",
                            ["PM10"],
                            '2025-05-18T17:00:00+01:00',
                            '2025-05-18T18:00:00+01:00')

    expected = pd.DataFrame({"datetime": pd.Timestamp("2025-05-18T18:00:00+01:00"),
                             "PM10": 14.14}, index=[0])
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False)


@mock.patch("app.test.test_utils.requests.sessions.Session.get")
def test_get_lubw_data_success(response_mock):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'station': 'Heilbronn',
        'komponente': 'Schwebstaub PM10 kontinuierlich',
        'einheit': 'µg/m³', 'messwerte': [
            {'startZeit': '2025-05-18T17:00:00+01:00',
             'endZeit': '2025-05-18T18:00:00+01:00',
             'wert': 14.14}
        ]
    }
    response_mock.return_value = mock_response

    parameters = {'komponente': 'PM10', 'von': '2025-05-18T17:00:00', 'bis': '2025-05-18T18:00:00', 'station': 'DEBW015'}

    result = get_lubw_data(session=requests.Session(), params=parameters, next_link=None)
    assert result == mock_response.json.return_value

