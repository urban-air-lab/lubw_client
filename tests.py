import pytest
import fetch_and_publish_lubw_data
from fetch_and_publish_lubw_data import *


def test_timestamp_with_offset_types():
    actual = fetch_and_publish_lubw_data.get_timestamps_with_offset()
    assert isinstance(actual, tuple)
    assert isinstance(actual[0], str)
    assert isinstance(actual[1], str)


def test_timestamp_with_offset_format():
    actual = fetch_and_publish_lubw_data.get_timestamps_with_offset()
    datetime.strptime(actual[0], "%Y-%m-%dT%H:%M:%S")
    datetime.strptime(actual[1], "%Y-%m-%dT%H:%M:%S")


def test_get_lubw_data_success(mocker):
    mock_response = mocker.Mock()
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
    mocker.patch("fetch_and_publish_lubw_data.requests.get", return_value=mock_response)

    parameters = {'komponente': 'PM10', 'von': '2025-05-18T17:00:00', 'bis': '2025-05-18T18:00:00', 'station': 'DEBW015'}

    result = get_lubw_data(None, params=parameters)
    assert result == mock_response.json.return_value

