import pytest
from fetch_and_publish_lubw_data import get_lubw_data


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

