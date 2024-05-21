import submit_stac
from unittest.mock import patch

@patch('submit_stac.IngestionApi')
def test_submission_handler_dry_run(mock_get_cognito_service_details, capsys):
  
  fake_event = {
    "dry_run": "dry run",
    "stac_file_url": "http://www.test.com",
    "stac_item": 123
  }

  res = submit_stac.submission_handler(fake_event)

  assert res == None
  captured = capsys.readouterr()
  assert "Dry run, not inserting" in captured.out
