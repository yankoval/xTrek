import importlib.util
from pathlib import Path
from unittest.mock import patch, MagicMock


def test_kin_info_uses_configured_true_api_source(monkeypatch):
    monkeypatch.delenv('HONEST_SIGN_TOKEN', raising=False)
    spec = importlib.util.spec_from_file_location('kin_info_checked', Path(__file__).parents[1] / 'kin_info.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    processor = MagicMock()
    processor.get_token_value_for.return_value = 'synthetic-selected-token'
    with patch('xtrek.tokens.TokenProcessor', return_value=processor), patch('xtrek.org_manager.OrganizationManager'), patch.object(Path, 'home', side_effect=AssertionError('Legacy cache must not be read')):
        assert module.find_token() == 'synthetic-selected-token'
    processor.get_token_value_for.assert_called_once_with(module.TARGET_INN, purpose='true_api')
