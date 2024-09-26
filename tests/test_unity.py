import pytest
import requests
from unittest.mock import patch
from src.main import main  # Corrigido para importar da pasta src

def test_main_success():
    # Mockando a resposta da API
    with patch('requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [{"id": 1, "title": "Test Title"}]
        
        # Redirecionar a saída para uma variável
        with patch('builtins.print') as mock_print:
            main()
            mock_print.assert_called_with("Response:", [{"id": 1, "title": "Test Title"}])

def test_main_failure():
    # Mockando a resposta da API para falha
    with patch('requests.get') as mock_get:
        mock_get.return_value.status_code = 404
        
        # Redirecionar a saída para uma variável
        with patch('builtins.print') as mock_print:
            main()
            mock_print.assert_called_with("GET request not worked. Status code:", 404)

if __name__ == "__main__":
    pytest.main()
