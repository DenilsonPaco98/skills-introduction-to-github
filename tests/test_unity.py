import pytest
from unittest.mock import patch, MagicMock
from datetime import date
import sys
import os

# Adicione o diretório raiz do projeto ao Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from src.main import verifica_arquivo, cria_diretorio, recupera_arquivo, importa_dados, define_dtype, valida_regras_qualidade, grava_resultado_dq, alocacao_hf
from src.funcoes import Funcoes # Assumindo que a classe Funcoes está definida aqui

@pytest.fixture
def setup_env():
    s3_mock = MagicMock()
    s3_resource_mock = MagicMock()
    funcoes_mock = Funcoes(s3_mock, s3_resource_mock)
    return funcoes_mock, "s3://input_path", "file.csv", "output_path"

@patch('src.main.Funcoes')
def test_verifica_arquivo(MockFuncoes, setup_env):
    funcoes_mock, S3_INPUT_PATH, S3_INPUT_NPS_FILE, _ = setup_env
    MockFuncoes.return_value = funcoes_mock

    funcoes_mock.verifica_arquivo_s3.return_value = True
    assert verifica_arquivo() == True

    funcoes_mock.verifica_arquivo_s3.return_value = False
    with pytest.raises(NameError, match=f"O arquivo '{S3_INPUT_NPS_FILE}' não existe no Bucket '{S3_INPUT_PATH}'. Favor providencias a cópia dos arquivos pra ele."):
        verifica_arquivo()

@patch('src.main.Funcoes')
def test_cria_diretorio(MockFuncoes, setup_env):
    funcoes_mock, _, _, S3_OUTPUT_PATH = setup_env
    MockFuncoes.return_value = funcoes_mock

    funcoes_mock.verifica_diretorio_existe.return_value = True
    assert cria_diretorio() == None

    funcoes_mock.verifica_diretorio_existe.return_value = False
    cria_diretorio()
    funcoes_mock.cria_diretorio.assert_called_with('tb_xt3_spec_fato_alocacao_hf', S3_OUTPUT_PATH)

@patch('src.main.Funcoes')
def test_recupera_arquivo(MockFuncoes, setup_env):
    funcoes_mock, S3_INPUT_PATH, _, _ = setup_env
    MockFuncoes.return_value = funcoes_mock

    funcoes_mock.recupera_arquivo_mais_atual_bucket.return_value = "latest_file.csv"
    arquivo = recupera_arquivo()
    assert arquivo == f"s3://{S3_INPUT_PATH}/latest_file.csv"

@patch('src.main.Funcoes')
@patch('src.main.spark')
def test_importa_dados(mock_spark, MockFuncoes, setup_env):
    funcoes_mock, _, S3_INPUT_NPS_FILE, _ = setup_env
    MockFuncoes.return_value = funcoes_mock

    df_mock = MagicMock()
    mock_spark.read.option.return_value.csv.return_value = df_mock
    df_mock.rdd.isEmpty.return_value = False

    result = importa_dados(S3_INPUT_NPS_FILE)
    assert result == df_mock

    df_mock.rdd.isEmpty.return_value = True
    with pytest.raises(NameError, match="O processo não conseguiu encontrar os dados."):
        importa_dados(S3_INPUT_NPS_FILE)

def test_define_dtype():
    df_mock = MagicMock()
    result = define_dtype(df_mock)
    assert result == df_mock

@patch('src.main.QualityEngine')
def test_valida_regras_qualidade(MockQualityEngine):
    df_mock = MagicMock()
    quality_engine_mock = MockQualityEngine.return_value
    quality_engine_mock.run_evaluate.return_value = MagicMock()
    
    result = valida_regras_qualidade(df_mock)
    assert result == quality_engine_mock.run_evaluate.return_value

@patch('src.main.ConnCatalog')
def test_grava_resultado_dq(MockConnCatalog):
    result_mock = MagicMock()
    result_mock.get_df.return_value = MagicMock()
    result_mock.is_rules_successful.return_value = True

    conn_catalog_mock = MockConnCatalog.return_value
    assert grava_resultado_dq(result_mock) == True
    conn_catalog_mock.put_df_to_table.assert_called()

@patch('src.main.verifica_arquivo')
@patch('src.main.cria_diretorio')
@patch('src.main.recupera_arquivo')
@patch('src.main.importa_dados')
@patch('src.main.define_dtype')
@patch('src.main.valida_regras_qualidade')
@patch('src.main.grava_resultado_dq')
@patch('builtins.print')
def test_alocacao_hf(mock_print, mock_grava, mock_valida, mock_define, mock_importa, mock_recupera, mock_cria, mock_verifica):
    mock_verifica.return_value = True
    mock_recupera.return_value = "s3://input_path/latest_file.csv"
    mock_importa.return_value = MagicMock()
    mock_define.return_value = MagicMock()
    mock_valida.return_value = MagicMock()
    mock_grava.return_value = True

    alocacao_hf()

    mock_verifica.assert_called_once()
    mock_cria.assert_called_once()
    mock_recupera.assert_called_once()
    mock_importa.assert_called_once()
    mock_define.assert_called_once()
    mock_valida.assert_called_once()
    mock_grava.assert_called_once()
    mock_print.assert_called()

    # Teste para o caso de falha na gravação
    mock_grava.return_value = False
    with pytest.raises(Exception, match="Falha ao gravar os resultados da qualidade dos dados"):
        alocacao_hf()