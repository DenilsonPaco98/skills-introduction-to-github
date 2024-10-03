############################

############################
import sys
import time
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, FloatType, DateType, IntegerType
from datetime import date
from itaudatautils.data_utils import DataUtils
from app.utils.funcoes import Funcoes
import logging

#Definindo log
log = logging.getLogger('–> tb_xt3_spec_fato_alocacao_hf: ')
log.setLevel(logging.DEBUG)

log.info("==== Iniciando a execução do job ====")

############################

#PARAMETROS
############################

#Lendo os parametros do Glue Job que serao utilizados
log.info("Capturando valor dos parametros do job.")
args = getResolvedOptions(sys.argv, ["S3_INPUT_PATH",
"FILE_FORMAT",
"S3_OUTPUT_PATH",
"GLUE_OUTPUT_DATABASE",
"GLUE_OUTPUT_DATABASE_QUALITY",
"S3_INPUT_FILE",
"GLUE_OUTPUT_TABLE_QUALITY",
"S3_BUCKET_SOR"])

############################

#SPARK SESSION
############################

#Instanciando a spark session, utilizando o DataUtils
datautils = DataUtils.get_spark()
spark = datautils.engine.spark_session
quality_engine = datautils.get_quality_engine()
conn_catalog = datautils.get_catalog_connector()
s3 = boto3.client("s3")
s3_resource = boto3.resource("s3")
funcoes = Funcoes(s3, s3_resource)

#Configurando variaveis que serão utilizadas
S3_INPUT_PATH = args["S3_INPUT_PATH"]
S3_INPUT_NPS_FILE = args["S3_INPUT_FILE"]
S3_FILE_FORMAT = args["FILE_FORMAT"]
S3_OUTPUT_PATH = args["S3_OUTPUT_PATH"]
GLUE_OUTPUT_DATABASE = args["GLUE_OUTPUT_DATABASE"]
GLUE_OUTPUT_DATABASE_QUALITY = args["GLUE_OUTPUT_DATABASE_QUALITY"]
GLUE_OUTPUT_TABLE_QUALITY = args["GLUE_OUTPUT_TABLE_QUALITY"]
S3_BUCKET_SOR = args["S3_BUCKET_SOR"]
PATH_DESTINO = "s3://" + args["S3_OUTPUT_PATH"] + "/" + GLUE_OUTPUT_DATABASE

JOB_NAME = "tb_xt3_spec_fato_alocacao_hf"
db_destino_quality = "db_corp_repositoriosdedados_sgit_sor_01"
tb_destino_quality = "dataquality_metrics"
today = date.today()

def verifica_arquivo():
    if not funcoes.verifica_arquivo_s3(S3_INPUT_PATH, S3_INPUT_NPS_FILE):
        raise NameError(f"O arquivo '{S3_INPUT_NPS_FILE}' não existe no Bucket '{S3_INPUT_PATH}'. Favor providencias a cópia dos arquivos pra ele.")

def cria_diretorio():
    if not funcoes.verifica_diretorio_existe(S3_OUTPUT_PATH, "tb_xt3_spec_fato_alocacao_hf"):
        log.info(f"Criando novo diretorio do processo no S3: '{PATH_DESTINO}'")
        funcoes.cria_diretorio("tb_xt3_spec_fato_alocacao_hf", S3_OUTPUT_PATH)

def recupera_arquivo():
    arquivo_mais_atual_envio = funcoes.recupera_arquivo_mais_atual_bucket(S3_FILE_FORMAT, S3_INPUT_PATH, S3_INPUT_NPS_FILE)
    return f"s3://{S3_INPUT_PATH}/{arquivo_mais_atual_envio}"

def importa_dados(arquivo_mais_atual_envio):
    log.info(f"Importando os dados dos arquivos para DataFrame spark.")
    cgitgluealocacaohf = spark.read.option("inferSchema", True).option("header", True).option("delimiter", "|").option("encoding", "latin1").csv(arquivo_mais_atual_envio)
    if cgitgluealocacaohf.rdd.isEmpty():
        raise NameError("O processo não conseguiu encontrar os dados.")
    return cgitgluealocacaohf

def define_dtype(cgitgluealocacaohf):
    log.info("Definindo dtype das colunas do DataFrame Spark")
    return cgitgluealocacaohf\
        .withColumn("cod_re", cgitgluealocacaohf["cod_re"].cast(StringType()))\
        .withColumn("cod_pre", cgitgluealocacaohf["cod_pre"].cast(StringType()))\
        .withColumn("cod_comunidade", cgitgluealocacaohf["cod_comunidade"].cast(StringType()))\
        .withColumn("cod_releasetrain", cgitgluealocacaohf["cod_releasetrain"].cast(StringType()))\
        .withColumn("cod_squad", cgitgluealocacaohf["cod_squad"].cast(StringType()))\
        .withColumn("des_especialidade", cgitgluealocacaohf["des_especialidade"].cast(StringType()))\
        .withColumn("cod_fornecedor", cgitgluealocacaohf["cod_fornecedor"].cast(StringType()))\
        .withColumn("cod_are", cgitgluealocacaohf["cod_are"].cast(StringType()))\
        .withColumn("des_status_alocacao", cgitgluealocacaohf["des_status_alocacao"].cast(StringType()))\
        .withColumn("qtd_horasplanejadas", cgitgluealocacaohf["qtd_horasplanejadas"].cast(FloatType()))\
        .withColumn("dt_inicio_alocacao", cgitgluealocacaohf["dt_inicio_alocacao"].cast(DateType()))\
        .withColumn("dt_fim_alocacao", cgitgluealocacaohf["dt_fim_alocacao"].cast(DateType()))\
        .withColumn("cod_funcional", cgitgluealocacaohf["cod_funcional"].cast(StringType()))\
        .withColumn("des_nome", cgitgluealocacaohf["des_nome"].cast(StringType()))\
        .withColumn("num_cpf", cgitgluealocacaohf["num_cpf"].cast(StringType()))\
        .withColumn("dt_criacao", cgitgluealocacaohf["dt_criacao"].cast(DateType()))\
        .withColumn("dt_atualizacao", cgitgluealocacaohf["dt_atualizacao"].cast(DateType()))\
        .withColumn("dt_ref", cgitgluealocacaohf["dt_ref"].cast(DateType()))\
        .withColumn("dt_processamento", cgitgluealocacaohf["dt_processamento"].cast(DateType()))
def valida_regras_qualidade(cgitgluealocacaohf):
    log.info("Iniciando a validação das regras de QD…")
    log.info("Aplicando regras de qualidade de dados.")
    rulesets = {
    "rules": [
    "IsPrimaryKey" "cod_re",
    "ColumnExists" "cod_funcional",
    "ColumnExists" "cod_pre",
    "IsComplete" "cod_funcional",
    "ColumnExists" "cod_pre"
    ],
    "database_name": GLUE_OUTPUT_DATABASE,
    "table_name": "tb_xt3_spec_fato_alocacao_hf",
    "partition": "n/a"
    }
    result = quality_engine.run_evaluate(dataframe=cgitgluealocacaohf, rulesets=rulesets)
    return result

def grava_resultado_dq(result):
    log.info("Coloca o resultado DQ DataFrame")
    df_result_dq = result.get_df()
    log.info(f"Resultado do DQ: {df_result_dq.show()}")
    log.info(f"Grava resultado DQ na tabela de QD "{db_destino_quality + "." + tb_destino_quality}'.'")
    conn_catalog.put_df_to_table(dataframe=df_result_dq, database=db_destino_quality, table=tb_destino_quality, write_mode="append")
    return result.is_rules_successful()

def grava_dados_processados(cgitgluealocacaohf):
    log.info("Gravando os dados processados pelo job na tabela do catalogo local.")
    conn_catalog.put_df_to_table(dataframe=cgitgluealocacaohf, database=GLUE_OUTPUT_DATABASE, table="tb_xt3_spec_fato_alocacao_hf", write_mode="append", format="parquet", compression="snappy", repartition=1)

def alocacao_hf():
    try:
        verifica_arquivo()
        cria_diretorio()
        arquivo_mais_atual_envio = recupera_arquivo()
        cgitgluealocacaohf = importa_dados(arquivo_mais_atual_envio)
        cgitgluealocacaohf = define_dtype(cgitgluealocacaohf)
        result = valida_regras_qualidade(cgitgluealocacaohf)
        if grava_resultado_dq(result):
            log.info("Teste DQ passou com sucesso!")
        else:
            log.info("Teste DQ falhou. Veja os detalhes a seguir.")
            grava_dados_processados(cgitgluealocacaohf)
            log.info("O processamento foi finalizado com sucesso!")
    except Exception as e:
        raise e
funcoes.observability(alocacao_hf, "tb_xt3_spec_fato_alocacao_hf", JOB_NAME)