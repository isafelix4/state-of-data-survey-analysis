import sys
import re
import unicodedata
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# CONFIGURAÇÃO — troque pelo nome do seu bucket
# ============================================================
BUCKET = "fiap26-data-analytics-824232672586"

# ============================================================
# limpeza de nomes de coluna: resolve as tuplas Python de 2023
# (ex. "('P1_a ', 'Idade')") e os acentos/pontuação de todos os anos
# ============================================================
def clean_col(name: str) -> str:
    m = re.match(r"^\(\s*'([^']+)'\s*,\s*'([^']*)'\s*\)$", name)
    if m:
        code, label = m.group(1).strip(), m.group(2).strip()
        name = f"{code}_{label}"
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name).strip('_').lower()
    return re.sub(r'_+', '_', name)

# renomeia só as colunas que existirem no rename_map; avisa (não quebra)
# se alguma coluna esperada não for encontrada — confira o log do Job
# se aparecer algum AVISO
def safe_rename(df, rename_map, year):
    existing = set(df.columns)
    faltando = [k for k in rename_map if k not in existing]
    if faltando:
        print(f"AVISO ({year}): colunas esperadas nao encontradas apos limpeza: {faltando}")
    exprs = [F.col(c).alias(rename_map[c]) if c in rename_map else F.col(c) for c in df.columns]
    return df.select(*exprs)

# ============================================================
# dicionário ano -> {nome limpo original: nome padrão}, obtido
# aplicando clean_col() nos headers reais dos 3 CSVs
# ============================================================
RENAME_2023 = {
    "p0_id": "respondente_id",
    "p1_a_idade": "idade",
    "p1_a_1_faixa_idade": "faixa_idade",
    "p1_b_genero": "genero",
    "p1_c_cor_raca_etnia": "cor_raca_etnia",
    "p1_d_pcd": "pcd",
    "p1_i_1_uf_onde_mora": "uf_onde_mora",
    "p1_i_2_regiao_onde_mora": "regiao_onde_mora",
    "p1_l_nivel_de_ensino": "nivel_ensino",
    "p1_m_area_de_formacao": "area_formacao",
    "p2_b_setor": "setor",
    "p2_c_numero_de_funcionarios": "numero_funcionarios",
    "p2_d_gestor": "atua_como_gestor",
    "p2_f_cargo_atual": "cargo_atual",
    "p2_g_nivel": "senioridade",
    "p2_h_faixa_salarial": "faixa_salarial",
    "p2_r_atualmente_qual_a_sua_forma_de_trabalho": "modelo_trabalho_atual",
    "p4_i_cloud_preferida": "cloud_preferida",
    "p4_m_utiliza_chatgpt_ou_llms_no_trabalho": "usa_chatgpt_llm_pessoal",
    "p3_e_ai_generativa_e_uma_prioridade_em_sua_empresa": "ai_generativa_prioridade_empresa",
}

RENAME_2024 = {
    "0_a_token": "respondente_id",
    "1_a_idade": "idade",
    "1_a_1_faixa_idade": "faixa_idade",
    "1_b_genero": "genero",
    "1_c_cor_raca_etnia": "cor_raca_etnia",
    "1_d_pcd": "pcd",
    "1_i_1_uf_onde_mora": "uf_onde_mora",
    "1_i_2_regiao_onde_mora": "regiao_onde_mora",
    "1_l_nivel_de_ensino": "nivel_ensino",
    "1_m_area_de_formacao": "area_formacao",
    "2_b_setor": "setor",
    "2_c_numero_de_funcionarios": "numero_funcionarios",
    "2_d_atua_como_gestor": "atua_como_gestor",
    "2_f_cargo_atual": "cargo_atual",
    "2_g_nivel": "senioridade",
    "2_h_faixa_salarial": "faixa_salarial",
    "2_r_modelo_de_trabalho_atual": "modelo_trabalho_atual",
    "4_i_cloud_preferida": "cloud_preferida",
    "4_m_usa_chatgpt_ou_copilot_no_trabalho": "usa_chatgpt_llm_pessoal",
    "3_e_ai_generativa_e_llm_e_uma_prioridade": "ai_generativa_prioridade_empresa",
}

RENAME_2025 = {
    "0_a_token": "respondente_id",
    "1_a_idade": "idade",
    "1_a_1_faixa_idade": "faixa_idade",
    "1_b_genero": "genero",
    "1_c_cor_raca_etnia": "cor_raca_etnia",
    "1_d_pcd": "pcd",
    "1_i_1_uf_onde_mora": "uf_onde_mora",
    "1_i_2_regiao_onde_mora": "regiao_onde_mora",
    "1_l_nivel_de_ensino": "nivel_ensino",
    "2_b_setor": "setor",
    "2_c_numero_de_funcionarios": "numero_funcionarios",
    "2_d_atua_como_gestor": "atua_como_gestor",
    "2_f_cargo_atual": "cargo_atual",
    "2_g_nivel": "senioridade",
    "2_h_faixa_salarial": "faixa_salarial",
    "2_q_modelo_de_trabalho_atual": "modelo_trabalho_atual",
    "4_f_cloud_preferida": "cloud_preferida",
    "4_j_usa_chatgpt_ou_copilot_no_trabalho": "usa_chatgpt_llm_pessoal",
    "3_e_ai_generativa_e_llm_e_uma_prioridade": "ai_generativa_prioridade_empresa",
}

RENAME_BY_YEAR = {2023: RENAME_2023, 2024: RENAME_2024, 2025: RENAME_2025}

# campo confirmado com codificação diferente por ano (0/1 em 2023,
# TRUE/FALSE em 2024/2025); se a query de validação achar outro campo
# assim, adicione aqui
BOOLEAN_FIELDS = ["atua_como_gestor"]

# ============================================================
# processa os 3 anos
# ============================================================
for year in [2023, 2024, 2025]:
    path = f"s3://{BUCKET}/bronze/state_of_data/ano={year}/"
    df = (spark.read
          .option("header", True)
          .option("multiLine", True)
          .option("quote", '"')
          .option("escape", '"')
          .option("encoding", "UTF-8")
          .csv(path))

    linhas_brutas = df.count()

    # captura o arquivo de origem logo na leitura, antes de qualquer
    # shuffle (o dropDuplicates mais abaixo é um) -- depois de um shuffle,
    # input_file_name() perde o contexto e volta vazio
    df = df.withColumn("etl_source_file", F.input_file_name())

    # um único select (em vez de várias chamadas encadeadas de
    # withColumnRenamed) -- evita aprofundar demais o plano do Spark; foi
    # exatamente isso que causou o StackOverflowError, com ~300
    # renomeações encadeadas por cima de outras ~300 do trim logo depois.
    # As colunas cruas podem ter ponto no nome (ex. "1.i.1_uf_onde_mora"),
    # que o Spark interpreta como acesso a campo aninhado -- por isso o
    # nome vem entre crases (`{old}`), forçando a leitura como um nome
    # literal só. etl_source_file já está pronta, só passa direto (sem
    # clean_col, senão perderia o "_" do começo).
    df = df.select(
        *[F.col(f"`{old}`").alias(clean_col(old)) for old in df.columns if old != "etl_source_file"],
        F.col("etl_source_file"),
    )

    df = safe_rename(df, RENAME_BY_YEAR[year], year)

    # trim + string vazia -> null, em todas as colunas texto -- também
    # num único select, pelo mesmo motivo
    trim_exprs = []
    for c, t in df.dtypes:
        if t == "string":
            trim_exprs.append(F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))).alias(c))
        else:
            trim_exprs.append(F.col(c))
    df = df.select(*trim_exprs)

    # duplicatas: primeiro linha inteira repetida, depois respondente_id
    # repetido (mantém a primeira ocorrência) — comum em respostas de
    # formulário reenviadas duas vezes
    df = df.dropDuplicates()
    linhas_apos_dedup_total = df.count()
    if "respondente_id" in df.columns:
        df = df.dropDuplicates(["respondente_id"])
    linhas_apos_dedup_id = df.count()

    df = df.withColumn("ano_pesquisa", F.lit(year))

    if "idade" in df.columns:
        df = df.withColumn("idade", F.col("idade").cast("int"))
        # faixa implausível -> null (não descarta a linha, só o valor suspeito)
        df = df.withColumn(
            "idade",
            F.when((F.col("idade") < 14) | (F.col("idade") > 100), None).otherwise(F.col("idade"))
        )

    for c in BOOLEAN_FIELDS:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(F.upper(F.col(c).cast("string")).isin("TRUE", "1"), F.lit(1))
                 .when(F.upper(F.col(c).cast("string")).isin("FALSE", "0"), F.lit(0))
                 .otherwise(None)
            )

    # coluna de linhagem/auditoria — etl_source_file já foi capturada logo na
    # leitura (antes do dropDuplicates); aqui só falta o timestamp
    df = df.withColumn("etl_control_column", F.current_timestamp())

    print(f"--- ano {year}: {linhas_brutas} linhas brutas -> "
          f"{linhas_apos_dedup_total} apos remover duplicatas exatas -> "
          f"{linhas_apos_dedup_id} apos remover respondente_id duplicado "
          f"({len(df.columns)} colunas) ---")

    dyf = DynamicFrame.fromDF(df, glueContext, f"silver_{year}")
    sink = glueContext.getSink(
        path=f"s3://{BUCKET}/silver/state_of_data/pesquisa_{year}/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        enableUpdateCatalog=True,
    )
    sink.setCatalogInfo(catalogDatabase="silver_db", catalogTableName=f"tbl_state_of_data_{year}")
    sink.setFormat("parquet", useGlueParquetWriter=True)
    sink.writeFrame(dyf)

job.commit()