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
# CONFIGURAÇÃO 
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
        print(f"AVISO ({year}): {len(faltando)} coluna(s) esperada(s) nao encontrada(s) apos limpeza: {faltando}")
    exprs = [F.col(c).alias(rename_map[c]) if c in rename_map else F.col(c) for c in df.columns]
    return df.select(*exprs)

# ============================================================
# mapeamento completo de colunas (405 campos)
# ============================================================
mapa_path = f"s3://{BUCKET}/silver/state_of_data/mapeamento_colunas_completo.csv"
mapa_rows = (spark.read
             .option("header", True)
             .option("quote", '"')
             .option("escape", '"')
             .csv(mapa_path)
             .collect())

def build_rename_map(rows, coluna_ano):
    rename = {}
    for row in rows:
        raw = row[coluna_ano]
        padrao = row["nome_padronizado"]
        if not raw or not padrao:
            continue
        rename[clean_col(raw)] = padrao
    return rename

RENAME_BY_YEAR = {
    2023: build_rename_map(mapa_rows, "nome_2023"),
    2024: build_rename_map(mapa_rows, "nome_2024"),
    2025: build_rename_map(mapa_rows, "nome_2025"),
}
print(f"mapeamento carregado: {len(mapa_rows)} campos padronizados")

# campos booleanos (TRUE/FALSE em 2024/2025, 0/1 em 2023 -- normalizar
# todos pro mesmo formato evita comparar string por engano em alguma
# análise futura). 
BOOLEAN_FIELDS = [
    "aspectos_prejudicados_qtd_vagas", "aspectos_prejudicados_senioridade_vagas",
    "aspectos_prejudicados_aprovacao_vagas", "aspectos_prejudicados_progressao_vagas",
    "aspectos_prejudicados_velocidade_progressao", "aspectos_prejudicados_nivel_stress",
    "aspectos_prejudicados_nivel_visibilidade", "aspectos_prejudicados_network_interno",
    "aspectos_prejudicados_network_externo", "vive_no_estado_de_formacao", "pais_atual_brasil",
    "satisfacao_atual", "insatisfacao_atual_salario", "insatisfacao_atual_beneficios",
    "insatisfacao_atual_proposito", "insatisfacao_atual_modalidade", "insatisfacao_atual_ambiente",
    "insatisfacao_atual_desenvolvimento", "insatisfacao_atual_progressao",
    "insatisfacao_atual_maturidade_dadosti", "insatisfacao_atual_relacao_gestao",
    "insatisfacao_atual_reputacao", "insatisfacao_atual_migrar", "criterios_emprego_salario",
    "criterios_emprego_beneficios", "criterios_emprego_proposito", "criterios_emprego_modalidade",
    "criterios_emprego_ambiente", "criterios_emprego_desenvolvimento", "criterios_emprego_progressao",
    "criterios_emprego_maturidade_dadosti", "criterios_emprego_relacao_gestao",
    "criterios_emprego_reputacao", "cargos_dados_analytics_engineer", "cargos_dados_data_engineer",
    "cargos_dados_data_analyst", "cargos_dados_data_scientist", "cargos_dados_dba",
    "cargos_dados_bi_analst", "cargos_dados_data_architect", "cargos_dados_dpm",
    "cargos_dados_business_analyst", "cargos_dados_mlai_engineer",
    "responsabilidades_como_gestor_lp_dados", "responsabilidades_como_gestor_treinamentos",
    "responsabilidades_como_gestor_selecao", "responsabilidades_como_gestor_ferramentas",
    "responsabilidades_como_gestor_equipe_engenharia", "responsabilidades_como_gestor_equipe_estudos",
    "responsabilidades_como_gestor_equipe_mlai", "responsabilidades_como_gestor_tecnico",
    "responsabilidades_como_gestor_projetos_dados", "responsabilidades_como_gestor_produtos_dados",
    "responsabilidades_como_gestor_pessoas", "desafios_como_gestor_contratar",
    "desafios_como_gestor_reter", "desafios_como_gestor_investir", "desafios_como_gestor_remoto",
    "desafios_como_gestor_multidisciplinaridade", "desafios_como_gestor_qualidade",
    "desafios_como_gestor_dados", "desafios_como_gestor_valor", "desafios_como_gestor_modelos",
    "desafios_como_gestor_expectativa", "desafios_como_gestor_manutencao",
    "desafios_como_gestor_inovacao", "desafios_como_gestor_roi", "desafios_como_gestor_tempo",
    "ai_uso_independente", "ai_uso_centralizado", "ai_uso_copilotos", "ai_uso_produtos_externos",
    "ai_uso_produtos_internos", "ai_uso_principal", "ai_uso_sem_uso", "ai_uso_sem_opiniao",
    "ai_motivo_desuso_cases", "ai_motivo_desuso_confiabilidade", "ai_motivo_desuso_regulamentacao",
    "ai_motivo_desuso_seguranca", "ai_motivo_desuso_roi", "ai_motivo_desuso_dados",
    "ai_motivo_desuso_expertise", "ai_motivo_desuso_lideranca",
    "ai_motivo_desuso_propriedade_intelectual", "origem_dados_relacionais", "origem_dados_nosql",
    "origem_dados_imagens", "origem_dados_documentos", "origem_dados_videos", "origem_dados_audios",
    "origem_dados_planilhas", "origem_dados_georeferenciados", "origem_dados_usados_relacionais",
    "origem_dados_usados_nosql", "origem_dados_usados_imagens", "origem_dados_usados_documentos",
    "origem_dados_usados_videos", "origem_dados_usados_audios", "origem_dados_usados_planilhas",
    "origem_dados_usados_georeferenciados", "linguagem_programacao_sql", "linguagem_programacao_r",
    "linguagem_programacao_python", "linguagem_programacao_c", "linguagem_programacao_net",
    "linguagem_programacao_java", "linguagem_programacao_julia", "linguagem_programacao_sas",
    "linguagem_programacao_vba", "linguagem_programacao_scala", "linguagem_programacao_matlab",
    "linguagem_programacao_rust", "linguagem_programacao_php", "linguagem_programacao_javascript",
    "linguagem_programacao_dax", "linguagem_programacao_sem_uso", "bancos_dados_mysql",
    "bancos_dados_oracle", "bancos_dados_sqlserver", "bancos_dados_rds", "bancos_dados_dynamodb",
    "bancos_dados_coachdb", "bancos_dados_cassandra", "bancos_dados_mongodb", "bancos_dados_mariadb",
    "bancos_dados_datomic", "bancos_dados_s3", "bancos_dados_postgresql",
    "bancos_dados_elasticsearch", "bancos_dados_db2", "bancos_dados_microsoftacess",
    "bancos_dados_sqlite", "bancos_dados_sybase", "bancos_dados_firebase", "bancos_dados_vertica",
    "bancos_dados_redis", "bancos_dados_neo4j", "bancos_dados_bigquery", "bancos_dados_firestone",
    "bancos_dados_redshift", "bancos_dados_athena", "bancos_dados_snowflake",
    "bancos_dados_databricks", "bancos_dados_hbase", "bancos_dados_presto", "bancos_dados_splunk",
    "bancos_dados_saphana", "bancos_dados_hive", "bancos_dados_firebird", "ferramenta_bi_powerbi",
    "ferramenta_bi_qlik", "ferramenta_bi_tableau", "ferramenta_bi_metabase",
    "ferramenta_bi_superset", "ferramenta_bi_redash", "ferramenta_bi_looker",
    "ferramenta_bi_lookerstudio", "ferramenta_bi_quicksight", "ferramenta_bi_alteryx",
    "ferramenta_bi_sap", "ferramenta_bi_oracle", "ferramenta_bi_salesforce", "ferramenta_bi_sas",
    "ferramenta_bi_grafana", "ferramenta_bi_pentaho", "ferramenta_bi_excel",
    "ferramenta_bi_sem_uso", "ai_uso_v2_independente", "ai_uso_v2_centralizado",
    "ai_uso_v2_copilotos", "ai_uso_v2_produtos_externos", "ai_uso_v2_produtos_internos",
    "ai_uso_v2_principal", "ai_uso_v2_sem_uso", "ai_uso_v2_sem_opiniao", "ai_uso_trabalho_sem_uso",
    "ai_uso_trabalho_gratuito", "ai_uso_trabalho_pago", "ai_uso_trabalho_empresa_paga",
    "ai_uso_trabalho_copilot", "rotina_de_pipelines", "rotina_de_etls", "rotina_de_sql",
    "rotina_de_integracao", "rotina_de_arquitetura", "rotina_de_manutencao", "rotina_de_modelagem",
    "rotina_de_qualidade", "rotina_de_nenhuma_listada", "ferramenta_etl_python",
    "ferramenta_etl_sql", "ferramenta_etl_airflow", "ferramenta_etl_nifi", "ferramenta_etl_luigi",
    "ferramenta_etl_glue", "ferramenta_etl_talend", "ferramenta_etl_pentaho",
    "ferramenta_etl_alteryx", "ferramenta_etl_stitch", "ferramenta_etl_fivetran",
    "ferramenta_etl_dataflow", "ferramenta_etl_oracle", "ferramenta_etl_ibm", "ferramenta_etl_sap",
    "ferramenta_etl_sqlserver", "ferramenta_etl_sas", "ferramenta_etl_qliksense",
    "ferramenta_etl_knime", "ferramenta_etl_databricks", "ferramenta_etl_sem_uso", "datalake_uso",
    "datawarehouse_uso", "tempo_gasto_pipelines", "tempo_gasto_etls", "tempo_gasto_sql",
    "tempo_gasto_integracao", "tempo_gasto_arquitetura", "tempo_gasto_manutencao",
    "tempo_gasto_modelagem", "tempo_gasto_qualidade", "tempo_gasto_nenhuma_listada",
    "ferramenta_etl_da_python", "ferramenta_etl_da_sql", "ferramenta_etl_da_airflow",
    "ferramenta_etl_da_nifi", "ferramenta_etl_da_luigi", "ferramenta_etl_da_glue",
    "ferramenta_etl_da_talend", "ferramenta_etl_da_pentaho", "ferramenta_etl_da_alteryx",
    "ferramenta_etl_da_stitch", "ferramenta_etl_da_fivetran", "ferramenta_etl_da_dataflow",
    "ferramenta_etl_da_oracle", "ferramenta_etl_da_ibm", "ferramenta_etl_da_sap",
    "ferramenta_etl_da_sqlserver", "ferramenta_etl_da_sas", "ferramenta_etl_da_qliksense",
    "ferramenta_etl_da_knime", "ferramenta_etl_da_databricks", "ferramenta_etl_da_sem_uso",
    "rotina_da_analise", "rotina_da_dashboard", "rotina_da_consultas", "rotina_da_extracao",
    "rotina_da_experimentos", "rotina_da_manutencao", "rotina_da_modelagem", "rotina_da_planilhas",
    "rotina_da_estatistica", "rotina_da_nenhuma_listada", "ferramenta_negocios_automl",
    "ferramenta_negocios_point_click", "ferramenta_negocios_product_metrics",
    "ferramenta_negocios_crm", "ferramenta_negocios_sem_uso", "ferramenta_negocios_sem_opiniao",
    "tempo_gasto_da_analise", "tempo_gasto_da_dashboard", "tempo_gasto_da_consultas",
    "tempo_gasto_da_extracao", "tempo_gasto_da_experimentos", "tempo_gasto_da_manutencao",
    "tempo_gasto_da_modelagem", "tempo_gasto_da_planilhas", "tempo_gasto_da_estatistica",
    "tempo_gasto_da_nenhuma_listada", "rotina_ds_estudos", "rotina_ds_coleta",
    "rotina_ds_contatos", "rotina_ds_modelagem", "rotina_ds_produtizacao", "rotina_ds_manutencao",
    "rotina_ds_dashboard", "rotina_ds_estatistica", "rotina_ds_pipeline",
    "rotina_ds_gerenciamento", "rotina_ds_infraestrutura", "rotina_ds_llm", "tecnicas_ds_regressao",
    "tecnicas_ds_redes_neurais", "tecnicas_ds_recomendacao", "tecnicas_ds_bayesianos",
    "tecnicas_ds_nlp", "tecnicas_ds_estatistica", "tecnicas_ds_markov",
    "tecnicas_ds_clusterizacao", "tecnicas_ds_series_temporais", "tecnicas_ds_reforco",
    "tecnicas_ds_ml", "tecnicas_ds_visao_computacional", "tecnicas_ds_churn", "tecnicas_ds_llm",
    "ferramenta_ds_bi", "ferramenta_ds_planilhas", "ferramenta_ds_local", "ferramenta_ds_nuvem",
    "ferramenta_ds_automl", "ferramenta_ds_etl", "ferramenta_ds_ml", "ferramenta_ds_feature_store",
    "ferramenta_ds_controle_versao", "ferramenta_ds_data_apps", "ferramenta_ds_estatistica",
    "tempo_gasto_ds_estudos", "tempo_gasto_ds_coleta", "tempo_gasto_ds_contatos",
    "tempo_gasto_ds_modelagem", "tempo_gasto_ds_produtizacao", "tempo_gasto_ds_manutencao",
    "tempo_gasto_ds_dashboard", "tempo_gasto_ds_estatistica", "tempo_gasto_ds_pipeline",
    "tempo_gasto_ds_gerenciamento", "tempo_gasto_ds_infraestrutura", "tempo_gasto_ds_llm",
    "servico_cloud_aws", "servico_cloud_google", "servico_cloud_azure", "servico_cloud_oracle",
    "servico_cloud_ibm", "servico_cloud_sem_uso", "servico_cloud_propria",
]
BOOLEAN_FIELDS_SET = set(BOOLEAN_FIELDS)

# valida se faixa_salarial bate com um padrão esperado ("Menos de R$
# X/mês", "de R$ X/mês a R$ Y/mês", "Acima de R$ X/mês") e é
# numericamente coerente -- detecta casos como "de R$ 25.001/mês a R$
# 3000/mês" (min > max, provável erro de digitação na fonte). 

def _faixa_salarial_valida(faixa):
    if faixa is None:
        return None
    numeros = re.findall(r"[\d.]+", faixa)
    valores = [float(n.replace(".", "")) for n in numeros]
    faixa_lower = faixa.lower()
    if faixa_lower.startswith("menos de") or faixa_lower.startswith("acima de") or faixa_lower.startswith("mais de"):
        return len(valores) >= 1
    if len(valores) >= 2:
        return valores[0] <= valores[1]
    return False

faixa_valida_udf = F.udf(_faixa_salarial_valida, "boolean")

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

    df = df.withColumn("etl_source_file", F.input_file_name())
 
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

    # duplicatas: primeiro linha inteira repetida, depois id repetido
    # (mantém a primeira ocorrência)
    df = df.dropDuplicates()
    linhas_apos_dedup_total = df.count()
    if "id" in df.columns:
        df = df.dropDuplicates(["id"])
    linhas_apos_dedup_id = df.count()

    df = df.withColumn("ano_pesquisa", F.lit(year))

    if "idade" in df.columns:
        df = df.withColumn("idade", F.col("idade").cast("int"))
        # faixa implausível -> null (não descarta a linha, só o valor suspeito)
        df = df.withColumn(
            "idade",
            F.when((F.col("idade") < 14) | (F.col("idade") > 100), None).otherwise(F.col("idade"))
        )

    if "faixa_salarial" in df.columns:
        df = df.withColumn("faixa_salarial_valida", faixa_valida_udf(F.col("faixa_salarial")))
        invalidas = df.filter(F.col("faixa_salarial_valida") == False).count()
        if invalidas > 0:
            print(f"AVISO ({year}): {invalidas} respondente(s) com faixa_salarial em formato inesperado/inconsistente")

    # normaliza os campos booleanos (0/1 em 2023, TRUE/FALSE em 2024/2025)
    bool_exprs = []
    for c in df.columns:
        if c in BOOLEAN_FIELDS_SET:
            bool_exprs.append(
                F.when(F.upper(F.col(c).cast("string")).isin("TRUE", "1"), F.lit(1))
                 .when(F.upper(F.col(c).cast("string")).isin("FALSE", "0"), F.lit(0))
                 .otherwise(None)
                 .alias(c)
            )
        else:
            bool_exprs.append(F.col(c))
    df = df.select(*bool_exprs)

    # coluna de linhagem/auditoria
    df = df.withColumn("etl_control_column", F.current_timestamp())

    print(f"--- ano {year}: {linhas_brutas} linhas brutas -> "
          f"{linhas_apos_dedup_total} apos remover duplicatas exatas -> "
          f"{linhas_apos_dedup_id} apos remover id duplicado "
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