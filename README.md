# Tech Challenge - State of Data Brasil | Fase 3

> Pipeline de dados em AWS com arquitetura **Bronze → Silver → Gold** (Amazon S3, AWS Glue, Spark/PySpark e Glue Data Catalog) e análise do **State of Data Survey Brasil 2023–2025**, com material executivo de DataViz e Storytelling.

---

## 1. Sobre o projeto

Solução desenvolvida para o **Tech Challenge (Fase 3)** da pós-graduação em Data Analytics da FIAP.

**Problema de negócio:** uma instituição financeira de grande porte quer expandir sua área de Dados, Analytics e IA e precisa entender o mercado brasileiro de Dados para definir estratégias de **contratação, capacitação e investimento em tecnologia**.

**Base analisada:** edições **2023, 2024 e 2025** do State of Data Survey Brasil (Data Hackers / Bain), totalizando **14.002 respondentes**.

| Edição | Respondentes |
|---|---:|
| 2023 | 5.293 |
| 2024 | 5.215 |
| 2025 | 3.494 |
| **Total** | **14.002** |

**Fonte:** [Data Hackers - Kaggle](https://www.kaggle.com/datahackers/datasets)

---

## 2. Entregáveis e aderência ao enunciado

| Requisito do Tech Challenge | Onde está no repositório |
|---|---|
| Material executivo (PDF) com DataViz e Storytelling | `documentos/DTAT_Tech_Challenge_Fase3_Apresentacao_Executiva.pdf` |
| Diagrama da arquitetura AWS (Draw.io), contido no material executivo | `documentos/TechChallenge3_Arquitetura.drawio.png` (também no slide de arquitetura do PDF) |
| Ingestão e organização das bases no S3 | `bronze/state_of_data/ano=AAAA/` |
| ETL e catalogação com Glue Jobs | `silver/transform_silver_state_of_data.py` (Glue Job → Parquet + `silver_db`) |
| Camadas Bronze, Silver e Gold | pastas `bronze/`, `silver/`, `gold/` |
| Tratamento e consultas analíticas em Glue Notebook | `gold/transform_gold_state_of_data.ipynb` e `analises/state_of_data_survey_analysis_glue.ipynb` |
| Uso de Spark no processamento | Glue Job (Silver) e Glue Interactive Sessions (Gold e análise) |
| Geração dos dados para gráficos e análises | notebooks em `analises/` |
| Respostas às 7 perguntas de negócio | PDF executivo, painel HTML e documentação analítica |

---

## 3. Arquitetura da solução

![Arquitetura AWS - Bronze / Silver / Gold](./documentos/TechChallenge3_Arquitetura.drawio.png)

```text
Data Hackers / Kaggle (CSV 2023, 2024, 2025)
        │ upload
        ▼
Amazon S3 - BRONZE        CSV bruto, particionado por ano (ano=AAAA)
        │ AWS Glue Job (Spark/PySpark)
        ▼
Amazon S3 - SILVER        Parquet tratado, 1 tabela por edição  → Glue Data Catalog: silver_db
        │ AWS Glue Interactive Session (Spark/PySpark)
        ▼
Amazon S3 - GOLD          Parquet modelado em fatos e dimensão  → Glue Data Catalog: gold_db
        │ AWS Glue Interactive Session (leitura via catálogo)
        ▼
Análises → gráficos → material executivo (PDF)
```

**Serviços e tecnologias:** Amazon S3 · AWS Glue Jobs · AWS Glue Interactive Sessions · AWS Glue Data Catalog · AWS Academy Lab · Apache Spark / PySpark · Python / Pandas · Jupyter · Draw.io

---

## 4. Estrutura do repositório

```text
state-of-data-survey-analysis/
│
├── bronze/
│   └── state_of_data/
│       ├── ano=2023/df_survey.csv
│       ├── ano=2024/df_survey.csv
│       └── ano=2025/df_survey.csv
│
├── silver/
│   ├── transform_silver_state_of_data.py      # Glue Job: Bronze → Silver
│   ├── mapeamento_colunas_completo.csv        # de-para de colunas entre edições (405 campos)
│   └── state_of_data/                         # snapshot CSV da Silver
│       ├── pesquisa_2023/
│       ├── pesquisa_2024/
│       └── pesquisa_2025/
│
├── gold/
│   ├── transform_gold_state_of_data.ipynb     # Glue Notebook: Silver → Gold
│   ├── tbl_dimensao_faixa_salarial/           # snapshots CSV das tabelas Gold
│   ├── tbl_fato_experiencia/
│   ├── tbl_fato_gestao/
│   ├── tbl_fato_ia_generativa/
│   ├── tbl_fato_perfil_profissional/
│   ├── tbl_fato_rotina/
│   ├── tbl_fato_satisfacao/
│   ├── tbl_fato_tecnologias/
│   └── tbl_fato_time_dados/
│
├── analises/
│   ├── state_of_data_survey_analysis_glue.ipynb   # análise na AWS (Glue Interactive Session)
│   └── state_of_data_survey_analysis_csv.ipynb    # mesma análise, execução local via CSV
│
├── documentos/
│   ├── DTAT_Tech_Challenge_Fase3_Apresentacao_Executiva.pdf   # entrega principal
│   ├── DTAT_Tech_Challenge_Fase3_Apresentacao.html            # painel de resultados (gráficos)
│   ├── Documentacao_Analise_Tech_Challenge_Fase3.docx         # documentação analítica detalhada
│   └── TechChallenge3_Arquitetura.drawio.png                  # diagrama da arquitetura
│
└── README.md
```

> **Sobre os arquivos de dados versionados:** no ambiente AWS, as camadas Silver e Gold são persistidas em **Parquet**. As pastas `silver/state_of_data/` e `gold/tbl_*` do repositório contêm **snapshots exportados em CSV** dessas tabelas, usados para inspeção e para a execução local da análise.

---

## 5. Pipeline de dados

### 5.1 Bronze - ingestão

Os arquivos originais de cada edição são carregados sem alteração no S3, particionados por ano (`bronze/state_of_data/ano=AAAA/`).

### 5.2 Silver - tratamento e padronização (`silver/transform_silver_state_of_data.py`)

Glue Job em Spark/PySpark que, para cada edição:

- lê o CSV bruto da Bronze;
- limpa os nomes das colunas e aplica o de-para `mapeamento_colunas_completo.csv`, que harmoniza os nomes entre 2023, 2024 e 2025;
- converte strings vazias em nulo e aplica `trim`;
- remove duplicatas exatas e duplicatas de `id`;
- valida idade (valores fora de 14–100 viram nulo) e formato da faixa salarial (`faixa_salarial_valida`);
- normaliza campos booleanos (0/1 em 2023, TRUE/FALSE em 2024/2025);
- adiciona `ano_pesquisa`, `etl_source_file` e `etl_control_column`;
- grava em Parquet e atualiza o Glue Data Catalog: `silver_db.tbl_state_of_data_2023`, `_2024` e `_2025`.

### 5.3 Gold - modelagem analítica (`gold/transform_gold_state_of_data.ipynb`)

Glue Interactive Session que lê a Silver pelo catálogo e grava tabelas Parquet particionadas por `ano_pesquisa` no banco `gold_db`.

As perguntas de múltipla escolha (uma coluna indicadora por opção) passam por um **unpivot padronizado**, gerando uma linha por respondente × item:

```text
respondente_id | ano_pesquisa | categoria | tecnologia
123            | 2025         | linguagem | SQL
123            | 2025         | linguagem | Python
```

| Tabela | Finalidade |
|---|---|
| `tbl_fato_perfil_profissional` | Perfil demográfico e profissional (1 linha por respondente) |
| `tbl_dimensao_faixa_salarial` | Faixas salariais com valor mínimo, máximo e ponto médio |
| `tbl_fato_tecnologias` | Linguagens, bancos de dados, BI e técnicas de ciência de dados |
| `tbl_fato_experiencia` | Aspectos da experiência profissional |
| `tbl_fato_ia_generativa` | Uso pessoal e corporativo de IA e barreiras de adoção |
| `tbl_fato_gestao` | Responsabilidades e desafios de gestores |
| `tbl_fato_time_dados` | Composição das equipes de Dados |
| `tbl_fato_satisfacao` | Critérios de escolha de emprego e motivos de insatisfação |
| `tbl_fato_rotina` | Atividades e rotinas profissionais |

---

## 6. Como executar

### 6.1 Variante A - AWS (pipeline completo + análise via Glue Data Catalog)

**Pré-requisitos:** conta no AWS Academy Lab com acesso a S3, Glue e à role `LabRole`.

1. **Bucket e dados:** crie um bucket S3 e faça upload de:
   - `bronze/state_of_data/ano=2023|2024|2025/df_survey.csv` → `s3://<bucket>/bronze/state_of_data/ano=AAAA/`
   - `silver/mapeamento_colunas_completo.csv` → `s3://<bucket>/silver/state_of_data/mapeamento_colunas_completo.csv` (**o Job da Silver lê o de-para desse caminho**)
2. **Bancos no Glue Data Catalog:** crie os databases `silver_db` e `gold_db`.
3. **Ajuste do bucket:** substitua o valor da variável `BUCKET` no início de `silver/transform_silver_state_of_data.py` e de `gold/transform_gold_state_of_data.ipynb` pelo nome do seu bucket.
4. **Silver (Glue Job):** crie um Glue Job do tipo Spark script com o conteúdo de `transform_silver_state_of_data.py`, role `LabRole`, e execute. O log mostra as contagens de linhas por ano e eventuais avisos de colunas não mapeadas. Ao final, as tabelas `silver_db.tbl_state_of_data_AAAA` ficam disponíveis.
5. **Gold (Glue Notebook):** abra `gold/transform_gold_state_of_data.ipynb` em Glue Studio → Notebooks (kernel Glue PySpark, role `LabRole`) e execute todas as células. A primeira célula já configura a sessão (`%glue_version 5.1`, `G.1X`, 2 workers). As 9 tabelas são criadas em `gold_db`.
6. **Análise (Glue Notebook):** abra `analises/state_of_data_survey_analysis_glue.ipynb` no mesmo ambiente e execute todas as células. Esse notebook **não define magics de sessão**; se quiser a mesma configuração da Gold, adicione no início `%glue_version 5.1`, `%worker_type G.1X` e `%number_of_workers 2`. As tabelas são lidas pelo catálogo (`gold_db` e `silver_db`) com Spark e convertidas para Pandas para as agregações.
7. **Validação:** a última célula compara os indicadores-chave com os valores publicados e falha (`AssertionError`) em caso de divergência.

### 6.2 Variante B - Local (análise a partir dos snapshots CSV)

Permite reproduzir todos os indicadores sem acesso à AWS, usando os CSVs das camadas Silver e Gold versionados no repositório.

**Pré-requisitos:** Python 3.10+ com `pandas` e `jupyter`.

```bash
git clone <url-do-repositorio>
cd state-of-data-survey-analysis
pip install pandas jupyter
jupyter notebook analises/state_of_data_survey_analysis_csv.ipynb
```

- Execute todas as células. O notebook localiza automaticamente as pastas `gold/` e `silver/` (procura em `.`, `./data`, `..` e `../data`). Se a estrutura for diferente, ajuste `DATA_DIR`.
- A última célula faz a mesma checagem de consistência da variante AWS.

### 6.3 Equivalência entre as duas variantes

Os dois notebooks aplicam **a mesma lógica de análise, célula a célula**. A diferença está na forma de carregar os dados: Glue Data Catalog na variante AWS, arquivos CSV na variante local. A versão Glue também inclui uma conversão numérica explícita (`pd.to_numeric`) nos campos de carreira prejudicada lidos da Silver. Os indicadores produzidos são idênticos.

A única diferença observada é informativa: na pergunta de cloud preferida, a quantidade de respostas residuais descartadas é **1.933 (CSV)** e **1.934 (Glue)**, reflexo da leitura de nulos em CSV × Parquet. Os percentuais de AWS, GCP e Azure são os mesmos nas duas versões.

---

## 7. Questões de negócio e premissas de leitura

| # | Pergunta | Como foi respondida |
|---|---|---|
| 1 | Como está estruturado o mercado brasileiro de Dados? | Setor, porte da empresa, região, senioridade, gênero e modelo de trabalho |
| 2 | Quais perfis são mais valorizados? | Representatividade por cargo × salário médio estimado por cargo e senioridade |
| 3 | Qual o cenário de diversidade de gênero? | Composição e evolução, % de mulheres por senioridade, salário e percepção de prejuízo na carreira |
| 4 | Quais tecnologias têm maior adoção? | Linguagens, bancos de dados, BI, cloud e técnicas de ciência de dados |
| 5 | Qual o índice de adoção de IA e seu impacto? | Uso pessoal ao longo dos anos, prioridade estratégica e barreiras de adoção |
| 6 | Há diferenças por região, senioridade e modelo de trabalho? | Distribuição e salário por região, senioridade e modelo, e evolução do modelo de trabalho |
| 7 | Quais oportunidades e desafios para quem investe em Dados e IA? | Critérios de atração, motivos de insatisfação, desafios de gestores e recomendações |

**Premissas:**

- **Denominadores:** perguntas de múltipla escolha usam como base os respondentes únicos de cada bloco, não o total de 14.002. Por isso as categorias podem somar mais de 100%.
- **Arredondamento:** partições completas (gênero, senioridade, região etc.) usam o método do maior resto para somar exatamente 100,0%.
- **Salário:** estimado pelo ponto médio da faixa salarial declarada (a faixa "Acima de R$ 40.001" usa o limite inferior).
- **"Valorização" de perfis:** o survey não contém vagas abertas, contratações ou turnover. A análise usa representatividade na base e salário estimado como proxy.
- **IA:** a pesquisa mede adoção, prioridade e barreiras declaradas, e não permite inferir impacto causal sobre produtividade ou resultados.
- **Nomenclatura de cargos:** "Engenheiro de Dados" aparece em **duas categorias** porque o rótulo mudou entre as edições (2023: *Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect*; 2024–2025: *Engenheiro de Dados/Data Engineer/Data Architect*). Decidimos manter as categorias separadas, como na fonte, para preservar a rastreabilidade. Somadas, representam 16,7% dos respondentes que informaram cargo.
- **Carreira prejudicada por gênero/raça:** esse bloco não foi materializado na Gold e é lido diretamente da Silver.

---

## 8. Principais achados

- **62,3%** dos profissionais estão no Sudeste e **44,6%** trabalham em empresas com mais de 3.000 funcionários; **Finanças/Bancos (19,9%)** é o setor que mais emprega.
- **Pleno e Sênior** somam **72,7%** da base. O salário médio praticamente dobra de Júnior (R$ 4.048) para Pleno (R$ 7.898) e chega a R$ 19.685 em Especialista/Staff+.
- **23,5%** dos respondentes são mulheres. A participação caiu de **24,4% (2023) para 21,9% (2025)** e o salário médio feminino é **18,6% menor**.
- **SQL (89,5%)** e **Python (85,3%)** são a base técnica; **Power BI (65,1%)** lidera em BI e **AWS (45,9%)** em cloud.
- Quem não usa IA para produtividade caiu de **19,7% (2023) para 2,1% (2025)**, mas só **49,3%** dos gestores dizem que IA é prioridade real na empresa.
- As barreiras de IA são de maturidade: **falta de expertise (36,8%)**, **falta de casos de uso (36,0%)** e **dados não prontos (33,1%)**.
- Remuneração é o principal critério de escolha (**81,9%**) e o principal motivo de insatisfação (**45,1%**), seguida de falta de crescimento (**40,4%**).

> Gráficos, narrativa e recomendações estão no material executivo em PDF.

---

## 9. Limitações e pontos de atenção conhecidos

- O survey não traz vagas abertas, turnover ou contratações; qualquer leitura de demanda futura exige fontes complementares.
- Comparações entre anos devem considerar que a base de cada pergunta varia por edição.
- Diferenças salariais por gênero, região e modelo de trabalho são **agregadas** e não controlam por cargo ou senioridade.
- `atua_como_gestor` mistura os formatos `0/1` (2023) e `TRUE/FALSE` (2024–2025) nas camadas Silver/Gold, pois o campo não entrou na lista de booleanos normalizados. As análises só verificam se o campo está preenchido, então os resultados não são afetados.
- Uma faixa salarial com erro de digitação na fonte ("de R$ 25.001/mês a R$ 3000/mês", **1 respondente**) é corrigida na dimensão salarial, mas fica fora do cálculo de salário médio nos notebooks. O efeito nas médias publicadas é nulo.
- 2 registros de `num_funcionarios` com valor inválido ("de 501 a 100") são excluídos da análise de porte.

---

## 10. Equipe

**Analistas de Dados**

- Tiago Antônio dos Santos
- Isabele Cristina Felix Santos
- Thais Marcondes Narbonne
