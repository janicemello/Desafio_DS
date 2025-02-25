import os
import subprocess
import sqlite3

# Caminho do diretório do projeto DBT
base_dir = os.path.dirname(os.path.abspath(__file__))  # Diretório onde o script está
dbt_project_dir = os.path.join(base_dir, '..', '..', 'meu_projeto_dbt')  # Diretório do DBT
output_sql_file = os.path.join(dbt_project_dir, 'models', 'dados_ficha_tratado.sql')  # Caminho do arquivo SQL

# Modelo SQL para o DBT
modelo_sql = """
{{ config(
    materialized='table'
) }}
WITH dados_ficha_a AS (
    SELECT
        id_paciente,
        sexo,
        bairro,
        raca_cor,
        ocupacao,
        religiao,
        CASE
            WHEN LOWER(CAST(luz_eletrica AS STRING)) IN ('true', '1') THEN TRUE
            WHEN LOWER(CAST(luz_eletrica AS STRING)) IN ('false', '0') THEN FALSE
            ELSE NULL
        END AS luz_eletrica,
        data_cadastro,
        escolaridade,
        nacionalidade,
        renda_familiar,
        data_nascimento,
        em_situacao_de_rua,
        frequenta_escola,
        meios_transporte,
        doencas_condicoes,
        COALESCE(NULLIF(TRIM(identidade_genero), ''), 'Indefinido') AS identidade_genero,
        CASE
            WHEN identidade_genero IN ('sim', 'não') THEN 'Indefinido'
            ELSE identidade_genero
        END AS identidade_genero_tratado,
        meios_comunicacao,
        orientacao_sexual,
        possui_plano_saude,
        em_caso_doenca_procura,
        situacao_profissional,
        vulnerabilidade_social,
        data_atualizacao_cadastro,
        familia_beneficiaria_auxilio_brasil,
        crianca_matriculada_creche_pre_escola,
        --COALESCE(altura, (SELECT median(altura) FROM teste)) AS altura,
        --COALESCE(peso, (SELECT median(peso) FROM teste)) AS peso,
        --COALESCE(pressao_sistolica, (SELECT median(pressao_sistolica) FROM teste)) AS pressao_sistolica,
        --COALESCE(pressao_diastolica, (SELECT median(pressao_diastolica) FROM teste)) AS pressao_diastolica,
        n_atendimentos_atencao_primaria,
        n_atendimentos_hospital,
        updated_at,
        tipo,
        CASE
            WHEN LOWER(CAST(obito AS STRING)) IN ('true', '1') THEN TRUE
            WHEN LOWER(CAST(obito AS STRING)) IN ('false', '0') THEN FALSE
            ELSE NULL
        END AS obito
    FROM teste
)
SELECT * FROM dados_ficha_a;
"""

# Salvar o modelo SQL no diretório correto
os.makedirs(os.path.dirname(output_sql_file), exist_ok=True)  # Cria a pasta 'models' se não existir
with open(output_sql_file, 'w', encoding='utf-8') as f:
    f.write(modelo_sql)

print(f"Modelo SQL salvo em: {output_sql_file}")

# Executa o comando DBT
result = subprocess.run(['dbt', 'run'], capture_output=True, text=True, cwd=dbt_project_dir)

if result.returncode == 0:
    print("DBT run executado com sucesso!")
    print(result.stdout)
else:
    print("Erro ao executar o DBT run:")
    print(result.stderr)
