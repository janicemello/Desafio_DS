
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
        -- Tratamento da coluna identidade_genero
        COALESCE(NULLIF(TRIM(identidade_genero), ''), 'Indefinido') AS identidade_genero,
        -- Tratamento dos valores de 'identidade_genero' para 'Indefinido' caso seja 'sim', 'não' ou vazio
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
        -- Preenchendo valores nulos nas colunas numéricas com a mediana
        --COALESCE(altura, (SELECT median(altura) FROM meu_banco)) AS altura,
       -- COALESCE(peso, (SELECT median(peso) FROM meu_banco)) AS peso,
        --COALESCE(pressao_sistolica, (SELECT median(pressao_sistolica) FROM meu_banco)) AS pressao_sistolica,
       -- COALESCE(pressao_diastolica, (SELECT median(pressao_diastolica) FROM meu_banco)) AS pressao_diastolica,
        n_atendimentos_atencao_primaria,
        n_atendimentos_hospital,
        updated_at,
        tipo,
        -- Tratamento da coluna obito
        CASE
            WHEN LOWER(CAST(obito AS STRING)) IN ('true', '1') THEN TRUE
            WHEN LOWER(CAST(obito AS STRING)) IN ('false', '0') THEN FALSE
            ELSE NULL
        END AS obito
    FROM teste
)

SELECT *
FROM dados_ficha_a;