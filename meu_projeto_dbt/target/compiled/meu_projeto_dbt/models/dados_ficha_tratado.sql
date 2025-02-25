
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
       -- COALESCE(pressao_sistolica, (SELECT median(pressao_sistolica) FROM teste)) AS pressao_sistolica,
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