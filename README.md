# Tratamento e Análise dos Dados
Desafio :
Explore e analise cada um dos campos disponíveis entendendo o tipo de preenchimento que possuem.
Dado o contexto de ingestão e criação da informação, traga características que chamam atenção e aponte problemas contidos no dataset. A ideia aqui é avaliar a sua capacidade analítica, fique a vontade para escrever inferências sobre como o problema pode ter sido criado, questionamentos que poderíamos levar aos fornecedores ou outras análises que podem ser feitas.
Tratamento de dados com DBT
Crie um modelo DBT que, a partir da tabela dada e os seus apontamentos de problemas, padronize os dados criando uma tabela tratada. Pense em escrever um código modularizado e legível.
Tecnologias Relacionadas
Python
SQL
DBT

## Processamento Inicial da Base de Dados

Devido ao grande volume de dados e às limitações do DBT em processar a base completa, foi necessário implementar uma estratégia de segmentação antes de executar o "DBT SEED".

### Processo de Segmentação e Unificação

1. Utilizei o parâmetro `chunk_size` para dividir o arquivo original em segmentos de 500 linhas cada
2. Como foram encontrados dados multivalorados, resolvi já fazer o tratamento antes de ir para o próximo passo
3. Cada segmento foi salvo individualmente e processado pelo DBT SEED
4. Após o processamento completo, realizei a unificação dos segmentos utilizando Python, em vez de SQL, para garantir performance adequada considerando o volume de mais de 100 arquivos
5. Com a base unificada, iniciei o tratamento dos dados via Python
6. Inicio do tratamento em SQL

## Análise das Inconsistências Encontradas

### Problemas de Tipagem
Identifiquei colunas com tipos de dados inconsistentes, incluindo:
- `luz_eletrica`
- `obito`
- `em_situacao_de_rua`
- `possui_plano_saude`
- `vulnerabilidade_social`
- `familia_beneficiaria_auxilio_brasil`
- `crianca_matriculada_creche_pre_escola`

### Falta de Padronização
Constatei ausência de padronização em campos críticos como:
- `identidade_genero`
- `religião`

### Valores Ausentes
Quantificação de registros com valores nulos:
- `identidade_genero`: 952 registros
- `altura`: 25 registros
- `peso`: 184 registros
- `pressao_sistolica`: 40 registros
- `pressao_diastolica`: 17 registros

## Estratégias de Tratamento Aplicadas

Para manter a integridade do dataset, implementei as seguintes soluções:

- Campos de `identidade_genero` nulos foram classificados como "indefinido"
- Para os campos `altura`, `peso`, `pressao_sistolica` e `pressao_diastolica`, apliquei imputação pela **mediana** em vez da média
- Após essas intervenções, verificou-se a ausência de valores nulos no dataset

### Justificativa para o Uso da Mediana em Parâmetros Biométricos

A escolha da mediana como método de imputação para variáveis como altura, peso e pressão arterial apresenta vantagens significativas:

1. **Robustez a valores extremos (outliers)**: Parâmetros biométricos frequentemente contêm valores atípicos que podem distorcer a média. A mediana não é afetada por estes valores extremos, fornecendo um valor central mais representativo da população.

2. **Preservação da distribuição natural dos dados**: Em dados biométricos, a distribuição raramente é simétrica. A mediana preserva melhor as características da distribuição original, enquanto a média pode criar valores artificiais que não representam adequadamente nenhum subgrupo da população.

3. **Maior relevância clínica**: Em contextos de saúde, valores centrais que ignoram outliers tendem a ser mais úteis para análises populacionais. A mediana oferece uma referência mais estável para parâmetros que variam conforme idade, sexo e outras características demográficas.

4. **Minimização de distorções em análises subsequentes**: A utilização da mediana reduz o risco de introduzir vieses nas análises estatísticas posteriores, especialmente em cálculos de correlação e regressão que são sensíveis a outliers.

5. **Compatibilidade com distribuições assimétricas**: Dados de pressão arterial e índices corporais frequentemente apresentam assimetria positiva (cauda à direita), tornando a mediana mais apropriada como medida de tendência central.

## Recomendações para Aprimoramento

### Completude dos Dados
- Aperfeiçoamento dos protocolos de coleta para minimizar lacunas em campos críticos para análises de saúde
- Implementação de validações no momento da entrada dos dados

### Análise Demográfica Avançada
- Exploração de correlações entre variáveis socioeconômicas (renda, escolaridade) e indicadores de saúde
- Desenvolvimento de análises estratificadas para identificação de padrões demográficos relevantes
- Explorar mais as tendências demográficas específicas. Por exemplo, como diferentes níveis de renda ou formação educacional se correlacionam com indicadores de saúde.
