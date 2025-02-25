import os
import pandas as pd
import subprocess
import ast
import numpy as np 
import re

# Caminhos
base_dir = os.path.dirname(os.path.abspath(__file__))  # Diretório onde o script está
input_file = os.path.join(base_dir, '..', 'original', 'dados_ficha_a_desafio.csv')  # Caminho para o arquivo de entrada
chunk_size = 500  # 500 linhas por pedaço

# Caminho completo para o diretório 'seeds' dentro de 'meu_projeto_dbt'
dbt_project_dir = os.path.join(base_dir, '..', '..', 'meu_projeto_dbt')  # Ajuste para garantir que o caminho seja correto
output_dir = os.path.join(dbt_project_dir, 'seeds')  

# Definição de colunas multivaloradas
cols_dados_multivalorados = [
    'meios_transporte',
    'doencas_condicoes',
    'meios_comunicacao',
    'em_caso_doenca_procura',
]

def trata_registro(valor):
    """
    Função melhorada para tratar registros multivalorados com tratamento de 
    vários formatos possíveis e melhor manipulação de encoding.
    """
    if pd.isna(valor) or valor == '[]' or valor == '':
        return None
        
    # Tenta usar ast.literal_eval para strings que representam listas
    try:
        if isinstance(valor, str) and ('[' in valor or ',' in valor):
            if valor.startswith('[') and valor.endswith(']'):
                try:
                    return [str(item).strip(' "\'') for item in ast.literal_eval(valor)]
                except:
                    return [item.strip(' "\'').encode('ISO-8859-1').decode('unicode_escape') 
                            for item in valor[1:-1].split(',')]
            # Se for formato com vírgulas mas sem colchetes
            else:
                return [item.strip() for item in valor.split(',')]
        # Retorna o valor como uma lista com um elemento se for um valor único
        return [str(valor)]
    except:
        # Último recurso: retornar como string simples em uma lista
        return [str(valor)]

# Função para converter listas vazias para None
def empty_to_none(lista):
    return None if lista is None or len(lista) == 0 or lista == [''] else lista

# Aplicação da função melhorada ao DataFrame
def processa_colunas_multivaloradas(df, colunas):
    """
    Processa colunas multivaloradas, substitui nulos pela mediana em colunas numéricas e cria colunas dummy.
    """
    df_resultado = df.copy()

    # Substituição da mediana em colunas numéricas
    colunas_numericas = ['altura', 'peso', 'pressao_sistolica', 'pressao_diastolica']
    for col_num in colunas_numericas:
        if col_num in df_resultado.columns:
            mediana = df_resultado[col_num].median()
            df_resultado[col_num].fillna(mediana, inplace=True)

    for col in colunas:
        # Verificar se a coluna existe no DataFrame
        if col not in df_resultado.columns:
            print(f"Aviso: Coluna '{col}' não encontrada no DataFrame")
            continue
            
        # Aplicar tratamento aos valores
        df_resultado[col] = df_resultado[col].apply(trata_registro).apply(empty_to_none)
        
        # Criar uma cópia explodida para gerar as colunas dummy
        df_temp = df_resultado.explode(col).reset_index()
        
        # Criar colunas dummy apenas se houver valores válidos
        if df_temp[col].notna().any():
            # Gera as colunas dummy
            dummies = pd.get_dummies(df_temp[col], prefix=col, prefix_sep='_')
            
            # Combinar com o índice original
            dummies_com_indice = pd.concat([df_temp['index'], dummies], axis=1)
            
            # Agregar por índice com max (equivalente a OR lógico para binários)
            dummies_agregadas = dummies_com_indice.groupby('index').max()
            
            # Juntar ao dataframe original
            df_resultado = df_resultado.join(dummies_agregadas)
    
    return df_resultado

# Verifica se o diretório de saída existe e cria, se necessário
if not os.path.exists(output_dir):
    os.makedirs(output_dir)  # Cria o diretório de saída se não existir

# Verifica se o arquivo de entrada existe
if not os.path.isfile(input_file):
    print(f"Erro: O arquivo {input_file} não foi encontrado!")
    exit()

# Lê o arquivo inteiro
try:
    total_lines = sum(1 for line in open(input_file, encoding='utf-8'))  # Conta o número de linhas no arquivo
    print(f'O arquivo tem {total_lines} linhas.')
except UnicodeDecodeError:
    total_lines = sum(1 for line in open(input_file, encoding='cp1252'))  # Tentando uma codificação diferente
    print(f'O arquivo tem {total_lines} linhas.')

# Determina a codificação correta para leitura
try:
    pd.read_csv(input_file, encoding='utf-8', nrows=5)
    encoding_to_use = 'utf-8'
except UnicodeDecodeError:
    encoding_to_use = 'cp1252'
print(f"Usando codificação: {encoding_to_use}")

# Lê o CSV em pedaços de 500 linhas com a codificação determinada
chunks = pd.read_csv(input_file, encoding=encoding_to_use, chunksize=chunk_size)

# Salva os pedaços em arquivos separados na pasta seeds dentro do projeto DBT
file_count = 0
for i, chunk in enumerate(chunks):
    # Processa as colunas multivaloradas em cada chunk
    chunk_processado = processa_colunas_multivaloradas(chunk, cols_dados_multivalorados)
    
    output_file = os.path.join(output_dir, f'parte_{i+1}.csv')  # Nome do arquivo de saída
    chunk_processado.to_csv(output_file, index=False)  # Salva o pedaço processado no arquivo
    file_count += 1
    print(f'Arquivo {output_file} criado com {len(chunk_processado)} linhas e {len(chunk_processado.columns)} colunas.')

# Verifica se o número de arquivos é o esperado
print(f'Foram criados {file_count} arquivos.')

# Executa o comando dbt seed
result = subprocess.run(['dbt', 'seed'], capture_output=True, text=True, cwd=dbt_project_dir)

if result.returncode == 0:
    print("Comando dbt seed executado com sucesso!")
    print(result.stdout) 
else:
    print("Erro ao executar dbt seed:")
    print(result.stderr)

# Após o dbt seed, junta todos os arquivos CSV criados
merged_file = os.path.join(output_dir, 'dados_completos.csv')

# Lê e concatena todos os arquivos CSV gerados
csv_files = [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith('.csv') and f != 'dados_completos.csv']

# Concatena os arquivos em um único DataFrame
df_list = []
for file in csv_files:
    try:
        df_chunk = pd.read_csv(file, encoding='utf-8')
        df_list.append(df_chunk)
    except UnicodeDecodeError:
        df_chunk = pd.read_csv(file, encoding='cp1252')
        df_list.append(df_chunk)
    except Exception as e:
        print(f"Erro ao ler o arquivo {file}: {e}")

# Junta todos os pedaços em um único DataFrame
final_df = pd.concat(df_list, ignore_index=True)

# Salva o arquivo final
final_df.to_csv(merged_file, index=False)
print(f"Arquivo final unido e salvo como {merged_file}.")