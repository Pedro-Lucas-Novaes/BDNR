import pandas as pd
import mysql.connector
import hashlib
import sys
import re
from datetime import date, datetime
from tabulate import tabulate
from colorama import Fore, init

# configura conexão com banco
conexao = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='dw_supermercado'
)

conecta = conexao.cursor() # conecta com o banco

"""
    1 - EXTRACT 
    Leitura dos CSVs
"""

# usa pandas para ler csv
produtos = pd.read_csv('01_produtos_bruto.csv', sep=',', quotechar='"', encoding='utf-8') 
lojas = pd.read_csv('02_lojas_bruto.csv', sep=',', quotechar='"', encoding='utf-8') 
clientes = pd.read_csv('03_clientes_bruto.csv', sep=',', quotechar='"', encoding='utf-8') 
vendas = pd.read_csv('04_vendas_bruto.csv', sep=',', quotechar='"', encoding='utf-8') 

"""
    2 - TRANSFORM
    Normaliza os dados
"""

# produtos
produtos['categoria'] = produtos['categoria'].str.strip().str.title() # strip() retira espaços iniciais e finais 
produtos['subcategoria'] = produtos['subcategoria'].str.strip().str.title()
produtos['marca'] = produtos['marca'].fillna('Sem marca') # fillna() procura e preenche campos vazios
produtos['preco_bruto'] = pd.to_numeric(produtos['preco_custo'], errors='coerce') # transforma p/ numérico
produtos['preco_venda'] = pd.to_numeric(produtos['preco_venda'], errors='coerce')
produtos['ativo'] = produtos['ativo'].fillna(1).astype(int)
produtos = produtos[
    (produtos['preco_venda'] >= 0) &
    (produtos['nome_produto'].notna()) & 
    (produtos['nome_produto'].str.strip() != '')
]

# lojas
lojas = lojas.drop_duplicates(subset='cod_loja') # dropa código duplicado de loja
lojas['formato'] = lojas['formato'].str.strip().str.title()
lojas['gerente'] = lojas['gerente'].str.strip().str.title()
lojas['area_m2'] = pd.to_numeric(lojas['area_m2'], errors='coerce')
lojas['num_checkouts'] = pd.to_numeric(lojas['num_checkouts'], errors='coerce').fillna(0).astype(int)
lojas['area_m2'] = lojas['area_m2'].fillna(lojas['area_m2'].mean()).astype(int)
lojas['estado'] = lojas['estado'].str.strip().str.upper()

mapa_regiao = [
    'SP': 'Sudeste',
    'RJ': 'Sudeste', 
    'MG': 'Sudeste',
    'ES': 'Sudeste',
    'PR': 'Sul',
    'SC': 'Sul',
    'RS': 'Sul',
    'RS': 'Sul',
    'BA': 'Nordeste',
    'PE': 'Nordeste',
    'CE': 'Nordeste'
]

lojas['regiao'] = lojas['estado'].map(mapa_regiao).fillna('Não Informado')

# cliente
clientes = clientes.drop_duplicates(subset='cod_cliente')
clientes['segmento'] = clientes['segmento'].str.strip().str.title()
clientes['estado'] = clientes['estado'].str.strip().str.title()
clientes['estado'] = clientes['estado'].fillna('XX').str.strip().str.upper()

# função que gera hash do CPF
def gerar_hash(cpf):
    if pd.isna(cpf):
        cpf='00000000000'
    return hashlib.sha256(str(cpf).encode()).hexdigest()

clientes['cpf_hash'] = clientes['cpf'].apply(gerar_hash)

clientes['regiao'] = clientes['estado'].map(mapa_regiao).fillna('Não Informado')

clientes = clientes.drop(columns = ['cpf']) # apaga coluna CPF e mantém a coluna CPF HASH

# dataframe para testar operações no banco usando dados padrão para cliente anônimo
cliente_anonimo = pd.DataFrame([{
    'cod_cliente' : 'CRM-000',
    'nome_cliente' : 'Cliente anônimo',
    'genero' : 'N',
    'cidade' : 'Não Informado',
    'estado' : 'XX',
    'canal_aquisicao' : 'Loja Física',
    'segmento' : 'Bronze',
    'data_cadastro' : '2010-01-01',
    'cpf_hash' : hashlib.sha256('00000000000'.encode()).hexdigest(),
    'regiao' : 'Não Informado',
}])

clientes = pd.concat([cliente_anonimo, clientes], ignore_index=True)

# tempo

datas = pd.date_range('2024-01-01','2024-12-31')
tempo = pd.DataFrame({
    'data_completa' : datas,
    'dia' : datas.day,
    'mes' : datas.month,
    'ano' : datas.year,
    'trimestre' : datas.quarter,
})

tempo['data_completa'] = tempo['data_completa'].astype(str)

# vendas
vendas['quantidade'] = pd.to_numeric(vendas['quantidade'], errors='coerce')
vendas['preco_unitario'] = pd.to_numeric(vendas['preco_unitario'], errors='coerce')
vendas['desconto_unitario'] = pd.to_numeric(vendas['desconto_unitario'], errors='coerce').fillna(0)

# filtrar vendas válidas
vendas = vendas[
    (vendas['data_venda'].isin(tempo['data_completa'])) &
    (vendas['cod_loja'].isin(lojas['cod_loja'])) &
    (vendas['cod_produto'].isin(produtos['cod_produto'])) &
    (vendas['quantidade'] > 0) &
    (vendas['preco_unitario'] > 0)
]

# cliente desconhecido vira cliente anônimo
vendas.loc[~vendas['cod_cliente'].isin(clientes['cod_cliente']), 'cod_cliente'] = 'CRM-000'

# calculos