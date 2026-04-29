import pandas as pd
import mysql.connector
import hashlib
import sys
import re
from datetime import date,datetime
from tabulate import tabulate
from colorama import Fore, init

# =========================
# Conexão
# =========================
conexao = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="dw_supermercado"
)

conecta= conexao.cursor()

# =========================
# Extract
# =========================
produtos = pd.read_csv("01_produtos_bruto.csv", encoding="utf-8")
lojas = pd.read_csv("02_lojas_bruto.csv", encoding="utf-8")
clientes = pd.read_csv("03_clientes_bruto.csv", encoding="utf-8")
vendas = pd.read_csv("04_vendas_bruto.csv", encoding="utf-8")

# =========================
# Transform
# =========================

# Produtos
produtos['categoria'] = produtos["categoria"].str.strip().str.title()
produtos['subcategoria'] = produtos["subcategoria"].str.strip().str.title()
produtos['marca'] = produtos["marca"].fillna("Sem marca")
produtos['preco_custo'] = pd.to_numeric(produtos['preco_custo'],errors="coerce")
produtos['preco_venda'] = pd.to_numeric(produtos['preco_venda'],errors="coerce")
produtos['ativo'] = produtos["ativo"].fillna(1).astype(int)

produtos=produtos[
    (produtos['preco_venda']>=0) &
    (produtos['nome_produto'].notna())&
    (produtos['nome_produto'].str.strip()!="")
]

# Loja
lojas= lojas.drop_duplicates(subset="cod_loja")
lojas['formato'] = lojas['formato'].str.strip().str.title()
lojas['gerente'] = lojas['gerente'].str.strip().str.title()
lojas['area_m2']= pd.to_numeric(lojas["area_m2"],errors="coerce")
lojas['num_checkouts']=pd.to_numeric(lojas["num_checkouts"],errors="coerce").fillna(0).astype(int)
lojas["area_m2"]=lojas["area_m2"].fillna(lojas["area_m2"].mean()).astype(int)
lojas['estado']=lojas["estado"].str.strip().str.upper()

mapa_regiao={
    'SP':'Sudeste', 'RJ':'Sudeste', 'MG':'Sudeste', 'ES': 'Sudeste',
    'PR':'Sul', 'SC':'Sul','RS':'Sul',
    'BA':'Nordeste','PE':'Nordeste','CE':'Nordeste'
}

lojas['regiao']= lojas['estado'].map(mapa_regiao).fillna("Não informado")

# Cliente
clientes=clientes.drop_duplicates(subset='cod_cliente')
clientes['segmento']= clientes['segmento'].str.strip().str.title()
clientes['estado']= clientes['estado'].str.strip().str.upper().fillna("XX")

def gerar_hash(cpf):
    if pd.isna(cpf):
        cpf="00000000000"
    return hashlib.sha256(str(cpf).encode()).hexdigest()

clientes['cpf_hash']=clientes['cpf'].apply(gerar_hash)
clientes['regiao']=clientes['estado'].map(mapa_regiao).fillna("Não informado")

clientes = clientes.drop(columns=['cpf'])

cliente_anonimo= pd.DataFrame([{
    "cod_cliente":"CRM-000",
    "nome":"Cliente Anonimo",
    "genero":"N",
    "cidade":"Não informado",
    "estado":"XX",
    "canal_aquisicao":"Loja Fisica",
    "segmento":"Bronze",
    "data_cadastro":"2010-01-01",
    "cpf_hash": hashlib.sha256("00000000000".encode()).hexdigest(),
    "regiao":"Não informado",
}])

clientes=pd.concat([cliente_anonimo,clientes],ignore_index=True)

# Tempo
datas = pd.date_range('2024-01-01','2024-12-31')

tempo = pd.DataFrame({
    'data_completa': datas.astype(str),
    'dia':datas.day,
    'mes':datas.month,
    'ano':datas.year,
    'trimestre':datas.quarter
})

# ✅ CORREÇÃO DO ERRO DO BANCO
tempo['dia_semana_num'] = pd.to_datetime(tempo['data_completa']).dt.weekday + 1

# Vendas
vendas['quantidade']=pd.to_numeric(vendas['quantidade'],errors="coerce")
vendas['preco_unitario']=pd.to_numeric(vendas['preco_unitario'],errors="coerce")
vendas['desconto_unitario']=pd.to_numeric(vendas['desconto_unitario'],errors="coerce").fillna(0)

vendas=vendas[
    (vendas['data_venda'].isin(tempo['data_completa']))&
    (vendas['cod_loja'].isin(lojas['cod_loja']))&
    (vendas['cod_produto'].isin(produtos['cod_produto']))&
    (vendas['quantidade']>0)&
    (vendas['preco_unitario']>0)
]

vendas.loc[~vendas['cod_cliente'].isin(clientes['cod_cliente']),'cod_cliente'] = 'CRM-000'

mapa_custo= produtos.set_index("cod_produto")['preco_custo'].to_dict()
vendas['preco_custo'] = vendas['cod_produto'].map(mapa_custo)

vendas["valor_bruto"] = vendas["quantidade"] * vendas["preco_unitario"]
vendas["valor_desconto"] = vendas["quantidade"] * vendas["desconto_unitario"]
vendas["valor_liquido"] = vendas["valor_bruto"] - vendas["valor_desconto"]
vendas["custo_total"] = vendas["quantidade"] * vendas["preco_custo"]
vendas["lucro_bruto"] = vendas["valor_liquido"] - vendas["custo_total"]
vendas["margem_percent"] = (vendas["lucro_bruto"] / vendas["valor_liquido"]).fillna(0)

# =========================
# Load
# =========================
conecta.execute("SET FOREIGN_KEY_CHECKS=0")
conecta.execute("TRUNCATE TABLE fato_venda")
conecta.execute("TRUNCATE TABLE dim_tempo")
conecta.execute("TRUNCATE TABLE dim_produto")
conecta.execute("TRUNCATE TABLE dim_loja")
conecta.execute("TRUNCATE TABLE dim_cliente")
conecta.execute("SET FOREIGN_KEY_CHECKS=1")

# Tempo
for _, linha in tempo.iterrows():
    conecta.execute(
        "INSERT INTO dim_tempo (data_completa, dia, mes, ano, trimestre, dia_semana_num) VALUES (%s,%s,%s,%s,%s,%s)",
        (
            linha["data_completa"],
            linha["dia"],
            linha["mes"],
            linha["ano"],
            linha["trimestre"],
            linha["dia_semana_num"]
        )
    )

# Produto
mapa_produto_sk = {}
for _, linha in produtos.iterrows():
    conecta.execute(
        "INSERT INTO dim_produto (cod_prod_erp, nome_produto, marca, categoria, subcategoria, unidade_medida, preco_custo, preco_tabela, fornecedor, ativo) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            linha["cod_produto"],
            linha["nome_produto"],
            linha["marca"],
            linha["categoria"],
            linha["subcategoria"],
            linha["unidade"],
            float(linha["preco_custo"]),
            float(linha["preco_venda"]),
            linha["fornecedor"],
            int(linha["ativo"]),
        )
    )
    mapa_produto_sk[linha["cod_produto"]] = conecta.lastrowid

# Loja
mapa_loja_sk = {}
for _, linha in lojas.iterrows():
    conecta.execute(
        "INSERT INTO dim_loja (cod_loja, nome_loja, formato, endereco, cidade, estado, regiao, area_m2, num_checkouts, gerente, data_inauguracao, ativa) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1)",
        (
            linha["cod_loja"],
            linha["nome_loja"],
            linha["formato"],
            linha["endereco"],
            linha["cidade"],
            linha["estado"],
            linha["regiao"],
            int(linha["area_m2"]),
            int(linha["num_checkouts"]),
            linha["gerente"],
            linha["data_inauguracao"],
        )
    )
    mapa_loja_sk[linha["cod_loja"]] = conecta.lastrowid

# Cliente
mapa_cliente_sk = {}
for _, linha in clientes.iterrows():
    conecta.execute(
        "INSERT INTO dim_cliente (cod_cliente, nome_cliente, cpf_hash, genero, cidade, estado, regiao, canal_aquisicao, segmento, data_cadastro) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            linha["cod_cliente"],
            linha["nome"],
            linha["cpf_hash"],
            linha["genero"],
            linha["cidade"],
            linha["estado"],
            linha["regiao"],
            linha["canal_aquisicao"],
            linha["segmento"],
            linha["data_cadastro"],
        )
    )
    mapa_cliente_sk[linha["cod_cliente"]] = conecta.lastrowid

# Tempo SK
conecta.execute("SELECT sk_tempo, data_completa FROM dim_tempo")
mapa_tempo_sk = {str(data): sk for sk, data in conecta.fetchall()}

# Fato
for _, linha in vendas.iterrows():
    conecta.execute(
        "INSERT INTO fato_venda (sk_tempo, sk_produto, sk_cliente, sk_loja, num_cupom_fiscal, quantidade, preco_unitario, desconto_unitario, valor_bruto, valor_desconto, valor_liquido, custo_total, lucro_bruto, margem_percent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            mapa_tempo_sk[linha["data_venda"]],
            mapa_produto_sk[linha["cod_produto"]],
            mapa_cliente_sk[linha["cod_cliente"]],
            mapa_loja_sk[linha["cod_loja"]],
            linha["num_cupom"],
            float(linha["quantidade"]),
            float(linha["preco_unitario"]),
            float(linha["desconto_unitario"]),
            float(linha["valor_bruto"]),
            float(linha["valor_desconto"]),
            float(linha["valor_liquido"]),
            float(linha["custo_total"]),
            float(linha["lucro_bruto"]),
            float(linha["margem_percent"]),
        )
    )

# Finalização
conexao.commit()
conecta.close()
conexao.close()

print("ETL concluído com sucesso!!!")