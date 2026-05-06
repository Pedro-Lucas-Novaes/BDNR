import pandas as pd
import mysql.connector
import hashlib
import sys
import re
from datetime import date, datetime
from tabulate import tabulate
from colorama import Fore, init

"""
Configuração com Banco de Dados MySQL
"""

conexao = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="dw_supermercado"
)

conecta = conexao.cursor() #Conectar com o BD

"""
1. Extract
"""
produtos = pd.read_csv("01_produtos_bruto.csv",sep=",", quotechar='"', encoding="utf-8")

lojas = pd.read_csv("02_lojas_bruto.csv",sep=",", quotechar='"', encoding="utf-8")

clientes = pd.read_csv("03_clientes_bruto.csv",sep=",", quotechar='"', encoding="utf-8")

vendas = pd.read_csv("04_vendas_bruto.csv",sep=",", quotechar='"', encoding="utf-8")

"""
2. Transform
"""
# Produtos
produtos['categoria'] = produtos["categoria"].str.strip().str.title()
produtos['subcategoria'] = produtos["subcategoria"].str.strip().str.title()
produtos['marca']= produtos["marca"].fillna("Sem Marca")
produtos['preco_custo']=pd.to_numeric(produtos['preco_custo'],errors="coerce")
produtos['preco_venda']=pd.to_numeric(produtos['preco_venda'],errors="coerce")
produtos['ativo']=produtos['ativo'].fillna(1).astype(int)

produtos = produtos[
    (produtos['preco_venda']>=0) &
    (produtos['nome_produto'].notna())&
    (produtos['nome_produto'].str.strip()!="")
]

#Lojas
lojas = lojas.drop_duplicates(subset="cod_loja")
lojas['formato'] = lojas['formato'].str.strip().str.title()
lojas['gerente'] = lojas['gerente'].str.strip().str.title()
lojas['area_m2'] = pd.to_numeric(lojas['area_m2'], errors="coerce")
lojas['num_checkouts'] = pd.to_numeric(lojas['num_checkouts'], errors="coerce").fillna(0).astype(int)
lojas['area_m2'] = lojas["area_m2"].fillna(lojas['area_m2'].mean()).astype(int)
lojas['estado'] = lojas['estado'].str.strip().str.upper()

lojas['data_inauguracao'] = pd.to_datetime(
    lojas['data_inauguracao'],
    format="%d/%m/%Y",
    errors="coerce"
)

lojas['data_inauguracao'] = lojas['data_inauguracao'].dt.strftime("%Y-%m-%d")


mapa_regiao = {
    'SP': 'Sudeste', 'RJ':'Sudeste','MG':'Sudeste','ES':'Sudeste',
    'PR': 'Sul', 'SC':'Sul','RS':'Sul',
    'BA': 'Nordeste', 'PE':'Nordeste', 'CE':'Nordeste'
}
lojas['regiao'] = lojas['estado'].map(mapa_regiao).fillna("Não Informado")

#Cliente
clientes=clientes.drop_duplicates(subset='cod_cliente')
clientes['segmento'] = clientes['segmento'].str.strip().str.title()
clientes['estado'] = clientes['estado'].fillna("XX").str.strip().str.upper()

def gerar_hash(cpf):
    if pd.isna(cpf):
        cpf="00000000000"
    return hashlib.sha256(str(cpf).encode()).hexdigest()

clientes['cpf_hash']=clientes['cpf'].apply(gerar_hash)
clientes['regiao'] = clientes['estado'].map(mapa_regiao).fillna("Não informado")

clientes = clientes.drop(columns=['cpf'])

cliente_anonimo= pd.DataFrame([{
    "cod_cliente":"CRM-000",
    "nome_cliente":"Cliente Anonimo",
    "genero":"N",
    "cidade":"Não Informado",
    "estado":"XX",
    "canal_aquisicao":"Loja Física",
    "segmento":"Bronze",
    "data_cadastro":"2010-01-01",
    "cpf_hash":hashlib.sha256("00000000000".encode()).hexdigest(),
    "regiao":"Não Informado",
}])

clientes=pd.concat([cliente_anonimo, clientes], ignore_index=True)

#tempo 
datas = pd.date_range('2024-01-01','2024-12-31')
tempo = pd.DataFrame({
    'data_completa':datas,
    'dia':datas.day,
    'mes':datas.month,
    'ano':datas.year,
    'trimestre':datas.quarter
})

tempo['data_completa'] = tempo['data_completa'].astype(str)

# Vendas
vendas['quantidade']=pd.to_numeric(vendas['quantidade'],errors='coerce')
vendas['preco_unitario']=pd.to_numeric(vendas['preco_unitario'],errors='coerce')
vendas['desconto_unitario']=pd.to_numeric(vendas['desconto_unitario'],errors='coerce').fillna(0)

#Filtar vendas validas
vendas=vendas[
    (vendas['data_venda'].isin(tempo['data_completa'])) &
    (vendas['cod_loja'].isin(lojas['cod_loja'])) &
    (vendas['cod_produto'].isin(produtos['cod_produto'])) &
    (vendas['quantidade']>0)&
    (vendas['preco_unitario']>0)
]

#Cliente desconhecido vira anonimo
vendas.loc[~vendas['cod_cliente'].isin(clientes['cod_cliente']),'cod_cliente']='CRM-000'

#calculos
mapa_custo=produtos.set_index("cod_produto")['preco_custo'].to_dict()
vendas["preco_custo"] = vendas["cod_produto"].map(mapa_custo)

vendas["valor_bruto"]= vendas["quantidade"]* vendas["preco_unitario"]
vendas["valor_desconto"]= vendas["quantidade"]* vendas["desconto_unitario"]
vendas["valor_liquido"]= vendas["valor_bruto"] - vendas["valor_desconto"]
vendas["custo_total"]= vendas["quantidade"]* vendas["preco_custo"]
vendas["lucro_bruto"]= vendas["valor_liquido"] - vendas["custo_total"]
vendas["margem_percent"]= vendas["lucro_bruto"] / vendas["valor_liquido"]
vendas["margem_percent"]= vendas["margem_percent"].fillna(0)


# 3. Load
conecta.execute("SET FOREIGN_KEY_CHECKS=0")

conecta.execute("TRUNCATE TABLE fato_venda")
conecta.execute("TRUNCATE TABLE dim_tempo")
conecta.execute("TRUNCATE TABLE dim_produto")
conecta.execute("TRUNCATE TABLE dim_loja")
conecta.execute("TRUNCATE TABLE dim_cliente")

conecta.execute("SET FOREIGN_KEY_CHECKS=1")

#Carregar dados

for _, linha in tempo.iterrows():
    conecta.execute(
        "insert into dim_tempo (data_completa, dia, mes, ano, trimestre) values (%s,%s,%s,%s,%s)", (
            linha["data_completa"],
            int(linha["dia"]),
            int(linha["mes"]),
            int(linha["ano"]),
            int(linha["trimestre"])
        )
    )

# Carregar dim_Produto
mapa_produto_sk={}
for _, linha in produtos.iterrows():
    conecta.execute(
        "Insert into dim_produto (cod_produto_erp, nome_produto, marca, categoria, subcategoria,unidade_medida,preco_custo,preco_tabela,fornecedor,ativo) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(
            linha["cod_produto"],
            linha["nome_produto"],
            linha["marca"],
            linha["categoria"],
            linha["subcategoria"],
            linha["unidade"],
            float(linha["preco_custo"]),
            float(linha["preco_venda"]),
            linha["fornecedor"],
            int(linha["ativo"])
        ))
    mapa_produto_sk[linha["cod_produto"]]=conecta.lastrowid

#Carregar dim_loja
mapa_loja_sk = {}

# Tratamento de valores nulos em lojas antes de carregar no MySQL
lojas["nome_loja"] = lojas["nome_loja"].fillna("Não Informado")
lojas["formato"] = lojas["formato"].fillna("Não Informado")
lojas["endereco"] = lojas["endereco"].fillna("Não Informado")
lojas["cidade"] = lojas["cidade"].fillna("Não Informado")
lojas["estado"] = lojas["estado"].fillna("XX")
lojas["regiao"] = lojas["regiao"].fillna("Não Informado")
lojas["gerente"] = lojas["gerente"].fillna("Não Informado")
lojas["data_inauguracao"] = lojas["data_inauguracao"].fillna("2010-01-01")

for _, linha in lojas.iterrows():
    conecta.execute(
        "insert into dim_loja (cod_loja, nome_loja, formato, endereco, cidade, estado, regiao, area_m2, num_checkouts, gerente, data_inauguracao, ativa) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1)",(
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

    mapa_loja_sk[linha["cod_loja"]]=conecta.lastrowid

# Tratamento de valores nulos em clientes antes de carregar no MySQL
clientes["nome_cliente"] = clientes["nome_cliente"].fillna("Não Informado")
clientes["genero"] = clientes["genero"].fillna("N")
clientes["cidade"] = clientes["cidade"].fillna("Não Informado")
clientes["estado"] = clientes["estado"].fillna("XX")
clientes["regiao"] = clientes["regiao"].fillna("Não Informado")
clientes["canal_aquisicao"] = clientes["canal_aquisicao"].fillna("Não Informado")
clientes["segmento"] = clientes["segmento"].fillna("Bronze")
clientes["data_cadastro"] = clientes["data_cadastro"].fillna("2010-01-01")

# carregar dim_clientes
mapa_cliente_sk={}
for _, linha in clientes.iterrows():
    conecta.execute(
        "Insert into dim_cliente(cod_cliente_crm,nome_cliente, cpf_hash, genero, cidade, estado, regiao, canal_aquisicao, segmento, data_cadastro) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(
            linha["cod_cliente"],
            linha["nome_cliente"],
            linha["cpf_hash"],
            linha["genero"],
            linha["cidade"],
            linha["estado"],
            linha["regiao"],
            linha["canal_aquisicao"],
            linha["segmento"],
            linha["data_cadastro"]
        ))
    mapa_cliente_sk[linha["cod_cliente"]]=conecta.lastrowid

# busca SK da dimensao tempo
conecta.execute("Select sk_tempo, data_completa from dim_tempo")
mapa_tempo_sk={str(data): sk for sk, data in conecta.fetchall()}

# carregar fato_venda

for _, linha in vendas.iterrows():
    conecta.execute(
        "Insert into fato_venda (sk_tempo, sk_produto, sk_cliente, sk_loja, num_cupom_fiscal, quantidade, preco_unitario, desconto_unit, valor_bruto, valor_desconto, valor_liquido, custo_total, lucro_bruto, margem_percent)values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(
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

conexao.commit()
conecta.close()
conexao.close()
print("ETL concluído com sucesso!!!")