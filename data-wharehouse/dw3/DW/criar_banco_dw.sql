-- ============================================================
--  DATA WAREHOUSE — REDE MART BRASIL
--  Script: criar_banco_dw.sql
--  Objetivo: Criar todas as tabelas do DW para o laboratório
--             de ETL com Python + MySQL
--  Execute ANTES de rodar o etl_pipeline.py
-- ============================================================

DROP DATABASE IF EXISTS dw_supermercado;
CREATE DATABASE dw_supermercado
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE dw_supermercado;

-- ──────────────────────────────────────────────────────────
-- DIM_TEMPO
-- ──────────────────────────────────────────────────────────
CREATE TABLE dim_tempo (
    sk_tempo          INT          NOT NULL AUTO_INCREMENT,
    data_completa     DATE         NOT NULL,
    dia               TINYINT      NOT NULL,
    dia_semana_num    TINYINT      NOT NULL COMMENT '1=Segunda ... 7=Domingo',
    dia_semana_nome   VARCHAR(20)  NOT NULL,
    semana_ano        TINYINT      NOT NULL,
    mes               TINYINT      NOT NULL,
    mes_nome          VARCHAR(15)  NOT NULL,
    trimestre         TINYINT      NOT NULL,
    semestre          TINYINT      NOT NULL,
    ano               SMALLINT     NOT NULL,
    dia_util          TINYINT(1)   NOT NULL DEFAULT 1,
    feriado           TINYINT(1)   NOT NULL DEFAULT 0,
    feriado_descricao VARCHAR(80)  NULL,
    PRIMARY KEY (sk_tempo),
    UNIQUE KEY uq_data (data_completa),
    INDEX idx_ano_mes (ano, mes),
    INDEX idx_ano_trimestre (ano, trimestre)
) ENGINE=InnoDB COMMENT='Dimensão Tempo — gerada pelo ETL Python';

-- ──────────────────────────────────────────────────────────
-- DIM_PRODUTO
-- ──────────────────────────────────────────────────────────
CREATE TABLE dim_produto (
    sk_produto        INT           NOT NULL AUTO_INCREMENT,
    cod_produto_erp   VARCHAR(20)   NOT NULL COMMENT 'Código original da fonte (CSV)',
    nome_produto      VARCHAR(120)  NOT NULL,
    marca             VARCHAR(60)   NOT NULL DEFAULT 'Sem Marca',
    categoria         VARCHAR(60)   NOT NULL,
    subcategoria      VARCHAR(60)   NOT NULL,
    unidade_medida    VARCHAR(10)   NOT NULL,
    preco_custo       DECIMAL(10,2) NOT NULL,
    preco_tabela      DECIMAL(10,2) NOT NULL,
    fornecedor        VARCHAR(80)   NOT NULL,
    ativo             TINYINT(1)    NOT NULL DEFAULT 1,
    PRIMARY KEY (sk_produto),
    INDEX idx_categoria (categoria),
    INDEX idx_cod_erp (cod_produto_erp)
) ENGINE=InnoDB COMMENT='Dimensão Produto — carregada via ETL do CSV 01_produtos_bruto.csv';

-- ──────────────────────────────────────────────────────────
-- DIM_CLIENTE
-- ──────────────────────────────────────────────────────────
CREATE TABLE dim_cliente (
    sk_cliente        INT           NOT NULL AUTO_INCREMENT,
    cod_cliente_crm   VARCHAR(20)   NOT NULL COMMENT 'Código original do CRM (CSV)',
    nome_cliente      VARCHAR(100)  NOT NULL,
    cpf_hash          VARCHAR(64)   NOT NULL COMMENT 'SHA-256 do CPF — anonimização LGPD',
    genero            CHAR(1)       NOT NULL DEFAULT 'N',
    faixa_etaria      VARCHAR(20)   NOT NULL DEFAULT 'Não informado',
    cidade            VARCHAR(80)   NOT NULL DEFAULT 'Não informado',
    estado            CHAR(2)       NOT NULL DEFAULT 'XX',
    regiao            VARCHAR(20)   NOT NULL DEFAULT 'Não informado',
    canal_aquisicao   VARCHAR(40)   NOT NULL,
    segmento          VARCHAR(30)   NOT NULL,
    data_cadastro     DATE          NOT NULL,
    PRIMARY KEY (sk_cliente),
    INDEX idx_segmento (segmento),
    INDEX idx_cod_crm (cod_cliente_crm)
) ENGINE=InnoDB COMMENT='Dimensão Cliente — carregada via ETL do CSV 03_clientes_bruto.csv';

-- ──────────────────────────────────────────────────────────
-- DIM_LOJA
-- ──────────────────────────────────────────────────────────
CREATE TABLE dim_loja (
    sk_loja           INT           NOT NULL AUTO_INCREMENT,
    cod_loja          VARCHAR(10)   NOT NULL,
    nome_loja         VARCHAR(80)   NOT NULL,
    formato           VARCHAR(30)   NOT NULL,
    endereco          VARCHAR(150)  NOT NULL,
    cidade            VARCHAR(80)   NOT NULL,
    estado            CHAR(2)       NOT NULL,
    regiao            VARCHAR(20)   NOT NULL DEFAULT 'Não informado',
    area_m2           INT           NOT NULL,
    num_checkouts     TINYINT       NOT NULL,
    gerente           VARCHAR(80)   NOT NULL,
    data_inauguracao  DATE          NULL,
    ativa             TINYINT(1)    NOT NULL DEFAULT 1,
    PRIMARY KEY (sk_loja),
    INDEX idx_estado (estado),
    INDEX idx_regiao (regiao),
    INDEX idx_cod_loja (cod_loja)
) ENGINE=InnoDB COMMENT='Dimensão Loja — carregada via ETL do CSV 02_lojas_bruto.csv';

-- ──────────────────────────────────────────────────────────
-- FATO_VENDA
-- ──────────────────────────────────────────────────────────
CREATE TABLE fato_venda (
    sk_venda          BIGINT        NOT NULL AUTO_INCREMENT,
    sk_tempo          INT           NOT NULL,
    sk_produto        INT           NOT NULL,
    sk_cliente        INT           NOT NULL,
    sk_loja           INT           NOT NULL,
    num_cupom_fiscal  VARCHAR(25)   NOT NULL,
    quantidade        DECIMAL(10,3) NOT NULL,
    preco_unitario    DECIMAL(10,2) NOT NULL,
    desconto_unit     DECIMAL(10,2) NOT NULL DEFAULT 0,
    valor_bruto       DECIMAL(12,2) NOT NULL,
    valor_desconto    DECIMAL(12,2) NOT NULL DEFAULT 0,
    valor_liquido     DECIMAL(12,2) NOT NULL,
    custo_total       DECIMAL(12,2) NOT NULL,
    lucro_bruto       DECIMAL(12,2) NOT NULL,
    margem_percent    DECIMAL(6,2)  NOT NULL,
    PRIMARY KEY (sk_venda),
    FOREIGN KEY (sk_tempo)   REFERENCES dim_tempo   (sk_tempo),
    FOREIGN KEY (sk_produto) REFERENCES dim_produto (sk_produto),
    FOREIGN KEY (sk_cliente) REFERENCES dim_cliente (sk_cliente),
    FOREIGN KEY (sk_loja)    REFERENCES dim_loja    (sk_loja),
    INDEX idx_tempo   (sk_tempo),
    INDEX idx_produto (sk_produto),
    INDEX idx_cliente (sk_cliente),
    INDEX idx_loja    (sk_loja)
) ENGINE=InnoDB COMMENT='Fato Venda — carregada via ETL do CSV 04_vendas_bruto.csv';

SELECT 'Banco dw_supermercado criado! Execute agora: python etl_pipeline.py' AS instrucao;
