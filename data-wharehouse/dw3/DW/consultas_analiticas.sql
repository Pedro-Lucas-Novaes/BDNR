-- ============================================================
--  DATA WAREHOUSE — REDE MART BRASIL
--  Script: consultas_analiticas.sql
--  Execute APÓS o etl_pipeline.py carregar o DW
-- ============================================================

USE dw_supermercado;

-- ── CONSULTA 1 — Faturamento mensal ──────────────────────
SELECT
    t.mes_nome                          AS mes,
    COUNT(DISTINCT f.num_cupom_fiscal)  AS total_cupons,
    ROUND(SUM(f.valor_liquido),  2)     AS faturamento_R$,
    ROUND(SUM(f.lucro_bruto),    2)     AS lucro_R$,
    ROUND(AVG(f.margem_percent), 2)     AS margem_media_pct
FROM fato_venda f
JOIN dim_tempo  t ON f.sk_tempo = t.sk_tempo
WHERE t.ano = 2024
GROUP BY t.mes, t.mes_nome
ORDER BY t.mes;

-- ── CONSULTA 2 — Ranking de categorias ───────────────────
SELECT
    p.categoria,
    ROUND(SUM(f.valor_liquido), 2)      AS faturamento_R$,
    ROUND(AVG(f.margem_percent),2)      AS margem_pct,
    ROUND(SUM(f.valor_liquido)*100.0
        / SUM(SUM(f.valor_liquido)) OVER(), 2) AS participacao_pct
FROM fato_venda  f
JOIN dim_produto p ON f.sk_produto = p.sk_produto
GROUP BY p.categoria
ORDER BY faturamento_R$ DESC;

-- ── CONSULTA 3 — Desempenho por loja ─────────────────────
SELECT
    l.nome_loja, l.formato, l.cidade, l.estado,
    ROUND(SUM(f.valor_liquido),  2)      AS faturamento_R$,
    ROUND(AVG(f.margem_percent), 2)      AS margem_pct,
    COUNT(DISTINCT f.num_cupom_fiscal)   AS total_cupons,
    ROUND(SUM(f.valor_liquido)
        / COUNT(DISTINCT f.num_cupom_fiscal), 2) AS ticket_medio_R$
FROM fato_venda f
JOIN dim_loja   l ON f.sk_loja = l.sk_loja
GROUP BY l.sk_loja, l.nome_loja, l.formato, l.cidade, l.estado
ORDER BY faturamento_R$ DESC;

-- ── CONSULTA 4 — Segmentação de clientes ─────────────────
SELECT
    c.segmento,
    COUNT(DISTINCT f.sk_cliente)         AS clientes_ativos,
    ROUND(SUM(f.valor_liquido), 2)       AS faturamento_R$,
    ROUND(SUM(f.valor_liquido)
        / COUNT(DISTINCT f.sk_cliente),2) AS gasto_medio_R$
FROM fato_venda  f
JOIN dim_cliente c ON f.sk_cliente = c.sk_cliente
WHERE c.cod_cliente_crm != 'CRM-000'
GROUP BY c.segmento
ORDER BY gasto_medio_R$ DESC;

-- ── CONSULTA 5 — Black Friday vs período normal ───────────
SELECT
    CASE
        WHEN t.data_completa = '2024-11-29' THEN 'Black Friday'
        WHEN t.data_completa BETWEEN '2024-12-20' AND '2024-12-24' THEN 'Véspera Natal'
        ELSE 'Período Normal'
    END                                       AS periodo,
    COUNT(DISTINCT f.num_cupom_fiscal)        AS cupons,
    ROUND(SUM(f.valor_liquido),  2)           AS faturamento_R$,
    ROUND(SUM(f.valor_desconto), 2)           AS descontos_R$,
    ROUND(AVG(f.margem_percent), 2)           AS margem_pct
FROM fato_venda f
JOIN dim_tempo  t ON f.sk_tempo = t.sk_tempo
GROUP BY periodo
ORDER BY faturamento_R$ DESC;

-- ── CONSULTA 6 — Tendência trimestral com variação ───────
WITH trim AS (
    SELECT t.trimestre,
           ROUND(SUM(f.valor_liquido),2) AS fat
    FROM fato_venda f
    JOIN dim_tempo  t ON f.sk_tempo = t.sk_tempo
    WHERE t.ano = 2024
    GROUP BY t.trimestre
)
SELECT
    CONCAT('T', trimestre) AS trimestre,
    fat                    AS faturamento_R$,
    ROUND((fat - LAG(fat) OVER (ORDER BY trimestre))
          / LAG(fat) OVER (ORDER BY trimestre) * 100, 2) AS variacao_pct
FROM trim
ORDER BY trimestre;
