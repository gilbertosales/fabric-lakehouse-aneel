-- Views no SQL analytics endpoint do Lakehouse (T-SQL).
-- Usadas por analistas que consultam direto em SQL, sem passar pelo modelo semântico.

CREATE OR ALTER VIEW dbo.vw_potencia_por_fonte_uf AS
SELECT
    l.uf,
    l.regiao,
    f.fonte_geracao,
    COUNT(*)                               AS qtd_empreendimentos,
    SUM(fa.potencia_instalada_kw) / 1000.0 AS potencia_mw
FROM dbo.gold_fato_geracao_distribuida AS fa
JOIN dbo.gold_dim_localidade AS l ON l.sk_localidade = fa.sk_localidade
JOIN dbo.gold_dim_fonte      AS f ON f.sk_fonte      = fa.sk_fonte
GROUP BY l.uf, l.regiao, f.fonte_geracao;
GO

CREATE OR ALTER VIEW dbo.vw_top_municipios_solar AS
SELECT TOP 50
    l.municipio,
    l.uf,
    SUM(fa.potencia_instalada_kw) / 1000.0 AS potencia_mw,
    COUNT(*)                               AS qtd_sistemas
FROM dbo.gold_fato_geracao_distribuida AS fa
JOIN dbo.gold_dim_localidade AS l ON l.sk_localidade = fa.sk_localidade
JOIN dbo.gold_dim_fonte      AS f ON f.sk_fonte      = fa.sk_fonte
WHERE f.tipo_geracao = 'UFV'
GROUP BY l.municipio, l.uf
ORDER BY potencia_mw DESC;
GO

CREATE OR ALTER VIEW dbo.vw_crescimento_anual AS
SELECT
    c.ano,
    COUNT(*)                                AS novos_empreendimentos,
    SUM(fa.potencia_instalada_kw) / 1000.0  AS potencia_mw_adicionada,
    SUM(SUM(fa.potencia_instalada_kw) / 1000.0) OVER (ORDER BY c.ano) AS potencia_mw_acumulada
FROM dbo.gold_fato_geracao_distribuida AS fa
JOIN dbo.gold_dim_calendario AS c ON c.sk_data = fa.sk_data_conexao
GROUP BY c.ano;
GO
