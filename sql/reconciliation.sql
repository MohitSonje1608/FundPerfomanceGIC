WITH eom_date AS (
    SELECT MAX(DATETIME) AS eom FROM (
        SELECT DATETIME FROM bond_prices
        UNION ALL
        SELECT DATETIME FROM equity_prices
    )
),

latest_bond_prices AS (
    SELECT ISIN, PRICE, DATETIME
    FROM (
        SELECT bp.ISIN,
               bp.PRICE,
               bp.DATETIME,
               ROW_NUMBER() OVER (PARTITION BY bp.ISIN ORDER BY bp.DATETIME DESC) AS rn
        FROM bond_prices bp
        JOIN eom_date eom ON bp.DATETIME <= eom.eom
    ) t
    WHERE rn = 1
),

latest_equity_prices AS (
    SELECT SYMBOL, PRICE, DATETIME
    FROM (
        SELECT ep.SYMBOL,
               ep.PRICE,
               ep.DATETIME,
               ROW_NUMBER() OVER (PARTITION BY ep.SYMBOL ORDER BY ep.DATETIME DESC) AS rn
        FROM equity_prices ep
        JOIN eom_date eom ON ep.DATETIME <= eom.eom
    ) t
    WHERE rn = 1
),

bond_fund_data AS (
    SELECT
        fd.SYMBOL,
        fd.SECURITY_NAME,
        fd.SEDOL,
        COALESCE(fd.ISIN, br.ISIN) AS ISIN,
        fd.PRICE AS fund_price,
        fd.QUANTITY,
        fd.REALISED_P_L,
        fd.MARKET_VALUE
    FROM financial_data fd
    LEFT JOIN bond_reference br
           ON TRIM(UPPER(fd.SEDOL)) = TRIM(UPPER(br.SEDOL))
    WHERE fd.FINANCIAL_TYPE = 'Government Bond'
),

equity_fund_data AS (
    SELECT
        fd.SYMBOL,
        fd.SECURITY_NAME,
        fd.SEDOL,
        fd.ISIN,
        fd.PRICE AS fund_price,
        fd.QUANTITY,
        fd.REALISED_P_L,
        fd.MARKET_VALUE
    FROM financial_data fd
    WHERE fd.FINANCIAL_TYPE = 'Equities'
)

-- Final reconciliation
SELECT
    'Bond' AS asset_type,
    bfd.SYMBOL,
    bfd.SECURITY_NAME,
    bfd.SEDOL,
    bfd.ISIN,
    lbp.DATETIME AS reference_price_date,
    lbp.PRICE AS reference_price,
    bfd.fund_price,
    ROUND(bfd.fund_price - lbp.PRICE, 4) AS price_difference
FROM bond_fund_data bfd
LEFT JOIN latest_bond_prices lbp
       ON TRIM(UPPER(bfd.ISIN)) = TRIM(UPPER(lbp.ISIN))

UNION ALL

SELECT
    'Equity' AS asset_type,
    efd.SYMBOL,
    efd.SECURITY_NAME,
    efd.SEDOL,
    efd.ISIN,
    lep.DATETIME AS reference_price_date,
    lep.PRICE AS reference_price,
    efd.fund_price,
    ROUND(efd.fund_price - lep.PRICE, 4) AS price_difference
FROM equity_fund_data efd
LEFT JOIN latest_equity_prices lep
       ON TRIM(UPPER(efd.SYMBOL)) = TRIM(UPPER(lep.SYMBOL));
