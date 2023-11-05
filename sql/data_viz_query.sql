-- 1. Exchange Rate US Dollar (TABLE)
WITH LatestExchangeRates AS (
  SELECT currency, MAX(time_last_update_unix) AS max_time
  FROM `finpro3.exchange_rate.exchange_rate`
  WHERE base_currency = 'USD'
  GROUP BY currency
)

SELECT er.currency, er.rates
FROM `finpro3.exchange_rate.exchange_rate` AS er
JOIN LatestExchangeRates AS ler
ON er.currency = ler.currency AND er.time_last_update_unix = ler.max_time
WHERE er.base_currency = 'USD'
ORDER BY er.rates DESC;

-- 2. Exhange Rates Indonesian Rupiah (TABLE)
WITH LatestExchangeRates AS (
  SELECT currency, MAX(time_last_update_unix) AS max_time
  FROM exchange_rates
  WHERE base_currency = 'IDR'
  GROUP BY currency
)

SELECT er.currency, er.rates
FROM `finpro3.exchange_rate.exchange_rate` AS er
JOIN LatestExchangeRates AS lerq
ON er.currency = ler.currency AND er.time_last_update_unix = ler.max_time
WHERE er.base_currency = 'IDR'
ORDER BY er.rates DESC;

-- 3. Exchange Rates Japanese Yen (TABLE)
WITH LatestExchangeRates AS (
  SELECT currency, MAX(time_last_update_unix) AS max_time
  FROM `finpro3.exchange_rate.exchange_rate`
  WHERE base_currency = 'JPY'
  GROUP BY currency
)

SELECT er.currency, er.rates
FROM `finpro3.exchange_rate.exchange_rate` AS er
JOIN LatestExchangeRates AS ler
ON er.currency = ler.currency AND er.time_last_update_unix = ler.max_time
WHERE er.base_currency = 'JPY'
ORDER BY er.rates DESC;

-- 4. Exchange Rates Russian Rubel (TABLE)
WITH LatestExchangeRates AS (
  SELECT currency, MAX(time_last_update_unix) AS max_time
  FROM `finpro3.exchange_rate.exchange_rate`
  WHERE base_currency = 'RUB'
  GROUP BY currency
)

SELECT currency, rates
FROM `finpro3.exchange_rate.exchange_rate`
WHERE (currency, time_last_update_unix) IN (
  SELECT currency, max_time
  FROM LatestExchangeRates
)
AND base_currency = 'RUB'
ORDER BY rates DESC;

-- 5. TOP 10 Currencies with the Most Stable Rates Bar chart
WITH RateVariation AS (
    SELECT currency, STDDEV(CAST(rates AS NUMERIC)) AS rate_stability
    FROM `finpro3.exchange_rate.exchange_rate`
    GROUP BY currency
)

SELECT currency, rate_stability
FROM RateVariation
ORDER BY rate_stability DESC
LIMIT 10;

-- 6. Currencies JPY with the Lowest Rates Bar Chart
SELECT currency, MIN(CAST(rates AS NUMERIC)) AS min_rate
FROM `finpro3.exchange_rate.exchange_rate`
WHERE base_currency = 'JPY'
GROUP BY currency
ORDER BY min_rate
LIMIT 10;

--7. Currencies with Consistently Increasing Rates (TABLE)
WITH RankedRates AS (
    SELECT currency, time_last_update_unix, rates,
        RANK() OVER (PARTITION BY currency ORDER BY time_last_update_unix) AS rank
    FROM `finpro3.exchange_rate.exchange_rate`
)
SELECT currency, rank
FROM RankedRates
WHERE rank = 1;

-- 8. Top 5 Currencies with the Highest Exchange Rates IDR
SELECT currency, MAX(rates) AS max_rate
FROM exchange_rates 
where base_currency ='IDR' 
GROUP BY currency
ORDER BY max_rate DESC
LIMIT 5;
