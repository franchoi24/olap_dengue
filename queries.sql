a- WITH TotalPopulation AS (
   SELECT SUM(population) AS total_population FROM municipality
),
PercentPopulation AS (
   SELECT total_population * 0.05 AS five_percent_population FROM TotalPopulation
),
RankedMunicipalities AS (
   SELECT
       name,
       population,
       SUM(population) OVER (ORDER BY population desc rows unbounded preceding) AS cumulative_population
   FROM
       municipality
)
SELECT name, population, cumulative_population
FROM RankedMunicipalities, PercentPopulation, TotalPopulation
WHERE cumulative_population - population < five_percent_population
 
-- 2A
CREATE MATERIALIZED VIEW Sixty_percent_cases AS (
WITH TotalCases AS (
    SELECT SUM(casos) AS total_cases FROM cases
),
PercentCases AS (
    SELECT total_cases * 0.6 AS sixty_percent_cases FROM TotalCases
),
SumCases AS (
    SELECT 
        name, 
        SUM(c.casos) AS cases
    FROM 
        municipality m 
        JOIN cases c ON m.id = c.municipality_id
    GROUP BY 
        m.id
),
RankedMunicipalitiesCases AS (
    SELECT 
        name, 
        cases,
        SUM(cases) OVER (ORDER BY cases DESC ROWS UNBOUNDED PRECEDING) AS cumulative_cases
    FROM 
        SumCases
)
SELECT 
    name, 
    cases, 
    (cases / total_cases) * 100 AS percentage
FROM 
    RankedMunicipalitiesCases rmc, PercentCases pc, TotalCases tc
WHERE 
    rmc.cumulative_cases - cases < sixty_percent_cases
);

-- 2B
CREATE MATERIALIZED VIEW Departments_sixty_percent AS (
    SELECT DISTINCT departament AS departments
    FROM Sixty_percent_cases spc 
    JOIN municipality m ON spc.name = m.name
    GROUP BY departament
);

-- 2C
CREATE MATERIALIZED VIEW Departments_percentage AS (
WITH TotalDepartments AS (
    SELECT COUNT(DISTINCT departament) AS total_departments FROM municipality
)
SELECT 
    COUNT(*) AS departments_quantity, 
    COUNT(*) * 100.0 / (SELECT total_departments FROM TotalDepartments) AS percentage
FROM 
    Departments_sixty_percent
);

CREATE MATERIALIZED VIEW Weather_numeric AS (
    SELECT 
        w.municipality_id, 
        COALESCE(CAST(NULLIF(w.precipitation, 'NA') AS numeric), 0) AS precipitation,
        COALESCE(CAST(NULLIF(w.temperature, 'NA') AS numeric), 0) AS temperature
    FROM 
        weather w
);

-- 2D
CREATE MATERIALIZED VIEW Precipitation_comparison AS (
WITH Avg_precipitation AS (
    SELECT AVG(precipitation) AS average_precipitation FROM Weather_numeric
)
SELECT 
    dsp.departments, 
    AVG(w.precipitation) AS avg_precipitation,
    CASE
        WHEN (SELECT average_precipitation FROM Avg_precipitation) < AVG(w.precipitation) THEN TRUE 
        ELSE FALSE 
    END AS higher_than_avg
FROM 
    Weather_numeric w 
    JOIN municipality m ON w.municipality_id = m.id 
    JOIN Departments_sixty_percent dsp ON m.departament = dsp.departments
GROUP BY 
    dsp.departments
);

-- 2E
CREATE MATERIALIZED VIEW Temperature_comparison AS (
WITH Avg_temperature AS (
    SELECT AVG(temperature) AS average_temperature FROM Weather_numeric
)
SELECT 
    dsp.departments, 
    AVG(w.temperature) AS avg_temperature,
    CASE
        WHEN (SELECT average_temperature FROM Avg_temperature) < AVG(w.temperature) THEN TRUE 
        ELSE FALSE 
    END AS higher_than_avg
FROM 
    Weather_numeric w 
    JOIN municipality m ON w.municipality_id = m.id 
    JOIN Departments_sixty_percent dsp ON m.departament = dsp.departments
GROUP BY 
    dsp.departments
);

-- 3A
CREATE MATERIALIZED VIEW Five_percent_population AS (
WITH Total_municipalities AS (
    SELECT COUNT(*) AS total_municipalities FROM municipality
),
PercentPopulation AS (
    SELECT CEIL(total_municipalities * 0.05) AS five_percent_municipalities FROM Total_municipalities
)
SELECT 
    name, 
    population
FROM 
    municipality
ORDER BY 
    population DESC
LIMIT (SELECT five_percent_municipalities FROM PercentPopulation)
);

-- 3B
CREATE MATERIALIZED VIEW cases_population AS (
SELECT f.name
FROM Five_percent_population f 
JOIN Sixty_percent_cases s ON f.name = s.name
);

WITH Total_municipalities AS (
    SELECT COUNT(*) AS total_municipalities FROM municipality
)
SELECT 
    COUNT(*) * 100.0 / (SELECT total_municipalities FROM Total_municipalities) AS proportion
FROM 
    cases_population;

-- 3C
CREATE MATERIALIZED VIEW municipality_numeric AS (
    SELECT 
        m.name, 
        COALESCE(CAST(NULLIF(m.numberofhousesperkm2, 'NA') AS numeric), 0) AS houseskm2,
        COALESCE(CAST(NULLIF(m.numberofhospitalsperkm2, 'NA') AS numeric), 0) AS hospitalskm2
    FROM 
        municipality m
);

WITH avg_houses AS (
    SELECT AVG(houseskm2) AS avg_houses FROM municipality_numeric
)
SELECT 
    m.name,
    CASE
        WHEN (SELECT avg_houses FROM avg_houses) < m.houseskm2 THEN TRUE
        ELSE FALSE
    END AS higher_than_avg
FROM 
    Sixty_percent_cases spc 
    JOIN municipality_numeric m ON spc.name = m.name;

-- 3D
CREATE MATERIALIZED VIEW Case_density AS (
WITH Total_municipalities AS (
    SELECT COUNT(*) AS total_municipalities FROM municipality
),
PercentPopulation AS (
    SELECT CEIL(total_municipalities * 0.05) AS five_percent_municipalities FROM Total_municipalities
),
Cases_by_municipalities_last_year AS (
    SELECT 
        m.name, 
        SUM(casos) * 100000 / AVG(population) AS cases_density
    FROM 
        cases c 
        JOIN municipality m ON c.municipality_id = m.id 
        JOIN date d ON d.id = c.date_id
    WHERE 
        d.year_actual = (SELECT MAX(year_actual) FROM date JOIN cases ON date.id = cases.date_id)
    GROUP BY 
        m.name
)
SELECT 
    name, 
    cases_density
FROM 
    Cases_by_municipalities_last_year
ORDER BY 
    cases_density DESC
LIMIT (SELECT five_percent_municipalities FROM PercentPopulation)
);


