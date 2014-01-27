-- Average CPU and DB usage by Domain
SELECT DOMAIN, AVG(CORE) Average_CPU_Usage, AVG(DB) Average_DB_Usage 
FROM WEB_STAT 
GROUP BY DOMAIN 
ORDER BY DOMAIN DESC;

-- Sum, Min and Max CPU usage by Salesforce grouped by day
SELECT TRUNC(DATE,'DAY') DAY, SUM(CORE) TOTAL_CPU_Usage, MIN(CORE) MIN_CPU_Usage, MAX(CORE) MAX_CPU_Usage 
FROM WEB_STAT 
WHERE DOMAIN LIKE 'Salesforce%' 
GROUP BY TRUNC(DATE,'DAY');

-- list host and total active users when core CPU usage is 10X greater than DB usage
SELECT HOST, SUM(ACTIVE_VISITOR) TOTAL_ACTIVE_VISITORS 
FROM WEB_STAT 
WHERE DB > (CORE * 10) 
GROUP BY HOST;
