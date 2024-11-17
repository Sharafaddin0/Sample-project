-- Data Analysis
-- Normally when we start to work on EDA project we should have some idea what we are looking for. But we just gonna explore it because
-- we don't have any specific thing to look at

-- Let's start with 'total_laid_off' and 'percentage_laid_off' columns
-- Here 'percentage_laid_off' is not super helpful bcs we don't know how large is company or how many employees work there.
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM world_layoffs.layoffs_staging2;

-- Let's say we are interested in to recognize if there is any company was laid off 100%
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC; -- to see which one went under the most (check the most funding in millions)

-- Find the 'company' with the sum of the most total laid off
SELECT company, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10; 

-- by 'location'
SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- Check as well as 'industry' (which industry hit the most layoffs during the given date range)
SELECT industry, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC; -- 'consumer' sector is the result

-- Let's see the 'date' ranges as well
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2; -- date range lies duration of post Covid-19


-- this it total in the past 3 years or in the dataset

SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;

SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;





 






