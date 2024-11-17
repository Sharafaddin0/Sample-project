-- Data Cleaning 
-- This stage of Data Analysis involves removing duplicates, handling null values, standardizing formats, correcting errors, converting data types, and filtering outliers. 
-- These steps ensure clean and consistent data for analysis. 

-- Let's take a quick look on this dataset
SELECT *
FROM world_layoffs.layoffs;

-- Step 1: Remove duplicates
-- Step 2: Standardize data
-- Step 3: Null values or blank values
-- Step 4: Remove any unnecessary rows or columns

-- Create new staging table of given raw dataset
CREATE TABLE layoffs_staging
LIKE world_layoffs.layoffs; -- copies all data from layoffs to staging

SELECT *
FROM world_layoffs.layoffs_staging; -- we can read the created staging table

-- Now insert all the data layoffs
INSERT world_layoffs.layoffs_staging
SELECT *
FROM world_layoffs.layoffs;  -- Why we did like this? Because we will work on layoffs_staging dataset a lot and we don't want to lose raw data.

-- ***************************************************************************************************************************************************--
-- Start with the Step 1: Get rid of duplicates
-- ***************************************************************************************************************************************************--


	-- For this we look at the columns where most probably can be dupliacates and partiton by on them
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
	FROM layoffs_staging; -- after execution most row numbers are unique(row_num = 1).If we see row_num >= 2, it means duplicates


	-- So we create CTE for dealing with this problem, we have to select only row_num >= 2
	WITH duplicate_CTE AS
	(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
	FROM layoffs_staging
	)
	SELECT *
	FROM duplicate_CTE
	WHERE row_num > 1;

	-- Let's look at final layoffs_staging data. And choose one company (for example, Oda company)
	SELECT *
	FROM layoffs_staging
	WHERE company = 'Oda'; -- we found we have to add other columns as well to be PARTITION BY

	-- Delete duplicates
	WITH duplicate_CTE AS
	(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
	FROM layoffs_staging
	)
	DELETE
	FROM duplicate_CTE
	WHERE row_num > 1; -- we will get error :)

	-- To solve this we create another table, and delete duplicates where row number is equal to 2
	-- Copy layoffs_staging to clipboard under CREATE statement
	CREATE TABLE `layoffs_staging2` (
	  `company` text,
	  `location` text,
	  `industry` text,
	  `total_laid_off` int DEFAULT NULL,
	  `percentage_laid_off` text,
	  `date` text,
	  `stage` text,
	  `country` text,
	  `funds_raised_millions` int DEFAULT NULL,
	  `row_num` INT -- add row_num column 
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

	-- Insert all the data inside new table layoffs_staging2
	INSERT INTO world_layoffs.layoffs_staging2
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
	FROM layoffs_staging;

	-- Now select all from new created table (layoffs_staging2) where row numbers are not unique
	SELECT *
	FROM world_layoffs.layoffs_staging2
	WHERE row_num > 1;

	-- Delete duplicates
	DELETE
	FROM world_layoffs.layoffs_staging2
	WHERE row_num > 1; -- Execute it again and we will see duplicates are deleted

	-- Execute it again and we will see duplicates are deleted and row numbers are unique
	SELECT *
	FROM layoffs_staging2;

-- ***************************************************************************************************************************************************--
-- Next one is Step 2: Standardize Data
-- ***************************************************************************************************************************************************--
	
    -- Let's check 'company' column and we see that there are some blank spaces in the beginning
    -- To solve this we use TRIM() function
	SELECT company, TRIM(company)
    FROM layoffs_staging2;
	
    -- |Then we need to update the company names
    UPDATE layoffs_staging2
    SET company = TRIM(company); -- execute it to apply changes 
    
    -- Now check 'industry' column and try to find blank industries 
    -- To solve it we can use ORDER BY (to see other problems as well)
    SELECT DISTINCT industry
    FROM layoffs_staging2
    ORDER BY 1;
    
    -- Found industry 'Crypto' several times 
    SELECT *
    FROM layoffs_staging2
    WHERE industry LIKE 'Crypto%';
    
    -- Update all the rows to be industry = 'Crypto'
    UPDATE layoffs_staging2
    SET industry = 'Crypto'
    WHERE industry LIKE 'Crypto%';
    
    -- Check 'location' column as well
    SELECT DISTINCT location
    FROM layoffs_staging2
    ORDER BY 1; -- we could not find any dirty data
    
	-- Check 'country' column
    -- We see there is a problem on name 'United States.'
	SELECT DISTINCT country
    FROM layoffs_staging2
    ORDER BY 1;
	
    -- Show all where country name is like United States
    -- Update country name
    UPDATE layoffs_staging2
    SET country =  TRIM(TRAILING '.' FROM country)
    WHERE country LIKE 'United States%'; -- there will be one row named US
    
    -- Check 'date' column. We see that it is 'TEXT' format but it should be 'DATEFRAME' format
    -- To solve it we use str_to_date() funcvtion
    SELECT `date`, 
    str_to_date(`date`, '%m/%d/%Y') -- it changes the format
    FROM layoffs_staging2;

	-- Update the date column
    UPDATE layoffs_staging2
    SET `date`= str_to_date(`date`, '%m/%d/%Y');
    
    -- Change the format
    ALTER TABLE layoffs_staging2
    MODIFY COLUMN `date` DATE;

-- ***************************************************************************************************************************************************--
-- Next one is Step 3: NULL or blank values
-- ***************************************************************************************************************************************************--
    
    -- After execution of all columns, first we see 'total_laid_off' column that contains NULL values
    SELECT *
    FROM layoffs_staging2
    WHERE total_laid_off is NULL
    AND percentage_laid_off is NULL; -- there are some rows that contain both columns are NULL
    
    -- There were some 'industry' columns that were NULL or blank
    SELECT *
    FROM layoffs_staging2
	WHERE industry is NULL 
    OR industry = '';
    
    -- Change industry to NULL where it is blank
    UPDATE layoffs_staging2
    SET industry = NULL
    WHERE industry = '';
    
    -- We check the company named 'Airbnb'
    SELECT *
    FROM layoffs_staging2
    WHERE company = 'Airbnb';
	
    -- Need to do JOIN statement to find blank or Null industries
    SELECT t1.industry, t2.industry
    FROM layoffs_staging2 AS t1
    JOIN layoffs_staging2 AS t2
		ON t1.company = t2.company
        AND t1.location = t2.location -- same company can be in different location
    WHERE (t1.industry is NULL OR t1.industry = '')
    AND t2.industry IS NOT NULL; 
    
    -- Update all
    UPDATE layoffs_staging2 AS t1
    JOIN layoffs_staging2 AS t2
		ON t1.company = t2.company
	SET t1.industry = t2.industry
    WHERE t1.industry is NULL
    AND t2.industry IS NOT NULL; -- execute it and we will see there is no any blank or NULL values in the company 'Airbnb' and others
    
    -- Check others where industry contains NULL values
	SELECT *
	FROM layoffs_staging2
    WHERE industry is null; -- we find 'Bally's Interactive' left
    
    -- Delete these rows 
    DELETE
	FROM layoffs_staging2
    WHERE company LIKE 'Bally%'
    AND industry is null; -- as a result, there is no row left 'industry' is NUll or blank values
    
    -- Delete the rows where total_laid_off and percentage_laid_off columns contain NULL values
    DELETE
    FROM layoffs_staging2
    WHERE total_laid_off is NULL
    AND percentage_laid_off is NULL; 
    
    ALTER TABLE layoffs_staging2
    DROP COLUMN row_num;
    
 -- ***************************************************************************************************************************************************--
-- At the end, check the modified dataset 
-- ***************************************************************************************************************************************************--
   
   SELECT *
   FROM layoffs_staging2;
    
    
    

	
	


