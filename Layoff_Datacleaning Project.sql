-- SQL Project - Data Cleaning

USE world_layoffs;
SELECT * FROM layoffs;

-- First, we want to create a staging table. 
-- This table will be used to work on and clean the data while keeping a 
-- table with the raw data intact, in case something goes wrong.
-- creating a copy of layoffs table named 'layoffs_staging' 
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

INSERT layoffs_staging
SELECT * FROM layoffs;

-- -- During data cleaning, we typically follow these steps:
-- 1. Identify and remove duplicates.
-- 2. Standardize the data and correct any errors.
-- 3. Assess and address null values.
-- 4. Eliminate any unnecessary columns and rows in various ways

-- --  1. Removing Duplicates
-- the ROW_NUMBER() function assigns a unique sequential integer to rows within a result set, starting at 1 for the first row in each partition 
-- checking for duplicate rows where row_num > 1
WITH duplicate_cte as (
SELECT *,
row_number() over(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

-- Creating a dupe of `layoffs_staging` inorder to remove the dupe rows 
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
row_number() over(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
FROM layoffs_staging;

SELECT * FROM layoffs_staging2
WHERE row_num > 1;
SET SQL_SAFE_UPDATES = 0;

DELETE FROM layoffs_staging2 
WHERE row_num > 1;

-- 2. Standardizing Data (Standardization means converting data into a consistent 
-- format or structure across the database to ensure accuracy and compatibility.)

-- removing trailing spaces in 'company' column
SELECT TRIM(company) from layoffs_staging2;

UPDATE layoffs_staging2 
SET company = TRIM(company);

-- updating correct spellings in 'location' column
UPDATE layoffs_staging2
SET location = REPLACE(
               REPLACE(
                   REPLACE(location, 'FlorianÃ³polis', 'Florianópolis'),
                   'MalmÃ¶', 'Malmö'),
               'DÃ¼sseldorf', 'Düsseldorf')
WHERE location IN ('FlorianÃ³polis', 'MalmÃ¶', 'DÃ¼sseldorf');

-- updating similar looking industries with a common name in 'industry' column
SELECT DISTINCT industry from layoffs_staging2
ORDER BY 1;
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE INDUSTRY LIKE 'Crypto%';

 -- removing trailing special characters in 'country' column
SELECT DISTINCT country from layoffs_staging2
ORDER BY 1;
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country like 'United States%';

-- updating the correct date format  (carefull &  check)
-- Stores date values in the format YYYY-MM-DD
SELECT `date`, 
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT DATE from layoffs_staging2;

-- Next change the data type of `date` column(text) to DATE data type
ALTER TABLE layoffs_staging2 -- DDL
MODIFY COLUMN `DATE` DATE; -- DML

-- 3. Null/Blank Values
 
 -- checking of NULL and empty strings values across table
SELECT * from layoffs_staging2;

SELECT * from layoffs_staging2
where company is null or company = '';

SELECT * from layoffs_staging2
where location is null or location = '';

SELECT * FROM layoffs_staging2 
WHERE industry IS NULL OR industry = ''; -- null & blanks

SELECT * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null; -- 348 rows (check for 2 columns combined )

SELECT * from layoffs_staging2
where date is null or ''; -- 1

SELECT * from layoffs_staging2
where stage is null or stage = ''; -- 6

SELECT * from layoffs_staging2
where country is null or country = '';

SELECT * from layoffs_staging2
where funds_raised_millions is null or funds_raised_millions = ''; -- 213 -- null & blanks
-- The `NULL` values in `total_laid_off`, `percentage_laid_off`, and `funds_raised_millions` 
-- seem acceptable as they are. I prefer keeping them as `NULL` because it simplifies 
-- calculations during the EDA phase.

-- now let's treat the null values in INDUSTRY based on Company

SELECT * FROM layoffs_staging2
WHERE company LIKE 'Bally%'; -- nothing wrong here

SELECT * from layoffs_staging2
where company like 'airbnb%';
-- it looks like airbnb is a travel, but this one just isn't populated.
-- Certainly it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- Let's set the blanks to nulls since those are typically easier to work with
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ''; -- now if we check blanks are all null

-- Now populate nulls
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT * FROM layoffs_staging2
WHERE industry like 'bally%'; -- and if we check it looks like Bally's was the only one 
-- without a populated row to populate this null values

-- 4. Remove any columns and rows wherever required
-- I deleted all rows where both total_laid_off and percentage_laid_off are null

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete 
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

select count(*) from layoffs_staging2;

select user();credit_card




-- After everything, now we have 1991 records from 2361 records of dataset