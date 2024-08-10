-- Data Cleaning.( Dataset downloaded from Kaggle)
#view data in the layoffs table
select * from layoffs;

-- 1. remove duplicate
-- 2. standardize data
-- 3. Null values
-- 4. Remove unnessary columns/Any columns

	CREATE TABLE layoffs_staging #create temporary table
    LIKE layoffs;
    
    SELECT * FROM layoffs_staging; #To view the temporary table
    
    INSERT layoffs_staging #insert data into temporary table
    SELECT *
    FROM layoffs;
    
     SELECT *,
    ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging;
    
    WITH duplicate_cte AS #view duplicates
    (
     SELECT *,
    ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
    )
    SELECT *
    FROM duplicate_cte
    WHERE row_num > 1;
    
    # Create a second temporary table to delete duplicates
    CREATE TABLE `layoffs_staging_2` (
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

SELECT * FROM layoffs_staging_2;

INSERT INTO layoffs_staging_2 #insert data into second temp table
SELECT *,
    ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging;
    
	SELECT *
    FROM layoffs_staging_2
    WHERE row_num > 1;
    
	DELETE  # delete duplicates
    FROM layoffs_staging_2
    WHERE row_num > 1;
    
    SELECT *
    FROM layoffs_staging_2;
    
    -- 2. standardizing data
    SELECT company, TRIM(company)
    FROM layoffs_staging_2;
    
    UPDATE layoffs_staging_2
    SET company = TRIM(company);
    
	SELECT DISTINCT industry
    FROM layoffs_staging_2
    ORDER BY 1;
    
    UPDATE layoffs_staging_2 #updating industry
    SET industry = 'Crypto'
    WHERE industry LIKE 'Crypto%';
    
    SELECT DISTINCT industry
    FROM layoffs_staging_2;
    
    SELECT DISTINCT location
    FROM layoffs_staging_2
    ORDER BY 1;
    
	SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
    FROM layoffs_staging_2
    ORDER BY 1;
    
    UPDATE layoffs_staging_2 #getting rid of duplicate US
    SET country = TRIM(TRAILING '.' FROM country)
    WHERE country LIKE 'United States%';
    
     SELECT *
    FROM layoffs_staging_2;
    
    SELECT `date`,
    STR_TO_DATE(`date`, '%m/%d/%Y')
    FROM layoffs_staging_2;
    
    UPDATE layoffs_staging_2 #changing date format
    SET `date` =  STR_TO_DATE(`date`, '%m/%d/%Y');
    
	SELECT `date`
	FROM layoffs_staging_2;
    
    ALTER TABLE layoffs_staging_2 #changing date data type from text to date
    MODIFY COLUMN `date` DATE;
    
    SELECT *
    FROM layoffs_staging_2;
    
    -- Removing NULLS and blanks
    
    SELECT *
    FROM layoffs_staging_2 
    WHERE industry IS NULL
    OR industry = '';
    
    SELECT *
    FROM layoffs_staging_2 
    WHERE company = 'Airbnb';
    
    SELECT *
    FROM layoffs_staging_2 t1
    JOIN layoffs_staging_2 t2
		ON t1.company = t2.company
	WHERE (t1.industry IS NULL OR t1.industry = '')
    AND t2.industry IS NOT NULL;
    
    UPDATE layoffs_staging_2 t1
	JOIN layoffs_staging_2 t2
		ON t1.company = t2.company
	SET t1.industry = t2.industry
    WHERE t1.industry IS NULL 
    AND t2.industry IS NOT NULL;
    
    UPDATE layoffs_staging_2
    SET industry = NULL
    WHERE industry = '';
    
    UPDATE layoffs_staging_2 t1
	JOIN layoffs_staging_2 t2
		ON t1.company = t2.company
	SET t1.industry = t2.industry
    WHERE (t1.industry IS NULL OR t1.industry = '')
    AND t2.industry IS NOT NULL;
    
    SELECT *
    FROM layoffs_staging_2;
    
    SELECT * #remove null columns
    FROM layoffs_staging_2
    WHERE total_laid_off IS NULL
    AND percentage_laid_off IS NULL;
    
    DELETE
    FROM layoffs_staging_2
    WHERE total_laid_off IS NULL
    AND percentage_laid_off IS NULL;
    
	SELECT * 
    FROM layoffs_staging_2;
    
    ALTER TABLE  layoffs_staging_2 #drop the temporary column row_num
    DROP COLUMN row_num;
    
    
    
    
    
    

    
    
    
    
    
    
    
