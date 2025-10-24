use world_layoffs;

-- remove duplicates if any 
-- standardize data 
-- null values or blank values 
-- remove any columns 

CREATE TABLE layoffs_staging LIKE layoffs;

INSERT layoffs_staging 
SELECT * from layoffs;

select * from layoffs_staging;


-- remove duplicates 


WITH duplicates as (
select *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)

select * from duplicates where row_num >1;

DELETE from duplicates 
where row_num>1;



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
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
select *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

select * from layoffs_staging2
WHERE row_num >1;


SET SQL_SAFE_UPDATES = 0;
DELETE FROM layoffs_staging2 WHERE row_num > 1;
SET SQL_SAFE_UPDATES = 1;


-- Standardize data -- finding issues in data and fixing it 
 
SELECT company FROM layoffs_staging2; -- takes white space off 

SET SQL_SAFE_UPDATES = 0;

UPDATE layoffs_staging2
SET company = TRIM(company);

SET SQL_SAFE_UPDATES = 1;

SELECT distinct industry from layoffs_staging2;

SET SQL_SAFE_UPDATES = 0;
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
SET SQL_SAFE_UPDATES = 1;

select distinct country from layoffs_staging2
order by 1;

SET SQL_SAFE_UPDATES = 0;
UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';
SET SQL_SAFE_UPDATES = 1;



select `date`
from layoffs_staging2;


SET SQL_SAFE_UPDATES = 0;
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
SET SQL_SAFE_UPDATES = 1;


ALTER TABLE layoffs_staging2
modify column `date` DATE;

-- null and blank values 
select * from layoffs_staging2 
where total_laid_off is null
and percentage_laid_off is null;


select distinct industry from layoffs_staging2
where industry is null or industry = ' '; 


SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
  AND t2.industry IS NOT NULL;







