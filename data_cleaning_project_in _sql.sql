select company, industry from world_layoffs.layoffs
order by 1,2;
select * from layoffs;
/*1. Remove Duplicates--- done
2. Standardize the Data
3. Null values or blanck values 
4. Remove Any columns
*/
create table layoffs_practice
like layoffs;
select * from layoffs_practice;
insert layoffs_practice 
select * from layoffs;
 -- For practicing raw data to create new table
 -- FIND Dulicate values 
 select * ,
 Row_Number() OVER ( PARTITION BY Company, location,industry,total_laid_off,percentage_laid_off,'date',
 stage,country,funds_raised_millions) as  ROW_NUM  from layoffs_practice;
 
 with dupilcate_cte AS (select * ,
 Row_Number() OVER 
 ( PARTITION BY Company, location,industry,total_laid_off,percentage_laid_off,'date',
 stage,country,funds_raised_millions) 
 as  ROW_NUM  from layoffs_practice
 
 ) delete from dupilcate_cte 
 where row_num > 1;
 
 select * from 
 layoffs_practice
 where company ='casper';
 -- delete dublicates 
with dupilcate_cte AS (select * ,
 Row_Number() OVER 
 ( PARTITION BY Company, location,industry,total_laid_off,percentage_laid_off,'date',
 stage,country,funds_raised_millions) 
 as  ROW_NUM  from layoffs_practice
 
 ) delete from dupilcate_cte 
 where row_num > 1;
 
 CREATE TABLE `layoffs_practice2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `Row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


 
 select * from layoffs_practice2
 order by 3;
 insert into layoffs_practice2
 select * ,
 Row_Number() OVER 
 ( PARTITION BY Company, location,industry,total_laid_off,percentage_laid_off,'date',
 stage,country,funds_raised_millions) 
 as  ROW_NUM  from layoffs_practice;
 -- dulicates deleted
 select * from  layoffs_practice2
 where company like "%airbnb%";
 
-- standardizing data
select  company,trim(company)  from layoffs_practice2
order by 1;
update layoffs_practice2
set company = trim(company);
update  layoffs_practice2
set industry =trim(industry);
select COUNTRY  from layoffs_practice2
where industry like "crypto%";
update layoffs_practice2  set industry = 'crypto'
where industry like 'crypto%';
select distinct industry from layoffs_practice2;
update layoffs_practice2
set country = trim( TRAILING '.' FROM COUNTRY)
WHERE COUNTRY LIKE 'COUNTRY%';
SELECT DATE , STR_TO_DATE (DATE,'%m/%d/%Y')
FROM layoffs_practice2;


update layoffs_practice2
set DATE = STR_TO_DATE (dATE,'%m/%d/%Y');
SELECT * FROM layoffs_practice2
ORDER BY 6;
-- alternation done to string to date type 
ALTER TABLE layoffs_practice2
MODIFY COLUMN Date DATE;
-- find the null values 
select * from layoffs_practice2
where total_laid_off is null
and percentage_laid_off is null;
select distinct * from  layoffs_practice2
where industry is null
or industry = '';

-- using  self join to combind self tables 
select p1.industry,p2.industry   from layoffs_practice2 p1
join layoffs_practice2 p2
on p1.company=p2.company
where (p1.industry is null or p1.industry ='')
and p2.industry is not null;

update layoffs_practice2  p1
join layoffs_practice2 p2
on p1.company=p2.company
set p1.industry = p2.industry
where p1.industry is null 
and p2.industry is not null;

update  layoffs_practice2
set industry = null
where industry = '';
select  * from layoffs_practice2
where industry = ''
or industry is null;

select * from layoffs_practice2
where total_laid_off is null
and percentage_laid_off is null;

delete  from layoffs_practice2
where total_laid_off is null
and percentage_laid_off is null;
select *from layoffs_practice2;
alter table layoffs_practice2
drop column row_num;


describe  layoffs_practice2 ;

select max(total_laid_off) , max(percentage_laid_off) , 
min(total_laid_off),min(percentage_laid_off)
from layoffs_practice2;

select distinct  *from layoffs_practice2
where percentage_laid_off =1
order by total_laid_off desc;

select distinct  company , sum(total_laid_off) from layoffs_practice2
group by company
order by 2 desc;
 select distinct company,industry, min(date),max(date) from  layoffs_practice2
 group by industry ,company ;
 -- year of total lid off
 select year(date),  sum(total_laid_off) from layoffs_practice2
 group by  year(date)
 order by 1 desc;
 
 select distinct stage,  sum(total_laid_off) from layoffs_practice2
 group by stage
 order by sum(total_laid_off)  desc;
 -- using substing and fetch the only month and year of total_laid_off
 select substring(date,1,7) as 'month', sum(total_laid_off) from layoffs_practice2
 where  substring(date,1,7) is not null
 group by substring(date,1,7) 
 order by 1,2 desc;
 
 -- using CTE method as rolling total each month encreased total amount
 select * from layoffs_practice2;
 
 with rolling_total as (
 select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off 
 from layoffs_practice2
 where  substring(`date`,1,7) is not null
 group by `month`
 order by 1 asc
 ) 
 select `month` , total_off , sum(total_off) over( order by `month`)  as rolling_total 
 from  rolling_Total;
 
 -- fetch all ranking companies with CTE
 
 with company_year (company,years,total_laid_off)  as(
 select company,year(`date`),sum(total_laid_off)
 from layoffs_practice2
 group by company,year(`date`)
 ) , company_years_rank as
 (select *, dense_rank() over (partition by years order by total_laid_off desc) ranking
 from company_year
 where years is not null
 order by ranking asc)
 select years,company,max(ranking) maxe ,total_laid_off from company_years_rank
 group by 1,2 
 order by maxe ,years  asc
 limit 10;
 
 
 
 
 
 
 

 
 












 

