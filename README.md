# AI_reviewer
This repository implements an automated pipeline for screening and extracting data from scientific publications using multiple AI reviewers.

Input data requirements:
Each paper should occupy a single row. The following column headings are mandatory but can be blank if data are not available: Year, firstAuthor, title, Abstract, Journal, DOI, Affiliations, Funding.Details.

Preprocessing
1. Import papers into a tabular dataset such as excel.  
2. Apply rule based preprocessing such as removing duplicates, filtering papers based on their listed article type and their year of publication.  
3. Add additional metadata as required, such as Citescores from Elsevier and reference counts from Crossref.  
4. Save as Dataset 1.xlsx  
  
Screening pipeline  
1. Run "AI reviewer screening.R" which allocates a score to each paper in each row and saves it as an excel document "Dataset 2.xlsx".  
2. Manually resolve all entries with score = 3.  
3. Run "Filter by score.R" which filters out scores that are not 4 or 5 and saves the table as "Dataset 3.xlsx".  

AI data extraction  
1. Run "AI reviewer 1.R", "AI reviewer 2.R" and "AI reviewer 3.R". The order does not matter. Each takes "Dataset 3.xlsx" as input and outputs "Dataset 2.xlsx", "Dataset 5.xlsx" and "Dataset 6.xlsx" respectively.  
2. Run "Combine complete AI extractions.R" which outputs "Dataset 7.xlsx"  

Final output  
The final data in "Dataset 7.xlsx" contains the screened and extracted dataset. This can then be analysed with standard methods.  
