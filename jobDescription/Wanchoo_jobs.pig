/* Load all the data sources: Job Data, Stopwords data, and Dictionary */
jobdata = LOAD 'pig/job_sample.csv' using PigStorage(',') as (id:chararray, line:chararray);
stopwords = LOAD 'pig/stopwords.txt' using TextLoader() as (sword:chararray);
dict = LOAD 'pig/dictionary.txt' using PigStorage() AS (dictword:chararray);

/* Keep only alphanumeric elements in the job data description */
jobdata_alpha = FOREACH jobdata GENERATE id, REPLACE(line, '[^A-Za-z0-9-\']', ' ') AS line;

/* Tokenize after trimming whitespaces and changing word to lower case */
token = FOREACH jobdata_alpha GENERATE id, FLATTEN(TOKENIZE(LOWER(TRIM(line)))) AS word;

/* Remove Stop words */
J = JOIN token BY word LEFT OUTER, stopwords BY sword using 'replicated';
clean1 = FILTER J BY stopwords::sword IS NULL;

/* 
Join the job description word list with the dictionary. Initially, this step was carried out
after stemming. However, the early stemming led to more misspelled words, so this approach was used. 
First, the word list was joined with the dictionary to create the list of correct and misspelled words.
Then, only the misspelled words were stemmmed.
*/
J2 = JOIN clean1 BY token::word LEFT OUTER, dict BY dictword using 'replicated';

correctSpelled = FILTER J2 BY dict::dictword IS NOT NULL;
correctSpelled2 = FOREACH correctSpelled generate clean1::token::id as id, clean1::token::word AS correctWord;
misspelled = FILTER J2 BY dict::dictword IS NULL;

/* Stem the misspelled words in each job description */
REGISTER '/home/huser88/pig/UDF/UDF_exercise1/STEM.jar'
stemmed = FOREACH misspelled GENERATE clean1::token::id AS id, STEM(clean1::token::word) AS stem, SUBSTRING(clean1::token::word,0,1) AS firstCh;

/*
For each dictionary word, generate a new column called firstDictCh which contains the first character of each word.
Save this as newdict. This is used to join with the list of misspelled words on only the first alphabet. This reduces
the number of dictionary words against which each misspelled word's levenshtein distance will be calculated. 
*/
newdict = FOREACH dict GENERATE dictword, SUBSTRING(dictword,0,1) AS firstDictCh;

REGISTER '/home/huser88/pig/UDF/UDF_Levenshtein/LEVENSHTEIN_DIST.jar';

/* 
Obtain a list of uniquely misspelled words from the original list of words.
This also helps reduce the number of levenshtein calculations by decreasing the number of misspelled words.
*/
x = FOREACH stemmed generate stem as stem;
uniqWords1 = DISTINCT x;

uniqWords2 = FOREACH uniqWords1 GENERATE stem AS stem, SUBSTRING(stem,0,1) AS firstCh;
J3 = JOIN uniqWords2 BY firstCh, newdict BY firstDictCh using 'replicated';

/* Calculate Levenshtein Distance */
distance = FOREACH J3 generate uniqWords2::stem AS stem, newdict::dictword AS dictword, LEVENSHTEIN_DIST(uniqWords2::stem, newdict::dictword) AS levDist: int;

grouped1 = group distance by stem;

/* Calculate the minimum distance for each word */
minDist = FOREACH grouped1 {
	ordered1 = ORDER distance BY levDist ASC;
	ordered2 = LIMIT ordered1 1;
	generate ordered2;
};

minDist2 = FOREACH minDist GENERATE FLATTEN(ordered2.$0) AS stem, FLATTEN(ordered2.$1) AS dictword, FLATTEN(ordered2.$2) as distance;

/* Join corrected word to the original id,misspelled word pair */
J4 = JOIN stemmed BY stem, minDist2 BY stem using 'replicated';

J4Filter = FOREACH J4 GENERATE stemmed::id as id, minDist2::dictword as correctWord;

/* Put correct and incorrect words together, and group them by Job ID */
final = UNION correctSpelled2, J4Filter;
finalGrouped = group final BY id; 

/* Print final Output */
finalOutput = FOREACH finalGrouped GENERATE group, final.$1;
store finalOutput into 'Wanchoo_jobs';
