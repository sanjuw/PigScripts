/* Load Data : 2 Tweet files, 2 Dictionaries : positive words, and negative words */
A1 = LOAD 'tweets/tweets_20121102.txt' using PigStorage('|') AS (timestamp:chararray, userid:bytearray, username:chararray, user:chararray, f5, f6, f7, f8, f9, tweet:chararray);
A2 = LOAD 'tweets/tweets_20121103.txt' using PigStorage('|') AS (timestamp:chararray, userid:bytearray, username:chararray, user:chararray, f5, f6, f7, f8, f9, tweet:chararray);
A = UNION A1, A2;

goodDict = LOAD 'tweets/good.txt' using TextLoader() AS (goodword:chararray);
badDict = LOAD 'tweets/bad.txt' using TextLoader() AS (badword:chararray);

/* Create unique ID for each tweet */
rankedTweets = RANK A;

/* Tokenize */
B = foreach rankedTweets generate rank_A, REPLACE( tweet, '([^a-zA-Z0-9\\s\']+)', ' ' ) as tweet;
tweet_words = foreach B generate rank_A as id, FLATTEN(TOKENIZE(LOWER(TRIM(tweet)))) AS t;

/* Generate positive or negative score for each word in dictionary. Combine to form one dictionary dict */
pos = foreach goodDict generate goodword as word, 1 as score: int;
neg = foreach badDict generate badword as word, -1 as score: int;
dict = UNION pos, neg;

/* Join each tweet word with the positive and negative dictionary (dict) to obtain score */
twords_score = JOIN tweet_words BY t , dict BY word USING 'REPLICATED';

/* Group tweet words by ID, and sum their sentiment scores */
grouped = GROUP twords_score by id;
tweetSentiment = foreach grouped GENERATE group, SUM(twords_score.score) as sent_score;

/* Find positive tweets - tweets with total sentiment score greater than 0 */
posTweets = filter tweetSentiment by sent_score>0;
/* Find negative tweets - tweets with total sentiment score less than 0 */
negTweets = filter tweetSentiment by sent_score<0;

/* Group postive and negative tweets separately, and then obtain the count */
groupPos = group posTweets all;
groupNeg = group negTweets all;

totalPos = foreach groupPos generate 1, COUNT(posTweets.sent_score) as posScore;
totalNeg = foreach groupNeg generate -1, COUNT(negTweets.sent_score) as negScore;

/* Combine outcome into one output and store output */
tweetOut = UNION totalPos, totalNeg;
store tweetOut into 'Wanchoo_tweets' using PigStorage();
