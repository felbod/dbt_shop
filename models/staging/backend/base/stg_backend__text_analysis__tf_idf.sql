/* https://stackoverflow.com/questions/47028576/how-can-i-compute-tf-idf-with-sql-bigquery */

WITH words_by_post AS (
  SELECT nps_answer_id as id, REGEXP_EXTRACT_ALL(
       REGEXP_REPLACE(
       REGEXP_REPLACE(
       REGEXP_REPLACE(
         LOWER(nps_answer_comment)
         , r'&amp;', '&')
         , r'&[a-z]*;', '')
         , r'<[= \-:a-z0-9/\."]*>', '')
      , r'[a-z]{2,20}\'?[a-z]+') words
    , nps_answer_comment
  , COUNT(*) OVER() docs_n
  from {{ref('stg_backend__surveys__nps_answers')}}

), words_tf AS (
  SELECT id, words
  , ARRAY(
     SELECT AS STRUCT w word, COUNT(*)/ARRAY_LENGTH(words) tf
     FROM UNNEST(words) a
     JOIN (SELECT DISTINCT w FROM UNNEST(words) w) b
     ON a=b.w
     WHERE w NOT IN (
       'und', 'oder',
       'ich', 'es',
       'der', 'die', 'das',
       'the', 'and', 'for', 'this', 'that', 'can', 'but')
     GROUP BY word ORDER BY word
   ) tfs
   , ARRAY_LENGTH((words)) words_in_doc
   , docs_n
   , nps_answer_comment
  FROM words_by_post
  WHERE ARRAY_LENGTH(words)>20
), docs_idf AS (
  SELECT *, LOG(docs_n/docs_with_word) idf
  FROM (
    SELECT id, word, tf.tf, COUNTIF(word IN UNNEST(words)) OVER(PARTITION BY word) docs_with_word, docs_n
    , nps_answer_comment
    FROM words_tf, UNNEST(tfs) tf
  )
)


SELECT id, ARRAY_AGG(STRUCT(word, tf*idf AS tf_idf, docs_with_word) ORDER BY tf*idf DESC) tfidfs
#  , ANY_VALUE(title) title, ANY_VALUE(body) body # makes query slower
FROM docs_idf
WHERE docs_with_word > 1
GROUP BY 1
