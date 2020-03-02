/* https://medium.com/@jeffrey.james/text-classification-using-bigquery-ml-and-ml-ngrams-6e365f0b5505 */

WITH data AS  (
  SELECT REGEXP_EXTRACT_ALL(LOWER(nps_answer_comment), '[a-z]+') as title_arr
  from {{ref('stg_backend__surveys__nps_answers')}}
  where nps_answer_score < 7
)

SELECT APPROX_TOP_COUNT(ngram, 100) top
FROM (
  SELECT ML.NGRAMS(title_arr, [3,3]) x
  FROM data
), UNNEST(x) ngram
-- WHERE LENGTH(ngram) > 10
where ngram like 'nicht%'
