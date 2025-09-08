select
  keyword,
  topic,
  category,
  DAY_POST_CREATED_AT,
  max(MONTH_POST_CREATED_AT) as month_post_created_at,
  max(YEAR_POST_CREATED_AT) as year_post_created_at,
  COUNT(author_thread_mentions) daily_author_thread_mentions,
  SUM(total_posts_with_keyword) as daily_total_posts_keyword,
  SUM(positive_mentions) AS daily_positive_mentions,
  SUM(negative_mentions) AS daily_negative_mentions,
  SUM(neutral_mentions) AS daily_neutral_mentions,
  AVG(pct_positive) as daily_pct_positive,
 AVG(pct_negative) as daily_pct_negative,
 AVG(pct_neutral) as daily_pct_neutral,
from {SF_DB}.{SF_SC}.timeseries_nlp
group by 
keyword
,topic
,category
,day_post_created_at
order by month_post_created_at,day_post_created_at asc;