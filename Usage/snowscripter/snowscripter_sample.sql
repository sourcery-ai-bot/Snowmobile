--snowscripter_sample.sql

/*-contrived_example_aggregation-*/
create or replace temp table index_agg as with
contrived_1 as (
	select
	  index
		,sum(col1) as col1
	from SAMPLE_TABLE
	group by 1
),
contrived_2 as (
	select
	  index
		,sum(col1) as col2
	from SAMPLE_TABLE
	group by 1
)
	select
		a.*
		,b.col2
	from contrived_1 a
	inner join contrived_2 b
		on a.index = b.index;

/*-verify_contrived_join-*/
SELECT
  index
	,count(*)
		as cnt_all
FROM index_agg
group by 1
having count(*) <> 1;