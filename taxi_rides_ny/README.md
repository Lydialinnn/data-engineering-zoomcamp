Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

Question 7:
SELECT * FROM `dulcet-record-450703-g3.dbt_lydialinnn.fct_fhv_monthly_zone_traveltime_p90` 
where pickup_zone = 'Yorkville East'  -- Newark Airport, SoHo
order by p90_trip_duration desc
LIMIT 1000
