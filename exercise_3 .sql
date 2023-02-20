-- Project 3 Instructions: Write an Event-Based Query That Will Scale with Business Growth
-- The Virtual Kitchen developers are making some changes to the search functionality on the website. After gathering customer feedback, they want to change the recipe suggestion algorithm in order to improve the customer experience.

-- We have a beta version of the website available and have opened it for use by a small number of customers. Next week we plan to increase this number from 200 customers to 5,000. To ensure everything is ready for the test, we have implemented logging and are saving results to a table in Snowflake called vk_data.events.website_activity.
-- The table contains: 
-- event_id: A unique identifier for the user action on the website
-- session_id: The identifier for the user session
-- user_id: The identifier for the logged-in user
-- event_timestamp: Time of the event
-- event_details: Details about the event in JSON — what action was performed by the user?
-- Once we expand the beta version, we expect the website_activity table to grow very quickly. While it is still fairly small, we need to develop a query to measure the impact of the changes to our search algorithm. 

------------Please create a query and review the query profile to ensure that the query will be efficient once the activity increases.*****************
-- We want to create a daily report to track:
-- Total unique sessions
-- The average length of sessions in seconds
-- The average number of searches completed before displaying a recipe 
-- The ID of the recipe that was most viewed 
-- In addition to your query, please submit a short description of what you determined from the query profile and how you structured your query to plan for a higher volume of events once the website traffic increases.


--check data
	--describe table VK_DATA.EVENTS.WEBSITE_ACTIVITY;
	--select * from VK_DATA.EVENTS.WEBSITE_ACTIVITY limit 1000 ;

-- dedup data with group by, gives unique value per row 
with event_data as (
	select 
    	event_id,
        session_id,
        event_timestamp,
        trim(parse_json(event_details):"recipe_id", '"') as recipe_id,
        trim(parse_json(event_details):"event", '"') as event_type
    from 
    	VK_DATA.EVENTS.WEBSITE_ACTIVITY
    group by 1,2,3,4,5
)
--select * from events
,group_event_sessions as (
	select 
		session_id,
        max(event_timestamp) as max_event_timestamp,
        min(event_timestamp) as min_event_timestamp,
        iff(count_if(event_type = 'view_recipe') = 0,null,
        	round(count_if(event_type = 'search')/ count_if(event_type = 'view_recipe'))) as search_per_recipe_view
	from 
    	event_data 
	group by session_id
)
--select * from group_event_sessions
,fav_recipe as (
	select 
    	date(event_timestamp) as event_day,
    	recipe_id,
        count(*) as total_views
    from event_data
    where recipe_id is not null 
	group by event_day,recipe_id
    qualify row_number() over (partition by event_day order by total_views desc) = 1

)
--select * from fav_recipe
,final_table as (
	select 
    	date(min_event_timestamp) as event_day,
        count(session_id) as total_sessions,
        round(avg(datediff('sec' , min_event_timestamp , max_event_timestamp))) as avg_session_time_secs,
        max(search_per_recipe_view) as avg_search_per_recipe_view
     --   max(recipe_name) as favorite_recipe only add if you want names instead of IDs
    from group_event_sessions
    inner join fav_recipe on date(group_event_sessions.min_event_timestamp) = fav_recipe.event_day
    inner join vk_data.chefs.recipe using(recipe_id) --dont need this join 
    group by 1
)
select * from final_table
