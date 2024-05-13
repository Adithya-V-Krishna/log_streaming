import psycopg2
from datetime import datetime

# Establish connection to Redshift
conn = psycopg2.connect(
    dbname='dev',
    user='admin',
    password='Achu1234',
    host="log-streaming-data-wg.339713101961.ap-south-1.redshift-serverless.amazonaws.com",
    port='5439',
    sslmode='require'
)
cursor = conn.cursor()


#selection and agregation of data and insertion in into reel_stats_temp table
try:

    cursor.execute("""
    INSERT INTO dev.public.reel_stats_temp 
    SELECT 
        reel_id, 
        trunc(created_at) AS created_date, 
        extract(hour from created_at) AS hour, 
        count(*) AS num_of_plays, 
        sum(touch_point) AS touch_points, 
        sum(reel_spent_time) AS played_time,
        sum(CASE WHEN ad_shown THEN 1 ELSE 0 END) AS num_of_ads_shown,
        sum(CASE WHEN ad_skipped THEN 1 ELSE 0 END) AS num_of_ads_skipped
    FROM dev.public.social_media_log_v2 
    WHERE created_at >= (SELECT etl_look_up_time FROM constant)
    GROUP BY 1, 2, 3
""",)

    print("Sucessfully inserted into reel_stats_temp table ")    
    conn.commit()    

except Exception as e:
    print("Error:", e)
    conn.rollback()


#Selection and agregation of data and insertion in into user_stats_temp table    
try:
    cursor.execute("""
        INSERT INTO dev.public.user_stats_temp 
        SELECT 
            user_account_id as account_id, 
            trunc(created_at) AS created_date, 
            extract(hour from created_at) AS hour,
            count(distinct reel_id) as num_of_unique_reels
            count(reel_id) as num_of_reels    
            sum(reel_spent_time) AS spent_time,
        FROM dev.public.social_media_log_v2 
        WHERE created_at >= (SELECT etl_look_up_time FROM constant)
        GROUP BY 1, 2, 3
    """,)

    print("Sucessfully inserted into user_stats_temp table ")    
    conn.commit()  

except Exception as e:
    print("Error:", e)
    conn.rollback()    


#To insert/update the constant table
try: 
    cursor.execute("SELECT created_at from social_media_log_v2 ORDER BY created_at DESC LIMIT 1")
    latest_creation = cursor.fetchone()

    cursor.execute("BEGIN;")

    update_constant_query = """ UPDATE constant set etl_look_up_time = %s"""
    cursor.execute(update_constant_query, latest_creation)

    if cursor.rowcount == 0:
        insert_constant_query = """ INSERT INTO constant VALUES(%s)"""
        cursor.execute(insert_constant_query, latest_creation)


    print("Successfully updated the constant table")
    conn.commit()    

except Exception as e:
    print("Error:", e)
    conn.rollback()


#To delete from the reel_stats table    
try:
    cursor.execute("""DELETE FROM reel_stats using reel_stats_temp
                WHERE reel_stats.created_date = reel_stats_temp.created_date 
                and reel_stats.hour = reel_stats_temp.hour
                """)
    
    print("Successfully deleted the reel_stats table")
    conn.commit()   

except Exception as e:
    print("Error:", e)
    conn.rollback()

#To delete from the user_stats table    
try:
    cursor.execute("""DELETE FROM user_stats using user_stats_temp
                WHERE user_stats.created_date = user_stats_temp.created_date 
                and user_stats.hour = user_stats_temp.hour
                """)
    
    print("Successfully deleted the user_stats table")
    conn.commit()   

except Exception as e:
    print("Error:", e)
    conn.rollback()

#To insert into reel_stats table
try:
    cursor.execute("""
        INSERT INTO dev.public.reel_stats
        SELECT 
            reel_id, 
            created_date, 
            hour, 
            num_of_plays, 
            touch_points, 
            played_time,
            num_of_ads_shown,
            num_of_ads_skipped
        FROM dev.public.reel_stats_temp
    """,)

    print("Successfully inserted into the reel_stats table from reel_stats_temp")
    conn.commit()  
    
except Exception as e:
    print("Error:", e)
    conn.rollback()


#To insert into user_stats table
try:
    cursor.execute("""
        INSERT INTO dev.public.user_stats
        SELECT 
            account_id, 
            created_date, 
            hour, 
            num_of_unique_reels, 
            num_of_reels, 
            spent_time
        FROM dev.public.user_stats_temp
    """,)

    print("Successfully inserted into the user_stats table from user_stats_temp")
    conn.commit()  
    
except Exception as e:
    print("Error:", e)
    conn.rollback()

#Trucating the reel_stats_temp table    
try:    
    cursor.execute("TRUNCATE TABLE reel_stats_temp")

    print("Successfully truncated the reel_stats_temp")
    conn.commit()  


except Exception as e:
    print("Error:", e)
    conn.rollback()

#Trucating the user_stats_temp table    
try:    
    cursor.execute("TRUNCATE TABLE user_stats_temp")

    print("Successfully truncated the user_stats_temp")
    conn.commit()  


except Exception as e:
    print("Error:", e)
    conn.rollback()
       
   
conn.close()

