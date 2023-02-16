from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd
import pandahouse as ph

default_args = {
    'owner': #insert owner,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 12)
}

connection = {
        'host': # insert host
        'password': # insert password
        'user': # insert user
        'database': # insert database
        }

schedule_interval = '0 1 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dbogoslovtsev_dag():
    
    @task()
    def extract_actions():
        
        q_feed = """SELECT toDate(time) as event_date,
                        user_id AS user,
                        IF(gender == 0, 'female', 'male') AS gender,
                        age,
                        os,
                        countIf(action = 'view') AS views,
                        countIf(action = 'like') AS likes
                    FROM simulator_20221220.feed_actions
                    WHERE toDate(time) = today() - 1
                    GROUP BY event_date, user, gender, age, os"""
        
        df_feed = ph.read_clickhouse(q_feed, connection=connection)
        return df_feed
    
    @task()
    def extract_messages():

        q_messages = """SELECT *
                        FROM 
                            (SELECT toDate(time) as event_date,
                                user_id AS user,
                                IF(gender == 0, 'female', 'male') AS gender,
                                age,
                                os,
                                count(user_id) AS messages_sent,
                                count(DISTINCT reciever_id) AS users_received
                            FROM simulator_20221220.message_actions
                            GROUP BY event_date, user, gender, age, os
                            )mes_sent FULL OUTER JOIN
                            (SELECT *
                            FROM 
                                (SELECT toDate(time) as event_date,
                                        reciever_id AS user,
                                        count(user_id) AS messages_received,
                                        count(DISTINCT user_id) AS users_sent
                                FROM simulator_20221220.message_actions 
                                GROUP BY event_date, user) temp1
                                JOIN
                                (SELECT DISTINCT user_id AS user,
                                        IF(gender == 0, 'female', 'male') AS gender,
                                        os,
                                        age
                                FROM simulator_20221220.message_actions
                                ) temp2 USING(user)
                            ) mes_recieved USING(event_date, user, gender, age, os)
                        WHERE event_date = today() - 1"""
        
        df_messages = ph.read_clickhouse(q_messages, connection=connection)
        return df_messages
    
    @task()
    def join(df_cube_feed,df_cube_messages):
        df_joined = df_cube_feed.merge(df_cube_messages, how='outer', on=['user', 'event_date', 'age', 'os',  'gender'])  
        return df_joined
    
    @task()
    def transform_os(df_cube):
        df_os = df_cube.groupby(['event_date','os'])\
        ['views','likes','messages_received','messages_sent','users_received','users_sent']\
        .sum().reset_index()
        df_os['dimension'] = 'os'
        df_os.rename(columns = {'os' : 'dimension_value'}, inplace = True)
        return df_os
    
    @task()
    def transform_age(df_cube):
        df_age = df_cube.groupby(['event_date','age'])\
       ['views','likes','messages_received','messages_sent','users_received','users_sent']\
        .sum().reset_index()
        df_age['dimension'] = 'age'
        df_age.rename(columns = {'age' : 'dimension_value'}, inplace = True)
        return df_age
    
    @task()
    def transform_gender(df_cube):
        df_gender = df_cube.groupby(['event_date','gender'])\
        ['views','likes','messages_received','messages_sent','users_received','users_sent']\
        .sum().reset_index()
        df_gender['dimension'] = 'gender'
        df_gender.rename(columns = {'gender' : 'dimension_value'}, inplace = True)
        return df_gender
    
    @task() 
    def concatenation(df_os, df_age, df_gender):
        final_data = pd.concat([df_os, df_age, df_gender])
        final_data = final_data.astype({
                                    'views': 'int32',
                                    'likes': 'int32',
                                    'messages_sent': 'int32',
                                    'users_sent': 'int32',
                                    'messages_received' : 'int32',
                                    'users_received' : 'int32'
                                   })
        return final_data
    
    @task()    
    def load(final_data):
        table = """CREATE TABLE IF NOT EXISTS test.dbogoslovtsev(
                                            event_date String,
                                            dimension String,
                                            dimension_value String,
                                            views Int32,
                                            likes Int32,
                                            messages_sent Int32,
                                            users_sent Int32,
                                            messages_received Int32,
                                            users_received Int32)
                                            ENGINE = MergeTree()
                                            ORDER BY event_date
                                            """
        ph.execute(query=table, connection=connect_test)
        ph.to_clickhouse(df = final_data, table = 'dbogoslovtsev', connection=connection, index=False)
        
    df_cube_feed = extract_actions()
    df_cube_messages = extract_messages()
    df_cube = join(df_cube_feed,df_cube_messages)
    df_os = transform_os(df_cube)
    df_gender = transform_gender(df_cube)
    df_age = transform_age(df_cube)
    final_data =  concatenation(df_os, df_gender, df_age)    
    load(final_data)
    
dbogoslovtsev_dag = dbogoslovtsev_dag()