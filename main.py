import pandas as pd
from insert_table import insert_main
from insert_table import insert_rel
from Collection_to_df import getCollectionFromMongo

table_names=['user', 'userstory', 'roles', 'module', 'team', 'release', 'sprint',
              'testcase', 'task', 'defect', 'epic', 'feature', 'execplantestcases',
                'execution', 'executionRuns', 'suites', 'steps', 'customer', 'project_user_role', 'project', 'session','fields']


data = {'table_name': ['userstory', 'suites', 'release', 'release', 'task', 'epic', 'feature', 'defect', 'team', 'testcase', 'sprint'],
        'column_name': ['ownerid', 'testcases', 'projects', 'owners', 'ownerid', 'projectids', 'projectids', 'selectedtestcase', 'users', 'defectid', 'owners'],
        'pg_table_name': ['userstory_ownerid_relation', 'suites_testcases_relation', 'release_projects_relation', 'release_owners_relation', 'task_ownerid_relation', 'epic_projectids_relation', 'feature_projectids_relation', 'defect_selectedtestcase_relation', 'team_users_relation', 'testcase_defectid_relation', 'sprint_owners_relation'],
        'rel_col_name': ['ownerid', 'testcaseid', 'projectid', 'ownerid', 'ownerid', 'projectids', 'projectids', 'testcaseid', 'userid', 'defectid', 'ownerid'] } 
    

def main():
     for table in table_names:
         try:
             insert_main(table=table)
         except Exception as e:
             msg = f"{table} was not created due to {e} "
             print(msg)
   
 #insert relation tables
     rel_df = pd.DataFrame(data) 
     for _,row in rel_df.iterrows():
         pg_table_name=row['pg_table_name']
         try:
             insert_rel(row['table_name'],row['column_name'],pg_table_name,row['rel_col_name'])
         except Exception as e:
             tab=row["pg_table_name"]
             print(f"{tab} was not created due to {e}")
        






if __name__ == '__main__':
    main()
