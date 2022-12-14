# %%
import sys
import pandas as pd
from tqdm import tqdm
from datetime import datetime

# %%
dump_landsat_path = sys.argv[1]
dump_postgres_filepath = sys.argv[2]
general_task_raw_filepath = sys.argv[3]
general_task_overview_filepath = sys.argv[4]
days_task_raw_filepath = sys.argv[5]
days_task_overview_filepath = sys.argv[6]
generate_days_out = eval(sys.argv[7])

# %%
colnames = ["task_id", "dataset", "region", "image_date", "state", "arrebol_job_id", "federation_member", "priority", "user_email", "inputdownloading_tag", "inputdownloading_digest", "preprocessing_tag", "preprocessing_digest", "processing_tag", "processing_digest", "creation_time", "updated_time", "status", "error_msg"]
df_task = pd.read_csv(dump_postgres_filepath, names=colnames)

df_task = df_task.drop(columns=["arrebol_job_id", "federation_member", "priority", "user_email", "inputdownloading_tag", "inputdownloading_digest",
"preprocessing_tag", "preprocessing_digest", "processing_tag", "processing_digest", "status"])

df_task.reset_index(drop="index", inplace=True)

# %%
spent_time = list()

for i, r in df_task.iterrows():
    creation_time_str = str(r['creation_time']).split('.')[0]
    creation_time = datetime.strptime(creation_time_str, '%Y-%m-%d %H:%M:%S')

    update_time_str = str(r['updated_time']).split('.')[0]
    update_time = datetime.strptime(update_time_str, '%Y-%m-%d %H:%M:%S')

    result = update_time - creation_time
    spent_time.append(result.total_seconds())

df_task['spent_time'] = spent_time


# %%
dfs_lan = dict()
df_landasat = pd.read_csv(dump_landsat_path).set_index('LANDSAT_KEY')

for i,r in tqdm(df_task.iterrows(), total=df_task.shape[0]):
    task_landsat_key = str(r['region']) + r['image_date'].replace('-', '')
    task_landsat_key = int(task_landsat_key)

    try:
      df_landasat_row = df_landasat.loc[task_landsat_key]
      dfs_lan[i] = {
          'task_id': r['task_id'],
          'state': r['state'],
          'creation_time': r['creation_time'],
          'updated_time': r['updated_time'],
          'spent_time': r['spent_time'],
          'total_size': df_landasat_row['TOTAL_SIZE'],
          'valid_image': True,
          'product id': df_landasat_row['PRODUCT_ID'],
          'error_msg': r['error_msg'],
          }

    except Exception as e:
        continue

valid_tasks_df = pd.DataFrame(data=dfs_lan.values(), index=dfs_lan.keys())


# %%
invalid_tasks = dict()

if valid_tasks_df.shape[0] > 0:
    for i, r in df_task.iterrows():
        if r['task_id'] not in valid_tasks_df['task_id'].values:
            invalid_tasks[i] = {
                'task_id': r['task_id'],
                'state': r['state'],
                'creation_time': r['creation_time'],
                'updated_time': r['updated_time'],
                'spent_time': r['spent_time'],
                'total_size': 0,
                'valid_image': False,
                'product id': '-',
                'error_msg': "The landsat image doesn't exists",
            }

invalid_tasks_df = pd.DataFrame(data=invalid_tasks.values(), index=invalid_tasks.keys())

# %%
def extract_phase(text):
    if 'preprocessing' in text:
        return 'preprocessing'
    elif 'downloading' in text:
        return 'downloading'
    elif 'running' in text:
        return 'running'
    elif 'available' in text:
        return 'archived'

if valid_tasks_df.shape[0] > 0:
    valid_tasks_df['last_phase'] = valid_tasks_df.error_msg.map(extract_phase)

if invalid_tasks_df.shape[0] > 0:
    invalid_tasks_df['last_phase'] = invalid_tasks_df.error_msg.map(extract_phase)

# %%
def generate_overview(df):
  all_valid = df[df['valid_image'] == True]
  all_invalid = df[df['valid_image'] == False]

  v_size_bt = all_valid['total_size'].sum() 
  v_size_gb = v_size_bt/1024/1024/1024

  running = 0
  downloading = 0
  preprocessing = 0
  values_sec = all_valid['last_phase'].value_counts()

  if 'downloading' in values_sec.keys():
    downloading = values_sec['downloading']
  if 'preprocessing' in values_sec.keys():
    preprocessing = values_sec['preprocessing']
  if 'running' in values_sec.keys():
    running = values_sec['running']

  archived_failed = 0
  archived_success = 0
  aux_arc_tasks = all_valid[all_valid['last_phase'] == 'archived']
  arc_tasks = aux_arc_tasks['state'].value_counts()

  if 'archived' in arc_tasks.keys():
    archived_success = arc_tasks['archived']
  if 'failed' in arc_tasks.keys():
    archived_failed = arc_tasks['failed']

  overview_data = {
    'valid_tasks': {
      'downloading (failed)': downloading, 
      'preprocessing (failed)': preprocessing, 
      'running (failed)': running, 
      'archived (failed)': archived_failed,
      'archived (success)': archived_success,
      'total_size (GB)': f'{v_size_gb}'
    },
    'invalid_tasks': {
      'downloading (failed)': len(all_invalid['last_phase']), 
      'preprocessing (failed)': 0, 
      'running (failed)': 0, 
      'archived (failed)': 0,
      'archived (success)': 0,
      'total_size (GB)': 0
    }
  }

  return pd.DataFrame(overview_data)

# %%
raw_df_day = pd.concat([valid_tasks_df, invalid_tasks_df])
raw_df_day.reset_index(drop="index", inplace=True)

# %%
if generate_days_out and raw_df_day.shape[0]:
  raw_df_day.to_csv(days_task_raw_filepath, index=False)
  overview_df_day = generate_overview(raw_df_day)
  overview_df_day.to_csv(days_task_overview_filepath)
  
# %%
try:
    raw_df_old = pd.read_csv(general_task_raw_filepath, converters={'valid_image': eval})
except Exception as e:
    raw_df_old = pd.DataFrame()

raw_df_new = pd.concat([raw_df_old, raw_df_day])
raw_df_new.to_csv(general_task_raw_filepath, index=False)

overview_df_general = generate_overview(raw_df_new)
overview_df_general.to_csv(general_task_overview_filepath)

