import os
import hashlib
import shutil

# config
NUM_USERS                       = 100
TEMPLATE_DIR                    = 'Zero to Snowflake Lab Templates'
OUTPUT_DIR                      = 'lab_content'
SNOWFLAKE_ACCOUNT_URL           = 'https://de93986.us-east-2.aws.snowflakecomputing.com/'
SNOWFLAKE_USERS_PASSWORD_PREFIX = 'aA1ffa' # This prefix covers the complexity requirements from snowflake regardless of the hash results

# load templates
SF_CREATE_USER_TEMPLATE      = open(f'{TEMPLATE_DIR}/2020-06-17 -- Create User Template.sql', 'r').read()
SF_LAB_TEMPLATE              = open(f'{TEMPLATE_DIR}/2020-06-17 -- Zero to Snowflake Lab Template.sql', 'r').read()
SF_LAB_CLEANUP_USER_TEMPLATE = open(f'{TEMPLATE_DIR}/2020-06-17 -- Cleanup User Template.sql', 'r').read()
SF_LAB_CLEANUP_LAB_TEMPLATE  = open(f'{TEMPLATE_DIR}/2020-06-17 -- Cleanup Lab Template.sql', 'r').read()

def generate_create_users_script(num_users):
  with open(f'{OUTPUT_DIR}/create_users_script.sql', 'w+') as file:
    for user_id in range(1, num_users+1):
      user = "USER_{:02d}".format(user_id)
      password = f'{SNOWFLAKE_USERS_PASSWORD_PREFIX}{hashlib.md5(user.encode("utf-8")).hexdigest()}'
      create_user_script = SF_CREATE_USER_TEMPLATE.format(user=user, password=password)
      file.write(create_user_script)

def generate_lab_scripts(num_users):
  os.mkdir(f'{OUTPUT_DIR}/lab_scripts')
  for user_id in range(1, num_users+1):
    user = "USER_{:02d}".format(user_id)
    with open(f'{OUTPUT_DIR}/lab_scripts/{user}_snowflake_lab.sql', 'w+') as file:
      lab_script = SF_LAB_TEMPLATE.format(user=user, snowflake_account_url=SNOWFLAKE_ACCOUNT_URL)
      file.write(lab_script)

def generate_cleanup_script(num_users):
  with open(f'{OUTPUT_DIR}/cleanup_script.sql', 'w+') as file:
    for user_id in range(1, num_users+1):
      user = "USER_{:02d}".format(user_id)
      cleanup_user_script = SF_LAB_CLEANUP_USER_TEMPLATE.format(user=user)
      cleanup_lab_script  = SF_LAB_CLEANUP_LAB_TEMPLATE.format(user=user)
      file.write(cleanup_user_script)
      file.write(cleanup_lab_script)

def main():
  if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)

  os.mkdir(OUTPUT_DIR)

  generate_create_users_script(NUM_USERS)
  generate_lab_scripts(NUM_USERS)
  generate_cleanup_script(NUM_USERS)
  

if __name__ == "__main__":
    main()