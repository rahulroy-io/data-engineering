#########################################TASK00#########################################
#IMPORTS
print ("STARTING JOB")

print ("STARTED TASK00 IMPORTS")

import sys
import boto3
from botocore.exceptions import ClientError
import time
from datetime import datetime, timedelta  
import json
import queue 
from datetime import datetime as dt, timedelta  

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F, types as T

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

appflow_client = boto3.client('appflow')
s3_client = boto3.resource('s3')
glue_client = boto3.client("glue")

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_TARGET_MAPPING', 'ODATA_PATH', 'FILTER_CONFIG','VALIDATION_CONFIG' ,'MAX_RETRIES', 'MAX_PARALLELISM', 'MAX_RUN', 'TARGET_BUCKET_INGEST', 'TARGET_BUCKET_STRUCTURED', 'TARGET_BUCKET_TEMP'])
# args = getResolvedOptions(sys.argv, ['JOB_NAME', 'FILTER_CONFIG', 'SELECT_FIELDS', 'VALIDATION_CONFIG', 'MAX_RETRIES', 'MAX_PARALLELISM', 'MAX_RUN', 'TARGET_BUCKET_INGEST', 'TARGET_BUCKET_STRUCTURED', 'TARGET_BUCKET_TEMP'])

if '--WORKFLOW_NAME' in sys.argv and '--WORKFLOW_RUN_ID' in sys.argv:
    glue_args = getResolvedOptions(
        sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID']
    )
    workflow_args = glue_client.get_workflow_run_properties(
        Name=glue_args['WORKFLOW_NAME'], 
        RunId=glue_args['WORKFLOW_RUN_ID']
    ).get('RunProperties', 'Not Found')
    print (f'running with workflow_args : {workflow_args}')
else:
    print (f'running without workflow_args')

job.init(args['JOB_NAME'], args)

print('##########TASK00-IMPORTS-COMPLETED-SUCCESSFULLY##########')
#########################################TASK01#########################################
#PARAMETERS
print ("STARTED TASK01 PARAMETERS INITIALIZING")

appflow_client = boto3.client('appflow')
s3_client = boto3.resource('s3')

JOB_NAME = str(args['JOB_NAME'])
JOB_RUN_ID = dt.now().strftime('%Y%m%d%H%M%S') 
ODATA_PATH = str(args['ODATA_PATH']) 
FILTER_CONFIG = str(args['FILTER_CONFIG']) 
SOURCE_TARGET_MAPPING = str(args['SOURCE_TARGET_MAPPING'])
VALIDATION_CONFIG = str(args['VALIDATION_CONFIG']) 
MAX_RETRIES = str(args['MAX_RETRIES'])
MAX_PARALLELISM = int(args['MAX_PARALLELISM'])
MAX_RUN = int(args['MAX_RUN'])
TARGET_BUCKET_INGEST = str(args['TARGET_BUCKET_INGEST'])
TARGET_BUCKET_STRUCTURED = str(args['TARGET_BUCKET_STRUCTURED'])
TARGET_BUCKET_TEMP = str(args['TARGET_BUCKET_TEMP'])

glue_job = 'nbtest_prod_jkt_ztbsaletgtq02srv_ztbsaletgtq02results_sap_appflow_s3_ingesthistory'
app, env, client, domain, entity, source, connect, target, action = glue_job.split("_")

flow_name = f"appflow-{env}-{client}-{domain}-{entity}-{source}-{target}-{action}"
appflow_ingest_prefix = f'{source}/{action}'
# appflow_ingest_path = f's3://{TARGET_BUCKET_INGEST}/{appflow_ingest_prefix}'
appflow_structured_prefix = f'{source}/{domain}/{entity}'
# appflow_structured_path = f's3://{TARGET_BUCKET_STRUCTURED}/{appflow_structured_prefix}'

source_target_mapping_json_string = SOURCE_TARGET_MAPPING

print(f"JOB_NAME: {JOB_NAME}")
print(f"JOB_RUN_ID: {JOB_RUN_ID}")
print(f"ODATA_PATH: {ODATA_PATH}")  
print(f"FILTER_CONFIG: {FILTER_CONFIG}")  
print(f"SOURCE_TARGET_MAPPING: {SOURCE_TARGET_MAPPING}")
print(f"VALIDATION_CONFIG: {VALIDATION_CONFIG}") 
print(f"MAX_RETRIES: {MAX_RETRIES}")  
print(f"MAX_PARALLELISM: {MAX_PARALLELISM}")  
print(f"MAX_RUN: {MAX_RUN}")
print(f"TARGET_BUCKET_INGEST: {TARGET_BUCKET_INGEST}")  
print(f"TARGET_BUCKET_STRUCTURED: {TARGET_BUCKET_STRUCTURED}")  
print(f"TARGET_BUCKET_TEMP: {TARGET_BUCKET_TEMP}")

print(f"app: {app}")
print(f"env: {env}")
print(f"client: {client}")  
print(f"domain: {domain}")  
print(f"entity: {entity}")  
print(f"source: {source}")
print(f"connect: {connect}")
print(f"target: {target}")
print(f"action: {action}")

print(f"appflow: {flow_name}")
print(f"appflow_ingest_prefix: {appflow_ingest_prefix}")
print(f"appflow_structured_prefix: {appflow_structured_prefix}")

try:
    validation_config = json.loads(VALIDATION_CONFIG)
except Exception as e:
    raise Exception (f'TASK01-PARAMETERS-INITIALIZATION FAILED WITH ERROR {e}')
    
try:
    load_type = ''
    filter_field = ''
    filter_values = []
    filter_type = ''
    filter_config = json.loads(FILTER_CONFIG)
    # validation_config = json.loads(VALIDATION_CONFIG)
    if filter_config.get('filter_field', '') != '':
        filter_field = filter_config['filter_field']
        print(f"filter_field: {filter_field}")
        if filter_config.get('filter_type', '_') not in ['str', 'daterange', 'list']:
            raise Exception ('Missing or Incorrect filter_type')
        else:
            if filter_config.get('filter_type', '_') == 'list':
                if filter_config.get('filter_values', '') != '':
                    filter_values = filter_config['filter_values']
                    filter_values = [filter_value.strip(' ') for filter_value in filter_values.split(',')]
                    print(f"filter_values: {filter_values}")
                else:
                    raise Exception ('Missing or Incorrect filter_type')
            elif filter_config.get('filter_type', '_') == 'daterange':
                if filter_config.get('filter_values', '').get('start_date', '') != '_':
                    start_date_str = filter_config['filter_values']['start_date']
                    end_date_str = filter_config['filter_values']['end_date']
                    if start_date_str == end_date_str == '':
                        start_date = end_date = dt.now()-timedelta(days=1)
                        load_type = 'incremental_load'
                    else:
                        load_type = 'historical_load'
                        start_date = dt.strptime(start_date_str, '%Y-%m-%d')  
                        end_date = dt.strptime(end_date_str, '%Y-%m-%d') 
                    # Check if start_date is greater than end_date  
                    if start_date > end_date:  
                        raise ValueError("Start date : {start_date} cannot be greater than end date {end_date}.")
                    # Generate the range of dates in 'YYYYMMDD' format  
                    filter_values = [(start_date + timedelta(days=x)).strftime('%Y%m%d') for x in range((end_date - start_date).days + 1)]
                    print(f"filter_values: {filter_values}")
                else:
                    raise Exception ('Missing or Incorrect filter_type')
    else:
        print('No Filter Applied')
except Exception as e:
    raise Exception (f'TASK01-PARAMETERS-INITIALIZATION FAILED WITH ERROR {e}')

if load_type == '':
    raise Exception ('Missing or Incorrect load_type')
else:
    print (f'load_type: {load_type}')
    
print('##########TASK01-PARAMETERS-INITIALIZED-COMPLETED-SUCCESSFULLY##########')
# raise Exception('fORCED')
############################################TASK03############################################
print ("STARTED TASK03 UDFs Defination")
class AppFlowWrapper:
    def _init_(self, flow_name, flow_client, flow_config, max_retries):
        """
        Initialize the AppFlowWrapper with a Boto3 AppFlow client supplied externally.

        :param flow_client: The AppFlow client object supplied during initialization.
        :param flow_name: The AppFlow name object supplied during initialization.
        """
        self.flow_name = flow_name
        self.client = flow_client
        self.flow_config = flow_config
        self.execution_id = None
        self.retry = 0
        self.max_retries = max_retries
        self.execution_completed = False
        self.started = False
        self.records_processed = 0
        self.dt = self.flow_config.get('tasks')[0].get('taskProperties', {}).get('VALUE', 'all_dates')

        
    def flow_exists(self):
        """
        Check if a flow with the given name exists.

        :param flow_name: The name of the flow to check.
        :return: True if the flow exists, False otherwise.
        """
        flow_name = self.flow_name
        try:
            self.client.describe_flow(flowName=flow_name)
        except Exception as e:
            print (f"Error checking if flow '{flow_name}' exists: {e}")
            return False
        else:
            return True

    def start_flow(self):
        """
        Start an AppFlow flow execution.

        :param flow_name: The name of the flow to start.
        :param client_token: (Optional) A unique, case-sensitive string to ensure idempotency.
        :return: The execution ID of the started flow, or None in case of error.
        """
        try:
            flow_name = self.flow_name
            response = self.client.start_flow(
                flowName=flow_name
            )
            execution_id = response.get('executionId')
            dt = self.dt
            print(f"{flow_name} : {dt} : {execution_id} started scuccessfully")
            self.execution_id = execution_id
            self.execution_start_time = time.time()
            time.sleep(1)
        except Exception as e:
            print (f"{flow_name} Error in starting flow : {e}")
            return False
        else:
            return True
    
    def get_execution_status(self):
        """
        Get the status of a specific flow execution.

        :param flow_name: The name of the flow.
        :param execution_id: The execution ID to check.
        :return: A tuple containing the execution status and the execution result (if available).
        """
        flow_name = self.flow_name
        execution_status = False
        dt = self.dt

        if self.execution_id==None:
            raise Exception(f"{flow_name} : {dt} : not yet started")
        else:
            execution_id = self.execution_id
        try:
            response = appflow_client.describe_flow_execution_records(flowName=flow_name)
            flow_executions = response.get('flowExecutions')
            for flow_execution in flow_executions:
                if flow_execution.get('executionId')==execution_id:
                    execution_status = flow_execution.get('executionStatus')
                    self.execution_status = flow_execution.get('executionStatus')
                    self.records_processed = flow_execution.get('executionResult').get('recordsProcessed', 0)
            if (execution_status==False):
                while 'nextToken' in response:
                    next_token = response.get('nextToken')
                    response = appflow_client.describe_flow_execution_records(flowName=flow_name, nextToken = next_token)
                    flow_executions = response.get('flowExecutions')
                    for flow_execution in flow_executions:
                        if flow_execution.get('executionId')==execution_id:
                            execution_status = flow_execution.get('executionStatus')
                            self.execution_status = flow_execution.get('executionStatus')
                            self.records_processed = flow_execution.get('executionResult').get('recordsProcessed', 0)
                            break
        except Exception as e:
            print(f"{flow_name} : {execution_id} : {dt} --> {e}")
        finally:
            if (execution_status):
                if self.in_terminal_state():
                    self.execution_completed = True
                return execution_status
            else:
                print(f"{flow_name} : {execution_id} : {dt} not found")
                #raise Exception((f"Execution ID '{execution_id}' not found for flow '{flow_name}'."))
                execution_status = 'UnKnown'
                self.execution_status = execution_status
                self.records_processed = 0
                if self.in_terminal_state():
                    self.execution_completed = True
                return execution_status

    def create_flow(self, flow_config):
        """
        Create a new AppFlow with the specified configuration.
        :param flow_name: Name of the new AppFlow.
        :param flow_config: Dictionary containing the flow configuration.
        :return: Flow creation status.
        """
        flow_name = self.flow_name
        dt = self.dt
        try:
            response = self.client.create_flow(
                flowName=flow_name,
                **flow_config
            )
            print(f"{flow_name} : {dt} created successfully")
            print (response)
        except Exception as e:
            print (f"{flow_name} : {dt} Unexpected error while creating flow : {e}")
            return False
        else:
            return True
        
    def update_flow(self, flow_config):
        """
        Update an existing flow with the given configuration.

        :param flow_name: The name of the flow to update.
        :param flow_config: A dictionary containing the updated flow configuration.
        """
        flow_name = self.flow_name
        dt = self.dt
        try:
            response = self.client.update_flow(
                flowName=flow_name,
                **flow_config
            )
            print(f"{flow_name} : {dt} updated successfully.")
        except Exception as e:
            print (f"{flow_name} : {dt} Failed to update flow : {e}")
            return False
        else:
            return True
        
    def create_or_update_flow(self):
        """
        Create a new flow or update an existing flow.

        :param flow_name: The name of the flow to create or update.
        :param flow_config: A dictionary containing the flow configuration.
        :return: True if the flow was created or updated successfully, False otherwise.
        """
        flow_config = self.flow_config
        flow_name = self.flow_name
        try:
            if self.flow_exists():
                print(f"{flow_name} : exists -> Updating...")
                self.update_flow(flow_config)
                #print(f"Flow '{flow_name}' updated successfully.")
            else:
                print(f"{flow_name} : does not exist -> Creating...")
                self.create_flow(flow_config)
                #print(f"Flow '{flow_name}' created successfully.")
        except Exception as e:
            print (f"Failed to create or update flow '{flow_name}': {e}")
            # return False
        else:
            return True
        
    def in_terminal_state(self):
        """
        Check if a flow execution has reached terminal state or not.

        :param flow_name: The name of the flow to check.
        :return: True if the flow exists, False otherwise.
        """
        flow_name = self.flow_name
        execution_id = self.execution_id
        dt = self.dt
        try:
            flow_status = self.execution_status
        except Exception as e:
            print (f"{flow_name} : {execution_id} : {dt} Error checking flow status exists: {e}")
            #raise (f"Error checking if flow '{flow_name}' exists: {e}")
            return False
        else:
            if flow_status in ['Successful', 'Error', 'CancelStarted', 'Canceled']:
                return True
            else:
                return False
    
    def create_and_start(self):
        if (self.execution_completed==False):
            self.create_or_update_flow()
            self.start_flow()
            self.get_execution_status()
            execution_id = self.execution_id
            self.retry = self.retry + 1
            self.started = True
            print (f'{self.flow_name} : {self.execution_id} : {self.dt} : retry -> {self.retry-1}')
        
    def excetion_monitor_and_retry(self):
        execution_id = self.execution_id
        self.get_execution_status()
        if self.execution_status=='Successful':
            return (True, self.execution_status)
        else:
            if self.retry<self.max_retries:
                if self.execution_status in ['Error', 'CancelStarted', 'Canceled']:
                    print (f"{flow_name} : {execution_id} : {dt} Restarting Flow")
                    self.create_and_start()
                else:
                    return (False, self.execution_status)
            else:
                return (True, self.execution_status)

def generate_date_range(start_date_str, end_date_str):  
    try:  
        # Parse the input date strings into datetime objects  
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')  
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')  
          
        # Check if start_date is greater than end_date  
        if start_date > end_date:  
            raise ValueError("Start date : {start_date} cannot be greater than end date {end_date}.")
          
        # Generate the range of dates in 'YYYYMMDD' format  
        date_range = [(start_date + timedelta(days=x)).strftime('%Y%m%d') for x in range((end_date - start_date).days + 1)]  
          
        return date_range
    except Exception as e:  
        raise Exception(f"An unexpected error occurred: {e}")
        
# print (appflow_ingest_prefix, appflow_structured_prefix, flow_name, task.execution_id, task.dt, FILTER_FIELD, source_target_mapping_dict, TARGET_BUCKET_INGEST, TARGET_BUCKET_STRUCTURED)
def push_to_structured(appflow_ingest_prefix, appflow_structured_prefix, flow_name, execution_id, dt, FILTER_FIELD, source_target_mapping_dict, TARGET_BUCKET_INGEST, TARGET_BUCKET_STRUCTURED):
    source_read_path = f's3://{TARGET_BUCKET_INGEST}/{appflow_ingest_prefix}' + '/' + flow_name + '/' + execution_id
    target_write_path = f's3://{TARGET_BUCKET_STRUCTURED}/{appflow_structured_prefix}'
    df = spark.read.format('csv').options(header='true').load(source_read_path)\
            .withColumn('last_updated_datetime', F.current_timestamp().cast('string'))
    dt = dt
    if dt=='all_dates':
        mapped_filter_field = source_target_mapping_dict[FILTER_FIELD]
        df = df.withColumn(FILTER_FIELD.lower(), F.col(mapped_filter_field))
        df.repartition(1).write.mode('append').option("header", "true").format("csv").partitionBy(FILTER_FIELD.lower()).save(target_write_path)
    else:
        target_write_path = target_write_path + '/' + f'{FILTER_FIELD.lower()}={dt}'
        df.drop(FILTER_FIELD.lower()).repartition(1).write.mode('overwrite').option("header", "true").format("csv").save(target_write_path)
        
def create_queue_from_list(task_list):  
    task_queue = queue.Queue()  
    for task in task_list:  
        task_queue.put(task)  
    return task_queue

def monitor_tasks(task_queue, n):  
    active_tasks = []  
    passed_dts = []
    failed_dts = []
    while not task_queue.empty() or active_tasks:  
        # Start monitoring up to n tasks  
        while len(active_tasks) < n and not task_queue.empty():  
            task = task_queue.get()  
            active_tasks.append(task) 
            # print(f"Started monitoring task {task.dt}")  
  
        # Process active tasks
        try:
            for task in active_tasks[:]:
                if task.started:
                    status = task.excetion_monitor_and_retry()
                    print(f"{task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1}")
                    time.sleep(2)
                else:
                    task.create_and_start()
                    time.sleep(4)
                    status = task.excetion_monitor_and_retry()
                    print(f"{task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1}")
                
                if status[0]:
                    # print(f"Task {task.dt} reached terminal state")
                    print(f"{task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1} : records_processed -> {task.records_processed}")
                    passed_dts.append(task.dt)
                    if task.records_processed>0:
                        push_to_structured(appflow_ingest_prefix, appflow_structured_prefix, flow_name, task.execution_id, task.dt, filter_field, source_target_mapping_dict, TARGET_BUCKET_INGEST, TARGET_BUCKET_STRUCTURED)
                        print (appflow_ingest_prefix, appflow_structured_prefix, flow_name, task.execution_id, task.dt, filter_field, source_target_mapping_dict, TARGET_BUCKET_INGEST, TARGET_BUCKET_STRUCTURED)
                    active_tasks.remove(task)
        except Exception as e:
            failed_dts.append(task.dt)
            print(f"ERROR : {e}: {task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1}")
    return passed_dts, failed_dts

print('##########TASK03-UDFs-DEFINED-COMPLETED-SUCCESSFULLY##########')

# raise Exception('fORCED')
############################################TASK04############################################
print ("STARTED TASK04 RAW INGESTION")

if FILTER_CONFIG == '{}':
    pass

flow_configs = []

source_target_mapping_dict = json.loads(source_target_mapping_json_string)
sub_tasks = []

for key in source_target_mapping_dict:
    value = source_target_mapping_dict[key]
    dict_element = {  
        "sourceFields": [  
            key  
        ],  
        "connectorOperator": {  
            "SAPOData": "NO_OP"  
        },  
        "destinationField": value,  
        "taskType": "Map",  
        "taskProperties": {  
            "DESTINATION_DATA_TYPE": "Edm.String",  
            "SOURCE_DATA_TYPE": "Edm.String"  
        }
    }
    sub_tasks.append(dict_element)

for val in filter_values:
    source_flow_config = {
            'connectorType': 'SAPOData',
            'connectorProfileName': 'GW_PROD',
            'sourceConnectorProperties': {
            'SAPOData': {
                'objectPath': ODATA_PATH,
                'parallelismConfig': {
                    'maxParallelism': MAX_PARALLELISM
                },
                'paginationConfig': {
                    'maxPageSize': 3000
                }
            }
        }
    }
    
    destination_flow_config_list = [
        {
            'connectorType': 'S3',
            'destinationConnectorProperties': {
                'S3': {
                    'bucketName': TARGET_BUCKET_INGEST,
                    'bucketPrefix': appflow_ingest_prefix,
                    's3OutputFormatConfig': {
                        'fileType': 'CSV',
                        'prefixConfig': {
                            #'pathPrefixHierarchy': ['SCHEMA_VERSION']
                        },
                        'aggregationConfig': {
                            'aggregationType': 'SingleFile'
                        }
                    }
                }
            }
        }
    ]

    tasks = [
        {
            'sourceFields': [
                filter_field
            ],
            'connectorOperator': {
                'SAPOData': 'CONTAINS'
            },
            'taskType': 'Filter',
            'taskProperties': {
                'DATA_TYPE': 'Edm.String',
                'VALUE': val
            }
        },
        {
            'sourceFields': list(source_target_mapping_dict.keys()),
            'connectorOperator': {
                'SAPOData': 'PROJECTION'
            },
            'taskType': 'Filter',
            'taskProperties': {}
        }
    ]
    for sub_task in sub_tasks:
        tasks.append(sub_task)
    
    if val == 'all_dates':
        tasks.pop(0)
    
    trigger_config = {
        'triggerType':'OnDemand'
    }
    
    flow_config = {
        'triggerConfig': trigger_config,
        'sourceFlowConfig': source_flow_config,
        'destinationFlowConfigList': destination_flow_config_list,
        'tasks': tasks
    }
    
    print (flow_config)
    flow_configs.append(flow_config)

flow_tasks = [AppFlowWrapper(flow_name, appflow_client, flow_config, max_retries=3) for flow_config in flow_configs]
print (flow_tasks)

task_queue = create_queue_from_list(flow_tasks)
# passed_dts, failed_dts = monitor_tasks(task_queue, MAX_RUN)

# print (f"passed_dts :: {passed_dts}")
# print (f"failed_dts :: {failed_dts}")

# print("All tasks completed.")

# print('##########TASK04-RAW-INGESTION-COMPLETED-SUCCESSFULLY##########')

# job.commit()
passed_dts, failed_dts = monitor_tasks(task_queue, MAX_RUN)

print (f"passed_dts :: {passed_dts}")
print (f"failed_dts :: {failed_dts}")

print("All tasks completed.")

print('##########TASK04-RAW-INGESTION-COMPLETED-SUCCESSFULLY##########')
task_queue, n = task_queue, MAX_RUN

active_tasks = []  
passed_dts = []
failed_dts = []

loop = 50
outer_loop = 0
inner_loop = 0

while not task_queue.empty() or active_tasks:  
    # Start monitoring up to n tasks  
    outer_loop = outer_loop + 1
    if outer_loop==loop: break
    while len(active_tasks) < n and not task_queue.empty():  
        task = task_queue.get()
        active_tasks.append(task) 
        # print(f"Started monitoring task {task.dt}")  
        inner_loop = inner_loop + 1
        if inner_loop==loop: break
    # Process active tasks
    try:
        for task in active_tasks[:]:
            if task.started:
                status = task.excetion_monitor_and_retry()
                print(f"{task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1}")
                time.sleep(2)
            else:
                task.create_and_start()
                time.sleep(4)
                status = task.excetion_monitor_and_retry()
                print(f"{task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1}")

            if status[0]:
                # print(f"Task {task.dt} reached terminal state")
                print(f"{task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1} : records_processed -> {task.records_processed}")
                passed_dts.append(task.dt)
                if task.records_processed>0:
                    push_to_structured(appflow_ingest_prefix, appflow_structured_prefix, flow_name, task.execution_id, task.dt, filter_field, source_target_mapping_dict, TARGET_BUCKET_INGEST, TARGET_BUCKET_STRUCTURED)
                    print (appflow_ingest_prefix, appflow_structured_prefix, flow_name, task.execution_id, task.dt, filter_field, source_target_mapping_dict, TARGET_BUCKET_INGEST, TARGET_BUCKET_STRUCTURED)
                active_tasks.remove(task)
                print (inner_loop, outer_loop)
                
    except Exception as e:
        failed_dts.append(task.dt)
        print (f"ERROR : {e}: {task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1}")
appflow_client.describe_flow(
    flowName='appflow-prod-jkt-ztbsaletgtq02srv-ztbsaletgtq02results-sap-s3-ingesthistory'
)
job.commit()