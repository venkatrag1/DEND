from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_checks=[],
                 retry_count=3,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_checks = sql_checks
        self.retry_count = retry_count

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Iterate through checks, getting the sql string and the condition to check
        for sql_query, pass_condition in self.sql_checks:
            # Retry on failure
            for retry_count in range(self.retry_count):
                try:
                    self.log.info("Runing SQL query")
                    self.log.info(sql_query)
                    result = str(redshift.get_records(sql_query)[0][0])
                    self.log.info("Result is {}".format(result))
                    quality_check = result + ' ' +  pass_condition
                    self.log.info('Running quality check result {}'.format(pass_condition))
                    if eval(quality_check): # Evaluate condition and check against result
                        # match
                        self.log.info("Test succeeded!")
                        break
                    else:
                        # no match
                        if (retry_count + 1) == self.retry_count:
                            raise ValueError("Data quality check failed!")
                        self.log.info("Test Failed! Retry no. {}".format(retry_count + 1))
                except Exception as e:
                    # log exception and retry
                    if (retry_count + 1) == self.retry_count:
                        raise ValueError("Data quality check failed!")
                    self.log.info("Get records threw an exception {}".format(str(e)))

        self.log.info("Quality check completed successfully!")






