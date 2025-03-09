from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, functions as F
from typing import List, Tuple, Literal

class FileFormat:
    CSV = {'format': 'csv', 'extension': '.csv'}
    PARQUET = 'parquet'

class BasePipeline():
   
    PIPELINE_BASE_PATH = './PipelineData/'
    RAW_DATA_BASE_PATH = './RawData/'
    BRONZE_PATH = PIPELINE_BASE_PATH + 'Bronze/'
    SILVER_PATH = PIPELINE_BASE_PATH + 'Silver/'
    GOLD_PATH = PIPELINE_BASE_PATH + 'Gold/'

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()

    @classmethod
    def get_pipeline_base_path(cls) -> str:
        return cls.PIPELINE_BASE_PATH

    @classmethod
    def get_raw_data_base_path(cls) -> str:
        return cls.RAW_DATA_BASE_PATH

    @classmethod
    def get_bronze_path(cls) -> str:
        return cls.BRONZE_PATH

    @classmethod
    def get_silver_path(cls) -> str:
        return cls.SILVER_PATH

    @classmethod
    def get_gold_path(cls) -> str:
        return cls.GOLD_PATH

    def read_data(self, file_path: str, file_format: str, has_header: bool=True, infer_schema: bool=False) -> DataFrame:
        return self.spark.read.format(file_format).options(header=has_header, inferSchema=infer_schema).load(file_path)

    def write_data(self, df: DataFrame, file_path: str, file_format: str, write_mode: str) -> None:
        try:
            if file_format == FileFormat.CSV['format']:
                df.repartition(1).write.format(file_format).mode(write_mode).option("header", "true").save(file_path)
            else:
                df.write.format(file_format).mode(write_mode).save(file_path)
                
            print(f"Successfully saved data in location:\n{file_path}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def read_bronze(self, table_name: str) -> DataFrame:
        return self.read_data(f'{self.get_bronze_path()}{table_name}', FileFormat.PARQUET)

    def read_silver(self, table_name: str) -> DataFrame:
        return self.read_data(f'{self.get_silver_path()}{table_name}', FileFormat.PARQUET)

    def stop_spark(self) -> None:
        self.spark.stop()


class Bronze(BasePipeline):

    def __init__(self, table_name: str, bronze_table_name: str):
        super().__init__()
        self.file_name = table_name + FileFormat.CSV['extension']
        self.table_name = table_name
        self.bronze_table_name = bronze_table_name
        
    def read_raw(self) -> DataFrame:
        return BasePipeline.read_data(self, f'{self.get_raw_data_base_path()}{self.file_name}', FileFormat.CSV['format'])

    def write_bronze(self) -> None:
        df = self.read_raw()
        BasePipeline.write_data(self, df, f'{self.get_bronze_path()}{self.bronze_table_name}', FileFormat.PARQUET, 'overwrite')

    def create_bronze(self) -> None:
        print(f"{'-'*50}\nReading from raw from: {self.file_name}")        
        self.read_raw()
        print(f"Writing into bronze: {self.bronze_table_name}")
        self.write_bronze()

    def run_test(self) -> None:
        df = self.read_raw()
        print(f"Row count ({self.bronze_table_name}): {df.count()}")
        print(f"Column count ({self.bronze_table_name}): {len(df.dtypes)}")
        print([i[0] for i in df.dtypes])


class Silver(BasePipeline):

    def __init__(self, 
                 pre_validation_table_name: str, 
                 validated_table_name: str, 
                 invalid_table_name: str, 
                 required_bronze_tables: List[str]
                ):
        super().__init__()
        self.pre_validation_table_name = pre_validation_table_name
        self.validated_table_name = validated_table_name
        self.invalid_table_name = invalid_table_name
        self.required_bronze_tables = required_bronze_tables
        
        self.bronze_tables_dfs = {bronze_table: self.read_bronze(bronze_table) for bronze_table in self.required_bronze_tables}

    def create_silver(self, pre_validation_df: DataFrame, validated_df: DataFrame, invalid_df: DataFrame) -> None:
        print(f"{'-'*50}\nWriting into silver: {self.pre_validation_table_name}")
        BasePipeline.write_data(self, pre_validation_df, f'{self.get_silver_path()}{self.pre_validation_table_name}', FileFormat.PARQUET, 'overwrite')
        
        print(f"{'-'*50}\nWriting into silver: {self.validated_table_name}")
        BasePipeline.write_data(self, validated_df, f'{self.get_silver_path()}{self.validated_table_name}', FileFormat.PARQUET, 'overwrite')
        
        print(f"{'-'*50}\nWriting into silver: {self.invalid_table_name}")
        BasePipeline.write_data(self, invalid_df, f'{self.get_silver_path()}{self.invalid_table_name}', FileFormat.CSV['format'], 'overwrite')

    def run_test(self, return_dfs: bool) -> None | Tuple[DataFrame, DataFrame, DataFrame]:
        pre_validation_df = BasePipeline.read_data(self, f'{self.get_silver_path()}{self.pre_validation_table_name}', FileFormat.PARQUET)
        
        valid_df = BasePipeline.read_data(self, f'{self.get_silver_path()}{self.validated_table_name}', FileFormat.PARQUET)
        invalid_df = BasePipeline.read_data(self, f'{self.get_silver_path()}{self.invalid_table_name}', FileFormat.CSV['format'])

        union_df = valid_df.unionByName(invalid_df, allowMissingColumns=True)

        print(f"{'-'*50}\nRunning test to check if counts match...")
        if pre_validation_df.count() == union_df.count():
            print(f"Counts match: {pre_validation_df.count()}")
            print(f"{self.pre_validation_table_name}: {pre_validation_df.count()}")
            print(f"Union of {self.validated_table_name} and {self.invalid_table_name}: {union_df.count()}")
        else:
            print(f"Counts do not match:")
            print(f"{self.pre_validation_table_name}: {pre_validation_df.count()}")
            print(f"Union of {self.validated_table_name} and {self.invalid_table_name}: {union_df.count()}")

        if return_dfs:
            return pre_validation_df, valid_df, invalid_df


class Gold(BasePipeline):

    def __init__(self, silver_table_name: str):
        super().__init__()
        self.silver_table_name = silver_table_name
        self.silver_table_df = self.read_silver(silver_table_name)

    @property
    def get_silver_table_df(self) -> DataFrame:
        return self.silver_table_df

    def create_gold(self, df: DataFrame, gold_table_name: str) -> None:
        print(f"{'-'*50}\nWriting into gold: {gold_table_name}")
        BasePipeline.write_data(self, df, f'{self.get_gold_path()}{gold_table_name}', FileFormat.CSV['format'], 'overwrite')


    # Gold Testing
    def get_gold_table_df(self, gold_table_name: str) -> DataFrame:
        return BasePipeline.read_data(self, f'{self.get_gold_path()}{gold_table_name}', FileFormat.CSV['format'])
    
    def read_filtered_silver(self, column_name: str, column_filter: str | int) -> DataFrame:
        return self.read_silver(self.silver_table_name).filter(F.col(column_name) == column_filter)

    def get_count(self, df: DataFrame, column_name: str) -> int:
        collect_row_value = df.select(column_name).collect()
        return len([val[column_name] for val in collect_row_value])

    def get_sum(self, df: DataFrame, column_name: str) -> int | float:
        collect_row_value = df.select(column_name).collect()
        return sum([float(val[column_name]) for val in collect_row_value])

    def select_aggregate_calculation(self, aggregation: Literal['avg', 'sum', 'count'], df: DataFrame, column_name: str) -> int | float | ValueError:
        '''
        Apply aggregation on a column in a DataFrame.

        Parameters:
        - aggregation (str): Aggregation type.
        - df (DataFrame): DataFrame to apply aggregation on.
        - column_name (str): Column to apply aggregation on.

        Returns:
        (int | float): Value to test on from silver.
        '''
        aggregation = aggregation.lower()

        if aggregation == 'avg':
            try:
                return self.get_sum(df, column_name) / self.get_count(df, column_name)
            except ZeroDivisionError:
                return 0
        elif aggregation == 'sum':
            return self.get_sum(df, column_name)
        elif aggregation == 'count':
            return self.get_count(df, column_name)
        else:
            raise ValueError(f"Unknown aggregation type: {aggregation} must be one of 'avg', 'sum', 'count'")
        
    def gold_value_collection(self, 
                 gold_table_name: str,
                 gold_column_collection: str,
                 gold_table_column_filter: str,
                 gold_filter: str | int,
                ) -> int | float:
        '''
        Collects value from a column in gold based on specific filter of another column.

        Parameters:
        - gold_table_name (str): Name of gold table.
        - gold_column_collection (str): Name of the column to collect value to test.
        - gold_table_column_filter (pd.DataFrame): Column to apply filter on.
        - gold_filter (str | int): Value to filter on.

        Returns:
        (int | float): Value to test on from silver.
        '''
        gold_df = self.get_gold_table_df(gold_table_name).select(gold_column_collection).filter(F.col(gold_table_column_filter) == gold_filter)
        return gold_df.collect()[0][gold_column_collection]

    def run_test(self, 
                 aggregation: Literal['avg', 'sum', 'count'],
                 filter_value: str | int,
                 
                 silver_table_column_filter: str,
                 silver_column_collection: str,
                 
                 gold_table_name: str,
                 gold_table_column_filter: str | int,
                 gold_column_collection: str,

                ) -> None:
        '''
        Compares filtered silver and gold values based on aggregation method.

        Parameters:
        - aggregation (str): Aggregation type.
        - filter_value (str | int): Value to filter on.
        - silver_table_column_filter (str): Column to apply filter on silver.
        - silver_column_collection (str): Column to collect value to test on from silver.
        - gold_table_name (str): Name of gold table.
        - gold_table_column_filter (str | int): Column to apply filter on gold.
        - gold_column_collection (str): Column to collect value to test on from gold.

        Returns:
        None
        '''
        aggregation = aggregation.lower()

        # get silver comparison
        silver_filtered_df = self.read_filtered_silver(silver_table_column_filter, filter_value)

        silver_value = self.select_aggregate_calculation(aggregation, silver_filtered_df, silver_column_collection)
        if isinstance(silver_value, float):
            silver_value = round(silver_value, 2)
        
        # get gold value
        gold_value = self.gold_value_collection(gold_table_name, gold_column_collection, gold_table_column_filter, filter_value)
        gold_value = 0 if gold_value is None else gold_value

        print(f"{'-'*50}")
        print(f"Validating the '{gold_table_name}' table and filtering the column '{silver_table_column_filter}' = '{filter_value}'")
        print(f"The gold '{gold_table_name}' filtering the column '{gold_table_column_filter}' = '{filter_value}'")
        print(f"{'Silver':6s}: {silver_value} ({aggregation})")
        print(f"{'Gold':6s}: {gold_value} ({aggregation})")