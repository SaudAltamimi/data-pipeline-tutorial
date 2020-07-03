from pyspark.sql import SparkSession
import databricks.koalas as ks
import pandas as pd


# helper function
def _split_and_expand(categories_text:str, by:str = ';'):
    '''
    A function that mimics pandas.str.split(expand=True) behavior, 
    to be applied against koalas DataFrame
    '''
    # split --> to get list of categories with their boolean values
    categories_text_splitted = categories_text.split(';')
    
    # get the categories boolean values only 
    cat_booleans = [cat.split('-')[1] 
                    for cat in categories_text_splitted
                   ]
    
    
    # expand the splitted list to multiple of columns
    cat_booleans = pd.Series(cat_booleans).astype(int)
    
    return cat_booleans


def create_spark_session():
    """
    Create spark session.

    Returns:
        spark (SparkSession) - spark session connected to AWS EMR cluster
    """
    spark = (SparkSession
            .builder
            .appName('data_pipeline_example')
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:2.7.0") 
            .getOrCreate()
        )

    return spark


def extract_data():
    '''
    Read the raw data 
    '''
    kdf_categories = ks.read_csv('s3://dendsparktut/raw_data/disaster_categories.csv')
    kdf_messages = ks.read_csv('s3://dendsparktut/raw_data/disaster_messages.csv')

    return kdf_messages,kdf_categories


def cleanse_categories(kdf:'DataFrame'):
    ''' 
    this function will do two things to cleanse categories:
    1- extract the categoreis for one row and assign them as columns
    2- extract the boolean values for each category 
    '''
    # extract columns names --> categories names
    cols = (kdf
            .categories.str.split(';')
            .apply(lambda cats_list: [cat.split('-')[0] 
                                      for cat in cats_list]
                                      )
            ).loc[0]
    
    
    # set 'id' as index to not lose it
    kdf = kdf.set_index('id')
    
    
    # extract the boolean values for each category
    kdf = (kdf
           .apply(lambda row: _split_and_expand(row.categories),
                  axis=1
                  )
          )
    
    
    # assign the new column names
    kdf.columns = cols

    return kdf.reset_index()


def merge_data(kdf:'DataFrame', other_data:'DataFrame', on:str):
    return kdf.merge(right = other_data, on = on)


def validate_data(kdf:'DataFrame'):
    ''' TODO: data quality checks '''
    return kdf
    

def load_data(kdf:'DataFrame', where:str, partition_by:list):
    ''' save the prepared data '''
    (kdf
    .to_spark()
    .write
    .partitionBy(*partition_by)
    .parquet(where)
    )



def main():

    # Create a sspark session
    spark_session = create_spark_session()

    # Extarct the raw data
    kdf_messages, kdf_categories = extract_data()

    # main etl pipeline
    (kdf_categories
    .pipe(cleanse_categories)
    .pipe(merge_data,
          other_data = kdf_messages,
          on = 'id'
          )
    .pipe(validate_data)
    .pipe(load_data,
          where = 's3://dendsparktut/cleansed_data',
          partition_by = ['genre']
         )
    )

    # stop the spark application
    spark_session.stop()


if __name__=='__main__':
    main()
