from pyspark.sql import SparkSession
import databricks.koalas as ks


def extract_data():
    '''
    Read the raw data 
    '''
    kdf_categories = ks.read_csv('../data/raw_data/disaster_categories.csv')
    kdf_messages = ks.read_csv('../data/raw_data/disaster_messages.csv')

    return kdf_messages,kdf_categories


def cleanse_categories(kdf:'DataFrame'):
    ''' TODO '''
    return kdf


def merge_data(kdf:'DataFrame', other_data:'DataFrame', on:str):
    return kdf.merge(right = other_data, on = on)


def validate_data(kdf:'DataFrame'):
    ''' TODO: data quality checks '''
    return kdf
    

def load_data(kdf:'DataFrame', where:str):
    ''' save the prepared data '''
    kdf.to_csv(where)



def main():

    # Create a sspark session
    spark_session = (SparkSession
                    .builder
                    .getOrCreate()
                    )

    # Extarct the raw data
    kdf_messages, kdf_categories = extract_data()

    # main etl pipeline
    (kdf_categories
    .pipe(cleanse_categories)
    .pipe(merge_data, other_data = kdf_messages, on = 'id')
    .pipe(validate_data)
    .pipe(load_data,
          where = '../data/cleansed_data.csv'
         )
    )


if __name__=='__main__':
    main()
