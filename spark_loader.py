#!/opt/virtualenv/py3.5.2_env1/bin/python
import argparse
import logging
import re
import glob
import pandas
import sys
import os
from urllib.parse import urlparse

def read_csv_schema(schema_file, sep="|"):
    '''
    Read schema of CSV file.
    :param schema_file: String to pipe delimited file containing a line each for a column definition.
                        Each line contains column name, sql data type, and whether nullable.
    :param sep: str, default ‘|’
                Delimiter to use. If sep is None, the C engine cannot automatically detect the separator, but the Python parsing engine can, meaning the latter will be used and automatically detect the separator by Python’s builtin sniffer tool, csv.Sniffer. In addition, separators longer than 1 character and different from '\s+' will be interpreted as regular expressions and will also force the use of the Python parsing engine. Note that regex delimiters are prone to ignoring quoted data. Regex example: '\r\t'
    '''
    df_schema = pandas.read_csv(schema_file, sep=sep, dtype=str)
    list_fields = []
    for i in range(len(df_schema)):
        field = df_schema.iloc[i].to_dict()
        #print(field)
        m = re.search("[\t| ]*(?P<type>\w*)[\t| ]*(?P<lparenthesis>\({0,1})[\t| ]*(?P<precision>\d*)[\t| ]*,{0,1}[\t| ]*(?P<scale>\d*)[\t| ]*(?P<rparenthesis>\){0,1})[\t| ]*", field["type"])
        #print(m.groups())
        
        if "(" in m.group("lparenthesis") and ")" in m.group("rparenthesis"):
            pass
        elif "(" in m.group("lparenthesis") and not ")" in m.group("rparenthesis"):
            raise ValueError("Invalid SQL data type {}".format(field["type"]))
        elif not "(" in m.group("lparenthesis") and ")" in m.group("rparenthesis"):
            raise ValueError("Invalid SQL data type {}".format(field["type"]))
        
        dtype = field["type"].upper()
        
        if field["nullable"].lower() in ("false", "0", "f"):
            nullable = False
        elif field["nullable"].lower() in ("true", "1", "t"):
            nullable = True
        else:
            raise ValueError("Invalid nullable value {}".format(field["nullable"]))
        
        #print(field["name"], dtype, nullable, type(nullable))
        
        if "VARCHAR" in dtype or "CHAR" in dtype:
            struct_field = pyspark.sql.types.StructField(field["name"], pyspark.sql.types.StringType(), nullable=nullable)
        elif "DOUBLE" in field["type"]:
            struct_field = pyspark.sql.types.StructField(field["name"], pyspark.sql.types.DoubleType(), nullable=nullable)
        elif "FLOAT" in field["type"]:
            struct_field = pyspark.sql.types.StructField(field["name"], pyspark.sql.types.FloatType(), nullable=nullable)
        elif "INTEGER" in field["type"]:
            struct_field = pyspark.sql.types.StructField(field["name"], pyspark.sql.types.IntegerType(), nullable=nullable)
        else:
            raise ValueError("Not supported SQL data type '{}'.".format(field["type"]))

        #print(struct_field.name, struct_field.dataType, struct_field.nullable)
        list_fields.append(struct_field)

    return pyspark.sql.types.StructType(list_fields)
    

def read_csv(spark, path, schema=None, schema_file=None, sep="|", encoding=None, quote='"', escape=None,
        comment=None, header=True, ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True, nullValue=None, nanValue=None, positiveInf=None,
        negativeInf=None, dateFormat=None, timestampFormat=None, maxColumns=None,
        maxCharsPerColumn=None, maxMalformedLogPerPartition=None, multiLine=None):
    '''
    :param spark: SparkSession, the entry point to programming Spark with the Dataset and DataFrame API.
    '''
    if schema is None:
        schema = SparkCSVLoader.read_schema(schema_file)
    
    sdf = spark.read.csv(path,
                         schema=schema, 
                         sep=sep, 
                         encoding=encoding, 
                         quote=quote, 
                         escape=escape,
                         comment=comment, 
                         header=header, 
                         inferSchema=False, 
                         ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
                         ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace, 
                         nullValue=nullValue, 
                         nanValue=nanValue, 
                         positiveInf=positiveInf, 
                         negativeInf=negativeInf, 
                         dateFormat=dateFormat, 
                         timestampFormat=timestampFormat, 
                         maxColumns=maxColumns,
                         maxCharsPerColumn=maxCharsPerColumn, 
                         maxMalformedLogPerPartition=maxMalformedLogPerPartition, 
                         mode="FAILFAST",
                         columnNameOfCorruptRecord=None,
                         multiLine=multiLine)
    return sdf 
        

    
if __name__ == "__main__":
    # Execute only if run as a script
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Spark CSV loader.')
    parser.add_argument("--spark_home", default=os.getenv("SPARK_HOME", "/opt/spark-2.2.1-bin-hadoop2.7/"), type=str, 
                        help="Spark home folder (default: %(default)s).")
    parser.add_argument("--spark.master", default="local[*]", type=str, dest="spark.master",
                        help="Spark master (default: %(default)s to use system default setting).")
    # parser.add_argument("--spark.pyspark.python", default="/opt/virtualenv/py3.5.2_env1/bin/python", type=str, 
    #                     help="Spark home directory (default: %(default)s).")
    
    parser.add_argument("--src_format", type=str, default='CSV', help="Source data format (default: %(default)s).")
    parser.add_argument("--src_data_file", type=str, required=True, help="Source data file absolute local or HDFS path, which can contain shell-style wildcards.")
    parser.add_argument("--src_schema_file", type=str, required=True, help="Source data schema file absolute local path.")
    parser.add_argument("--dest_format", default="org.apache.phoenix.spark", type=str, help="Destination data format (default: %(default)s).")
    parser.add_argument('--dest_table', type=str, required=True, help="Destination table.")
    parser.add_argument('--dest_url', type=str, required=True, help="Destination URL.")
    parser.add_argument("--write_mode", default="error", type=str, help="Behavior of the upload operation when destination data already exists. (default: %(default)s).")

    dict_args = vars(parser.parse_args())

    # Get logger
    logging.basicConfig(filename=None, 
                        level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s")
    logger = logging.getLogger(__name__)

    # Get SparkSession
    logger.info("Starting Spark session using imported pyspark library from '{}'...".format(dict_args["spark_home"]))
    # Load pyspark library if not loaded
    pyspark_lib_path = os.path.join(dict_args["spark_home"], "python/")
    pyspark_lib_py4j_zip = os.path.join(dict_args["spark_home"], "python/lib/py4j-0.10.4-src.zip")
    if not pyspark_lib_path in sys.path:
        sys.path.insert(0, pyspark_lib_path)
    if not pyspark_lib_py4j_zip in sys.path:
        sys.path.insert(0, pyspark_lib_py4j_zip)
    import pyspark
    import pyspark.sql

    spark = pyspark.sql.SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext

    logger.info("Set SparkContext log level to WARN")
    sc.setLogLevel("WARN")

    # Spark configuration
    list_tmp = []
    for k,v in sc.getConf().getAll():
        list_tmp.append("{} = {}".format(k, v))
    logger.info("Spark session has been started with following configuration:\n{}".format("\n".join(list_tmp)))

    # Reading source data files
    if dict_args["src_format"].lower() == 'csv':
        logger.info("Loading from CSV data file '{}'".format(dict_args["src_data_file"]))
        schema_parse_result = urlparse(dict_args["src_schema_file"])
        logger.info("Loading from CSV data schema file '{}'".format(schema_parse_result.path))
    
        schema = read_csv_schema(schema_parse_result.path)
        list_tmp = []
        for f in schema.fields:
            list_tmp.append("StructField({},{},{})".format(f.name, f.dataType, f.nullable))
        logger.info("CSV data schema format:\n{}".format("\n".join(list_tmp)))

        sdf = read_csv(spark, 
            dict_args["src_data_file"],
            schema=schema,
            header=True,
            ignoreLeadingWhiteSpace=True,
            ignoreTrailingWhiteSpace=True,
            sep=",")

        logger.info("Sample records of CSV data file:")
        sdf.show()
        logger.info("Schema of CSV data file:")
        sdf.printSchema()
        logger.info("Total number of records: {}".format(sdf.count()))
    else:
        msg = "Source data format '{}' is not supported.".format(dict_args["src_format"])
        logger.info(msg)
        raise ValueError(msg)


    # Writing to destination
    if dict_args["dest_format"] == 'org.apache.phoenix.spark':
        logger.info("Uploading data to '{}' into table '{}' in '{}' format using '{}' write mode ...".format(dict_args["dest_url"],
            dict_args["dest_table"],
            dict_args["dest_format"],
            dict_args["write_mode"]))
        sdf.write.format("org.apache.phoenix.spark") \
            .mode(dict_args["write_mode"]) \
            .option("table", dict_args["dest_table"]) \
            .option("zkUrl", dict_args["dest_url"]) \
            .save()
        logger.info("Completed data upload to destination.")
    else:
        msg = "Destination data format '{}' is not supported".format(dict_args["dest_format"])
        logger.info(msg)
        raise ValueError(msg)
