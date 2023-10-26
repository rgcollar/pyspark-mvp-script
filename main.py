from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()

    l_file_paths = ["df_part1.csv", "df_part2.csv", "df_part3.csv"]

    final_df = join_files(l_file_paths, spark, append = True)

    print(final_df.show())

    spark.stop()


def join_files(file_list, spark, append = True):
    
    l_df = []
    for file in file_list:
        df = spark.read.csv(file)
        l_df.append(df)

    final_df = l_df[0]

    for df in l_df[1:]:
        # if need to append
        if append:
            final_df = final_df.union(df)
        
        # if need to join
        else:
            final_df = final_df.select(*df.columns)
            final_df = final_df.union(df.select(*df.columns))

    return final_df


if __name__ == '__main__':
    main()
