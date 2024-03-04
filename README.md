# MapReduce vs. Apache Spark Comparison using Airline Delay Dataset

This repository compares the performance of MapReduce and Apache Spark using the Airline Delay dataset. The dataset provides information about flight delays, which is analyzed using both MapReduce and Spark to assess their efficiency in handling large-scale data processing tasks.

## Dataset
The dataset used for this comparison can be found on Kaggle: [Airline Delay EDA Exploratory Data Analysis](https://www.kaggle.com/code/adveros/flight-delay-eda-exploratory-data-analysis/notebook)

## Prerequisites:
- Create a S3 bucket and upload the dataset
- Create two EMR clusters. One for MapReduce and the other for Spark
- Both clusters should be in the waiting state

## MapReduce Directory
This directory contains scripts and instructions related to MapReduce implementation:

- **delay_analysis.hql**: HiveQL script for running queries over the dataset. It requires providing `INTERATION_NO` as CLI arguments, indicating the iteration number. This script will be added as a step and repeated five times by changing the `INTERATION_NO` on the EMR Hadoop Cluster.

## Spark Directory
This directory contains scripts and instructions related to Apache Spark implementation:

- **delay_analysis.py**: Python script for running queries over the dataset using Apache Spark. Each query is run five times. Add this query as a step and repeat it on the EMR Spark Cluster.

## Output Directory
Contains the output folder which was created after running the scripts for both MapReduce and Spark.

## Screenshots Directory
Contains graphs showing comparison using time taken between MapReduce and Spark.

- **graph_analysis.ipynb**: Jupyter notebook containing aggregated outputs and plot graph comparisons.

This repository aims to provide insights into the performance differences between MapReduce and Apache Spark in processing the given dataset. Screenshots and analysis are provided to visualize and interpret the results effectively.
