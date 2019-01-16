"""
    web
    Created by  User : woodnata
    Date: 2019/1/7
    Time: 下午 2:24
"""
from flask import Flask, request, render_template, redirect, url_for, make_response
from pyspark.sql import SparkSession
import json
import pandas as pd
from project.scatter_plot import StatisticAnaly
from project.DAO.dao import read_table, tables
import sys
from argparse import ArgumentParser

parser = ArgumentParser()

parser.add_argument('--host', '--host',
                    help='Set host, Default is 127.0.0.1',
                    dest='host',
                    type=str,
                    default='127.0.0.1')

parser.add_argument('-p', '--port',
                    help='Set the port number, Default is 5000',
                    dest='port',
                    type=int,
                    default=5000)

spark = SparkSession.builder \
    .appName("VenRaaS upload") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate();

sc = spark.sparkContext
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
fs = FileSystem.get(Configuration())

app = Flask(__name__)


def _map_to_pandas(rdds):
    """ Needs to be here due to pickling issues """
    return [pd.DataFrame(list(rdds))]


def to_pandas(df, n_partitions=None):
    """
    Returns the contents of `df` as a local `pandas.DataFrame` in a speedy fashion. The DataFrame is
    repartitioned if `n_partitions` is passed.
    :param df:              pyspark.sql.DataFrame
    :param n_partitions:    int or None
    :return:                pandas.DataFrame
    """
    if n_partitions is not None:
        df = df.repartition(n_partitions)
    df_pandas = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pandas = pd.concat(df_pandas)
    df_pandas.columns = df.columns
    return df_pandas


def query_data(table_name, sql=None):
    table = read_table(table_name)
    if table["type"] == "parquet":
        df = spark.read.parquet(path=table["path"])
    elif table["type"] == "csv":
        df = spark.read.csv(path=table["path"], sep=table["sep"], header=table["header"])
    elif table["type"] == "json":
        df = spark.read.json(path=table["path"])
    else:
        pass

    if sql is not None:
        df.createOrReplaceTempView(table_name)
        df = spark.sql(sql)

    return df


def description(df):
    schema_df = pd.DataFrame([{'name': data['name'],
                               'type': data['type'],
                               'nullable': data['nullable']} for data in df.schema.jsonValue()['fields']])
    schema_df = schema_df.set_index('name')
    describe_df = to_pandas(df.describe())
    describe_df = describe_df.set_index('summary')
    describe_df = describe_df.T
    describe_df = pd.concat([schema_df, describe_df], axis=1)
    describe_df = describe_df.reset_index()
    return describe_df


@app.route("/tables", methods=["POST"])
def draw_tables():
    if request.method == 'POST':
        results = request.form
        print(results)
        if results["submit"] == 'plot':
            if (results["x_1"] == 'not use') or (results["y_1"] == 'not use'):
                return False

            if 'sql' in request.cookies:
                sql = request.cookies.get('sql')
                table_name = sql.split(" ")[sql.lower().split(" ").index("from") + 1]
                df = query_data(table_name, sql)
                lines = int(request.cookies.get('lines'))
                data = to_pandas(df.limit(lines))
            else:
                print("full table")
                sql = ""
                table_name = request.cookies.get('table_name')
                df = query_data(table_name)
                data = description(df)
            columns = ["not use"]
            columns.extend(df.columns)
            plot_object = StatisticAnaly(df, spark)

            y_params = []
            for k, v in results.items():
                key = k.split("_")
                if key[0] == "y":
                    y_params.append(v)
                    continue
                if key[0] == "x":
                    x_column = v
                if key[0] == "cohortColumn":
                    cohort_column = v
            y_params = [tuple(y_params[i:i + 3]) for i in range(0, len(y_params), 3) if y_params[i] is not "not use"]
            if cohort_column != "not use":
                ploting_datas = plot_object.cohort_query(cohort_column=cohort_column,
                                                         x_column=x_column,
                                                         y_column=y_params[0][0],
                                                         plot_type=y_params[0][2],
                                                         aggregate_type=y_params[0][1])
            else:
                ploting_datas = plot_object.multi_scatter_query(x_column, y_params)
            ploting_datas = [json.dumps(x) for x in ploting_datas]
            print(ploting_datas)
            # return
            return render_template('index.html',
                                   tables=[data.to_html(classes='page')],
                                   titles=[table_name],
                                   table_name=tables(),
                                   names=['na', 'table_name'],
                                   sql=sql,
                                   options=columns,
                                   selector=["select"],
                                   aggregate_type=["count", "sum", "avg", "min", "max", "point_data"],
                                   plot_type=["scatter", "line", "area", "column", "bar", "pie", "doughnut"],
                                   ploting_datas=ploting_datas)


@app.route("/tables")
def show_tables():
    sql = request.cookies.get('sql')
    lines = int(request.cookies.get('lines'))
    print(sql.lower().split(" "))
    table_name = sql.split(" ")[sql.lower().split(" ").index("from") + 1]
    df = query_data(table_name, sql)
    data = to_pandas(df.limit(lines))
    columns = ["not use"]
    columns.extend(df.columns)
    return render_template('index.html',
                           tables=[data.to_html(classes='page')],
                           titles=[table_name],
                           table_name=tables(),
                           names=['na', 'table_name'],
                           sql=sql,
                           options=columns,
                           selector=["select"],
                           aggregate_type=["count", "sum", "avg", "min", "max", "point_data"],
                           plot_type=["scatter", "line", "area", "column", "bar", "pie", "doughnut"])


@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == 'POST':
        results = request.form
        if (results["submit"] == 'search') & \
           (results["sql_input"] != '') & \
           ({"from", "select"} < set(results["sql_input"].lower().split(" "))):
            response = make_response(redirect(url_for('show_tables')))
            response.delete_cookie('sql')
            response.delete_cookie('lines')
            response.set_cookie('sql', results["sql_input"].replace("\r\n", " "))
            response.set_cookie('lines', results["lines"])
            return response

        elif results["submit"] != 'search':
            df = query_data(results["submit"])
            describe_df = description(df)
            columns = ["not use"]
            columns.extend(df.columns)
            response = make_response(render_template('index.html',
                                     table_name=tables(),
                                     names=['na', 'table_name'],
                                     tables=[describe_df.to_html(classes='page')],
                                     titles=['na', results["submit"]],
                                     options=columns,
                                     selector=["select"],
                                     aggregate_type=["count", "sum", "avg", "min", "max", "point_data"],
                                     plot_type=["scatter", "line", "area", "column", "bar", "pie", "doughnut"]))
            response.delete_cookie('sql')
            response.delete_cookie('lines')
            response.delete_cookie('table_name')
            response.set_cookie('table_name', results["submit"])

            return response

    return render_template('index.html', table_name=tables(), names=['na', 'table_name'])


if __name__ == "__main__":
    args = parser.parse_args()
    app.run(host=args.host, port=args.port, debug=True)



