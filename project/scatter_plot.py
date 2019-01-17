"""
    analy_api scatter_plot
    Created by  User : woodnata
    Date: 2019/1/8
    Time: 下午 2:56
"""
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.functions as F
from pyspark.sql.types import *
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import io


class StatisticAnaly:
    """
    def cohort_plot : for cohort x column and aggregate y column {data_set_name: {cohort_name: {x:x, y:y}}
    def scatter_plot : plot x column as x-axis and y column as y-axis {data_set_name: {aggregate_type: {x:x, y:y}}
    def multi_scatter_plot : plot multiple scatter_plot in one plot
    """

    def __init__(self, df, spark):
        # {data_set_name: {cohort_name: {x:x, y:y}}
        # {data_set_name: {aggregate_type: {x:x, y:y}}
        self.data_set = {}
        self.df = df
        self.spark = spark

    def cohort_data(self, cohort_column, x_column, y_column, aggregate_type):
        data_set_name = cohort_column + "_" + x_column + "_" + aggregate_type + "_" + y_column
        if data_set_name not in self.data_set:
            self.data_set[data_set_name] = {}
            plot_df = self.df.select(cohort_column, x_column, y_column)
            plot_name = [x.asDict()[cohort_column] for x in self.df.select(cohort_column).distinct().toLocalIterator()]

            sql = "SELECT " + cohort_column + "," + x_column + "," + aggregate_type + "(" + y_column + ") as " + aggregate_type + "_of_" + y_column \
                  + " FROM temp GROUP BY " + cohort_column + "," + x_column \
                  + " ORDER BY " + x_column

            plot_df.createOrReplaceTempView("temp")
            plot_df = self.spark .sql(sql)
            for name in plot_name:
                x = []
                y = []
                datas = plot_df.filter(F.col(cohort_column) == name).drop(cohort_column).toLocalIterator()
                [(x.append(data.asDict()[x_column]),
                  y.append(data.asDict()[aggregate_type + "_of_" + y_column])) for data in datas]
                self.data_set[data_set_name][name] = {"x": x, "y": y}
        return self.data_set[data_set_name]

    def scatter_data(self, x_column, y_column, aggregate_type):
        data_set_name = "easy_scatter_" + x_column + "_" + aggregate_type + "_" + y_column
        if data_set_name not in self.data_set:
            self.data_set[data_set_name] = {}
            plot_df = self.df.select(x_column, y_column)
            if aggregate_type is not "point_data":
                sql = "SELECT {0},{2}({1}) as {1} FROM temp GROUP BY {0} ORDER BY {0}".format(x_column, y_column,
                                                                                              aggregate_type)
                plot_df.createOrReplaceTempView("temp")
                plot_df = self.spark .sql(sql)

            x = []
            y = []
            [(x.append(data.asDict()[x_column]), y.append(data.asDict()[y_column])) for data in plot_df.collect()]
            self.data_set[data_set_name][aggregate_type] = {"x": x, "y": y}
        return self.data_set[data_set_name]

    def cohort_plot(self, cohort_column, x_column, y_column=None, plot_type=["scatter"], aggregate_type="count"):
        """
        :param cohort_column: type = String; the cohort

        :param x_column: type = String; the x

        :param y_column: type = String; the y

        :param plot_type: type = List of String; you can choose ["scatter", "line", "bar", "pie"]

        :param aggregate_type: type = String; you can choose ["count", "sum", "avg", "min", "max"], Default is "count"

        :return: nothing

        :example:
        df_plot = StatisticAnaly(df)
        df_plot.cohort_plot("Gender", "Age", "Age", ["scatter", "line", "bar"], aggregate_type="count")
        """
        if y_column is None:
            y_column = x_column
        datas = self.cohort_data(cohort_column, x_column, y_column, aggregate_type)
        size = str(len(datas)) + "1"
        count = 1
        plt.figure(figsize=(8, 6 * int(size[:-1])))
        for name, data in datas.items():
            plt.subplot(int(size + str(count)))
            if "bar" in plot_type:
                plt.bar(data["x"], data["y"], label=y_column, color='coral')
            if "line" in plot_type:
                plt.plot(data["x"], data["y"], label=y_column, color='green')
            if "scatter" in plot_type:
                plt.scatter(data["x"], data["y"], marker="o", label=y_column, color='blue')
            if "pie" in plot_type:
                plt.pie(data["y"], autopct="%1.1f%%", labels=data["x"])
            plt.title(
                cohort_column + "_" + name + " vs " + x_column + "({})".format(aggregate_type + "_of_" + y_column),
                fontsize=20)
            count += 1
        plt.tight_layout()
        plt.show()

    def scatter_plot(self, x_column, y_column, plot_type=["point_data"], aggregate_type="scatter"):
        """
        :param x_column: type = String; the x

        :param y_column: type = String; the y

        :param plot_type: type = List of String; you can choose ["scatter", "line", "bar", "pie"]

        :param aggregate_type: type = String; you can choose ["point_data", "count", "sum", "avg", "min", "max"], Default is "point_data"
        if the data scale is large, carefully using "scatter"

        :return: nothing

        :example:
        df_plot = StatisticAnaly(df)
        df_plot.scatter_plot("Occupation", "Purchase", ["scatter"], aggregate_type="scatter")
        """
        data = self.scatter_data(x_column, y_column, aggregate_type)[aggregate_type]
        plt.figure(figsize=(8, 6))
        if "bar" in plot_type:
            plt.bar(data["x"], data["y"], color='coral')
        if "line" in plot_type:
            plt.plot(data["x"], data["y"], color='green')
        if "scatter" in plot_type:
            plt.scatter(data["x"], data["y"], marker="o", label=y_column, color='blue')
        if "pie" in plot_type:
            plt.pie(data["y"], autopct="%1.1f%%", labels=data["x"])
        plt.title(x_column + " vs " + aggregate_type + "_of_" + y_column, fontsize=20)
        plt.show()

    def multi_scatter_plot(self, x_column, y_params):
        """
        :param x_column: type = String; the x

        :param y_params: type = list of tuple; the y,
        you can choose ["scatter", "count", "sum", "avg", "min", "max"] as scatter type
        you can choose ["scatter", "line", "bar", "pie"] as plot type

        look like [("y column name1", "aggregate type1", "plot type1"), ("y column name2", "aggregate type2", "plot_type2")]

        :return: nothing

        :example:
        df_plot = StatisticAnaly(df)
        df_plot.multi_scatter_plot("Occupation", [("Purchase", "avg", "line"), ("Purchase", "point_data", "scatter")])
        """
        plt.figure(figsize=(8, 6))
        for column, aggregate_type, plot_type in y_params:
            data = self.scatter_data(x_column, column, aggregate_type)[aggregate_type]
            if "bar" in plot_type:
                plt.bar(data["x"], data["y"], label=aggregate_type + " of " + column, color='coral')
            if "line" in plot_type:
                plt.plot(data["x"], data["y"], label=aggregate_type + " of " + column, color='green')
            if "scatter" in plot_type:
                plt.scatter(data["x"], data["y"], marker="o", label=aggregate_type + " of " + column, color='blue')
            if "pie" in plot_type:
                plt.pie(data["y"], autopct="%1.1f%%", labels=data["x"])
        plt.title(x_column + " vs " + ", ".join(set(map(lambda x: x[0], y_params))), fontsize=20)
        plt.legend(loc='upper left')
        plt.show()

    def cohort_fig(self, cohort_column, x_column, y_column=None, plot_type=["scatter"], aggregate_type="count"):
        """
        :param cohort_column: type = String; the cohort

        :param x_column: type = String; the x

        :param y_column: type = String; the y

        :param plot_type: type = List of String; you can choose ["scatter", "line", "bar", "pie"]

        :param aggregate_type: type = String; you can choose ["count", "sum", "avg", "min", "max"], Default is "count"

        :return: nothing

        :example:
        df_plot = StatisticAnaly(df)
        df_plot.cohort_fig("Gender", "Age", "Age", ["scatter", "line", "bar"], aggregate_type="count")
        """
        if y_column is None:
            y_column = x_column
        datas = self.cohort_data(cohort_column, x_column, y_column, aggregate_type)
        size = str(len(datas)) + "1"
        count = 1
        fig = Figure(figsize=(8, 6 * int(size[:-1])))
        for name, data in datas.items():
            plot = fig.add_subplot(int(size + str(count)))
            if "bar" in plot_type:
                plot.bar(data["x"], data["y"], label=y_column, color='coral')
            if "line" in plot_type:
                plot.plot(data["x"], data["y"], label=y_column, color='green')
            if "scatter" in plot_type:
                plot.scatter(data["x"], data["y"], marker="o", label=y_column, color='blue')
            if "pie" in plot_type:
                plot.pie(data["y"], autopct="%1.1f%%", labels=data["x"])
            plot.title(
                cohort_column + "_" + name + " vs " + x_column + "({})".format(aggregate_type + "_of_" + y_column),
                fontsize=20)
            count += 1
        fig.tight_layout()
        canvas = FigureCanvas(fig)
        output = io.BytesIO()
        canvas.print_png(output)
        return output

    def scatter_fig(self, x_column, y_column, plot_type=["point_data"], aggregate_type="scatter"):
        """
        :param x_column: type = String; the x

        :param y_column: type = String; the y

        :param plot_type: type = List of String; you can choose ["scatter", "line", "bar", "pie"]

        :param aggregate_type: type = String; you can choose ["point_data", "count", "sum", "avg", "min", "max"], Default is "point_data"
        if the data scale is large, carefully using "scatter"

        :return: nothing

        :example:
        df_plot = StatisticAnaly(df)
        df_plot.scatter_fig("Occupation", "Purchase", ["scatter"], aggregate_type="scatter")
        """
        data = self.scatter_data(x_column, y_column, aggregate_type)[aggregate_type]
        fig = Figure(figsize=(8, 6))
        plot = fig.add_subplot(1, 1, 1)
        if "bar" in plot_type:
            plot.bar(data["x"], data["y"], color='coral')
        if "line" in plot_type:
            plot.plot(data["x"], data["y"], color='green')
        if "scatter" in plot_type:
            plot.scatter(data["x"], data["y"], marker="o", label=y_column, color='blue')
        if "pie" in plot_type:
            plot.pie(data["y"], autopct="%1.1f%%", labels=data["x"])
        fig.title(x_column + " vs " + aggregate_type + "_of_" + y_column, fontsize=20)
        canvas = FigureCanvas(fig)
        output = io.BytesIO()
        canvas.print_png(output)
        return output

    def multi_scatter_fig(self, x_column, y_params):
        """
        :param x_column: type = String; the x

        :param y_params: type = list of tuple; the y,
        you can choose ["scatter", "count", "sum", "avg", "min", "max"] as scatter type
        you can choose ["scatter", "line", "bar", "pie"] as plot type

        look like [("y column name1", "aggregate type1", "plot type1"), ("y column name2", "aggregate type2", "plot_type2")]

        :return: nothing

        :example:
        df_plot = StatisticAnaly(df)
        df_plot.multi_scatter_fig("Occupation", [("Purchase", "avg", "line"), ("Purchase", "point_data", "scatter")])
        """
        fig = Figure(figsize=(8, 6))
        plot = fig.add_subplot(1, 1, 1)
        for column, aggregate_type, plot_type in y_params:
            data = self.scatter_data(x_column, column, aggregate_type)[aggregate_type]
            if "bar" in plot_type:
                plot.bar(data["x"], data["y"], label=aggregate_type + " of " + column, color='coral')
            if "line" in plot_type:
                plot.plot(data["x"], data["y"], label=aggregate_type + " of " + column, color='green')
            if "scatter" in plot_type:
                plot.scatter(data["x"], data["y"], marker="o", label=aggregate_type + " of " + column, color='blue')
            if "pie" in plot_type:
                plot.pie(data["y"], autopct="%1.1f%%", labels=data["x"])
        fig.title(x_column + " vs " + ", ".join(set(map(lambda x: x[0], y_params))), fontsize=20)
        fig.legend(loc='upper left')
        canvas = FigureCanvas(fig)
        output = io.BytesIO()
        canvas.print_png(output)
        return output

    def cohort_query(self, cohort_column, x_column, y_column=None, plot_type=["scatter"], aggregate_type="count"):
        """
        :param cohort_column: type = String; the cohort

        :param x_column: type = String; the x

        :param y_column: type = String; the y

        :param plot_type: type = List of String; you can choose ["scatter", "line", "bar", "pie", "column", "area", "doughnut"]

        :param aggregate_type: type = String; you can choose ["count", "sum", "avg", "min", "max"], Default is "count"

        :return: nothing

        :example:
        df_plot = StatisticAnaly(df)
        df_plot.cohort_query("Gender", "Age", "Age", ["scatter", "line", "bar"], aggregate_type="count")
        """
        plots = []
        if y_column is None:
            y_column = x_column
        datas = self.cohort_data(cohort_column, x_column, y_column, aggregate_type)
        for name, data in datas.items():
            plot = {"title": {"text": cohort_column + "_" + name + " vs " + x_column + "({})".format(aggregate_type + "_of_" + y_column)},
                    "data": []}
            if "scatter" in plot_type:
                plot["data"].append({
                        "type": "scatter",
                        "toolTipContent": "{label}: {y}",
                        "dataPoints": list(map(lambda x: {"label": x[0], "y": x[1]}, zip(data["x"], data["y"])))
                    })
            if plot_type in ["line", "area", "bar", "column"]:
                plot["axisY"] = {"title": y_column, "includeZero": True}
                plot["axisX"] = {"interval": 1}
                plot["data"].append({
                    "type": plot_type,
                    "toolTipContent": "{label}: {y}",
                    "dataPoints": list(map(lambda x: {"label": x[0], "y": x[1]}, zip(data["x"], data["y"])))
                })
            if "pie" in plot_type:
                plot["data"].append({
                    "type": "pie",
                    "indexLabel": "{label}: {y}%",
                    "toolTipContent": "{label}: {y}%",
                    "dataPoints": list(map(lambda x: {"label": x[0], "y": x[1]}, zip(data["x"], data["y"])))
                })
            if "doughnut" in plot_type:
                plot["data"].append({
                    "type": "doughnut",
                    "indexLabel": "{label}: {y}%",
                    "toolTipContent": "{label}: {y}%",
                    "dataPoints": list(map(lambda x: {"label": x[0], "y": x[1]}, zip(data["x"], data["y"])))
                })
            plots.append(plot)

        return plots

    def multi_scatter_query(self, x_column, y_params):
        """
        :param x_column: type = String; the x

        :param y_params: type = list of tuple; the y,
        you can choose ["scatter", "count", "sum", "avg", "min", "max"] as scatter type
        you can choose ["scatter", "line", "bar", "pie", "column", "area", "doughnut"] as plot type

        look like [("y column name1", "aggregate type1", "plot type1"), ("y column name2", "aggregate type2", "plot_type2")]

        :return: nothing

        :example:
        df_plot = StatisticAnaly(df)
        df_plot.multi_scatter_query("Occupation", [("Purchase", "avg", "line"), ("Purchase", "point_data", "scatter")])
        """
        plot = {"title": {"text": x_column + " vs " + ", ".join(set(map(lambda x: x[0], y_params)))},
                "data": []}
        for column, aggregate_type, plot_type in y_params:
            data = self.scatter_data(x_column, column, aggregate_type)[aggregate_type]
            if "scatter" in plot_type:
                plot["data"].append({
                        "type": "scatter",
                        "toolTipContent": "{label}: {y}",
                        "dataPoints": list(map(lambda x: {"label": x[0], "y": x[1]}, zip(data["x"], data["y"])))
                    })
            if plot_type in ["line", "area", "bar", "column"]:
                plot["axisY"] = {"title": column, "includeZero": True}
                plot["axisX"] = {"interval": 1}
                plot["data"].append({
                    "type": plot_type,
                    "toolTipContent": "{label}: {y}",
                    "dataPoints": list(map(lambda x: {"label": x[0], "y": x[1]}, zip(data["x"], data["y"])))
                })
            if "pie" in plot_type:
                plot["data"].append({
                    "type": "pie",
                    "indexLabel": "{label}: {y}%",
                    "toolTipContent": "{label}: {y}%",
                    "dataPoints": list(map(lambda x: {"label": x[0], "y": x[1]}, zip(data["x"], data["y"])))
                })
            if "doughnut" in plot_type:
                plot["data"].append({
                    "type": "doughnut",
                    "indexLabel": "{label}: {y}%",
                    "toolTipContent": "{label}: {y}%",
                    "dataPoints": list(map(lambda x: {"label": x[0], "y": x[1]}, zip(data["x"], data["y"])))
                })
        return [plot]

