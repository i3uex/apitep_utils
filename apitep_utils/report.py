import random

import numpy as np
import plotly as py
import plotly.express as px
import plotly.graph_objs as go
from scipy import stats
import os


class Report:
    numerical_html_files = []
    categorical_html_files = []

    def generate_advanced(self, ds, name, path):
        if not os.path.exists(path):
            os.makedirs(path + '/individual_reports')
        elif not os.path.exists(path + '/individual_reports'):
            os.makedirs(path + '/individual_reports')

        self.generate_numeric_plots(ds, path)

        self.generate_categorical_plots_ploty(ds, path)

        if self.numerical_html_files or self.categorical_html_files:

            html_string = '''
                <html>
                <head>
                    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
                    <style>body{ margin:0 100; background:whitesmoke; }</style>
                </head>
                <body>
                <h1>Plots of dataframe ''' + name + ''' </h1>
                '''
            if self.numerical_html_files:
                html_string = html_string + '''<h2>Plots of numerical columns</h2>'''
                for file in self.numerical_html_files:
                    html_string = html_string + '''
                        <iframe width="1000" height="550" frameborder="0" seamless="seamless" scrolling="no" 
                        src="
                        ''' + file + '''"></iframe>'''
            if self.categorical_html_files:
                html_string = html_string + '''
                    <h2>Plots of categorical columns</h2>
                    '''
                for file in self.categorical_html_files:
                    html_string = html_string + '''
                        <iframe width="1000" height="550" frameborder="0" seamless="seamless" scrolling="no" 
                        src="
                        ''' + file + '''"></iframe>'''
            html_string = html_string + '''
                </body>
                </html>
                '''
            f = open(path + '/' + name + '_advanced_report.html', 'w')
            f.write(html_string)
            f.close()

            self.numerical_html_files = []

            self.categorical_html_files = []
        else:
            raise Exception("The dataset " + name + "no have columns of type 'category', 'int64' or 'float64' ")

    def generate_numeric_plots(self, ds, path):
        numerics = ['int64', 'float64']
        ds_numeric = ds.select_dtypes(include=numerics)
        for col in ds_numeric:
            col_numeric = ds_numeric[col].dropna()
            fig_histogram = Report.generate_histogram_ploty(ds, col)
            fig_boxplot = Report.generate_boxplot_ploty(ds, col)
            fig_qqplot = Report.generate_qqplot_ploty(col_numeric, col)
            py.offline.plot(fig_histogram, filename=path + '/individual_reports' + '/histogram_' + col + '.html')
            py.offline.plot(fig_boxplot, filename=path + '/individual_reports' + '/boxplot_' + col + '.html')
            py.offline.plot(fig_qqplot, filename=path + '/individual_reports' + '/qqplot_' + col + '.html')
            self.numerical_html_files.append(path + '/individual_reports' + '/histogram_' + col + '.html')
            self.numerical_html_files.append(path + '/individual_reports' + '/boxplot_' + col + '.html')
            self.numerical_html_files.append(path + '/individual_reports' + '/qqplot_' + col + '.html')

    def generate_categorical_plots_ploty(self, ds, path):
        ds_cat = ds.select_dtypes(include=['category', 'object'])
        for col in ds_cat:
            fig_barplot = self.generate_histogram_ploty(ds, col)
            py.offline.plot(fig_barplot, filename=path + '/individual_reports' + '/barplot' + col + '.html')
            self.categorical_html_files.append(path + '/individual_reports' + '/barplot' + col + '.html')

    @staticmethod
    def rand_web_color_hex():
        rgb = ""
        for _ in "RGB":
            i = random.randrange(0, 2 ** 8)
            rgb += i.to_bytes(1, "big").hex()
        return rgb

    @staticmethod
    def generate_histogram_ploty(ds, name):
        fig = px.histogram(
            ds,
            x=name,
            color_discrete_sequence=['#' + Report.rand_web_color_hex()],
            labels=(dict(x=name.lower())),
            title="Histogram of " + name.lower())
        return fig

    @staticmethod
    def generate_boxplot_ploty(ds, name):
        fig = px.box(
            ds,
            y=name,
            labels=(dict(y=name.lower())),
            title="Boxplot of " + name.lower())
        return fig

    @staticmethod
    def generate_qqplot_ploty(x, name):
        qq = stats.probplot(x, dist='lognorm', sparams=(1,))
        x = np.array([qq[0][0][0], qq[0][0][-1]])

        fig = go.Figure()
        fig.add_scatter(x=qq[0][0], y=qq[0][1], mode='markers')
        fig.add_scatter(x=x, y=qq[1][1] + qq[1][0] * x, mode='lines')
        fig.layout.update(showlegend=False)
        fig.update_layout(title="Q-Q Plot of " + name.lower(), xaxis_title="Theorical Quantiles",
                          yaxis_title="Sample Quantiles", )
        return fig
