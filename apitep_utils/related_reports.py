import pandas as pd
import os
import plotly.express as px
import plotly as py
from enum import Enum


class RelatedReport:
    numerical_html_files = []
    categorical_html_files = []

    class TargetFeatureType(Enum):
        Categorical = "categorical"
        Numerical = "numerical"

    def generate_related_report(self, ds: pd.DataFrame, name: str, target_feature: str, path: str,
                                target_feature_type: TargetFeatureType):
        if target_feature_type is self.TargetFeatureType.Categorical:
            if not os.path.exists(path):
                os.makedirs(path + '/individual_reports')
            elif not os.path.exists(path + '/individual_reports'):
                os.makedirs(path + '/individual_reports')

            self.generate_numeric_plots(ds, path, target_feature)

            self.generate_categorical_plots(ds, path, target_feature)

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
        else:
            raise NotImplementedError

    def generate_numeric_plots(self, ds: pd.DataFrame, path: str, target_feature: str):
        ds_numeric = ds.select_dtypes(include=['int64', 'float64'])
        for col in ds_numeric:
            fig_histogram = RelatedReport.generate_histogram(ds, col, target_feature)
            fig_boxplot = RelatedReport.generate_boxplot(ds, col, target_feature)
            py.offline.plot(fig_histogram, filename=path + '/individual_reports' + '/histogram_' + col + '.html')
            py.offline.plot(fig_boxplot, filename=path + '/individual_reports' + '/boxplot_' + col + '.html')
            self.numerical_html_files.append(path + '/individual_reports' + '/histogram_' + col + '.html')
            self.numerical_html_files.append(path + '/individual_reports' + '/boxplot_' + col + '.html')

    def generate_categorical_plots(self, ds: pd.DataFrame, path: str, target_feature: str):
        ds_cat = ds.select_dtypes(include=['category', 'object'])
        for col in ds_cat:
            fig_barplot = self.generate_histogram(ds, col, target_feature)
            py.offline.plot(fig_barplot, filename=path + '/individual_reports' + '/barplot' + col + '.html')
            self.categorical_html_files.append(path + '/individual_reports' + '/barplot' + col + '.html')

    @staticmethod
    def generate_histogram(ds: pd.DataFrame, col: str, target_feature: str):
        fig = px.histogram(
            ds,
            x=col,
            color=target_feature,
            labels=(dict(x=col.lower())),
            title="Histogram of " + col.lower() + " and " + target_feature.lower())
        return fig

    @staticmethod
    def generate_boxplot(ds: pd.DataFrame, col: str, target_feature: str):
        fig = px.box(
            ds,
            x=target_feature,
            y=col,
            color=target_feature)
        return fig
