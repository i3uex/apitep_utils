# MD4DSPRR Utils Library

Set of tools design in MD4DSPRR project, easily exportable to other data projects.

## Prerequisites
- **Python 3.10+** (virtual environment recommended)
- **Operating System:** Linux (or Windows Subsystem for Linux)

## Installation

Get a copy of this repository. Then, open a terminal in the same folder this file is and execute the command:

```shell
pip install -e .
```

This way, you will install this library in editable mode, meaning that a symlink to the library will be used instead of a copy. Any changes in the library will be instantly available without reinstalling it. Using a symlink is not a requirement, but as this library is now in constant development, it can save precious time.

### MD4DSPRR Project

MD4DSPRR Model-Driven Data Science Projects Reproducibility & Replicability is a model-driven proposal for defining pipelines with Reproducibility and Replicability (R&R). 

This framework understands the process of developing a data science project divided into the following phases: ETL (Extract Transform and Load data), Feature Engineering, Feature Selection and Analysis and Modelling.

Based on this structure, we will develop a series of classes composed of Transformations, which will allow us to structure data science projects in this way and provide a structure that maximises the reproducibility and replicability of the project.

For more details on this framework, please refer to the following paper: [Melchor, F., Rodriguez-Echeverria, R., Conejero, J.M., Prieto, Á.E., Gutiérrez, J.D. (2022). A Model-Driven Approach for Systematic Reproducibility and Replicability of Data Science Projects. In: Franch, X., Poels, G., Gailly, F., Snoeck, M. (eds) Advanced Information Systems Engineering. CAiSE 2022. Lecture Notes in Computer Science, vol 13295. Springer, Cham.](https://doi.org/10.1007/978-3-031-07472-1_9)

## License
This project is licensed under the [MIT License](https://github.com/i3uex/apitep_utils/blob/main/LICENSE.md)

## Authors and Affiliations
- __Juan D. Gutiérrez__ - Universidade de Santiago de Compostela - [juandiego.gutierrez@usc.es](mailto:juandiego.gutierrez@usc.es)
- __Fran Melchor__ - University of Extremadura - [frmelchorg@unex.es](mailto:frmelchorg@unex.es)

## Classes

### Transformation
Transformation is our base class for implementing the different data processing operations that make up our pipeline.

### Report
This class will enable us to generate reports on the input and output datasets for each phase, to see how their structure has changed after applying the different transformations.

The following classes have a series of basic functions structured according to this logic:

- __init__: initialise the class with the necessary arguments to configure the class: input_path_segments, output_path_segments, input_separator, output_separator, save_report_on_load, save_report_on_save.
- __load()__: load the datasets to be used in that class.
- __process()__: process the data by calling different functions that make up the data processing.
- __save()__: save the resulting datasets.
Each class consists of a series of basic transformations related to the corresponding pipeline stage. For example, feature selection executes a series of statistical tests in process() that allow the different variables to be selected or not.

### ETL

Common enhance, transform and load tasks.

### FeatureEngineering

Feature Engineering is the process of creating or transforming features to enhance model performance.

### FeatureSelection

Feature selection is the process of selecting a set of features based on statistical tests or other methods.

### AnalysisModeling

Analysis and modelling is the process of analysing data using machine learning models or other techniques.
