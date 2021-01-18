# APITEP Utils Library

Set of tools used by APITEP project, easily exportable to other data projects.

## Installation

Get a copy of this repository. Then, open a terminal in the same folder this file is and execute the command:

```shell
pip install -e .
```

This way, you will install this library in editable mode, meaning that a symlink to the library will be used instead of a copy. Any changes in the library will be instantly available without reinstalling it. Using a symlink is not a requirement, but as this library is now in constant development, it can save precious time.

## Classes

- **DatasetSubsampler**: get a random subsample of a CSV dataset, either especifying the number of rows or a percentage of them. 
- **Timestamp**: generate timestamps. You can use them to add a suffix to a filename, for example.
