# APITEP Utils Library

Set of tools used by APITEP project, easily exportable to other data projects.

## Installation

Get a copy of this repository. Then, open a terminal in the same folder this file is and execute the command:

```shell
pip install -e .
```

This way, you will install this library in editable mode, meaning that a symlink to the library will be used instead of a copy. Any changes in the library will be instantly available without reinstalling it. Using a symlink is not a requirement, but as this library is now in constant development, it can save precious time.

## Classes

### DatasetSubsampler

- Get a subsample of a CSV dataset, either specifying the number of rows or a percentage of them.
- The dataset must have a header row. This row won't count towards the subset.
- Choose random values or number of rows from the beginning.
- Let the class generate the output file name for you or choose one yourself.
- If the number of rows or percentage is 0 or less, no subsample will be generated.
- If the number of rows is greater than the total number of rows, or the percentage is greater than 100%, the whole dataset will be copied.

### Timestamp

Generate timestamps. You can use them to add a suffix to a filename, for example.
