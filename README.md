# lightdf - Light-weighted Dataframe

lightdf is a simple class object of dataframe. Comparing to pandas, it provides less methods but higher performance so that it is a good data container for non data science use cases.

## Create a new lightdf

lightdf is a object managing multiple column objects. Each column object manage a list of data and its data type. To create a new dataframe, users must define the name and data type of the key column and the value columns.

The options of data types are `int`, `float`, `str` and `bool`. Every writing data to the column, column object will check the data type of inputs and try to convert it to fit the column setting. Users can input `None` for the data type setting, which column object will skip the data type checking.

Furthermore, users can set the column be `unique` or not `none`. For column set to be not `none`, column will reject the `None` as value and initialize value basing on data type (e.g. 0 for `int` type) instead of using `None`. For column set to be `unique`, the column will set to not `none` and not allow duplicuted values.

```python
import lightdf

df = lightdf.Dataframe("id", int) # name the key column, define the data type of keys

# define the columns
df.add_col("name", str, unique = True) # set unique
df.add_col("gender", str, none = False) # set not none
df.add_col("age", None) # not control the data type
```

## Read, Write and Delete Data

Users should use key value and column name to read and write values.

```python
# write new data
df.write(1, "name", "Nelson") # .write(key_value, column_name, value_to_write)
df.write(1, "gender", "M")
df.write(1, "age", 25)
df.write(2, "name", "John")
df.write(2, "gender", "M")
df.write(2, "age", 30)

# read data
name = df.write(1, "name") # .read(key_value, column_name)
print(name)

# drop (entire row)
df.drop(1) # .delete(key_value)
```

Get key values list:

```python
key_values = df.get_key_list()
```

Get column names list:

```python
key_values = df.get_col_list()
```

Get the data type of a column:

```python
data_type = df.get_data_type("col_name")
```

Get a row of data as list:

```python
data = df.get_row_data("key_value")
```

Get a column of data as list:

```python
data = df.get_col_data("col_name")
```

## Control Columns

Add and drop a column:

```python
# add column
df.add_col("col_name", str)

# drop column
df.drop_col("col_name")
```

Set data type of object:

```python
df.set_data_type("col_name", int)
```

Set column unique or not none:

```python
df.set_unique("col_name", True)
df.set_not_none("col_name", False)
```

## Control Keys

Users can select a column to replace the key column, the selected column must contains no duplicated values. If `keep` set True, the original key column will be append to the dataframe as value column.

```python
df.set_column_to_key("col_name", True)
```

## Export Dataframe

Users can export the dataframe to 2-d list, dictionary or csv.

```python
# export to list
li = df.to_list()

# export to dictionary
d = df.to_dict()

# export to csv
df.to_csv("path_of_csv")
```

## Update Dataframe from other data format

Read the data from 2d-list, dictionary, csv or another dataframe. The values will be updated if they having same key value and column name.

```python
# You should define the dataframe before
# reading/updating data from other data format
df = lightdf.Dataframe("id", int)
df.add_col("name", str, unique = True)
df.add_col("gender", str)
df.add_col("age", int)

# update dataframe form list
df.from_list(li)

# update dataframe from dictionary
df.from_dict(d)

# update dataframe form csv
# if header is Ture, the dataframe will skip reading first row
df.from_csv("path_of_csv", ignore_header_row=True)

# you can use join function to update dataframe from another dataframe
df.from_df(self, other_df)
```

## Data Process Methods

`drop_duplicated()` drop the row with duplicated value, which will keep the first found value.

```python
df2 = df.drop_duplicated("col_name")
```

`sort()` sort the column values using python list sorting function.

```python
df.sort("col_name", ascending=True)
```
