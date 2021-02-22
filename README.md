# lightdf - Light-weighted Dataframe

lightdf is a 2d-table-structure dataframe, which only provide essential functions to keep a light weight.

## Create a new lightdf

To create a new lightdf, you should define the data type for key column and data columns.

```python
import lightdf

df = lightdf.Dataframe("id", int) # you should define a key column to control dataframe

# define the columns
df.add_col("name", str, unique = True) # unique column cannot contains duplicated data
df.add_col("gender", str)
df.add_col("age", int)
```

## Read, Write and Delete Data

```python
# write new data
df.write(1, "name", "Nelson") # 1 is the id and it is the key of the data
df.write(1, "gender", "M")
df.write(1, "age", 25)

# read data
name = df.write(1, "name)
print(name)

# print out datafame
df.write(2, "name", "John")
df.write(2, "gender", "M")
df.write(2, "age", 30)
df.print()

# delete
df.delete(1) # delete row with key value = 1
df.print()
```

Outputs:
```bush
Nelson

id  | ["name", "gender", "age"]
1   | ["Nelson", "M", 25]
2   | ["John", "M", 30]

id  | ["name", "gender", "age"]
2   | ["John", "M", 30]
```

## Control Columns

```python
# add column
df.add_col("col_name", str)

# drop column
df.drop_col("col_name")

# change the data type of column
df.set_col_type("col_name", int)

# list out columns in df
column_list = df.list_col()

# list out column types in df
column_type_list = df.list_col_type()
```

## Control Keys

```python
# get the data type of keys
key_type = df.get_keys_type()

# set a column to be key column
# if keep is True, currenct keys column will append to be data column.
df.set_keys_column("col_name", True)
```

## Export Dataframe

```python
# export to list
li = df.to_list()

# export to dictionary
d = df.to_dict()

# export to csv
df.to_csv("path_of_csv")
```

## Update Dataframe from other data format

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
df.from_csv("path_of_csv", header=True)

# you can use join function to update dataframe from another dataframe
df.join_dataframe(self, other_df)
```
