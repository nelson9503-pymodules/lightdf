# lightdf - Light-weighted Dataframe

lightdf is a dataframe for controlling 2D-table-structure data. It acts like pandas but only includes the methods that users will use them in 80% times. Only providing essencial methods keeps lightdf in light weight so that it is a good data intermediary on simple tasks.

## Methods Discovery

**func |** new ( datalist: `list`, indexlist: `list`, headerlist: `list` )  -> lightdf: `lightdf Class`

**func |** read_dict ( d: `dict` ) -> lightdf: `lightdf Class`

**func |** read_csv ( path: `str`, header: `bool` ) -> lightdf: `lightdf Class`

**class |** lightdf

* **func |** data ( index: `str`/`int`, header: `str`/`int` ) -> data: `any`
* **func |** wdata ( index: `str`/`int`, header: `str`/`int`, value: `any` )
* **func |** list_index ( ) -> indexlist: `list`
* **func |** drop_index ( index: `str`/`int` )
* **func |** add_index ( index: `str`/`int` )
* **func |** set_index ( indexlist: `list` )
* **func |** list_header ( ) -> headerlist: `list`
* **func |** drop_header ( header: `str`/`int` )
* **func |** add_header ( header: `str`/`int` )
* **func |** set_header ( headerlist: `list` )
* **func |** to_list ( index: `bool`, header: `bool`  ) -> list: `list`
* **func |** to_dict ( ) -> dict: `dict`
* **func |** to_csv ( path: `str`, index: `bool`, header: `bool` )
* **func |** print ( )
