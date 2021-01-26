# lightdf - Light-weighted Dataframe

lightdf is a dataframe for controlling 2D-table-structure data. It only provides methods that users will use them in 80% times so that it can keep in light weight. Plus, lightdf can handle datatime keys and values.

## Methods Discovery

**func |** new ( keyName: `str`, keyType: `type`, cols: `dict` ) **->** Dataframe: `object`

**func |** from_dict ( d: `dict`, keyName: `str`, keyType: `type` ) **->** Dataframe: `object`

**func |** from_list ( li: `list` ) **->** Dataframe: `object`

**func |** from_csv ( path: `str` ) **->** Dataframe: `object`

> Users cannot access Dataframe class directly.

**class |** Dataframe ( keyName: `str`, keyType: `type` )

* **func |** listKeys( ) **->** keys: `list`
* **func |** listCols( ) **->** cols: `dict`
* **func |** getData( key: `any`, column: `any` ) **->** value: `any`
* **func |** writeData( key: `any`, column: `any`, value: `any` )
* **func |** dropData( key: `any` )
* **func |** addCol( column: `str`, dataType: `type` )
* **func |** dropCol( column: `str` )
* **func |** changeColType( column: `str`, newType: `type` )
* **func |** changeKeyType( newType: `type` )
* **func |** to_list( ) **->** list: `list`
* **func |** to_dict( ) **->** dict: `dict`
* **func |** to_csv( path: `str` )
