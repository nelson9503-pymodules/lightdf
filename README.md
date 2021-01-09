# lightdf - Methods

**func |** `new` `(` `datalist`: *list*, `indexlist`: *list*, `headerlist`: *list* `)`  `->` `lightdf`: *LightDF Class*

**func |** `read_dict` `(` `d`: *dict* `)` `->` `lightdf`: *LightDF Class*

**func |** `read_csv` `(` `path`: *str*, `header`: *bool* `)` `->` `lightdf`: *LightDF Class*

**class |** `lightdf`

* **func |** `data` `(` `index`: *str*/*int*, `header`: *str*/*int* `)` `->` `data`: *any*
* **func |** `wdata` `(` `index`: *str*/*int*, `header`: *str*/*int*, `value`: *any* `)`
* **func |** `list_index` `(` `)` `->` `indexlist`: *list*
* **func |** `drop_index` `(` `index`: *str*/*int* `)`
* **func |** `add_index` `(` `index`: *str*/*int* `)`
* **func |** `set_index` `(` `indexlist`: *list* `)`
* **func |** `list_header` `(` `)` `->` `headerlist`: *list*
* **func |** `drop_header` `(` `header`: *str*/*int* `)`
* **func |** `add_header` `(` `header`: *str*/*int* `)`
* **func |** `set_header` `(` `headerlist`: *list* `)`
* **func |** `to_list` `(` `index`: *bool*, `header`: *bool*  `)` `->` `list`: *list*
* **func |** `to_dict` `(` `)` `->` `dict`: *dict*
* **func |** `to_csv` `(` `path`: *str*, `index`: *bool*, `header`: *bool* `)`
* **func |** `print` `(` `)`
