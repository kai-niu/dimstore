# Nebula
The Nebula is a lightweight feature store designed to streamline DSE working pipeline by sharing high-quality features among team members and enable the minimum amount of effort to reuse the features. It designs to be extensible and versatile to accommodate different team set up on different computation platform, including Waston Studio Local.

[![Build Status](https://travis.ibm.com/Kai-Niu/nebula.svg?token=uqbL1pAUo2sCHeqp1yJV&branch=master)](https://travis.ibm.com/Kai-Niu/nebula)

# Design
<img style="float: center;" src="docs/diagrams/nebula_design_diagram.jpg">
The Nebula is built for sharing features extracted using pySpark, but works for other framwork as well, e.g., pandas. The core idea is to share the feature extraction logic instead of actual dataset. The provider pattern enables the Nebula to work with different backend persistent layer such as MongoDB, Hive, etc. Cache Layer is used to address the performance issue when the actual feature extraction operation is computationally expensive.

# Install
```
pip install Nebula  #package is not published yet
```

# Configuration
* Step 1, Use the utility function generate the store configuration json file.
```python
 from nebula import ConfigBuilder
 config = ConfigBuilder()
 config.build('./', 'foo_config')
```

* Step 2, Edit the store_config.json based on the environment. The following is an example of the generated configuration file:

```javascript
{
    "store_name": "Kai's Feature Store",
    "meta_manager": "default",
    "meta_manager_providers": {
        "default":{
            "root_dir": "/Users/kai/repository/nebula/example/storage",
            "folder_name":"catalog",
            "file_name":"catalog.nbl"
        }
    },
    "persistor_providers":{
        "default":{
            "root_dir": "/Users/kai/repository/nebula/example/storage",
            "folder_name":"features"
        }
    },
    "serializer_providers":{
        "default":{
        }
    }
}
```

# Examples

1. Create the feature store object

```python
 from nebula import Store
 from nebula import FeatureMeta
 
 # create feature store
 store = Store('store_config.json')
 
 # show store info
 store.info()
```
```
 # output
 == Kai's Feature Store Information ==
- Meta Data Manager: default
- Supported Meta Data managers:  ['flat file meta manager (default)']
- Supported Persistors:  ['flat file persistor (default)']
- Supported Serializers:  ['dill Serializer (default)']
- Supported Cache Layers:  ['None']
- Features Available:  2
```

2. Register feature into the feature store

``` python
 # init feature meta data
 feature_meta = FeatureMeta('foo_feature')
 feature_meta.author = 'Kai Niu'
 feature_meta.params = {'context':'pySpark Context, required','alpha':'the ceof of foo transform,optional'}
 feature_meta.comment = 'Dummy feature for demostration purpose only.'
 
 # register foo feature extraction logics
 store.register(feature_meta, foo_feature)
 
 # show features registered in store
 store.catalog()
```
```
 # output
 == Feature Catalog ==
 foo_scaler 	 e1c95611-cf7d-4097-9c41-4d564f8b0483 	 04, Jun 2019 	 Kai Niu
 foo_scaler 	 4c4c7b9d-aafe-494f-860e-b136db4615f7 	 05, Jun 2019 	 Kai Niu
```

3. Checkout feature from the feature store

``` python
 # init uid and params
 params = {'context':sqlcontext,'alpha':2.0}
 uid = 'e1c95611-cf7d-4097-9c41-4d564f8b0483'
 
 # check out feature by uid
 p = store.checkout(uid, params)
 p.show(3)
```
```
# output
+------------------+------------------+------------------+------------------+------------------+
|                X1|                X2|                X3|                X4|             Label|
+------------------+------------------+------------------+------------------+------------------+
|0.6251974793973963|0.2179858165402263|0.9961564637159082|0.9423549790353055|  5.05700613142282|
|0.9900334629744841|0.8166846267145481|0.6039442937781169|0.4705352882294497|6.9252293085928445|
|0.3186070198700508|0.5352212239478372|0.3459134832875863|0.6050918418285824| 4.075017743935206|
+------------------+------------------+------------------+------------------+------------------+
```

# Version
The package is still under concept-proofing phase. The plan is supported flat file and MongoDB as the persistent layers and sparks parquet as the cache layer in the release of the alpha version. As of now, the library only supports flat file as a persistent layer and no cache layer support.
