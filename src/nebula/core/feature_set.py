"""
"
" class represents the set of features and provide CRUD functions
" the idea is similar to the object mapping to data in database.
"
"""

class FeatureSet():
    def __init__(self, store_proxy, ufd = None):
        if ufd != None and isinstance(ufd,dict):
            self.__ufd__ = ufd
        else:
            self.__ufd__ = {}
        if store_proxy == None:
            raise Exception('> FeatureSet init: Store Proxy object can not be None!')
        else:
            self.__store_proxy__ = store_proxy

    """
    "
    " filter features by tags
    "
    """
    def tags(self, include=None, exclude=None):
        """
        @param::include: a list of tags which the associated feature will be included
        @param::exclude: a list of tags which the associated feature will be excluded
        return self object to enable chaining function call
        """
        if self.__ufd__ != None and len(self.__ufd__) > 0:
            if include != None:
                include_set = set(include)
                tag_include_filter = lambda f: len(f.tags.intersection(include_set)) > 0
                self.__query__(filter=tag_include_filter)
            if exclude != None:
                exclude_set = set(exclude)
                tag_exclude_filter = lambda f: len(f.tags.intersection(exclude_set)) == 0
                self.__query__(filter=tag_exclude_filter)
        return self

    """
    "
    " select feature by filter functions
    "
    """
    def select(self, keep=None, exclude=None, filter=None):
        """
        @param::include: a list of feature will be included
        @param::exclude: a list of feature will be excluded
        @param::filter_func: filter function which return boolean
        return self object to enable chaining function call
        """
        if self.__ufd__ != None and len(self.__ufd__) > 0:
            if keep != None:
                include_feature_set = set(keep)
                include_filter = lambda f: f.name in include_feature_set
                self.__query__(filter=include_filter)
            if exclude != None:
                exclude_feature_set = set(exclude)
                exclude_filter = lambda f: f.name not in exclude_feature_set
                self.__query__(filter=exclude_filter)
            if filter != None:
                self.__query__(filter=filter)
        return self
    
    """
    "
    " build dataset from selected features
    "
    """
    def build(self, dataframe='pyspark', verbose=True, **kwargs):
        return self.__store_proxy__.build(self.__ufd__, dataframe=dataframe, verbose=verbose, **kwargs)

    """
    "
    " render the list of features to output
    "
    """
    def show(self):
        """
        empty param intended
        return self object to enable chaining function call
        """
        self.__store_proxy__.list_features(self.__ufd__)
        return self

    """
    "
    " union two featureset together
    "
    """
    def union(self, input_feature_set):
        """
        @param::input_feature_set: the input feature set to union against
        return the FeatureSet object contains the unioned list of feature metadata
        """
        if input_feature_set != None:
            # check edge case
            if not isinstance(input_feature_set, type(self)):
                raise TypeError('> FeatureSet.union(): the input object is not instance FeatureSet.')
            # union the ufd dictionary
            self.__ufd__.update(input_feature_set.__ufd__)
        return self

    """
    "
    " get the list of features selected
    "
    """
    def values(self):
        """
        empty param intended
        return list of feature metadata objects
        """
        return list(self.__ufd__.values())

            
    
    """
    "
    " query the features from the catalog objects
    "
    """
    def __query__(self, filter=None):
        # handle edge cases
        if filter != None and not callable(filter):
            raise Exception('> query_catalog: the filter object is not callable!')

        # query the features
        if filter == None:
            filter = lambda foo : True
        for u,f in self.__ufd__.copy().items():
            try:
                if not filter(f):
                    self.__ufd__.pop(u)
            except Exception as e:
                print('> query_function: query operation raise error: \n', e)

    
    """
    "
    " build a set of canonical namespaces optimized for query/filter operation
    " e.g., foo.bar.kai => {(foo,0),(bar,1),(kai,2)}
    " check whether or not namepace match take O(N), where N is the number of tuples in namespace.
    "
    """
    def __build_canonical_namespace__(self, namespace):
        """
        @param::namespace: string representation of namespace.
        return a set contains canonical namespace
        """
        canonical_ns = []
        existed_parts = set([])
        if namespace == None or namespace.strip() == '':
            canonical_ns.append((0,'default'))
        else:
            for index,part in enumerate(namespace.split('.')):
                if not part.isalnum():
                    raise Exception("> The namespace is invalid! Make sure the namespace contains alphanumeric and '.' symbol only.")
                if part.lower() in existed_parts:
                    raise Exception("> The namespace can not contains duplicated tuples!")
                else:
                    canonical_ns.append((index,part.lower()))
                    existed_parts.add(part.lower())

        return tuple(canonical_ns)

    """
    "
    " check if the canonical namespaces match. The match operation is not communicative.
    " e.g., a match b  !=> b match a
    " operation takes O(N), where N is the number of tuples in namespace
    "
    """
    def __namespace_match__(self,namespace1, namespace2):
        if namespace1 == None or namespace2 == None:
            return False
        for part in namespace1:
            if part not in namespace2:
                return False
        return True