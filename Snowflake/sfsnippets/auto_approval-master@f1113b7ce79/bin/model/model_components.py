class ModelComponents:
    u"""Hold all model components for convenience"""

    def __init__(self, criterion, max_depth, max_features, n_estimators, rf_hyper_option, pickle_filepath, archived_filepath, features):
        self.__criterion = criterion
        self.__max_depth = max_depth
        self.__max_features = max_features
        self.__n_estimators = n_estimators
        self.__rf_hyper_option = rf_hyper_option
        self.__pickle_filepath = pickle_filepath
        self.__archived_filepath = archived_filepath
        self.__features = features
        self.__version_num = 0

    def __repr__(self):
        return '{}, {}, {}, {}, {}, {}, {}, ({}), {}'.format(self.__criterion,
                                                             self.__max_depth,
                                                             self.__max_features,
                                                             self.__n_estimators,
                                                             self.__rf_hyper_option,
                                                             self.__pickle_filepath,
                                                             self.__archived_filepath,
                                                             self.__features,
                                                             self.__version_num)

    def __str__(self):
        return str({'version': self.__version_num,
                    'criterion': self.__criterion,
                    'max_depth': self.__max_depth,
                    'max_features': self.__max_features,
                    'n_estimators': self.__n_estimators,
                    'rf_hyper_option': self.__rf_hyper_option,
                    'pickle_filepath': self.__pickle_filepath,
                    'archived_filepath': self.__archived_filepath,
                    'features': self.__features})

    @property
    def criterion(self):
        return self.__criterion

    @criterion.setter
    def criterion(self, criterion):
        self.__criterion = criterion

    @property
    def max_depth(self):
        return self.__max_depth

    @max_depth.setter
    def max_depth(self, max_depth):
        self.__max_depth = max_depth

    @property
    def max_features(self):
        return self.__max_features

    @max_features.setter
    def max_features(self, max_features):
        self.__max_features = max_features

    @property
    def n_estimators(self):
        return self.__n_estimators

    @n_estimators.setter
    def n_estimators(self, n_estimators):
        self.__n_estimators = n_estimators

    @property
    def rf_hyper_option(self):
        return self.__rf_hyper_option

    @rf_hyper_option.setter
    def rf_hyper_option(self, rf_hyper_option):
        self.__rf_hyper_option = rf_hyper_option

    @property
    def pickle_filepath(self):
        return self.__pickle_filepath

    @pickle_filepath.setter
    def pickle_filepath(self, pickle_filepath):
        self.__pickle_filepath = pickle_filepath

    @property
    def archived_filepath(self):
        return self.__archived_filepath

    @archived_filepath.setter
    def archived_filepath(self, archived_filepath):
        self.__archived_filepath = archived_filepath

    @property
    def features(self):
        return self.__features

    @features.setter
    def features(self, features):
        self.__features = features

    @property
    def version_num(self):
        return self.__version_num

    @version_num.setter
    def version_num(self, version_num):
        self.__version_num = version_num
