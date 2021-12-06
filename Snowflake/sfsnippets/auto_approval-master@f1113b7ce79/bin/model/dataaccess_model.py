"""
Sample Execution:
# --request_data "{\"FUNCTION_AREA\": \"CXP\",\"DOMAIN_NAME\": \"REF\",\"OBJECT_DB_NAME\": \"EDW_REF_ETL_DB\",\"SCHEMA\" : \"SS\",\"OBJ_NAME\": \"MY_NEW_TABLE\",\"TARGET_ROLE\": \"CX_ROITRIP_BUSINESS_ANALYST_ROLE\",\"IS_RESTRICTED\": 1 ,\"ACTUAL_SANDBOX\": 1,\"REQUESTOR_ORG\": 0,\"HAS_ACCESS_TO_DOMAIN\": 1}"
# --rebuild Y
# --hyper_tune Y
"""

import utils.util as utils
from model_components import ModelComponents
import numpy as np
import pickle
import json
import os
import logging
import argparse
import warnings
import pandas as pd
import re
from shutil import copyfile
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split, GridSearchCV, RandomizedSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, accuracy_score, classification_report
from sklearn.feature_selection import SelectFromModel
from imblearn.over_sampling import SMOTE
from dataaccess_connection import fetch_historical_data

# from sklearn.tree import export_graphviz
# from sklearn.metrics import roc_curve, auc, mean_squared_error
# import pydot
# import seaborn as sn
# from matplotlib.legend_handler import HandlerLine2D
# from matplotlib import pyplot as plt


def load_dataset(filepath_to_model_data):
    """
    Load a provided dataset as a dataframe

    :param filepath_to_model_data: Filepath to data on which to build the model
    :return: dataframe of the model data
    """
    dataset = pd.read_csv(filepath_to_model_data, sep=",")
    dataset = dataset.loc[dataset["REQUEST_STATUS"] != -1]  # Filter Expired Records
    logging.info("\n********Data Imported Successfully********")

    return dataset


def oversample_test_data(x_train, y_train):
    """
    Oversample the minority class for a model to effectively learn the decision boundary

    :param x_train: dependent data
    :param y_train: independent data
    :return: ???
    """
    smt = SMOTE()
    x_train, y_train = smt.fit_sample(x_train, y_train)
    logging.info("\n*******SMOTE for Negative Scenarios********")

    return x_train, y_train


# def get_correlation_matrix(dataset):
#     u"""
#     TODO: Explain what this does
#
#     :param dataset: Model data
#     :return: None
#     """
#     correlation_matrix = dataset.corr()
#     sn.heatmap(correlation_matrix, annot=True)
#     # plt.show()


# def get_scatter_plot(dataset):
#     u"""
#     Create a scatter plot of the data
#
#     :param dataset: Model data
#     :return: None
#     """
#     pd.scatter_matrix(dataset)
#     # plt.show()


#
# def model_accuracy_plot(x_train, x_test, y_train, y_test, estimators, criterion):
#     u"""
#     Plotting the model accuracy
#
#     :param x_train:
#     :param x_test:
#     :param y_train:
#     :param y_test:
#     :param estimators:
#     :param criterion:
#     :return:
#     """
#     train_results = []
#     test_results = []
#
#     logging.info('\n*******Inside model_accuracy_plot method********')
#
#     for nb_trees in estimators:
#         clf = RandomForestClassifier(n_estimators=nb_trees, criterion=criterion)
#         clf.fit(x_train, y_train)
#
#         train_results.append(mean_squared_error(y_train, clf.predict(x_train)))
#         test_results.append(mean_squared_error(y_test, clf.predict(x_test)))
#
#     line1, = plt.plot(list_nb_trees, train_results, color="r", label="Training Score")
#     line2, = plt.plot(list_nb_trees, test_results, color="g", label="Testing Score")
#
#     logging.info('\n*******Model Accuracy with Different estimators********')
#
#     plt.legend(handler_map={line1: HandlerLine2D(numpoints=2)})
#     plt.ylabel('MSE')
#     plt.xlabel('n_estimators')
#     # plt.show()


# def get_roc(classifier, x_test, y_test):
#     u"""
#     TODO: What is this???
#
#     :param classifier:
#     :param x_test:
#     :param y_test:
#     :return:
#     """
#     logging.info('\n*******ROC Curve********')
#     probs = classifier.predict_proba(x_test)
#     n_probs = probs[:, 1]
#     fpr, tpr, thresholds = roc_curve(y_test, n_probs)
#     roc_auc = auc(fpr, tpr)
#
#     # Plotting
#     plt.title('Receiver Operating Characteristic')
#     plt.plot(fpr, tpr, 'b', label='AUC = %0.3f' % roc_auc)
#     plt.legend(loc='lower right')
#     plt.plot([0, 1], [0, 1], 'r--')
#     plt.xlim([0, 1])
#     plt.ylim([0, 1])
#     plt.ylabel('True Positive Rate')
#     plt.xlabel('False Positive Rate')
#     plt.show()

# def load_model(x_train, y_train, x_test, model_components: ModelComponents):
#     u"""
#     TODO: What does this do??? Check score of model?
#
#     :param x_train:
#     :param y_train:
#     :param x_test:
#     :param model_components:
#     :return:
#     """
#     # Load the Model back from file
#     logging.info('\n*******Load the Model back from File********')
#     with open(model_components.pickle_filepath, 'rb') as file:
#         pickled_lr_model = pickle.load(file)
#
#     logging.info('\n*******Model Loaded Successfully********')
#     # Use the Reloaded Model to Calculate the accuracy score and predict target values
#
#     # Calculate the Score
#     score = pickled_lr_model.score(x_train, y_train)
#
#     # logging.info the Score
#     logging.info("\nTest score: {0:.3f} %".format(100 * score))
#
#     # Predict the Labels using the reloaded Model
#     ypred = pickled_lr_model.predict(x_test)
#     logging.info(ypred)
#
#     return pickled_lr_model

# def desc_tree(classifier, features):
#     u"""
#     Show the decision tree
#
#     :param classifier:
#     :param features:
#     :return:
#     """
#     # Pull out one tree from the forest
#     tree = classifier.estimators_[5]
#     # Export the image to a dot file
#     export_graphviz(tree, out_file='tree.dot', feature_names=features, rounded=True, precision=1)
#     # Use dot file to create a graph
#     (graph, ) = pydot.graph_from_dot_file('tree.dot')
#     # Write graph to a png file
#     graph.write_png('tree.png')


def dep_indep_dataset(dataset, model_components: ModelComponents):
    """
    Load the dependent and independent variables

    :param dataset: Model data
    :param model_components: Instance of ModelComponents
    :return:
    """
    logging.info(
        "\n********Dataset will be divided into Dependent and Independent Variables********"
    )
    #print(model_components.features)
    x = dataset[model_components.features]
    y = dataset[["REQUEST_STATUS"]]

    return x, y


def label_encoding(x):
    """
    Convert Independent variables to numeric form to make it machine readable

    :param x:
    :return:
    """
    labelencoder_X_0 = LabelEncoder()
    x["DOMAIN_NAME_LE"] = labelencoder_X_0.fit_transform(x["DOMAIN_NAME"])

    labelencoder_X_1 = LabelEncoder()
    x["OBJECT_DB_NAME_LE"] = labelencoder_X_1.fit_transform(x["OBJECT_DB_NAME"])

    labelencoder_X_2 = LabelEncoder()
    x["SCHEMA_LE"] = labelencoder_X_2.fit_transform(x["SCHEMA"])

    labelencoder_X_3 = LabelEncoder()
    x["OBJ_NAME_LE"] = labelencoder_X_3.fit_transform(x["OBJ_NAME"])

    labelencoder_X_4 = LabelEncoder()
    x["DATA_DOMAIN"] = labelencoder_X_4.fit_transform(x["DATA_DOMAIN"])

    labelencoder_X_5 = LabelEncoder()
    x["APP_NAME"] = labelencoder_X_5.fit_transform(x["APP_NAME"])

    labelencoder_X_6 = LabelEncoder()
    x["APP_TYPE"] = labelencoder_X_6.fit_transform(x["APP_TYPE"])

    labelencoder_X_7 = LabelEncoder()
    x["ROLE_TYPE"] = labelencoder_X_7.fit_transform(x["ROLE_TYPE"])

    x = x.iloc[:, 7:]

    logging.info("\n********Independent Variables Encoded********")

    return x


def add_data_with_hist(x, df):
    """
    Add new data to the historical data set for a current prediction

    :param x: ???
    :param df: ???
    :return: unioned data
    """
    x_data = x.copy()
    logging.info("\n********Shape of Hist Datatset********")
    logging.info(x_data.shape)
    logging.info("\n********Appending new elements to Hist Datatset********")

    for a in range(len(df)):
        x_data = x_data.append(df.loc[a], ignore_index=True)

    logging.info("\n********Shape after adding new elements to Hist Datatset********")
    logging.info(x_data.shape)
    return x_data


def split_train_test_data(x_enc, y, split_size):
    """
    Split the data into training and and test data sets.

    :param x_enc: label_encoding
    :param y: independent variable data
    :param split_size: Default 25%
    :return: Test and Training data
    """
    x_train, x_test, y_train, y_test = train_test_split(
        x_enc, y, test_size=split_size, random_state=0
    )

    logging.info("\n********Dataset split into Test and Train********")
    return x_train, x_test, y_train, y_test


def get_best_params_rf(x_train, y_train, model_components: ModelComponents):
    """
    TODO: What does this do???

    :param x_train:
    :param y_train:
    :param model_components:
    :return:
    """
    logging.info(
        "\n********Find the best parameter combination for Random Forest Model********"
    )

    if model_components.rf_hyper_option == 1:
        param_grid = {
            "n_estimators": list_nb_trees,
            "max_features": ["auto", "sqrt", "log2"],
            "max_depth": [4, 5, 6, 7, 8, 10, 12, 14, 16, 18, 20],
            "criterion": ["gini", "entropy"],
        }

        rfc = RandomForestClassifier(random_state=42)

        CV_rfc = GridSearchCV(
            estimator=rfc,
            param_grid=param_grid,
            scoring="neg_mean_squared_error",
            cv=5,
            n_jobs=-1,
        )
        CV_rfc.fit(x_train, y_train)

        best_param_rf = CV_rfc.best_params_

        logging.info(
            "\n********Best Combination of Params for given Dataset using GridSearchCV********"
        )
        logging.info(best_param_rf)

    else:
        # Number of trees in random forest
        n_estimators = [int(x) for x in np.linspace(start=50, stop=500, num=10)]
        # Number of features to consider at every split
        max_features = ["auto", "sqrt"]
        # Maximum number of levels in tree
        max_depth = [int(x) for x in np.linspace(1, 40, num=10)]
        max_depth.append(None)
        # Minimum number of samples required to split a node
        min_samples_split = [2, 5, 10]
        # Minimum number of samples required at each leaf node
        min_samples_leaf = [1, 2, 4]
        # Method of selecting samples for training each tree
        bootstrap = [True, False]

        # Create the random grid
        random_grid = {
            "n_estimators": n_estimators,
            "max_features": max_features,
            "max_depth": max_depth,
            "min_samples_split": min_samples_split,
            "min_samples_leaf": min_samples_leaf,
            "bootstrap": bootstrap,
        }

        # Use the random grid to search for best hyper parameters
        # First create the base model to tune
        rf = RandomForestClassifier()

        # Random search of parameters, using 3 fold cross validation,
        # search across 100 different combinations, and use all available cores
        rf_random = RandomizedSearchCV(
            estimator=rf,
            param_distributions=random_grid,
            n_iter=100,
            cv=3,
            verbose=2,
            random_state=42,
            n_jobs=-1,
        )

        # Fit the random search model
        rf_random.fit(x_train, y_train)

        best_param_rf = rf_random.best_params_

        logging.info(
            "\n********Best Combination of Params for given Dataset using RandomizedSearchCV********"
        )
        logging.info(best_param_rf)

    return best_param_rf


def model_execute(x_train, y_train, model_components: ModelComponents):
    """
    For Training. Execute the model for a given input using the local pickle

    :param x_train: dependent variable, training data set
    :param y_train: independent variable, training data set
    :param model_components: Instance of ModelComponents
    :return:
    """
    # Fitting Random Forest Classification to the Training set
    classification = RandomForestClassifier(
        n_estimators=model_components.n_estimators, criterion=model_components.criterion
    )
    classification.fit(x_train, y_train)

    logging.info("\n********Model Executed Successfully********")

    return classification


def model_stats(y_test, y_pred):
    """
    Collect model statistics

    :param y_test: Test data???
    :param y_pred: Predication data???
    :return:
    """
    # View The Accuracy Of Our Full Feature Model
    logging.info("\n********Model Stats********")
    logging.info("Model Accuracy: " + str(accuracy_score(y_test, y_pred)))

    # The F1 Score or F-score is a weighted average of precision and recall.
    logging.info("\n********Model Report********")
    logging.info("Classification Report:" + classification_report(y_test, y_pred))


def get_confusion_matrix(y_test, y_pred):
    """
    Don't worry I'm confused. :)

    Confusion Matrix provides insights on how accurate our model is by comparing test with model prediction
    data

    :param y_test: actual test results data
    :param y_pred: testing independent variables with classifier results
    :return:
    """
    cm = confusion_matrix(y_test, y_pred)
    logging.info("\n*******Confusion Matrix********")
    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
    logging.info("True Negatives: ", tn)
    logging.info("False Positives: ", fp)
    logging.info("False Negatives: ", fn)
    logging.info("True Positives: ", tp)
    accuracy = (tn + tp) * 100 / (tp + tn + fp + fn)
    logging.info("Accuracy {:0.2f}%".format(accuracy))


def get_feature_selection(
    classifier,
    x_train,
    y_train,
    feature_imp_threshold,
    model_components: ModelComponents,
):
    """
    calculate the Gini (measure and compare the impurity) -- relation between independent variables
    and the dependent variable

    :param classifier:
    :param x_train:
    :param y_train:
    :param feature_imp_threshold:
    :param model_components:
    :return:
    """
    # logging.info the name and gini importance of each feature
    logging.info("\n*******List the name and importance of each feature********")
    for feature in zip(model_components.features, classifier.feature_importances_):
        logging.info(feature)

    # Create a selector object that will use the random forest classifier to identify
    # features that have an importance of more than 0.15
    sfm = SelectFromModel(classifier, threshold=feature_imp_threshold)

    # Train the selector
    sfm.fit(x_train, y_train)

    # logging.info the names of the most important features

    logging.info("\n*******List the names of the most important features********")
    for feature_list_index in sfm.get_support(indices=True):
        logging.info(model_components.features[feature_list_index])

    logging.info("\n*******Feature Selection Historgram********")
    # pd.Series(sfm.estimator_.feature_importances_.ravel()).hist()


def save_model_n_dataset(classification, dataset, model_components: ModelComponents):
    """
    Save the created model as a pickle file for future evaluation.

    :param classification: classification relevance???
    :param dataset:
    :param model_components:
    :return:
    """
    # pull current version number
    version_num = model_components.version_num

    # copy old pickle file to archived_models directory and remove it from out folder
    copyfile(model_components.pickle_filepath, model_components.archived_filepath)
    os.remove(model_components.pickle_filepath)

    # create new filepath with updated version number
    pickle_name = re.sub(r"\d+", "", model_components.pickle_filepath[:-4])
    new_filepath = f"{pickle_name}{version_num+1}.pkl"

    with open(new_filepath, "wb") as file:
        pickle.dump(classification, file)
        logging.info("\n*******Model Saved to File********")

    dataset = dataset.drop_duplicates()

    dataset.to_csv(entire_dataset, index=False)
    logging.info("\n*******Dataset Written to File********")


def add_to_base_data(df_req, ypred_new, filename):
    """
    Add new elements to dataset for use with a predication

    :param df_req:
    :param ypred_new:
    :param filename:
    :return:
    """
    logging.info("\n********Load Dataset from File********")
    dataset = pd.read_csv(filename, sep=",")

    logging.info("\n********Size of Base Datatset********")
    logging.info(dataset.shape)

    logging.info("\n********Appending new elements to Base Dataset********")

    df_req["REQUEST_STATUS"] = ypred_new

    logging.info("\n********Appending new elements to Dataset********")
    dataset = dataset.append(df_req, ignore_index=True)

    dataset = dataset.drop_duplicates()

    logging.info("\n********Size of Base Datatset********")
    logging.info(dataset.shape)

    dataset.to_csv(filename, index=False)
    logging.info("\n*******add_to_base_data - Dataset Written to File********")

    return df_req


def model_execute_reload(hyper_tune, model_components: ModelComponents):
    """
    Execute a rebuild of the model.

    :param hyper_tune: This is the option to tune the model. Hypertuning will show suggested new values
    :param model_components:
    :return: None
    """
    logging.info("*******Rebuilding the model **********")

    # Get Data from ODSPROD into a local file
    fetch_historical_data(filename)

    # Load Dataset from file to DataFrame
    data = load_dataset(filename)
    # process the data

    data["MODIFIED_TARGET_ROLE"] = data["TARGET_ROLE"].str.replace("_ROLE", "")
    data["MODIFIED_TARGET_ROLE"] = data["MODIFIED_TARGET_ROLE"].str.replace(
        "BUS_ANALYST", "ANALYST"
    )
    data["MODIFIED_TARGET_ROLE"] = data["MODIFIED_TARGET_ROLE"].str.replace(
        "BUSINESS_ANALYST", "ANALYST"
    )
    data["MODIFIED_TARGET_ROLE"] = data["MODIFIED_TARGET_ROLE"].str.replace(
        "BUS_RESTRICT", "RESTRICT"
    )
    data["SPLIT_ROLE"] = data["MODIFIED_TARGET_ROLE"].str.split("_")
    data["DATA_DOMAIN"] = data["SPLIT_ROLE"].apply(
        lambda x: x[-3] if -3 >= -len(x) else x[-len(x)]
    )
    data["APP_NAME"] = data["SPLIT_ROLE"].apply(
        lambda x: x[-2] if -2 >= -len(x) else x[-len(x)]
    )
    data["APP_TYPE"] = data["SPLIT_ROLE"].str[0]
    data["ROLE_TYPE"] = data["SPLIT_ROLE"].str[-1]
    del data["SPLIT_ROLE"]
    del data["MODIFIED_TARGET_ROLE"]
    del data["TARGET_ROLE"]
    # identify Dependent (y_set) and Independent (x_set) Variables
    x_set, y_set = dep_indep_dataset(data, model_components)
    x_enc = label_encoding(x_set)

    # Split dataset into test and training set
    x_train, x_test, y_train, y_test = split_train_test_data(
        x_enc, y_set, split_size=0.25
    )

    # apply SMOTE
    x_train, y_train = oversample_test_data(x_train, y_train)

    # Tune the hyper parameters for the model using RandomizedSearchCV
    if hyper_tune == "Y":
        return get_best_params_rf(x_train, y_train, model_components)

    # Execute the Random Forest Model
    classifier = model_execute(x_train, y_train, model_components)

    # Predicting the Test set results
    y_pred = classifier.predict(x_test)
    model_stats(y_test, y_pred)

    get_confusion_matrix(y_test, y_pred)

    get_feature_selection(
        classifier,
        x_train,
        y_train,
        feature_imp_threshold=0.15,
        model_components=model_components,
    )

    # Optional Plotting
    # get_roc(classifier, x_test, y_test)

    save_model_n_dataset(classifier, data, model_components)

    logging.info("\n*******model_execute_reload executed Successfully********")


def model_request(json_request, model_components: ModelComponents):
    """
    Execute against the stored pickle for a given input.

    :param json_request: User's request with all parameters
    :param model_components: Model parameter configuration
    :return: response of approve/deny
    """

    data = load_dataset(filename)

    data["MODIFIED_TARGET_ROLE"] = data["TARGET_ROLE"].str.replace("_ROLE", "")
    data["MODIFIED_TARGET_ROLE"] = data["MODIFIED_TARGET_ROLE"].str.replace(
        "BUS_ANALYST", "ANALYST"
    )
    data["MODIFIED_TARGET_ROLE"] = data["MODIFIED_TARGET_ROLE"].str.replace(
        "BUSINESS_ANALYST", "ANALYST"
    )
    data["MODIFIED_TARGET_ROLE"] = data["MODIFIED_TARGET_ROLE"].str.replace(
        "BUS_RESTRICT", "RESTRICT"
    )
    data["SPLIT_ROLE"] = data["MODIFIED_TARGET_ROLE"].str.split("_")
    data["DATA_DOMAIN"] = data["SPLIT_ROLE"].apply(
        lambda x: x[-3] if -3 >= -len(x) else x[-len(x)]
    )
    data["APP_NAME"] = data["SPLIT_ROLE"].apply(
        lambda x: x[-2] if -2 >= -len(x) else x[-len(x)]
    )
    data["APP_TYPE"] = data["SPLIT_ROLE"].str[0]
    data["ROLE_TYPE"] = data["SPLIT_ROLE"].str[-1]
    del data["SPLIT_ROLE"]
    del data["MODIFIED_TARGET_ROLE"]
    del data["TARGET_ROLE"]
    x_set, y_set = dep_indep_dataset(data, model_components)
    my_data = x_set.copy()

    with open(model_components.pickle_filepath, "rb") as file:
        pickled_lr_model = pickle.load(file)

    logging.info("\n*******Convert Input JSON Request to DataFrame********")
    req_data = json.loads(json_request)
    actual_role = req_data["TARGET_ROLE"]
    actual_role = actual_role.replace("BUS_ANALYST", "ANALYST")
    actual_role = actual_role.replace("BUS_RESTRICT", "RESTRICT")
    actual_role = actual_role.replace("_ROLE", "")
    role_split = actual_role.split("_")
    max_index = 0 - len(role_split)
    req_data["APP_TYPE"] = role_split[0]
    req_data["ROLE_TYPE"] = role_split[-1]
    req_data["DATA_DOMAIN"] = (
        role_split[-3] if -3 >= max_index else role_split[max_index]
    )
    req_data["APP_NAME"] = role_split[-2] if -2 >= max_index else role_split[max_index]
    del req_data["TARGET_ROLE"]
    df_req = pd.json_normalize(req_data)
    data_to_predict = df_req[model_components.features]

    # Add new data points to existing dataset for prediction
    logging.info("\n*******Add New Data to Historical Set for Prediction********")
    x_new = add_data_with_hist(my_data, data_to_predict)

    # Encode the new data points
    logging.info(
        "\n*******Encode New Data along with Historical Set for Prediction********"
    )
    x_new_enc = label_encoding(x_new)

    # prepare the new independent varaibles for prediction
    logging.info("\n*******Prepare New Data for Prediction********")
    x_to_pred = pd.DataFrame(x_new_enc.loc[x_new.index > len(my_data) - 1])
    logging.info(x_to_pred)

    if len(x_to_pred) == 0:
        logging.info(
            "\n*******Data is already provisioned or Rejected, Please check the Input Request*******"
        )
        return {}

    # predict the value for new depenedent varaible
    y_new_pred = pickled_lr_model.predict(x_to_pred)
    logging.info("\n*******New Prediction********")

    for num in range(len(y_new_pred)):
        if y_new_pred[num] == 0:
            logging.info("Access Denied")
        else:
            logging.info("Access Approved")

    logging.info(y_new_pred)

    pred_response = add_to_base_data(df_req, y_new_pred, entire_dataset)
    return json.loads(pred_response.to_json(orient="records"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Execute the ML Model for Data Access Auto Provisioning \n"""
    )
    parser.add_argument(
        "--request_data",
        required=False,
        help="JSON payload (request). Refer to readme for description of the <k,v> pairs.",
    )
    parser.add_argument(
        "--rebuild",
        choices=["N", "Y"],
        default="N",
        help="Rebuilds Model. Execute after contrary data.",
    )
    parser.add_argument(
        "--hyper_tune",
        choices=["N", "Y"],
        default="N",
        help="Tunes the Model. Run infrequently as data increases in size.",
    )
    args = parser.parse_args()

    # Collect Properties
    general_properties = utils.collect_property_file_contents(
        "../../properties/config.ini", "GENERAL"
    )
    model_properties = utils.collect_property_file_contents(
        "../../properties/config.ini", "MODEL-PROPERTIES"
    )

    log_level = general_properties["log_level"]
    model_components = ModelComponents(
        model_properties["CRITERION"],
        int(model_properties["MAX_DEPTH"]),
        model_properties["MAX_FEATURES"],
        int(model_properties["N_ESTIMATORS"]),
        int(model_properties["RF_HYPER_OPTION"]),
        model_properties["FILEPATH_FOR_OUTPUT_PICKLE"],
        model_properties["FILEPATH_FOR_ARCHIVED_PICKLE"],
        model_properties["FEATURES"].split(","),
    )

    # update model_components version number, pickle_filepath, and archived_filepath
    for f_name in os.listdir("../../out"):
        if "pkl" in f_name:
            pickle_name = f_name

    version_num = int(re.findall(r"\d+", pickle_name)[0])
    model_components.version_num = version_num
    model_components.pickle_filepath = (
        f"{model_components.pickle_filepath[:-4]}{version_num}.pkl"
    )
    model_components.archived_filepath = (
        f"{model_components.archived_filepath[:-4]}{version_num}.pkl"
    )

    filename = model_properties["FILEPATH_FOR_REQUIRED_MODEL_DATA"]
    entire_dataset = model_properties["FILEPATH_FOR_ENTIRE_DATASET"]

    dep_col = ["REQUEST_STATUS"]
    list_nb_trees = [5, 10, 15, 30, 45, 60, 80, 100, 120, 150, 180, 200]

    logging.info(os.getcwd())
    logging.basicConfig()
    logging.getLogger().setLevel(log_level)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
    )

    warnings.filterwarnings("ignore")

    if args.rebuild.upper() == "Y" or args.hyper_tune.upper() == "Y":
        param_combo = model_execute_reload(args.hyper_tune, model_components)

    if args.request_data:
        response = model_request(args.request_data, model_components)
        print(response)
        print(model_components)
