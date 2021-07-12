# -------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------
import os
import unittest

import numpy as np
from systemds.context import SystemDSContext
from systemds.examples.tutorials.adult import DataManager
from systemds.operator import OperationNode, Matrix
from systemds.operator.algorithm import kmeans, multiLogReg, multiLogRegPredict, l2svm, confusionMatrix, scale, scaleApply, split, winsorize
from systemds.script_building import DMLScript


class Test_DMLScript(unittest.TestCase):
    """
    Test class for adult dml script tutorial code.
    """

    sds: SystemDSContext = None
    d: DataManager = None
    neural_net_src_path: str = "./tests/examples/tutorials/neural_net_source.dml"
    preprocess_src_path: str = "./tests/examples/tutorials/preprocess.dml"
    dataset_path_train: str = "../../test/resources/datasets/adult/train_data.csv"
    dataset_path_train_mtd: str = "../../test/resources/datasets/adult/train_data.csv.mtd"
    dataset_path_test: str = "../../test/resources/datasets/adult/test_data.csv"
    dataset_path_test_mtd: str = "../../test/resources/datasets/adult/test_data.csv.mtd"
    dataset_jspec: str = "../../test/resources/datasets/adult/jspec.json"

    @classmethod
    def setUpClass(cls):
        cls.sds = SystemDSContext()
        cls.d = DataManager()

    @classmethod
    def tearDownClass(cls):
        cls.sds.close()

    def test_train_data(self):
        x = self.d.get_train_data()
        self.assertEqual((32561, 14), x.shape)

    def test_train_labels(self):
        y = self.d.get_train_labels()
        self.assertEqual((32561,), y.shape)

    def test_test_data(self):
        x_l = self.d.get_test_data()
        self.assertEqual((16281, 14), x_l.shape)

    def test_test_labels(self):
        y_l = self.d.get_test_labels()
        self.assertEqual((16281,), y_l.shape)

    def test_preprocess(self):
        #assumes certain preprocessing
        train_data, train_labels, test_data, test_labels = self.d.get_preprocessed_dataset()
        self.assertEqual((30162,104), train_data.shape)
        self.assertEqual((30162, ), train_labels.shape)
        self.assertEqual((15060,104), test_data.shape)
        self.assertEqual((15060, ), test_labels.shape)

    def test_multi_log_reg(self):
        # Reduced because we want the tests to finish a bit faster.
        train_count = 15000
        test_count = 5000

        train_data, train_labels, test_data, test_labels = self.d.get_preprocessed_dataset()

        # Train data
        X = self.sds.from_numpy( train_data[:train_count])
        Y = self.sds.from_numpy( train_labels[:train_count])
        Y = Y + 1.0

        # Test data
        Xt = self.sds.from_numpy(test_data[:test_count])
        Yt = self.sds.from_numpy(test_labels[:test_count])
        Yt = Yt + 1.0

        betas = multiLogReg(X, Y)

        [_, y_pred, acc] = multiLogRegPredict(Xt, betas, Yt).compute()

        self.assertGreater(acc, 80)

        confusion_matrix_abs, _ = confusionMatrix(self.sds.from_numpy(y_pred), Yt).compute()

        self.assertTrue(
            np.allclose(
                confusion_matrix_abs,
                np.array([[3503, 503],
                          [268, 726]])
            )
        )

    def test_neural_net(self):
        # Reduced because we want the tests to finish a bit faster.
        train_count = 15000
        test_count = 5000

        train_data, train_labels, test_data, test_labels = self.d.get_preprocessed_dataset(interpolate=True, standardize=True, dimred=0.1)

        # Train data
        X = self.sds.from_numpy( train_data[:train_count])
        Y = self.sds.from_numpy( train_labels[:train_count])

        # Test data
        Xt = self.sds.from_numpy(test_data[:test_count])
        Yt = self.sds.from_numpy(test_labels[:test_count])

        FFN_package = self.sds.source(self.neural_net_src_path, "fnn", print_imported_methods=True)

        network = FFN_package.train(X, Y, 1, 16, 0.01, 1)

        self.assertTrue(type(network) is not None) # sourcing and training seems to works

        FFN_package.save_model(network, '"model/python_FFN/"').compute(verbose=True)

        # TODO This does not work yet, not sure what the problem is
        #probs = FFN_package.predict(Xt, network).compute(True)
        # FFN_package.eval(Yt, Yt).compute()



    def test_level1(self):
        # Reduced because we want the tests to finish a bit faster.
        train_count = 15000
        test_count = 5000
        ################################################################################################################
        '''
        This example shows how one can work with Numpy data within the system ds framework. 
        More precisely we will make use of the build in DataManager, Multinomial Logistic Regression function and the 
        confusion Matrix function.
        The dataset used in this tutorial is a preprocessed version
        of the "UCI Adult Data Set". If you are interested in data preprocessing there is a separate tutorial on this topic.
        If one wants to skip the explanation then the full script is available at the bottom of this page.
        
        Our goal will be to predict whether the income of a person exceeds $50K/yr based on census data. The Adult 
        Dataset contains attributes like: age, workclass, education, marital-status, occupation, race, [...] and the labels
        >50K or <=50K. We will train a Multinomial Logistic Regression model on the training dataset and subsequently we 
        will use the test dataset to asses how well our model can predict if the income is above or below $50K/yr based on the
        attributes. 
        '''
        ################################################################################################################
        ################################################################################################################
        '''
        Step 1: Load and prepare data
        First we get our training and testing data from the build in DataManager. Since the multiLogReg function the 
        labels (Y) to be > 0 we add 1 to all labels. This ensures that the smallest label is >= 1. 
        '''
        ################################################################################################################
        train_data, train_labels, test_data, test_labels = self.d.get_preprocessed_dataset(interpolate=True,
                                                                                           standardize=True, dimred=0.1)
        #####Alternate way to get DataManager
        '''
        from systemds.examples.tutorials.mnist import DataManager
        d = DataManager()
        X = d.get_preprocessed_dataset()
        Y = d.get_preprocessed_dataset()
        '''
        #### Code
        # Train data
        X = self.sds.from_numpy(train_data[:train_count])
        Y = self.sds.from_numpy(train_labels[:train_count])
        Y = Y + 1.0

        # Test data
        Xt = self.sds.from_numpy(test_data[:test_count])
        Yt = self.sds.from_numpy(test_labels[:test_count])
        Yt = Yt + 1.0

        ################################################################################################################
        '''
        Step 2: Training
        Now that we prepared the data we can use the multiLogReg function. First we will train the model on our 
        training data. Afterwards we can make predictions on the test data and asses the performance of the model.
        '''
        ################################################################################################################
        #####Alternate way
        '''
        from systemds.operator.algorithm import multiLogReg
        bias = multiLogReg(X_ds, Y_ds)
        '''
        #### Code
        betas = multiLogReg(X, Y)
        ################################################################################################################
        '''
        Note that nothing has been calculated yet. In SystemDS the calculation is executed once .compute() is called. 
        E.g. betas_res = betas.compute(). 
        
        
        We can now use the trained model to make predictions on the test data. 
        '''
        ################################################################################################################
        [_, y_pred, acc] = multiLogRegPredict(Xt, betas, Yt).compute()
        self.assertGreater(acc, 80) #Todo remove?
        # todo add text how high acc should be with this config
        ################################################################################################################
        '''
        The multiLogRegPredict function has three return values:
            -m, a matrix with the mean probability of correctly classifying each label. We not use it further in this example.
            -y_pred, is the predictions made using the model
            -acc, is the accuracy achieved by the model.
        '''
        ################################################################################################################
        ################################################################################################################
        '''
        Step 3: Confusion Matrix
        A confusion matrix is a useful tool to analyze the performance of the model and to obtain a better understanding
        which classes the model has difficulties to separate. 
        The confusionMatrix function takes the predicted labels and the true labels. It then returns the confusion matrix
        for the predictions and the confusion matrix averages of each true class.
        
        If you followed the tutorial you should be able to verify the results with the provided assertTrue function call. 
        
        '''
        ################################################################################################################


        confusion_matrix_abs, _ = confusionMatrix(self.sds.from_numpy(y_pred), Yt).compute()
        # todo print confusion matrix? Explain cm?
        self.assertTrue(
            np.allclose(
                confusion_matrix_abs,
                np.array([[3583, 502],
                          [245, 670]])
            )
        )

    def test_level2(self):
        ################################################################################################################
        """" 
        Level 2:
        This part of the tutorial shows a more in depth overview for the preprocessing capabilities that SystemDS has to offer.
        We will take a new and raw dataset using the csv format and read it with SystemDS. Then do the heavy lifting for the preprocessing with SystemDS. We will also show how to 
        switch from SystemDS to numpy and then back to SystemDS as this might be relevant for certain use cases.
        
        Step 1:
        First of all, we need to download the dataset and create a mtd-file for specifying different properties that the dataset has.
        We downloaded the train and test dataset from: https://archive.ics.uci.edu/ml/datasets/adult 
        The downloaded dataset has been slightly modified for convenience. These modifications entail removing unnecessary newlines at the end of the files,
        adding column names at the top of the file and finally removing dots at the end of each entry inside the test_dataset file.
        
        After these modifications we have to define a mtd-file for each file we want to read. This mtd file has to be in the same directory as the dataset.
        In this particular example, the dataset is split into two files "train_data.csv" and "test_data.csv". We want to read both, which means that we will define a mtd-file for 
        each of them. Those files will be called "train_data.csv.mtd" and "test_data.csv.mtd".
        In these files we can define certain properties that the file has and also specify which values are supposed to get treated like missing values.
        
        The content of the train_data.csv.mtd file is:
        ### Content start
        {
        "data_type": "frame",
        "format": "csv",
        "header": true,
        "naStrings": [ "?", "" ],
        "rows": 32561,
        "cols": 15
        }
        ### Content end
        
        The "format" of the file is csv, and "header" is set to true because we added the feature names as headers to the csv files.
        "data_type" is set to frame as the preprocessing functions that we use require this datatype. 
        The value of "naStrings" is a list of all the String values that should be treated as unknown values during the preprocessing.
        Also, "rows" in our example is set to 32561, as we have this many entries and "cols" is set to 15 as we have this many features in our datasets.
        
        After these requirements are completed, we have to define a SystemDSContext for reading our dataset. We can do this in the following way.
        """""
        ################################################################################################################

        #### General way to define SystemDSContext
        """"
        from systemds.context import SystemDSContext
        with SystemDSContext() as sds:
        """""
        #### END
        train_count = 32561
        test_count = 16281
        ################################################################################################################
        """""
        With this context we can now define a read operation using the path of the dataset and a schema.
        The schema simply defines the data types for each column.
        
        As already mentioned, SystemDS supports lazy execution by default, which means that the read operation is only executed after calling the compute() function.
        """""

        ################################################################################################################

        SCHEMA = '"DOUBLE,STRING,DOUBLE,STRING,DOUBLE,STRING,STRING,STRING,STRING,STRING,DOUBLE,DOUBLE,DOUBLE,STRING,STRING"'

        F1 = self.sds.read(
            self.dataset_path_train,
            schema=SCHEMA
        )
        F2 = self.sds.read(
            self.dataset_path_test,
            schema=SCHEMA
        )
        ################################################################################################################
        """""
        Step 2:
        Now that the read operation has been declared, we can define an additional file for the further preprocessing of the dataset. 
        For this we create a .json file that holds information about the operations that will be performed on individual columns. 
        For the sake of this tutorial we will use the file "jspec.json" with the following content:
        ### Content start
        {
        "impute":
        [ { "name": "age", "method": "global_mean" }
         ,{ "name": "workclass" , "method": "global_mode" }
         ,{ "name": "fnlwgt", "method": "global_mean" }
         ,{ "name": "education", "method": "global_mode"  }
         ,{ "name": "education-num", "method": "global_mean" }
         ,{ "name": "marital-status"      , "method": "global_mode" }
         ,{ "name": "occupation"        , "method": "global_mode" }
         ,{ "name": "relationship" , "method": "global_mode" }
         ,{ "name": "race"        , "method": "global_mode" }
         ,{ "name": "sex"        , "method": "global_mode" }
         ,{ "name": "capital-gain", "method": "global_mean" }
         ,{ "name": "capital-loss", "method": "global_mean" }
         ,{ "name": "hours-per-week", "method": "global_mean" }
         ,{ "name": "native-country"        , "method": "global_mode" }
        ],
        "bin": [ { "name": "age"  , "method": "equi-width", "numbins": 3 }],
        "dummycode": ["age", "workclass", "education", "marital-status", "occupation", "relationship", "race", "sex", "native-country"],
        "recode": ["income"]
        }
        ### Content end
        Our dataset has missing values. An easy way to deal with that circumstance is to use the "impute" option that SystemDS supports.
        We simply pass a list that holds all the relations between column names and the method of interpolation. A more specific example  is the "education" column.
        In the dataset certain entries have missing values for this column. As this is a string feature,
        we can simply define the method as "global_mode" and replace every missing value with the global mode inside this column. It is important to note that
        we first had to define the values of the missing strings in our selected dataset using the .mtd files. 
        
        With the "bin" keyword we can discretize continuous values into a small number of bins. Here the column with age values
        is discretized into three age intervals. The only method that is currently supported is equi-width binning.
        
        The column-level data transformation "dummycode" allows us to one-hot-encode a categorical column. We split a column into multiple 
        columns of zeros and ones, which collectively capture the full information about the categorical variable.
        In our example we first bin the "age" column into 3 different bins. This means that we now have one column where one entry can belong to one of 3 age groups. After using 
        "dummycode", we transform this one column into 3 different columns, one for each bin.

        
        At last we make use of the "recode" transformation for categorical columns, it maps all distinct categories in 
        the column into consecutive numbers, starting from 1. In our example we recode the "income" column, which 
        transforms it from "<=$50K" and ">$50K" to "0" and "1" respectively.
        
        After defining the .jspec file we can read it by passing the filepath, data_type and value_type using the following command:
        """""

        ################################################################################################################
        jspec = self.sds.read(self.dataset_jspec, data_type="scalar", value_type="string")
        # scaling does not have effect yet. We need to replace labels in test set with the same string as in train dataset

        ################################################################################################################
        """"
        We now can combine the train and the test dataset by using the rbind() function. This function simply appends the Frame F2 at the end of Frame F1.
        This is necessary to ensure that the encoding is identical between training and test dataset.
        """""
        ################################################################################################################
        X1 = F1.rbind(F2)
        ################################################################################################################
        """"
        In order to use our jspec file we can apply the transform_encode() function. We simply have to pass the read .json file from before.
        Another good resource for further ways of processing is: https://apache.github.io/systemds/site/dml-language-reference.html
        There we provide different examples for defining jspec's and what functionality is currently supported.
        In our particular case we obtain the Matrix X1 and the Frame M1 from the operation. X1 holds all the encoded values and M1 holds a mapping between the encoded values
        and all the initial values. Columns that have not been specified in the .json file were not altered. 
        """""
        ################################################################################################################
        X1, M1 = X1.transform_encode(spec=jspec)

        # better alternative for encoding
        # X1, M = F1.transform_encode(spec=jspec)
        # X2 = F2.transform_apply(spec=jspec, meta=M)
        # testX2 = X2.compute(True)

        ################################################################################################################
        """"
        First we re-split out data into a training and a test set with the corresponding labels. 
        """""
        ################################################################################################################
        PREPROCESS_package = self.sds.source(self.preprocess_src_path, "preprocess", print_imported_methods=True)

        X = PREPROCESS_package.get_X(X1, 1, train_count)
        Y = PREPROCESS_package.get_Y(X1, 1, train_count)

        #We lose the column count information after using the Preprocess Package. This triggers an error on multilogregpredict. Otherwise its working
        Xt = PREPROCESS_package.get_X(X1, train_count, train_count+test_count)
        Yt = PREPROCESS_package.get_Y(X1, train_count, train_count+test_count)
        # since the test set contains dots in the income column and the training set does not we need to make sure that they are labled as the same value
        Yt = PREPROCESS_package.replace_value(Yt, 3.0, 1.0)
        Yt = PREPROCESS_package.replace_value(Yt, 4.0, 2.0)

        # TODO somehow throws error at predict with this included
        #X, mean, sigma = scale(X, True, True)
        #Xt = scaleApply(Xt, mean, sigma)

        betas = multiLogReg(X, Y)

        [_, y_pred, acc] = multiLogRegPredict(Xt, betas, Yt)

        confusion_matrix_abs, _ = confusionMatrix(y_pred, Yt).compute()
        print(confusion_matrix_abs)
        self.assertTrue(
            np.allclose(
                confusion_matrix_abs,
                np.array([[11593.,  1545.],
                          [842., 2302.]])
            )
        )

    def test_level3(self):
        # Reduced because we want the tests to finish a bit faster.
        train_count = 30000
        test_count = 10000
        # self.sds.read(self.dataset_path_train, schema=self.dataset_path_train_mtd).compute(verbose=True)
        print("")

        SCHEMA = '"DOUBLE,STRING,DOUBLE,STRING,DOUBLE,STRING,STRING,STRING,STRING,STRING,DOUBLE,DOUBLE,DOUBLE,STRING,STRING"'

        F1 = self.sds.read(
            self.dataset_path_train,
            schema=SCHEMA
        )
        F2 = self.sds.read(
            self.dataset_path_test,
            schema=SCHEMA
        )
        jspec = self.sds.read(self.dataset_jspec, data_type="scalar", value_type="string")
        # scaling does not have effect yet. We need to replace labels in test set with the same string as in train dataset
        X1, M1 = F1.rbind(F2).transform_encode(spec=jspec)

        PREPROCESS_package = self.sds.source(self.preprocess_src_path, "preprocess", print_imported_methods=True)

        X = PREPROCESS_package.get_X(X1, train_count)
        Y = PREPROCESS_package.get_Y(X1, train_count)
        Xt = PREPROCESS_package.get_Xt(X1, train_count)
        Yt = PREPROCESS_package.get_Yt(X1, train_count)

        X, mean, sigma = scale(X, True, True)
        Xt = scaleApply(Xt, mean, sigma)

        features = X
        labels = Y

        # Train data
        """
        SystemDS also supports feed-forward neural networks for classification tasks.
        We use a neural network with 2 hidden layers, each consisting of 200 neurons.
        First we need to source the dml-file for neural networks.
        This file includes all the necessary functions for training, evaluating and storing the model.
        The returned object of the source call is further used for calling the functions.
        """
        ################################################################################################################
        FFN_package = self.sds.source(self.neural_net_src_path, "fnn", print_imported_methods=True)
        ################################################################################################################
        """
        Training a neural network in SystemDS using the train function is very straightforward.
        The first two arguments are the training features and the target values we want to fit our model on.
        Then we need set the hyperparameters of the model.
        We choose to train for 1 epoch with a batch size of 16 and a learning rate of 0.01, which are common parameters for neural networks.
        The seed argument ensures that running the code again yields the same results.
        """
        ################################################################################################################
        epochs = 1
        batch_size = 16
        learning_rate = 0.01
        seed = 42

        network = FFN_package.train(features, labels, epochs, batch_size, learning_rate, seed)
        ################################################################################################################
        """
        If more ressources are available, one can also choose to train the model using a parameter server.
        Here we use the same parameters as before, however we need to specifiy a few more.
        TODO get more information about the parameters
        """
        ################################################################################################################
        workers = 1
        utype = '"BSP"'
        freq = '"EPOCH"'
        mode = '"LOCAL"'
        network = FFN_package.train_paramserv(features, labels, epochs,
                                              batch_size, learning_rate, workers, utype, freq, mode,
                                              seed)
        ################################################################################################################
        """
        For later usage we can save the trained model.
        We only need to specify the name of our model and the file-path.
        This call stores the weights and biases of our model.
        """
        ################################################################################################################
        FFN_package.save_model(network, '"model/python_FFN/"').compute(verbose=True)
        ################################################################################################################
        """
        Next we evaluate our network on the test set which was not used for training.
        The predict function with the test features and our trained network returns a matrix of class probabilities.
        This matrix contains for each test sample the probabilities for each class.
        For predicting the most likely class of a sample, we choose the class with the highest probability.
        """
        ################################################################################################################
        test_features = Xt
        probs = FFN_package.predict(test_features, network)
        ################################################################################################################
        """
        To evaluate how well our model performed on the test set, we can use the probability matrix from the predict call and the real test labels
        and compute the log-cosh loss.
        """
        ################################################################################################################
        FFN_package.eval(probs, Yt).compute(True)
        ################################################################################################################



if __name__ == "__main__":
    unittest.main(exit=False)
