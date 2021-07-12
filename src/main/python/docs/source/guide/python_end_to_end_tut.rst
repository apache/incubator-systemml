.. -------------------------------------------------------------
.. 
.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
.. 
..   http://www.apache.org/licenses/LICENSE-2.0
.. 
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.
.. 
.. ------------------------------------------------------------

Python end to end
===================

Prerequisite: 

- :doc:`/getting_started/install`


Level 1
-------------------

This example shows how one can work with NumPy data within the SystemDs framework. More precisely we will make use of the
built-in DataManager, Multinomial Logistic Regression function, and the Confusion Matrix function. The dataset used in this
tutorial is a preprocessed version of the "UCI Adult Data Set". If you are interested in data preprocessing there is a
separate tutorial on this topic. If one wants to skip the explanation then the full script is available at the bottom of this page.

Our goal will be to predict whether the income of a person exceeds $50K/yr based on census data. The Adult Dataset
contains attributes like age, workclass, education, marital-status, occupation, race, [...] and the labels >50K or <=50K.
We will train a Multinomial Logistic Regression model on the training dataset and subsequently we will use the test dataset
to assess how well our model can predict if the income is above or below $50K/yr based on the attributes.


Step 1: Load and prepare data
~~~~~~~~~~

First, we get our training and testing data from the built-in DataManager. Since the multiLogReg function requires the
labels (Y) to be > 0, we add 1 to all labels. This ensures that the smallest label is >= 1. Additionally we will only take
a fraction of the training and test set into account to speed up the execution.

.. code-block:: python

    from systemds.context import SystemDSContext
    from systemds.examples.tutorials.adult import DataManager

    sds = SystemDSContext()
    d = DataManager()

    # limit the sample size
    train_count = 15000
    test_count = 5000

    train_data, train_labels, test_data, test_labels = d.get_preprocessed_dataset(interpolate=True, standardize=True, dimred=0.1)

    # Train data
    X = sds.from_numpy(train_data[:train_count])
    Y = sds.from_numpy(train_labels[:train_count])
    Y = Y + 1.0

    # Test data
    Xt = sds.from_numpy(test_data[:test_count])
    Yt = sds.from_numpy(test_labels[:test_count])
    Yt = Yt + 1.0

Here the DataManager contains the code for downloading and setting up NumPy arrays containing the data.

Step 2: Training
~~~~~~~~~~

Now that we prepared the data we can use the multiLogReg function. First, we will train the model on our
training data. Afterward, we can make predictions on the test data and assess the performance of the model.

.. code-block:: python

    from systemds.operator.algorithm import multiLogReg
    betas = multiLogReg(X, Y)

Note that nothing has been calculated yet. In SystemDS the calculation is executed once compute() is called.
E.g. betas_res = betas.compute().


We can now use the trained model to make predictions on the test data.

.. code-block:: python

    from systemds.operator.algorithm import multiLogRegPredict
    [_, y_pred, acc] = multiLogRegPredict(Xt, betas, Yt)

The multiLogRegPredict function has three return values:
    - m, a matrix with the mean probability of correctly classifying each label. We do not use it further in this example.
    - y_pred, is the predictions made using the model
    - acc, is the accuracy achieved by the model.

Step 3: Confusion Matrix
~~~~~~~~~~

A confusion matrix is a useful tool to analyze the performance of the model and to obtain a better understanding
which classes the model has difficulties separating.
The confusionMatrix function takes the predicted labels and the true labels. It then returns the confusion matrix
for the predictions and the confusion matrix averages of each true class.

.. code-block:: python

    from systemds.operator.algorithm import confusionMatrix
    confusion_matrix_abs, _ = confusionMatrix(y_pred, Yt).compute()
    print(confusion_matrix_abs)



Full Script
~~~~~~~~~~

In the full script, some steps are combined to reduce the overall script.

.. code-block:: python

    import numpy as np
    from systemds.context import SystemDSContext
    from systemds.examples.tutorials.adult import DataManager
    from systemds.operator.algorithm import multiLogReg, multiLogRegPredict, confusionMatrix

    sds = SystemDSContext()
    d = DataManager()

    # limit the sample size
    train_count = 15000
    test_count = 5000

    train_data, train_labels, test_data, test_labels = d.get_preprocessed_dataset(interpolate=True, standardize=True, dimred=0.1)

    # Train data
    X = sds.from_numpy(train_data[:train_count])
    Y = sds.from_numpy(train_labels[:train_count])
    Y = Y + 1.0

    # Test data
    Xt = sds.from_numpy(test_data[:test_count])
    Yt = sds.from_numpy(test_labels[:test_count])
    Yt = Yt + 1.0

    betas = multiLogReg(X, Y)
    [_, y_pred, acc] = multiLogRegPredict(Xt, betas, Yt)

    confusion_matrix_abs, _ = confusionMatrix(y_pred, Yt).compute()
    print(confusion_matrix_abs)

Level 2
-------------------
This part of the tutorial shows a in-depth overview of the preprocessing capabilities that SystemDS has to offer.
We will take a new and raw dataset using the csv format and read it with SystemDS. Then do the heavy lifting for the preprocessing with SystemDS.

Step 1:
~~~~~~~~~~

First of all, we need to download the dataset and create a mtd-file for specifying different properties that the dataset has.
We downloaded the train and test dataset from: https://archive.ics.uci.edu/ml/datasets/adult
The downloaded dataset has been slightly modified for convenience. These modifications entail removing unnecessary newlines at the end of the files,
adding column names at the top of the file.

After these modifications, we have to define a mtd-file for each file we want to read. This mtd file has to be in the same directory as the dataset.
In this particular example, the dataset is split into two files "train_data.csv" and "test_data.csv". We want to read both, which means that we will define a mtd-file for
each of them. Those files will be called "train_data.csv.mtd" and "test_data.csv.mtd".
In these files, we can define certain properties that the file has and also specify which values are supposed to get treated like missing values.

The content of the train_data.csv.mtd file is:

.. code-block:: json

    {
    "data_type": "frame",
    "format": "csv",
    "header": true,
    "naStrings": [ "?", "" ],
    "rows": 32561,
    "cols": 15
    }

The "format" of the file is csv, and "header" is set to true because we added the feature names as headers to the csv files.
"data_type" is set to frame as the preprocessing functions that we use require this datatype.
The value of "naStrings" is a list of all the String values that should be treated as unknown values during the preprocessing.
Also, "rows" in our example is set to 32561, as we have this many entries and "cols" is set to 15 as we have this many features in our datasets.

After these requirements are completed, we have to define a SystemDSContext for reading our dataset. We can do this in the following way:

.. code-block:: python

    sds = SystemDSContext()

    train_count = 32561
    test_count = 16281

With this context we can now define a read operation using the path of the dataset and a schema.
The schema simply defines the data types for each column.

As already mentioned, SystemDS supports lazy execution by default, which means that the read operation is only executed after calling the compute() function.

.. code-block:: python

    SCHEMA = '"DOUBLE,STRING,DOUBLE,STRING,DOUBLE,STRING,STRING,STRING,STRING,STRING,DOUBLE,DOUBLE,DOUBLE,STRING,STRING"'

    dataset_path_train = "adult/train_data.csv"
    dataset_path_test = "adult/test_data.csv"

    F1 = sds.read(
        dataset_path_train,
        schema=SCHEMA
    )
    F2 = sds.read(
        dataset_path_test,
        schema=SCHEMA
    )

Step 2:
~~~~~~~~~~

Now that the read operation has been declared, we can define an additional file for the further preprocessing of the dataset.
For this we create a .json file that holds information about the operations that will be performed on individual columns.
For the sake of this tutorial we will use the file "jspec.json" with the following content:

.. code-block:: json

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

Our dataset has missing values. An easy way to deal with that circumstance is to use the "impute" option that SystemDS supports.
We simply pass a list that holds all the relations between column names and the method of interpolation. A more specific example  is the "education" column.
In the dataset certain entries have missing values for this column. As this is a string feature,
we can simply define the method as "global_mode" and replace every missing value with the global mode inside this column. It is important to note that
we first had to define the values of the missing strings in our selected dataset using the .mtd files (naStrings": [ "?", "" ]).

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

.. code-block:: python

    dataset_jspec = "adult/jspec.json"
    jspec = sds.read(dataset_jspec, data_type="scalar", value_type="string")

Step 3:
~~~~~~~~~~

We now can combine the train and the test dataset by using the rbind() function. This function simply appends the Frame F2 at the end of Frame F1.
This is necessary to ensure that the encoding is identical between training and test dataset.

.. code-block:: python

    X1 = F1.rbind(F2)

In order to use our jspec file we can apply the transform_encode() function. We simply have to pass the read .json file from before.
Another good resource for further ways of processing is: https://apache.github.io/systemds/site/dml-language-reference.html
There we provide different examples for defining jspec's and what functionality is currently supported.
In our particular case we obtain the Matrix X1 and the Frame M1 from the operation. X1 holds all the encoded values and M1 holds a mapping between the encoded values
and all the initial values. Columns that have not been specified in the .json file were not altered.

.. code-block:: python

    X1, M1 = X1.transform_encode(spec=jspec)

First we re-split out data into a training and a test set with the corresponding labels.

.. code-block:: python
    preprocess_src_path = "preprocess.dml"
    PREPROCESS_package = sds.source(preprocess_src_path, "preprocess", print_imported_methods=True)

    X = PREPROCESS_package.get_X(X1, 1, train_count)
    Y = PREPROCESS_package.get_Y(X1, 1, train_count)

    Xt = PREPROCESS_package.get_X(X1, train_count, train_count+test_count)
    Yt = PREPROCESS_package.get_Y(X1, train_count, train_count+test_count)

    Yt = PREPROCESS_package.replace_value(Yt, 3.0, 1.0)
    Yt = PREPROCESS_package.replace_value(Yt, 4.0, 2.0)
Step 4: Training
~~~~~~~~~~

Now that we prepared the data we can use the multiLogReg function. First we will train the model on our
training data. Afterwards we can make predictions on the test data and asses the performance of the model.

.. code-block:: python

    from systemds.operator.algorithm import multiLogReg
    betas = multiLogReg(X, Y)

Note that nothing has been calculated yet. In SystemDS the calculation is executed once .compute() is called.
E.g. betas_res = betas.compute().


We can now use the trained model to make predictions on the test data.

.. code-block:: python

    from systemds.operator.algorithm import multiLogRegPredict
    [_, y_pred, acc] = multiLogRegPredict(Xt, betas, Yt)

The multiLogRegPredict function has three return values:
    - m, a matrix with the mean probability of correctly classifying each label. We not use it further in this example.
    - y_pred, is the predictions made using the model
    - acc, is the accuracy achieved by the model.

Step 5: Confusion Matrix
~~~~~~~~~~

A confusion matrix is a useful tool to analyze the performance of the model and to obtain a better understanding
which classes the model has difficulties to separate.
The confusionMatrix function takes the predicted labels and the true labels. It then returns the confusion matrix
for the predictions and the confusion matrix averages of each true class.

If you followed the tutorial you should be able to verify the results with the provided assertTrue function call.

.. code-block:: python

    from systemds.operator.algorithm import confusionMatrix
    confusion_matrix_abs, _ = confusionMatrix(y_pred, Yt).compute()
    print(confusion_matrix_abs)

Full Script
~~~~~~~~~~

.. code-block:: python

    import numpy as np
    from systemds.context import SystemDSContext
    from systemds.examples.tutorials.adult import DataManager
    from systemds.operator.algorithm import multiLogReg, multiLogRegPredict, confusionMatrix

    train_count = 32561
    test_count = 16281

    dataset_path_train = "adult/train_data.csv"
    dataset_path_test = "adult/test_data.csv"
    dataset_jspec = "adult/jspec.json"
    preprocess_src_path = "preprocess.dml"

    sds = SystemDSContext()

    SCHEMA = '"DOUBLE,STRING,DOUBLE,STRING,DOUBLE,STRING,STRING,STRING,STRING,STRING,DOUBLE,DOUBLE,DOUBLE,STRING,STRING"'

    F1 = sds.read(
        dataset_path_train,
        schema=SCHEMA
    )
    F2 = sds.read(
        dataset_path_test,
        schema=SCHEMA
    )

    jspec = sds.read(dataset_jspec, data_type="scalar", value_type="string")

    X1 = F1.rbind(F2)

    X1, M1 = X1.transform_encode(spec=jspec)

    PREPROCESS_package = sds.source(preprocess_src_path, "preprocess", print_imported_methods=True)

    X = PREPROCESS_package.get_X(X1, 1, train_count)
    Y = PREPROCESS_package.get_Y(X1, 1, train_count)

    Xt = PREPROCESS_package.get_X(X1, train_count, train_count+test_count)
    Yt = PREPROCESS_package.get_Y(X1, train_count, train_count+test_count)

    Yt = PREPROCESS_package.replace_value(Yt, 3.0, 1.0)
    Yt = PREPROCESS_package.replace_value(Yt, 4.0, 2.0)

    betas = multiLogReg(X, Y)

    [_, y_pred, acc] = multiLogRegPredict(Xt, betas, Yt)

    confusion_matrix_abs, _ = confusionMatrix(y_pred, Yt).compute()
    print(confusion_matrix_abs)


Level 3
-------------------

