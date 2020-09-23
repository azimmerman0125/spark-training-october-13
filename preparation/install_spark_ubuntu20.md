# Installing PySpark 3.0.1 with Jupyter Notebook on Ubuntu 20.04

The following steps show how to install and run PySpark integrated Jupyter Notebook on Ubuntu 20.04. Note that, you will need **_root_** access to perform some of the following steps.


## Step 1: Install Java and Python 3

```bash
$ sudo apt update
$ sudo apt install python3-pip python3-dev
$ sudo apt install default-jre
```

Then upgrade pip to the latest version and install virtualenv.

```bash
$ sudo -H pip3 install --upgrade pip 
$ sudo -H pip3 install virtualenv
```

Let's check if everything looks right:

```bash
$ java -version
openjdk version "11.0.8" 2020-07-14
OpenJDK Runtime Environment (build 11.0.8+10-post-Ubuntu-0ubuntu120.04)
OpenJDK 64-Bit Server VM (build 11.0.8+10-post-Ubuntu-0ubuntu120.04, mixed mode, sharing)

$ pip --version
pip 20.2.3 from /usr/local/lib/python3.8/dist-packages/pip (python 3.8)

$ virtualenv --version
virtualenv 20.0.31 from /usr/local/lib/python3.8/dist-packages/virtualenv/__init__.py
```

## Step 2: Create a virtual Python environment

We'll run spark and ipython notebook from a virtual python3 environment created by the virtualenv command. This ensures that all the installed python dependencies will be isolated from the libraries installed on the system level preventing potential conflicts.

Creating and jumping into a virtualenv is as simple as described below:

```bash
$ mkdir ipython_spark_env
$ virtualenv ipython_spark_env
$ source ipython_spark_env/bin/activate
```

## Step 3: Install PySpark and IPython Notebook

Once you're in the previously created virtual environment, you can use the **pip** command to install python packages. We will use pip to install both PySpark and IPython notebook:

```bash
(ipyton_spark_env)$ pip install pyspark
(ipyton_spark_env)$ pip install jupyter
```

## Step 4: Protecting your notebook with password

If you're exposing your IPython notebook instance to the Internet (e.g. you're running on top of an AWS EC2 instance and want to connect to the notebook remotely through the public IP assigned to the EC2 instance), it is a good idea to set up a password first which protects your environment. The steps below shows how you can set up a password easily:

1. First, generate the default IPython notebook configuration file with the following command:

```bash
jupyter notebook --generate-config
```

You can set up a password in this file, however due to security reason it is not allowed to expose the password in plain text. First you have to calculate the hash value of the password and add the hash value to the file only. You can calculate the hash of the password with the following small python script. Start the python command line interpreter from your virtual environment and start the following script:

```python
from IPython.lib import passwd

passwd()
```

This script asks your password and gives back its hash value.

2. You need to configure the above generated password hash in the IPython notebook configuration file which is located at **~/.jupyter/jupyter_notebook_config.py**. You can add your hashed password to the **c.NotebookApp.password** variable, see below:

```python
c.NotebookApp.password = u'sha1:324560a5389bXXXXXXXXXXXX....'
```

## Step 5: Starting IPython Notebook and running a sample Spark application

Create a folder where you'll store your notebooks. You can start the IPython Notebook from that folder. Don't forget to specify which network interface should be used, as by default it uses the *localhost*, making the notebook inaccassible remotely. If you'd like to connect your notebook remotely, you can specify the *0.0.0.0* network interface as below:

```bash
mkdir notebooks
cd notebooks
jupyter notebook --ip=0.0.0.0
```

The IPython notebook listens on port 8888 by default, so now you should be able to access it through this port. You can test if you're Apache Spark installation is working by starting the following sample Spark application which estimates the value of PI:

```python
import pyspark
import random

sc = pyspark.SparkContext(appName="Pi")
num_samples = 10000000

def inside(p):     
  x, y = random.random(), random.random()
  return x*x + y*y < 1

count = sc.parallelize(range(0, num_samples)).filter(inside).count()

pi = 4 * count / num_samples
print(pi)

sc.stop()
```
