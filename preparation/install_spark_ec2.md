# Installing Apache Spark on EC2 AWS


## Creating the EC2 instance

If you're plan to use an EC2 instance to run Spark during the training, we recommend to use an **Ubuntu Server 20.04** AMI for the instance. Moreover, you should add a bit more storage capacity than the default 8GB, preferred is 62GB. You can go as low as to a t2.micro instance, which you can use in the AWS Free Tier program, but we rather recommend a slightly more powerful option, like *m3.medium".

### Suggested EC2 configuration

Configuration | Parameter
------------ | -------------
**AMI:** | Ubuntu Server 20.04 
**Instance Type:** | m3.medium
**Storage:** | 32 GB
**Security Group:** | Allow SSH inbound and open port 8888

### Installing Apache Spark on EC2

Please follow the [installation guide for Ubuntu 20.04](https://github.com/garzoand/spark-training-october-13/blob/master/preparation/install_spark_ubuntu20.md)
