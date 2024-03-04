package com.github.fescalhao.spark_project.core

import org.rogach.scallop.ScallopConf

class ApplicationParams(arguments: Seq[String]) extends ScallopConf(arguments){
  val project = opt[String](name = "project", descr = "Project", required = true)
  val layer = opt[String](name = "layer", descr = "Data Layer", required = true)
  val entity = opt[String](name = "entity", descr = "Entity to be processed", required = true)
  val aws_user_access_key = opt[String](name="aws_user_access_key", descr = "AWS user access key to assume role", required = true)
  val aws_user_secret_key = opt[String](name="aws_user_secret_key", descr = "AWS user secret key to assume role", required = true)
  val aws_assume_role_arn = opt[String](name="aws_assume_role_arn", descr = "AWS Assume Role ARN", required = true)
  val aws_assume_role_duration = opt[String](name="aws_assume_role_duration", descr = "AWS Assume Role duration", required = false, default = Option("1h"))
  val aws_assume_sts_endpoint = opt[String](name="aws_assume_sts_endpoint", descr = "AWS STS Endpoint", required = false, default = Option("sts.us-east-1.amazonaws.com"))
  val aws_assume_sts_endpoint_region = opt[String](name="aws_assume_sts_endpoint_region", descr = "AWS STS Endpoint Region", required = false, default = Option("us-east-1"))
  val extra_args = propsLong[String](name = "extra_args", descr = "Extra Arguments (key=value)")
  verify()

  val appName: String = s"${project}_${layer()}_${entity()}"

}