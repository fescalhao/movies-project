package com.github.fescalhao.movies_project.generics

import org.rogach.scallop.{ScallopConf, ScallopOption}

class ApplicationParams(arguments: Seq[String]) extends ScallopConf(arguments){
  var layer = opt[String](name = "layer", descr = "Data Layer", required = true)
  var entity = opt[String](name = "entity", descr = "Entity to be processed", required = true)
  var aws_user_access_key = opt[String](name="aws_user_access_key", descr = "AWS user access key to assume role", required = true)
  var aws_user_secret_key = opt[String](name="aws_user_secret_key", descr = "AWS user secret key to assume role", required = true)
  var aws_assume_role_arn = opt[String](name="aws_assume_role_arn", descr = "AWS Assume Role ARN", required = true)
  var aws_assume_role_duration = opt[String](name="aws_assume_role_duration", descr = "AWS Assume Role duration", required = false, default = Option("1h"))
  var aws_assume_sts_endpoint = opt[String](name="aws_assume_sts_endpoint", descr = "AWS STS Endpoint", required = false, default = Option("sts.us-east-1.amazonaws.com"))
  var aws_assume_sts_endpoint_region = opt[String](name="aws_assume_sts_endpoint_region", descr = "AWS STS Endpoint Region", required = false, default = Option("us-east-1"))
  verify()

  val appName: String = s"movies_${layer()}_${entity()}"

}