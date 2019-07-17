# kafka-connect-lambda
The AWS Lambda connector plugin provides the ability to use AWS Lambda functions as a sink (out of a Kafka topic into a Lambda function).

## Supported Kafka and AWS versions
The `kafka-connect-lambda` connector has been tested with `connect-api:2.1.0` and `aws-java-sdk-lambda:1.11.592`

# Building
You can build the connector with Maven using the standard lifecycle goals:
```
mvn clean
mvn package
```

## Sink Connector

A sink connector reads from a Kafka topic and sends events to an AWS Lambda function.

A sink connector configuration has two required fields:
 * `aws.lambda.function.arn`: The AWS ARN of the Lambda function to send events to.
 * `topics`: The Kafka topic to be read from.
 
### AWS Assume Role Support options
 The connector can assume an IAM Role. The role must include a policy that allows lambda:InvokeFunction and lambda:InvokeAsync actions:
 * `aws.credentials.provider.class=com.nordstrom.kafka.connect.auth.AWSAssumeRoleCredentialsProvider`: REQUIRED The credentials provider class.
 * `aws.credentials.provider.role.arn`: REQUIRED AWS Role ARN providing the access.
 * `aws.credentials.provider.session.name`: REQUIRED Session name
 * `aws.credentials.provider.external.id`: OPTIONAL (but recommended) External identifier used by the `kafka-connect-lambda` when assuming the role.

### Sample Configuration
```json
{
    "name": "aws-lambda-sink-test",
    "config": {
        "connector.class": "com.nordstrom.kafka.connect.lambda.LambdaSinkConnector",
        "tasks.max": "1",
        "aws.lambda.function.arn":"arn:aws:lambda:{AWS_REGION}:{AWS_ACCOUNT_NUMBER}:function:test-lambda",
        "aws.lambda.invocation.timeout.ms":"300000",
        "aws.lambda.invocation.mode":"SYNC",
        "aws.lambda.batch.enabled":"true",
        "aws.lambda.json.wrapper.enabled":"true",
        "aws.region":"us-west-2",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.storage.StringConverter",
        "topics": "test"
    }
}
```

## AWS IAM Policies

The IAM Role that Kafka Connect is running under must have policies set for Lambda resources in order
to invoke target functions.

For a `sink` connector, the minimum actions can be scripted as follows:

```
#!/usr/bin/env bash
export AWS_PROFILE=yourprofile

aws lambda add-permission \
--region ${AWS_REGION} \
--function-name yourlambdaFunction \
--principal ${AWS_ACCOUNT_NUMBER} \
--action lambda:InvokeFunction \

aws lambda get-policy --function-name yourLambdaFunction
```

# Running the Demo

The demo uses the Confluent Platform which can be downloaded here: https://www.confluent.io/download/

You can use either the Enterprise or Community version.

The rest of the tutorial assumes the Confluent Platform is installed at $CP and $CP/bin is on your PATH.

## AWS

The demo assumes you have an AWS account and have valid credentials in ~/.aws/credentials as well as
setting the `AWS_PROFILE` and `AWS_REGION` to appropriate values.

These are required so that Kafka Connect will be able to call your Lambda function.

## Create AWS Lambda function

Create `test-lambda` using the AWS Console.  Take note of the ARN value as you will need it to configure the connector later.

## Build the connector plugin

Build the connector jar file and copy to the the classpath of Kafka Connect:

```shell
mvn clean package
mkdir $CP/share/java/kafka-connect-lambda
cp target/kafka-connect-lambda-1.0-SNAPSHOT.jar $CP/share/java/kafka-connect-lambda/
```

## Restart Kafka Connect

Submit a POST for a new connector instance with the JSON configuration above.

## Send messages

Using the Kafka console producer, send a message to the topic `test`.

The `sink` connector will read the message from the topic and send the event to the AWS Lambda.

Use the AWS Console to read your messages sent from the CloudWatch logs for the Lambda.
