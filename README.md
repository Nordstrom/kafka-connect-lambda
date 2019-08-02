# kafka-connect-lambda

A Kafka Connect sink plugin to invoke AWS Lambda functions.

# Building

Build the connector with Maven using the standard lifecycle goals:

```
mvn clean
mvn package
```

_The `kafka-connect-lambda` connector has been tested with `connect-api:2.1.0` and `aws-java-sdk-lambda:1.11.592`_

# Configuring

In addition to the standard [Kafka Connect connector configuration](https://kafka.apache.org/documentation/#connect_configuring) properties, the `kafka-connect-lambda` properties available:

| Property | Required | Default value | Description |
|:---------|:---------|:--------|:------------|
| `aws.credentials.provider.class` | No | [Default AWS provider chain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) | Class name of an `AWSCredentialsProvider` implementation |
| `aws.lambda.function.arn` | Yes | | Full ARN of the Lambda function |
| `aws.lambda.invocation.timeout.ms` | No | `300000` | Time to wait for a lambda invocation before continuing |
| `aws.lambda.invocation.mode` | No | `SYNC` | `SYNC` for a synchronous invocation; otherwise `ASYNC` |
| `aws.lambda.invocation.failure.mode` | No | `STOP` | Whether to `STOP` processing, or `DROP` and continue after an invocation failure |
| `aws.lambda.batch.enabled` | No | `true` | `true` to batch messages together before an invocation; otherwise `false` |
| `aws.region` | Yes | | AWS region of the Lambda function |
| `http.proxy.host` | No | | HTTP proxy host name |
| `http.proxy.port` | No | | HTTP proxy port number |
| `retriable.error.codes` | No | `500,503,504` | HTTP status codes that will trigger an invocation retry |
| `retry.backoff.millis` | No | `500` | Time to append between invocation retries |
| `retries.max` | No | `5` | Maximum number of invocation retries |
| `topics` | Yes | | Comma-delimited Kafka topics names to sink |

An example configuration represented as JSON data for use with the [Kafka Connect REST interface](https://docs.confluent.io/current/connect/references/restapi.html):

```json
{
  "name": "example-lambda-connector",
  "config": {
    "tasks.max": "1",
    "connector.class": "com.nordstrom.kafka.connect.lambda.LambdaSinkConnector",
    "topics": "<Your Kafka topics>",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "aws.region": "<Your AWS region>",
    "aws.lambda.function.arn": "<Your function ARN>",
    "aws.lambda.batch.enabled": "false"
  }
}
```

## IAM assume-role options

By supplying `com.nordstrom.kafka.connect.auth.AWSAssumeRoleCredentialsProvider` as the `aws.credentials.provider.class` configuration, the connector can assume an IAM Role. The role must include a policy that allows `lambda:InvokeFunction` and `lambda:InvokeAsync` actions.

| Property | Required | Description |
|:---------|:---------|:------------|
| `aws.credentials.provider.role.arn` | Yes | Full ARN of the IAM Role to assume |
| `aws.credentials.provider.session.name` | Yes | Name that uniquely identifies a session while the role is being assumed |
| `aws.credentials.provider.external.id` | No | External identifier used by the `kafka-connect-lambda` when assuming the role |

# Try the example demo

Follow the demo in order to: create an AWS Lambda function, build the connector plugin, run the connector, and send a message.

## Create an AWS Lambda function

With an active AWS account, can create a simple AWS Lambda function using the [CloudFormation](https://aws.amazon.com/cloudformation) template in the `config/` directory:

```
aws cloudformation create-stack \
  --stack-name example-lambda-stack \
  --capabilities CAPABILITY_NAMED_IAM \
  --template-body file://config/cloudformation.yml
```

To make sure our Lambda works, invoke it directly and view the result payload in `result.txt`:

```
aws lambda invoke --function-name example-function --payload '{"value": "my example"}' result.txt
```

The function simply sends the `payload` back to you in `result.txt`.

Use the `describe-stacks` command to fetch the CloudFormation output value for `ExampleFunctionArn`, which we'll need later when setting up our connector configuration:

```
aws cloudformation describe-stacks --stack-name example-lambda-stack --query "Stacks[0].Outputs[]"
```

## Build the connector plugin

```
mvn clean package
```

Once built, a `kafka-connect-lambda` uber-jar is in the `target/` directory.

## Run the connector using Docker Compose

Ensure you have `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables exported in your shell. Docker Compose will pass these values into the `connect` container.

Use the provided [Docker Compose](https://docs.docker.com/compose) file and run `docker-compose up`.

With the [Kafka Connect REST interface](https://docs.confluent.io/current/connect/references/restapi.html), verify the Lambda sink connector is installed and ready: `curl http://localhost:8083/connector-plugins`.

Next, supply a connector configuration. You can use `config/connector.json.example` as a starting-point. Fill in values for `<Your AWS Region>` and `<Your function ARN>` and run:

```
curl -XPOST -H 'Content-Type: application/json' http://localhost:8083/connectors -d @config/connector.json
```

## Run the connector using the Confluent Platform

Run the ZooKeeper and Kafka components from the [Confluent Platform](https://www.confluent.io/download).

Next, configure a Java properties-file containing your connector configuration. You can use `config/connector.properties.example` as a starting-point. Fill in values for `<Your AWS Region>` and `<Your function ARN>`.

Ensure you have `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables exported in your shell. Then, run the connector in "standalone-mode":

```
connect-standalone config/worker.properties config/connector.properties
```

## Send messages

Using the Kafka console producer, send a message to the `example-stream` topic. Your `example-lambda-connector` will read the message from the topic and invoke the AWS Lambda `example-function`.

Use the AWS Console to read the output of your message sent from the CloudWatch logs for the Lambda.
