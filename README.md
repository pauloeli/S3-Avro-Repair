# S3 Avro Repair

NodeJs application to connect and download avro from S3 bucket, test and repair. You can configure to upload de recoverd
file, if you want. This application has been builded to solve a specific problem, so improvements can be done.

I wish this project can be used to "start" from other people needs. In no way can I be held responsible for any loss of
data or problems that the application may cause **(use at your own risk)**

### Avro Tools

Documentation about the tool and details can be
seen [here](https://www.michael-noll.com/blog/2013/03/17/reading-and-writing-avro-files-from-the-command-line/).

More about `cat` command you can read in [this](https://www.mail-archive.com/dev@avro.apache.org/msg07299.html) feature
documentation.

### Settings

It's necessary set the path of Java will be used to run Avro Tools. To configure edit
the [application.yml](resources/application.yml).
If you don't know where is the Java, you can use the `whereis` linux command to show.

### Install

```bash
npm install --no-optional 
```

### Parameters

The parameters must be set via process variables for NodeJS. Here are the available settings:

|Variable|Description|
|:---------|:---------|
|AWS_ACCESS_KEY_ID|Specifies an AWS access key associated with an IAM user or role.|
|AWS_REGION|Specifies the AWS Region to send the request to.|
|AWS_SECRET_ACCESS_KEY|Specifies the secret key associated with the access key. This is essentially the "password" for the access key.|
|AWS_SESSION_TOKEN|Specifies the session token value that is required if you are using temporary security credentials that you retrieved directly from AWS STS operations.|
|DEBUG_LEVEL|Specify application logging level|

### Run

```bash
DEBUG_LEVEL="debug" AWS_ACCESS_KEY_ID="" AWS_REGION="" AWS_SECRET_ACCESS_KEY="" AWS_SESSION_TOKEN="" node --require ts-node/register src/app.ts
```

### Related projects

- [Avro Tools AWS](https://github.com/Segence/avro-tools-aws)
