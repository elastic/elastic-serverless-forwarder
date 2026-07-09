This document contains details about how to build ESF Lambda locally.
Once built, the Lambda can be deployed to validate functionality.

## Building lambda

To build the Lambda, you may use one of the options below,

### Using Makefile

To build,

```shell
make package
```

This will generate a Lambda zip named `local_esf.zip`.

To clean up any leftover resources,

```shell
make clean
```

### Using Task file

#### Requirements

- [Terraform](https://www.terraform.io/)
- (Optional) [Taskfile](https://taskfile.dev/installation/)


#### Building

**Important note**: ESF dependencies have been tested on architecture `x86_64`. Make sure to use it as well.

You can build your own, or you can choose to run:
```bash
task
```
To build it automatically.

You can update the task variables in the `.env` file:
- The list of python dependencies, `DEPENDENCIES`.
- The list of python requirement files, `REQUIREMENTS`.
- The name of the zip file, `FILENAME`.


## Deploying Lambda

Once Lambda zip is ready, you should use the code in [ESF terraform repository](https://github.com/elastic/terraform-elastic-esf).

> **NOTE**: ESF lambda function is using architecture `x86_64`.

Place your `local_esf.zip` (or `<FILENAME>` if you changed the value) in the same directory as ESF terraform.

Go to `esf.tf` file and edit:

```terraform
locals {
  ...
  dependencies-file = "local_esf.zip" # value of FILENAME in .env
  ...
}
```

Remove/comment these lines from `esf.tf` file:

```terraform
#resource "terraform_data" "curl-dependencies-zip" {
#  provisioner "local-exec" {
#    command = "curl -L -O ${local.dependencies-bucket-url}/${local.dependencies-file}"
#  }
#}
```

And fix the now missing dependency in `dependencies-file`:

```terraform
resource "aws_s3_object" "dependencies-file" {
  bucket = local.config-bucket-name
  key    = local.dependencies-file
  source = local.dependencies-file

  depends_on = [aws_s3_bucket.esf-config-bucket] #, terraform_data.curl-dependencies-zip]
}
```

Now follow the README file from [ESF terraform repository](https://github.com/elastic/terraform-elastic-esf) on how to configure the remaining necessary variables. You will have to configure `release-version` variable, but it will not be relevant to this. You can set any value you want for it.

