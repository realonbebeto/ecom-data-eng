terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "eu-central-1" #can be changed to a preferred region
}


resource "aws_instance" "ABC_Pipeline" {
  ami           = "ami-0c79a55dda52434da"
  instance_type = "a1.2xlarge"

  tags = {
    Name = "Data2Bots"
  }
}
