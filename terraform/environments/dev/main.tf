# terraform/environments/dev/main.tf
# RetailPulse cloud infrastructure (AWS).
# This is a STUB — fill in account-specific values before applying.
#
# Resources provisioned:
#   - S3 buckets: bronze, silver, gold (versioned, SSE-KMS)
#   - Kafka cluster (MSK Serverless)
#   - Snowflake warehouse (via Snowflake Terraform provider)
#   - IAM roles for Airflow task execution
#
# Usage:
#   cd terraform/environments/dev
#   terraform init
#   terraform plan -var-file=dev.tfvars
#   terraform apply -var-file=dev.tfvars

terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.90"
    }
  }
  # Remote state — configure before first apply
  backend "s3" {
    bucket = "retailpulse-terraform-state"
    key    = "dev/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region"      { default = "us-east-1" }
variable "env"             { default = "dev" }
variable "project"         { default = "retailpulse" }
variable "kms_key_arn"     { description = "KMS key ARN for S3 SSE" }
variable "pii_salt_secret" { description = "ARN of the PII salt in Secrets Manager" }

# ── KMS key for S3 encryption ──────────────────────────────────────────────────
resource "aws_kms_key" "retailpulse" {
  description             = "RetailPulse data encryption key"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = { Project = var.project, Env = var.env }
}

# ── S3 buckets (Bronze / Silver / Gold) ───────────────────────────────────────
locals {
  buckets = ["bronze", "silver", "gold"]
}

resource "aws_s3_bucket" "data" {
  for_each = toset(local.buckets)
  bucket   = "${var.project}-${each.key}-${var.env}"

  tags = { Project = var.project, Env = var.env, Layer = each.key }
}

resource "aws_s3_bucket_versioning" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id

  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.retailpulse.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle: expire Bronze after 365 days, Silver after 90 days
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.data["bronze"].id
  rule {
    id     = "expire-bronze"
    status = "Enabled"
    expiration { days = 365 }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  bucket = aws_s3_bucket.data["silver"].id
  rule {
    id     = "expire-silver"
    status = "Enabled"
    expiration { days = 90 }
  }
}

# ── MSK Serverless Kafka cluster ──────────────────────────────────────────────
resource "aws_msk_serverless_cluster" "retailpulse" {
  cluster_name = "${var.project}-${var.env}"

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl { iam { enabled = true } }
  }

  tags = { Project = var.project, Env = var.env }
}

variable "subnet_ids" {
  type        = list(string)
  description = "VPC subnet IDs for MSK"
  default     = []
}

resource "aws_security_group" "msk" {
  name        = "${var.project}-msk-${var.env}"
  description = "MSK Serverless security group"

  ingress {
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]  # internal VPC only
  }
}

# ── IAM role for Airflow task execution ───────────────────────────────────────
resource "aws_iam_role" "airflow_task" {
  name = "${var.project}-airflow-task-${var.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "airflow_s3" {
  name = "s3-access"
  role = aws_iam_role.airflow_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
        Resource = flatten([
          for b in aws_s3_bucket.data : [b.arn, "${b.arn}/*"]
        ])
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = [aws_kms_key.retailpulse.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [var.pii_salt_secret]
      }
    ]
  })
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "bronze_bucket" { value = aws_s3_bucket.data["bronze"].bucket }
output "silver_bucket" { value = aws_s3_bucket.data["silver"].bucket }
output "gold_bucket"   { value = aws_s3_bucket.data["gold"].bucket }
output "kms_key_id"    { value = aws_kms_key.retailpulse.key_id }
output "airflow_role"  { value = aws_iam_role.airflow_task.arn }
