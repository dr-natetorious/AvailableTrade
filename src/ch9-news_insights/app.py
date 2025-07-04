#!/usr/bin/env python3
"""
News Insights CDK Stack
Chapter 9 - News Feed Ingestion Architecture

CDK stack for news feed ingestion with article downloading, 
data replication, and search capabilities.
"""

import os
import subprocess
import sys
import shutil
from pathlib import Path
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_elasticache as elasticache,
    aws_opensearchservice as opensearch,
    aws_lambda as lambda_,
    aws_apigateway as apigateway,
    aws_iam as iam,
    aws_logs as logs,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3 as s3,
    RemovalPolicy,
    Duration,
    CfnOutput
)
from constructs import Construct

# Environment variables for AWS account and region
if not (AWS_ACCOUNT_ID:= os.environ.get("AWS_DEFAULT_ACCOUNT", "593793064122")):
    raise ValueError("AWS_DEFAULT_ACCOUNT environment variable is not set.")
if not (AWS_DEFAULT_REGION:= os.environ.get("AWS_DEFAULT_REGION", "us-east-1")):
    raise ValueError("AWS_DEFAULT_REGION environment variable is not set.")

def build_lambda_layer():
    """
    Build Lambda layer with Redis and OpenSearch dependencies.
    Returns the path to the layer directory.
    """
    current_dir = Path(__file__).parent
    layers_dir = current_dir / "layers"
    python_dir = layers_dir / "python"
    layer_zip = layers_dir / "lambda-layer.zip"
    
    # Check if layer already exists and is recent
    if layer_zip.exists():
        print(f"Layer zip already exists at {layer_zip}")
        return str(layers_dir)
    
    print("Building Lambda layer for Redis and OpenSearch dependencies...")
    
    # Create the layers directory structure
    if python_dir.exists():
        shutil.rmtree(python_dir)
    
    python_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Install packages directly to the python directory
        print("Installing dependencies...")
        subprocess.run([
            sys.executable, "-m", "pip", "install",
            "redis>=5.0.0,<6.0.0",
            "opensearch-py>=2.4.0,<3.0.0",
            "-t", str(python_dir),
            "--upgrade",
            "--no-cache-dir"
        ], check=True, capture_output=True, text=True)
        
        # Clean up unnecessary files
        cleanup_patterns = [
            "*.dist-info",
            "__pycache__",
            "*.pyc",
        ]
        
        for pattern in cleanup_patterns:
            for item in python_dir.rglob(pattern):
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                elif item.is_file():
                    item.unlink(missing_ok=True)
        
        print("Layer built successfully!")
        return str(layers_dir)
        
    except subprocess.CalledProcessError as e:
        print(f"Error building layer: {e}")
        # Fall back to using existing structure if build fails
        if layers_dir.exists():
            return str(layers_dir)
        raise

class NewsInsightsStack(Stack):
    """
    CDK Stack for News Feed Ingestion Architecture
    
    Components:
    - Article Downloader: ECS-based orchestrated download system
    - Data Replicator: CDC process using Redis Streams
    - Search API: Decoupled search interface with OpenSearch
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Use default VPC
        vpc = ec2.Vpc.from_lookup(self, "DefaultVPC", is_default=True)

        # S3 bucket for storing downloaded articles
        articles_bucket = s3.Bucket(
            self, "ArticlesBucket",
            versioned=False,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Redis cluster for MemoryDB and CDC
        redis_subnet_group = elasticache.CfnSubnetGroup(
            self, "RedisSubnetGroup",
            description="Subnet group for Redis cluster",
            subnet_ids=[subnet.subnet_id for subnet in vpc.public_subnets]
        )

        redis_security_group = ec2.SecurityGroup(
            self, "RedisSecurityGroup",
            vpc=vpc,
            description="Security group for Redis cluster",
            allow_all_outbound=True
        )

        redis_security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(6379),
            description="Redis access from VPC"
        )

        redis_cluster = elasticache.CfnReplicationGroup(
            self, "RedisCluster",
            replication_group_description="Redis cluster for news insights",
            cache_node_type="cache.t3.micro",
            engine="redis",
            num_cache_clusters=1,
            cache_subnet_group_name=redis_subnet_group.ref,
            security_group_ids=[redis_security_group.security_group_id],
            automatic_failover_enabled=False,
            multi_az_enabled=False
        )

        # OpenSearch cluster for search capabilities
        opensearch_security_group = ec2.SecurityGroup(
            self, "OpenSearchSecurityGroup",
            vpc=vpc,
            description="Security group for OpenSearch cluster",
            allow_all_outbound=True
        )

        opensearch_security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(443),
            description="OpenSearch HTTPS access from VPC"
        )

        opensearch_domain = opensearch.Domain(
            self, "NewsSearchDomain",
            version=opensearch.EngineVersion.OPENSEARCH_2_11,
            capacity=opensearch.CapacityConfig(
                data_nodes=1,
                data_node_instance_type="t3.small.search"
            ),
            ebs=opensearch.EbsOptions(
                volume_size=10,
                volume_type=ec2.EbsDeviceVolumeType.GP3
            ),
            vpc=vpc,
            vpc_subnets=[ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC, one_per_az=True)],
            security_groups=[opensearch_security_group],
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ECS cluster for article downloader
        ecs_cluster = ecs.Cluster(
            self, "NewsDownloaderCluster",
            vpc=vpc,
        )

        # ECS task definition for article downloader
        article_downloader_task = ecs.FargateTaskDefinition(
            self, "ArticleDownloaderTask",
            memory_limit_mib=512,
            cpu=256
        )

        article_downloader_task.add_container(
            "ArticleDownloader",
            image=ecs.ContainerImage.from_registry("public.ecr.aws/lambda/python:3.12"),
            command=[
                "sh", "-c",
                "pip install requests beautifulsoup4 boto3 && python -c \"import time; print('Article downloader running'); time.sleep(300)\""
            ],
            environment={
                "REDIS_ENDPOINT": redis_cluster.attr_primary_end_point_address,
                "ARTICLES_BUCKET": articles_bucket.bucket_name,
                "OPENSEARCH_ENDPOINT": opensearch_domain.domain_endpoint
            },
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="article-downloader",
                log_retention=logs.RetentionDays.ONE_WEEK
            )
        )

        # Grant permissions to task
        articles_bucket.grant_read_write(article_downloader_task.task_role)
        opensearch_domain.grant_read_write(article_downloader_task.task_role)

        # ECS security group (create once and reuse)
        ecs_security_group = self._create_ecs_security_group(vpc)

        # ECS service for article downloader (using public subnets for demo)
        article_downloader_service = ecs.FargateService(
            self, "ArticleDownloaderService",
            cluster=ecs_cluster,
            task_definition=article_downloader_task,
            desired_count=1,
            assign_public_ip=True,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_groups=[ecs_security_group]
        )

        # Lambda security group (create once and reuse)
        lambda_security_group = self._create_lambda_security_group(vpc)

        # Build or check for Lambda layer
        layer_path = build_lambda_layer()
        
        # Lambda layer for dependencies (using standard LayerVersion)
        lambda_layer = lambda_.LayerVersion(
            self, "NewsInsightsDependenciesLayer",
            code=lambda_.Code.from_asset(layer_path),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            layer_version_name="news-insights-deps",
            description="Redis and OpenSearch dependencies for news insights"
        )

        # Lambda function for data replication (CDC)
        data_replicator = lambda_.Function(
            self, "DataReplicator",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handlers.DataReplicator_handler",
            code=lambda_.Code.from_asset("functions"),
            environment={
                'REDIS_ENDPOINT': redis_cluster.attr_primary_end_point_address,
                'OPENSEARCH_ENDPOINT': opensearch_domain.domain_endpoint
            },
            timeout=Duration.minutes(5),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            allow_public_subnet=True,
            security_groups=[lambda_security_group],
            layers=[lambda_layer]
        )

        # Grant permissions to data replicator
        opensearch_domain.grant_read_write(data_replicator)

        # EventBridge rule to trigger data replication
        replication_rule = events.Rule(
            self, "DataReplicationRule",
            description="Trigger data replication every 5 minutes",
            schedule=events.Schedule.rate(Duration.minutes(5))
        )

        replication_rule.add_target(targets.LambdaFunction(data_replicator))

        # Lambda function for search API
        search_api = lambda_.Function(
            self, "SearchAPI",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handlers.SearchAPI_handler",
            code=lambda_.Code.from_asset("functions"),
            environment={
                'OPENSEARCH_ENDPOINT': opensearch_domain.domain_endpoint
            },
            timeout=Duration.seconds(30),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            allow_public_subnet=True,
            security_groups=[lambda_security_group],
            layers=[lambda_layer]
        )

        # Grant permissions to search API
        opensearch_domain.grant_read(search_api)

        # API Gateway for search API
        search_api_gateway = apigateway.RestApi(
            self, "NewsSearchAPI",
            rest_api_name="news-search-api",
            description="API for searching news articles",
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS
            )
        )

        # API Gateway resources
        search_resource = search_api_gateway.root.add_resource("search")
        
        search_resource.add_method(
            "GET",
            apigateway.LambdaIntegration(search_api),
            method_responses=[
                apigateway.MethodResponse(
                    status_code="200",
                    response_models={
                        "application/json": apigateway.Model.EMPTY_MODEL
                    }
                )
            ]
        )

        # EventBridge rule for scheduled article downloading
        download_rule = events.Rule(
            self, "ArticleDownloadRule",
            description="Trigger article download every hour",
            schedule=events.Schedule.rate(Duration.hours(1))
        )

        download_rule.add_target(targets.EcsTask(
            cluster=ecs_cluster,
            task_definition=article_downloader_task,
            subnet_selection=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_groups=[ecs_security_group]
        ))

        # Outputs
        CfnOutput(
            self, "SearchAPIURL",
            value=search_api_gateway.url,
            description="Search API URL"
        )

        CfnOutput(
            self, "OpenSearchDomainEndpoint",
            value=opensearch_domain.domain_endpoint,
            description="OpenSearch domain endpoint"
        )

    def _create_ecs_security_group(self, vpc: ec2.Vpc) -> ec2.SecurityGroup:
        """Create security group for ECS tasks"""
        sg = ec2.SecurityGroup(
            self, "ECSSecurityGroup",
            vpc=vpc,
            description="Security group for ECS tasks",
            allow_all_outbound=True
        )
        return sg

    def _create_lambda_security_group(self, vpc: ec2.Vpc) -> ec2.SecurityGroup:
        """Create security group for Lambda functions"""
        sg = ec2.SecurityGroup(
            self, "LambdaSecurityGroup",
            vpc=vpc,
            description="Security group for Lambda functions",
            allow_all_outbound=True
        )
        return sg


# CDK App
app = cdk.App()
NewsInsightsStack(app, "NewsInsightsStack",
                  env=cdk.Environment(
                    account=AWS_ACCOUNT_ID,
                    region=AWS_DEFAULT_REGION
                ))
app.synth()