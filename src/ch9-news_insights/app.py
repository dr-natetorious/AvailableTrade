#!/usr/bin/env python3
"""
News Insights CDK Stack
Chapter 9 - News Feed Ingestion Architecture

CDK stack for news feed ingestion with article downloading, 
data replication, and search capabilities.
"""

import os
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
            vpc_subnets=[ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC)],
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

        # Lambda function for data replication (CDC)
        data_replicator = lambda_.Function(
            self, "DataReplicator",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_inline("""
import json
import boto3
import redis
from opensearchpy import OpenSearch
import os

def handler(event, context):
    \"\"\"
    Data replication function using Redis Streams for CDC
    \"\"\"
    
    redis_endpoint = os.environ['REDIS_ENDPOINT']
    opensearch_endpoint = os.environ['OPENSEARCH_ENDPOINT']
    
    # Connect to Redis
    redis_client = redis.Redis(host=redis_endpoint, port=6379, decode_responses=True)
    
    # Connect to OpenSearch
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_endpoint, 'port': 443}],
        http_compress=True,
        use_ssl=True,
        verify_certs=True,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )
    
    # Process Redis stream events
    try:
        # Read from Redis stream
        streams = redis_client.xread({'article_stream': '$'}, count=10, block=1000)
        
        for stream_name, messages in streams:
            for message_id, fields in messages:
                # Replicate to OpenSearch
                doc_id = fields.get('article_id')
                doc_body = {
                    'title': fields.get('title', ''),
                    'content': fields.get('content', ''),
                    'url': fields.get('url', ''),
                    'timestamp': fields.get('timestamp', ''),
                    'source': fields.get('source', '')
                }
                
                # Index in OpenSearch
                opensearch_client.index(
                    index='articles',
                    id=doc_id,
                    body=doc_body
                )
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data replication completed successfully')
        }
        
    except Exception as e:
        print(f"Error in data replication: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
            """),
            environment={
                'REDIS_ENDPOINT': redis_cluster.attr_primary_end_point_address,
                'OPENSEARCH_ENDPOINT': opensearch_domain.domain_endpoint
            },
            timeout=Duration.minutes(5),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            allow_public_subnet=True,
            security_groups=[lambda_security_group]
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
            handler="index.handler",
            code=lambda_.Code.from_inline("""
import json
import boto3
from opensearchpy import OpenSearch
import os

def handler(event, context):
    \"\"\"
    Search API function for querying articles
    \"\"\"
    
    opensearch_endpoint = os.environ['OPENSEARCH_ENDPOINT']
    
    # Connect to OpenSearch
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_endpoint, 'port': 443}],
        http_compress=True,
        use_ssl=True,
        verify_certs=True,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )
    
    try:
        # Extract query parameters
        query_params = event.get('queryStringParameters', {})
        search_query = query_params.get('q', '*')
        size = int(query_params.get('size', 10))
        
        # Search in OpenSearch
        search_body = {
            'query': {
                'multi_match': {
                    'query': search_query,
                    'fields': ['title^2', 'content']
                }
            },
            'size': size,
            'sort': [
                {'timestamp': {'order': 'desc'}}
            ]
        }
        
        response = opensearch_client.search(
            index='articles',
            body=search_body
        )
        
        # Format results
        results = []
        for hit in response['hits']['hits']:
            results.append({
                'id': hit['_id'],
                'title': hit['_source'].get('title', ''),
                'content': hit['_source'].get('content', '')[:200] + '...',
                'url': hit['_source'].get('url', ''),
                'timestamp': hit['_source'].get('timestamp', ''),
                'source': hit['_source'].get('source', ''),
                'score': hit['_score']
            })
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'total': response['hits']['total']['value'],
                'results': results
            })
        }
        
    except Exception as e:
        print(f"Error in search API: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': str(e)
            })
        }
            """),
            environment={
                'OPENSEARCH_ENDPOINT': opensearch_domain.domain_endpoint
            },
            timeout=Duration.seconds(30),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            allow_public_subnet=True,
            security_groups=[lambda_security_group]
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