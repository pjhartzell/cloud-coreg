import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_sqs as sqs
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from aws_cdk.aws_apigatewayv2_alpha import HttpApi, HttpMethod
from aws_cdk.aws_apigatewayv2_integrations_alpha import HttpLambdaIntegration
from aws_cdk import RemovalPolicy, Stack, Duration, Size
from constructs import Construct
from aws_solutions_constructs.aws_apigateway_sqs import ApiGatewayToSqs


class CloudCoRegStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        aoi_bucket = s3.Bucket(
            self,
            "aoi",
            bucket_name="cloud-coreg-aoi",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        aoi_trigger_bucket = s3.Bucket(
            self,
            "aoi-trigger",
            bucket_name="cloud-coreg-aoi-trigger",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        foundation_bucket = s3.Bucket(
            self,
            "foundation",
            bucket_name="cloud-coreg-foundation",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        registered_bucket = s3.Bucket(
            self,
            "registered",
            bucket_name="cloud-coreg-registered",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        dead_letter_queue = sqs.Queue(
            self,
            "dead-letter",
            retention_period=Duration.days(4),
        )
        lambda_feeder_queue = sqs.Queue(
            self,
            "coreg-feeder",
            dead_letter_queue={
                "queue": dead_letter_queue,
                "max_receive_count": 1,
            },
            visibility_timeout=Duration.hours(1),
            retention_period=Duration.days(4),
        )
        api_gateway_to_sqs_pattern = ApiGatewayToSqs(
            self,
            "api-gateway-to-sqs",
            existing_queue_obj=lambda_feeder_queue,
            allow_create_operation=True,
        )
        aoi_trigger_bucket.add_object_created_notification(
            s3n.SqsDestination(api_gateway_to_sqs_pattern.sqs_queue)
        )

        # dead_letter_queue = sqs.Queue(
        #     self,
        #     "dead-letter",
        #     retention_period=Duration.days(4),
        # )
        # lambda_feeder_queue = sqs.Queue(
        #     self,
        #     "coreg-feeder",
        #     dead_letter_queue={
        #         "queue": dead_letter_queue,
        #         "max_receive_count": 1,
        #     },
        #     visibility_timeout=Duration.hours(1),
        #     retention_period=Duration.days(4),
        # )
        # aoi_trigger_bucket.add_object_created_notification(
        #     s3n.SqsDestination(lambda_feeder_queue)
        # )

        coreg_lambda = _lambda.DockerImageFunction(
            self,
            "coreg",
            code=_lambda.DockerImageCode.from_image_asset("lambda"),
            environment={
                "API_AOI_BUCKET": aoi_bucket.bucket_name,
                "API_FND_BUCKET": foundation_bucket.bucket_name,
                "RESULT_BUCKET": registered_bucket.bucket_name,
                "BUFFER_FACTOR": "2",
                "MPLCONFIGDIR": "tmp/matplotlib",
                "SOLVE_SCALE": "True",
            },
            timeout=Duration.minutes(10),
            memory_size=2048,
            ephemeral_storage_size=Size.mebibytes(2048),
        )
        coreg_lambda.add_event_source(
            SqsEventSource(api_gateway_to_sqs_pattern.sqs_queue, batch_size=1)
        )

        aoi_trigger_bucket.grant_read(coreg_lambda)
        aoi_bucket.grant_read(coreg_lambda)
        foundation_bucket.grant_read(coreg_lambda)
        registered_bucket.grant_read_write(coreg_lambda)
