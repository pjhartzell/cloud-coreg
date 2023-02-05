import aws_cdk.aws_apigateway as apigateway
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_sqs as sqs
from aws_cdk import CfnOutput, Duration, RemovalPolicy, Size, Stack
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from constructs import Construct


class CloudCoRegStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._create_roles()

        self._create_buckets()

        self._create_queues()

        self._create_rest_api()

        self._create_lambda()

        self.aoi_trigger_bucket.add_object_created_notification(
            s3n.SqsDestination(self.lambda_feeder_queue)
        )
        self.aoi_trigger_bucket.grant_read(self.coreg_lambda)
        self.aoi_bucket.grant_read(self.coreg_lambda)
        self.foundation_bucket.grant_read(self.coreg_lambda)
        self.registered_bucket.grant_read_write(self.coreg_lambda)

        self._output_names()

    def _create_roles(self) -> None:
        self.integration_role = iam.Role(
            self,
            "integrationRole",
            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com"),
        )

    def _create_buckets(self) -> None:
        prefix = self.node.try_get_context("bucket_prefix")
        self.aoi_bucket = s3.Bucket(
            self,
            "aoi",
            bucket_name=f"{prefix}-aoi",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.aoi_trigger_bucket = s3.Bucket(
            self,
            "aoiTrigger",
            bucket_name=f"{prefix}-aoi-trigger",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.foundation_bucket = s3.Bucket(
            self,
            "foundation",
            bucket_name=f"{prefix}-foundation",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.registered_bucket = s3.Bucket(
            self,
            "registered",
            bucket_name=f"{prefix}-registered",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

    def _create_queues(self) -> None:
        self.dead_letter_queue = sqs.Queue(
            self,
            "deadLetter",
            retention_period=Duration.days(4),
        )
        self.lambda_feeder_queue = sqs.Queue(
            self,
            "coregFeeder",
            dead_letter_queue={
                "queue": self.dead_letter_queue,
                "max_receive_count": 1,
            },
            visibility_timeout=Duration.hours(1),
            retention_period=Duration.days(4),
        )
        self.lambda_feeder_queue.grant_send_messages(self.integration_role)

    def _create_rest_api(self) -> None:
        self.api = apigateway.RestApi(
            self, "coregApi", deploy_options={"stage_name": "test"}
        )

        sqs_integration = apigateway.AwsIntegration(
            service="sqs",
            integration_http_method="POST",
            path=f"{self.account}/{self.lambda_feeder_queue.queue_name}",
            options=apigateway.IntegrationOptions(
                credentials_role=self.integration_role,
                integration_responses=[
                    apigateway.IntegrationResponse(
                        status_code="200",
                        response_templates={
                            "application/json": '{ "Enqueued": "True" }'
                        },
                    )
                ],
                passthrough_behavior=apigateway.PassthroughBehavior.NEVER,
                request_parameters={
                    "integration.request.header.Content-Type": "'application/x-www-form-urlencoded'"
                },
                request_templates={
                    "application/json": "Action=SendMessage&MessageBody=$input.body"
                },
            ),
        )

        request_model = self.api.add_model(
            "requestModel",
            schema=apigateway.JsonSchema(
                schema=apigateway.JsonSchemaVersion.DRAFT4,
                title="RequestModel",
                type=apigateway.JsonSchemaType.OBJECT,
                properties={
                    "aoiFile": apigateway.JsonSchema(
                        type=apigateway.JsonSchemaType.STRING
                    ),
                    "fndFile": apigateway.JsonSchema(
                        type=apigateway.JsonSchemaType.STRING
                    ),
                    "fndBufferFactor": apigateway.JsonSchema(
                        type=apigateway.JsonSchemaType.NUMBER, minimum=1, maximum=10
                    ),
                    "codemMinResolution": apigateway.JsonSchema(
                        type=apigateway.JsonSchemaType.NUMBER,
                        minimum=0,
                        exclusive_minimum=True,
                    ),
                    "codemSolveScale": apigateway.JsonSchema(
                        type=apigateway.JsonSchemaType.BOOLEAN
                    ),
                },
                required=["aoiFile"],
                additional_properties=False,
            ),
        )

        self.api_coregister_resource = self.api.root.add_resource("coregister")
        self.api_coregister_resource.add_method(
            http_method="POST",
            integration=sqs_integration,
            request_models={"application/json": request_model},
            request_validator_options=apigateway.RequestValidatorOptions(
                request_validator_name="request-validator",
                validate_request_body=True,
                validate_request_parameters=False,
            ),
            method_responses=[apigateway.MethodResponse(status_code="200")],
        )

    def _create_lambda(self) -> None:
        self.coreg_lambda = _lambda.DockerImageFunction(
            self,
            "coreg",
            code=_lambda.DockerImageCode.from_image_asset("lambda"),
            environment={
                "API_AOI_BUCKET": self.aoi_bucket.bucket_name,
                "API_FND_BUCKET": self.foundation_bucket.bucket_name,
                "RESULT_BUCKET": self.registered_bucket.bucket_name,
                "BUFFER_FACTOR": "2",
                "MPLCONFIGDIR": "tmp/matplotlib",
                "SOLVE_SCALE": "True",
            },
            timeout=Duration.minutes(10),
            memory_size=2048,
            ephemeral_storage_size=Size.mebibytes(2048),
        )
        self.coreg_lambda.add_event_source(
            SqsEventSource(self.lambda_feeder_queue, batch_size=1)
        )

    def _output_names(self) -> None:
        CfnOutput(self, "aoiBucketName", value=self.aoi_bucket._physical_name)
        CfnOutput(
            self, "aoiTriggerBucketName", value=self.aoi_trigger_bucket._physical_name
        )
        CfnOutput(
            self, "foundationBucketName", value=self.foundation_bucket._physical_name
        )
        CfnOutput(
            self, "registeredBucketName", value=self.registered_bucket._physical_name
        )
