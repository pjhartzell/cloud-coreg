#!/usr/bin/env python3

import aws_cdk as cdk

from cdk_cloud_coreg.cdk_stack import CloudCoRegStack

app = cdk.App()
CloudCoRegStack(app, "cdkCloudCoreg")

app.synth()
