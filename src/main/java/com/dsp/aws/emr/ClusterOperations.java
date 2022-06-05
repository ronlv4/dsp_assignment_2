package com.dsp.aws.emr;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.ec2.model.InstanceType;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.util.ArrayList;
import java.util.List;

public class ClusterOperations {
    public static String createCluster(EmrClient emr,
                                       String keys,
                                       String logUri,
                                       int instanceCount) {
        try {
            JobFlowInstancesConfig instancesConfig = JobFlowInstancesConfig.builder()
//                    .ec2SubnetId("subnet-206a9c58")
                    .ec2KeyName(keys)
                    .instanceCount(instanceCount)
                    .hadoopVersion("3.3.3")
                    .keepJobFlowAliveWhenNoSteps(true)
                    .masterInstanceType(InstanceType.M3_XLARGE.toString())
                    .slaveInstanceType(InstanceType.M3_XLARGE.toString())
                    .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                    .build();


            RunJobFlowRequest jobFlowRequest = RunJobFlowRequest.builder()
                    .name("My Job Flow")
                    .releaseLabel("emr-6.6.0")
                    .logUri(logUri)
                    .serviceRole("EMR_DefaultRole")
                    .jobFlowRole("EMR_EC2_DefaultRole")
                    .instances(instancesConfig)
                    .build();

            RunJobFlowResponse response = emr.runJobFlow(jobFlowRequest);
            return response.jobFlowId();
        } catch (EmrException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        return "";
    }

    public static String createClusterWithSteps(
            EmrClient emr,
            String keys,
            String logUri,
            int instanceCount,
            StepConfig... stepConfigs) {
        try {

            JobFlowInstancesConfig instancesConfig = JobFlowInstancesConfig.builder()
                    .ec2KeyName(keys)
                    .instanceCount(instanceCount)
                    .hadoopVersion("3.3.3")
                    .keepJobFlowAliveWhenNoSteps(true)
                    .masterInstanceType(InstanceType.M3_XLARGE.toString())
                    .slaveInstanceType(InstanceType.M3_XLARGE.toString())
                    .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                    .build();


            RunJobFlowRequest jobFlowRequest = RunJobFlowRequest.builder()
                    .name("My Job Flow")
                    .releaseLabel("emr-6.6.0")
                    .logUri(logUri)
                    .serviceRole("EMR_DefaultRole")
                    .jobFlowRole("EMR_EC2_DefaultRole")
                    .instances(instancesConfig)
                    .steps(stepConfigs)
                    .build();

            RunJobFlowResponse response = emr.runJobFlow(jobFlowRequest);
            return response.jobFlowId();
        } catch (EmrException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        return "";

    }

    public static String createAppClusterWithStep(EmrClient emrClient,
                                                  String jar,
                                                  String myClass,
                                                  String keys,
                                                  String logUri,
                                                  String name,
                                                  int instanceCount
    ) {

        try {
            HadoopJarStepConfig jarStepConfig = HadoopJarStepConfig.builder()
                    .jar(jar)
                    .mainClass(myClass)
                    .build();

            Application spark = Application.builder()
                    .name("Spark")
                    .build();

            Application hive = Application.builder()
                    .name("Hive")
                    .build();

            Application zeppelin = Application.builder()
                    .name("Zeppelin")
                    .build();

            List<Application> apps = new ArrayList<Application>();
            apps.add(spark);
            apps.add(hive);
            apps.add(zeppelin);

            StepConfig wordCountStep = StepConfig.builder()
                    .name("myWordCountApp")
                    .actionOnFailure(ActionOnFailure.CANCEL_AND_WAIT)
                    .hadoopJarStep(jarStepConfig)
                    .build();

            JobFlowInstancesConfig instancesConfig = JobFlowInstancesConfig.builder()
//                    .ec2SubnetId("subnet-206a9c58")
                    .ec2KeyName(keys)
                    .instanceCount(instanceCount)
                    .keepJobFlowAliveWhenNoSteps(true)
                    .masterInstanceType(InstanceType.M3_XLARGE.toString())
                    .slaveInstanceType(InstanceType.M3_XLARGE.toString())
                    .build();


            RunJobFlowRequest jobFlowRequest = RunJobFlowRequest.builder()
                    .name(name)
                    .releaseLabel("emr-5.20.0")
                    .steps(wordCountStep)
                    .applications(apps)
                    .logUri(logUri)
                    .serviceRole("EMR_DefaultRole")
                    .jobFlowRole("EMR_EC2_DefaultRole")
                    .instances(instancesConfig)
                    .build();

            RunJobFlowResponse response = emrClient.runJobFlow(jobFlowRequest);
            return response.jobFlowId();

        } catch (EmrException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        return "";
    }
    // snippet-end:[emr.java2._create_cluster.main]
}