package com.dsp.aws.emr;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

public class StepsOperations {
    public static void addNewStep(EmrClient emrClient, String jobFlowId, String jar, String myClass, String[] args, String stepName) {

        try {
            HadoopJarStepConfig jarStepConfig = HadoopJarStepConfig.builder()
                    .jar(jar)
                    .mainClass(myClass)
                    .args(args)
                    .build();

            StepConfig stepConfig = StepConfig.builder()
                    .hadoopJarStep(jarStepConfig)
                    .actionOnFailure(ActionOnFailure.CANCEL_AND_WAIT)
                    .name(stepName)
                    .build();

            AddJobFlowStepsRequest jobFlowStepsRequest = AddJobFlowStepsRequest.builder()
                    .jobFlowId(jobFlowId)
                    .steps(stepConfig)
                    .build();

            emrClient.addJobFlowSteps(jobFlowStepsRequest);
            System.out.println("You have successfully added a step!");

        } catch (EmrException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}