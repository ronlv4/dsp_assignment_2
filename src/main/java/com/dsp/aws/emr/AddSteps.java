package com.dsp.aws.emr;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

public class AddSteps {
    public static void main(String[] args) {

        final String usage = "\n" +
                "Usage: " +
                "   <jar> <myClass> <jobFlowId> \n\n" +
                "Where:\n" +
                "   jar - A path to a JAR file run during the step. \n\n" +
                "   myClass - The name of the main class in the specified Java file. \n\n" +
                "   jobFlowId - The id of the job flow. \n\n";

        if (args.length != 3) {
            System.out.println(usage);
            System.exit(1);
        }

        String jar = args[0];
        String myClass = args[1];
        String jobFlowId = args[2];
        Region region = Region.US_WEST_2;
        EmrClient emrClient = EmrClient.builder()
                .region(region)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();

        addNewStep(emrClient, jobFlowId, jar, myClass, new String[]{}, "running bash script");
        emrClient.close();
    }

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