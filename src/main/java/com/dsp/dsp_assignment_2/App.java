package com.dsp.dsp_assignment_2;

import com.dsp.aws.emr.CreateCluster;
import software.amazon.awssdk.services.emr.EmrClient;

public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");

        EmrClient emr = EmrClient.builder().build();
        CreateCluster.createAppCluster(emr, );
        ELasticMAp

    }
}
