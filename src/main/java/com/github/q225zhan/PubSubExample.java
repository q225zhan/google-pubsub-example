package com.github.q225zhan;

import com.google.api.client.util.Preconditions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by q225zhan on 2017-03-30.
 */
public class PubSubExample {
    public static void main(String[] args) throws IOException {
        // Need setup GOOGLE_APPLICATION_CREDENTIALS first
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        System.out.printf("Input project: ");
        String project = br.readLine().trim();

        PubSubSDK pubSub = new PubSubSDK(project);

        while (true) {
            try {
                String s = br.readLine().trim();

                if (s.length() == 0)
                    continue;

                String ss[] = s.split(" ");

                switch (ss[0]) {
                    case "exit":
                        break;
                    case "h":
                        help();
                        break;
                    case "ct":
                        checkArgs(ss, 2);
                        pubSub.createTopic(ss[1]);
                        break;
                    case "s":
                        checkArgs(ss, 3);
                        pubSub.send(ss[1], ss[2]);
                        break;
                    case "cs":
                        checkArgs(ss, 3);
                        pubSub.createSubscriber(ss[1], ss[2]);
                        break;
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }

    private static void help() {
        System.out.println("Usage:");
        System.out.println(" h: show help");
        System.out.println(" ct <topic>: create topic");
        System.out.println(" s <topic> <message>: send message to topic");
        System.out.println(" cs <topic> <subscriber>: create subscriber");
    }

    private static void checkArgs(String[] args, int expectedLength) {
        Preconditions.checkArgument(args.length == expectedLength, "should have " + expectedLength + " argument ");
    }
}


