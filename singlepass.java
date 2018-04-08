package singlepass;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import com.opencsv.CSVReader;
import org.apache.commons.lang3.ArrayUtils;
import java.util.Arrays;

/**
 *
 * @author User
 */
public class SinglePass {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {

        int noOfDocuments = 5;
        int noOfTokens = 5;
        float threshhold = 0.6f;
        SinglePassAlgorithm(threshhold);

    }

    private static void SinglePassAlgorithm(float threshhold) throws IOException {
        int docNo = 0;

        //variable to store the clusters
        ArrayList<int[]> cluster = new ArrayList<int[]>();

        //variable to store the clusterRepresentations-centroids
        ArrayList< Float[]> clusterRepresentative = new ArrayList< Float[]>();

        //initial no of cluster
        int noOfClusters = 1;
        //declared but intialised later on first read
        int noOfTokens;

        //Code to read from a file one line at a time without consuming memory
        //"C:\\Users\\User\\Desktop\\IITpatna\\code\\SinglePass\\src\\singlepass\\foo.csv"
        //  "C:\\Users\\User\\Desktop\\IITpatna\\code\\twitter_streams\\foo.csv"
        CSVReader reader = new CSVReader(new FileReader("C:\\Users\\User\\Desktop\\IITpatna\\code\\twitter_streams\\foo-comma_old.csv"), ',');

        //stores the current line read from the stream
        String[] nextLine;

        //loop until no more entry exists in the stream
        while ((nextLine = reader.readNext()) != null) {

            //TODO may be check for the number of dimension for every record
            //handle blanks and nulls
            nextLine = Arrays.stream(nextLine)
                    .filter(s -> (s != null && s.length() > 0))
                    .toArray(String[]::new);

            //set no of tokens from single feature
            noOfTokens = nextLine.length;

            //if first record
            if (docNo == 0) {

                float fResult[] = new float[nextLine.length];
                //parse the string feature into Float
                for (int i = 0; i < nextLine.length; i++) {
                    fResult[i] = Float.parseFloat(nextLine[i]);

                }
                //add the parsed record as the zeroth record
                // i.e. since it is the first document read it can put into cluster 
                //as first cluster without any harm

                cluster.add(new int[]{docNo});

                //convert the read features into float and add them to the clusterRepresentative Store as the first centroid    
                Float[] temp = new Float[noOfTokens];
                temp = convertintArrToFloatArr(fResult);
                clusterRepresentative.add(temp);

            } else {

                //it is not the first record...any other record
                //parse it into float
                float fResult[] = new float[nextLine.length];
                for (int i = 0; i < nextLine.length; i++) {
                    fResult[i] = Float.parseFloat(nextLine[i]);

                }
                //variable to capture the max similarity till now
                float max = -1;
                //variable to capture the max similarity clusterId
                int clusterId = -1;
                //since we are in else part we assume there are other cluster
                //loop through every current cluster found till now to calculate the similarity and the cluster id
                for (int j = 0; j < noOfClusters; ++j) {

                    //compute the cosine similarity
                    float similarity = calculateSimilarity(convertintArrToFloatArr(fResult), clusterRepresentative.get(j));
                    //check if greater than the threshold
                    if (similarity > threshhold) {
                        //check if greater than max
                        if (similarity > max) {
                            max = similarity;
                            clusterId = j;
                        }
                    }
                }
                if (max == -1) {
                    //case when the similarity value never crossed the threshold 
                    //it means new cluster needs to be created
                    //add the current doc as new entry in to the cluster
                    cluster.add(new int[]{docNo});
                    noOfClusters++;
                    //add the current doc as new represenation for itself
                    clusterRepresentative.add(convertintArrToFloatArr(fResult));
                } else {
                    //else we found a candidate for merging with existing cluster
                    //cluster contains other docs so fetch them
                    int[] values = cluster.get(clusterId);

                    //create a new array with size one
                    int[] newValue = new int[1];
                    //add the newly found doc into the newValue 
                    newValue[0] = docNo;

                    //merge both the values from the cluster ..old and the latest found
                    cluster.set(clusterId, ArrayUtils.addAll(values, newValue));

                    //compute the new centroid representation for the newly modified cluster
                    clusterRepresentative.set(clusterId,
                            calculateClusterRepresentative(cluster.get(clusterId), fResult, clusterId, clusterRepresentative, noOfTokens));
                }
            }
            System.out.println(docNo);
            docNo += 1;

        }

        for (int i = 0; i < noOfClusters; ++i) {
            System.out.print("\n" + i + "\t");
            for (int j = 0; j < cluster.get(i).length; ++j) {
                System.out.print(" " + cluster.get(i)[j]);
            }
        }
    }

    private static Float[] convertintArrToFloatArr(float[] input) {
        int size = input.length;
        Float[] answer = new Float[size];
        for (int i = 0; i < input.length; ++i) {
            answer[i] = (float) input[i];
        }
        return answer;
    }

    private static float calculateSimilarity(Float[] vectorA, Float[] vectorB) {
        float answer = 0;

        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < vectorA.length; i++) {
            dotProduct += vectorA[i] * vectorB[i];
            normA += Math.pow(vectorA[i], 2);
            normB += Math.pow(vectorB[i], 2);
        }
        answer = (float) ((float) dotProduct / (Math.sqrt(normA) * Math.sqrt(normB)));

        return answer;
    }

    private static Float[] calculateClusterRepresentative(int[] cluster,
            float[] input,
            int clusterId,
            ArrayList< Float[]> clusterRepresentative,
            int noOFTokens) {

        //create a answer variable equal to the dimension of the noOFTokens
        Float[] answer = new Float[noOFTokens];
        for (int i = 0; i < noOFTokens; ++i) {
            answer[i] = Float.parseFloat("0");
        }

        //get the cluster representation
        Float[] clusRepresent = clusterRepresentative.get(clusterId);

        //get the number of members in the cluster
        int clusterMemberSize = cluster.length;

        for (int i = 0; i < noOFTokens; ++i) {
            //so we multiply the previous cluster represenation by one number less and add it to new member features
            answer[i] = clusRepresent[i] * (clusterMemberSize - 1) + input[i];

        }

        for (int i = 0; i < noOFTokens; ++i) {
            //divide the sum of all the cluster members by the total number of memebers to calculate the new centroid
            answer[i] /= clusterMemberSize;
        }

        return answer;
    }
}